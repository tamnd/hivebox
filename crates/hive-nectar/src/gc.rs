//! Garbage collection for a store that can be listed, a [`PosixStore`] or an [`S3Store`]. A blob
//! is kept when an image reachable from the roots names it, or when it was written or used again
//! after the cutoff, and every other blob goes.
//!
//! The roots are images by their manifests' names. Several nodes may share one store, so they have
//! to be every name and every cell's image on all of them. Snapshots that only a client knows of
//! last as long as the cutoff allows. Manifests and recipes younger than the cutoff are roots too,
//! since an import or a snapshot still being written has no name yet.
//!
//! A put that finds its blob already stored, and a manifest written over blobs stored before,
//! give those blobs a new time (see [`crate::store::REFRESH`]), so a collection that starts
//! meanwhile keeps them. Just before it removes anything, a sweep lists the store again and also
//! keeps every blob that is new or has a new time since the plan, with what it names.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::PathBuf;
use std::time::SystemTime;

use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};

use crate::cas::{self, Recipe};
use crate::image::Manifest;
use crate::store::{Listed, blocking};
use crate::{BlobId, BlobStore, PosixStore, ReadReq, S3Store};

/// A store that can be collected, which lists its blobs with their times.
pub trait Collect: BlobStore {
    /// Every blob in the store, with its size and time.
    fn list(&self) -> BoxFuture<'_, io::Result<Vec<Listed>>>;

    /// What puts left behind before `cutoff`, which no blob is made of.
    fn leftovers(&self, cutoff: SystemTime) -> BoxFuture<'_, io::Result<Vec<Leftover>>>;

    /// Removes `blobs`. One that is gone already is fine.
    fn remove<'a>(&'a self, blobs: &'a [BlobId]) -> BoxFuture<'a, io::Result<()>>;

    /// Removes `left`. One that is gone already is fine.
    fn clear<'a>(&'a self, left: &'a Leftover) -> BoxFuture<'a, io::Result<()>>;
}

/// Something a put left behind when it died.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Leftover {
    /// A file in the temporary directory of a store in a directory.
    File(PathBuf),
    /// A multipart upload to a bucket that was never finished, which holds its parts.
    Upload {
        /// The object it was for.
        key: String,
        /// Its upload id.
        id: String,
    },
}

impl Collect for PosixStore {
    fn list(&self) -> BoxFuture<'_, io::Result<Vec<Listed>>> {
        Box::pin(PosixStore::list(self))
    }

    fn leftovers(&self, cutoff: SystemTime) -> BoxFuture<'_, io::Result<Vec<Leftover>>> {
        let tmp = self.tmp();
        Box::pin(blocking(move || {
            let mut stale = Vec::new();
            for f in std::fs::read_dir(tmp)? {
                let f = f?;
                if f.metadata().and_then(|m| m.modified()).is_ok_and(|t| t < cutoff) {
                    stale.push(Leftover::File(f.path()));
                }
            }
            Ok(stale)
        }))
    }

    fn remove<'a>(&'a self, blobs: &'a [BlobId]) -> BoxFuture<'a, io::Result<()>> {
        let deletes = blobs.iter().map(|b| self.delete(*b));
        Box::pin(futures::stream::iter(deletes).buffer_unordered(IN_FLIGHT).try_collect())
    }

    fn clear<'a>(&'a self, left: &'a Leftover) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let Leftover::File(f) = left else { return Ok(()) };
            match tokio::fs::remove_file(f).await {
                Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
                _ => Ok(()),
            }
        })
    }
}

impl Collect for S3Store {
    fn list(&self) -> BoxFuture<'_, io::Result<Vec<Listed>>> {
        Box::pin(self.list_blobs())
    }

    fn leftovers(&self, cutoff: SystemTime) -> BoxFuture<'_, io::Result<Vec<Leftover>>> {
        Box::pin(async move {
            let uploads = self.uploads(cutoff).await?;
            Ok(uploads.into_iter().map(|(key, id)| Leftover::Upload { key, id }).collect())
        })
    }

    fn remove<'a>(&'a self, blobs: &'a [BlobId]) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(self.delete_many(blobs))
    }

    fn clear<'a>(&'a self, left: &'a Leftover) -> BoxFuture<'a, io::Result<()>> {
        Box::pin(async move {
            let Leftover::Upload { key, id } = left else { return Ok(()) };
            self.abort(key, id).await
        })
    }
}

/// What a collection would remove and what it would keep.
#[derive(Debug)]
pub struct Plan {
    /// The blobs to remove: manifests first, then recipes, then the rest, so a collection cut
    /// short never leaves a recipe whose chunks are gone, which an import would take as stored.
    pub remove: Vec<Listed>,
    /// How many blobs it keeps, and their bytes.
    pub kept: (u64, u64),
    /// The roots the store does not have.
    pub missing: Vec<BlobId>,
    /// How many manifests and recipes it keeps as roots for being younger than the cutoff.
    pub young: u64,
    /// What puts left behind before the cutoff.
    pub stale: Vec<Leftover>,
    /// How many of `remove` are manifests, and how many recipes.
    phases: [usize; 2],
    /// The blobs as the plan listed them.
    listed: HashMap<BlobId, SystemTime>,
}

/// What a sweep removed.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Swept {
    /// How many blobs, and their bytes.
    pub removed: (u64, u64),
    /// How many it kept after all, for being written or used since the plan.
    pub spared: u64,
    /// How many temporary files.
    pub stale: u64,
}

/// The biggest blob read to see whether it is a manifest. One for an image of a thousand layers
/// is under 1 MiB.
const MANIFEST_MAX: u64 = 16 << 20;

/// Blobs read at once to see what they are, and removed at once by a sweep.
const IN_FLIGHT: usize = 16;

/// What a blob turned out to be when it was read.
enum Kind {
    Manifest(Box<Manifest>),
    Recipe(Recipe),
    Other,
}

/// Lists `store` and works out which blobs to remove: those no image reachable from `roots` names,
/// written or last used before `cutoff`.
///
/// # Errors
///
/// The store cannot be listed or read, or a root or a blob it names is not what it should be.
pub async fn plan<S: Collect>(store: &S, roots: &[BlobId], cutoff: SystemTime) -> io::Result<Plan> {
    let all = store.list().await?;
    let mut live = HashSet::new();
    let mut missing = Vec::new();
    for &root in roots {
        if !mark(store, root, &mut live).await? {
            missing.push(root);
        }
    }
    let fresh: Vec<Listed> =
        all.iter().filter(|b| b.modified >= cutoff && !live.contains(&b.blob)).copied().collect();
    let young = young_roots(store, &fresh, &mut live).await?;
    let mut kept = (0, 0);
    let mut old = Vec::new();
    for b in &all {
        if live.contains(&b.blob) || b.modified >= cutoff {
            kept = (kept.0 + 1, kept.1 + b.size);
        } else {
            old.push(*b);
        }
    }
    let (mut manifests, mut recipes, mut rest) = (Vec::new(), Vec::new(), Vec::new());
    for (b, k) in old.iter().zip(kinds(store, &old).await?) {
        match k {
            Kind::Manifest(_) => manifests.push(*b),
            Kind::Recipe(_) => recipes.push(*b),
            Kind::Other => rest.push(*b),
        }
    }
    let stale = store.leftovers(cutoff).await?;
    let phases = [manifests.len(), recipes.len()];
    manifests.extend(recipes);
    manifests.extend(rest);
    let listed = all.iter().map(|b| (b.blob, b.modified)).collect();
    Ok(Plan { remove: manifests, kept, missing, young, stale, phases, listed })
}

/// Removes what `plan` found, but for blobs written or used since, and what they name. Manifests
/// go before recipes and recipes before the rest.
///
/// # Errors
///
/// The store cannot be listed, read or changed. What was removed by then stays removed.
pub async fn sweep<S: Collect>(store: &S, plan: Plan) -> io::Result<Swept> {
    let changed: Vec<Listed> = store
        .list()
        .await?
        .into_iter()
        .filter(|b| plan.listed.get(&b.blob) != Some(&b.modified))
        .collect();
    let mut live = HashSet::new();
    young_roots(store, &changed, &mut live).await?;
    live.extend(changed.iter().map(|b| b.blob));
    let mut swept = Swept::default();
    let (manifests, rest) = plan.remove.split_at(plan.phases[0]);
    let (recipes, rest) = rest.split_at(plan.phases[1]);
    for phase in [manifests, recipes, rest] {
        let doomed: Vec<&Listed> = phase.iter().filter(|b| !live.contains(&b.blob)).collect();
        swept.spared += (phase.len() - doomed.len()) as u64;
        let ids: Vec<BlobId> = doomed.iter().map(|b| b.blob).collect();
        store.remove(&ids).await?;
        let bytes: u64 = doomed.iter().map(|b| b.size).sum();
        swept.removed = (swept.removed.0 + ids.len() as u64, swept.removed.1 + bytes);
    }
    for left in &plan.stale {
        store.clear(left).await?;
        swept.stale += 1;
    }
    Ok(swept)
}

/// Gives every blob `manifest` names a new time where the store keeps times, before a new image
/// names them without putting them again.
pub(crate) async fn reuse(store: &dyn BlobStore, manifest: &Manifest) {
    for blob in manifest.layers.iter().flat_map(crate::LayerRef::blobs) {
        // A blob it does not have is one the layer may name without needing, such as data kept
        // as a recipe.
        let _ = store.reuse(blob).await;
    }
}

/// Adds the manifest `id`, the blobs it names and the chunks of its recipes to `live`. Says
/// whether the store has it.
async fn mark(store: &dyn BlobStore, id: BlobId, live: &mut HashSet<BlobId>) -> io::Result<bool> {
    if live.contains(&id) {
        return Ok(true);
    }
    let m = match crate::oci::load_manifest(store, id).await {
        Ok(m) => m,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(e) => return Err(io::Error::new(e.kind(), format!("image {id}: {e}"))),
    };
    live.insert(id);
    names(store, &m, live).await?;
    Ok(true)
}

/// Adds the blobs `m` names and the chunks of its recipes to `live`.
async fn names(store: &dyn BlobStore, m: &Manifest, live: &mut HashSet<BlobId>) -> io::Result<()> {
    for layer in &m.layers {
        live.extend(layer.blobs());
        for recipe in [layer.data_chunks, layer.relaid_chunks].into_iter().flatten() {
            match cas::load(store, recipe).await {
                Ok(r) => live.extend(r.chunks()),
                // An image that has lost a recipe is broken already, and keeps what it has left.
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(io::Error::new(e.kind(), format!("recipe {recipe}: {e}"))),
            }
        }
    }
    Ok(())
}

/// Adds the manifests and recipes among `blobs` to `live`, with what they name, and says how
/// many there were. They are read [`IN_FLIGHT`] at a time, and one that a manifest or recipe
/// read before names is not read at all, which after an import is most of its chunks.
async fn young_roots(
    store: &dyn BlobStore,
    blobs: &[Listed],
    live: &mut HashSet<BlobId>,
) -> io::Result<u64> {
    let mut found = 0;
    for batch in blobs.chunks(IN_FLIGHT) {
        let unread: Vec<Listed> =
            batch.iter().filter(|b| !live.contains(&b.blob)).copied().collect();
        for (b, k) in unread.iter().zip(kinds(store, &unread).await?) {
            match k {
                Kind::Manifest(m) => names(store, &m, live).await?,
                Kind::Recipe(r) => live.extend(r.chunks()),
                Kind::Other => continue,
            }
            // One that a manifest read just before it in the batch names is not counted.
            if live.insert(b.blob) {
                found += 1;
            }
        }
    }
    Ok(found)
}

/// What each of `blobs` is, in order, reading [`IN_FLIGHT`] at a time.
async fn kinds(store: &dyn BlobStore, blobs: &[Listed]) -> io::Result<Vec<Kind>> {
    futures::stream::iter(blobs).map(|b| kind(store, b)).buffered(IN_FLIGHT).try_collect().await
}

/// What `b` is. Only blobs that start as JSON does and are small enough, or have a recipe's
/// length, are read whole, and one counts as a manifest or a recipe only when it parses as one
/// and hashes to its name. One gone since it was listed counts as neither.
async fn kind(store: &dyn BlobStore, b: &Listed) -> io::Result<Kind> {
    let Some(first) = read(store, b.blob, 1).await? else { return Ok(Kind::Other) };
    let json = first.first() == Some(&b'{');
    if b.size == 0 || (json && b.size > MANIFEST_MAX) || (!json && !Recipe::fits(b.size)) {
        return Ok(Kind::Other);
    }
    let Some(bytes) = read(store, b.blob, b.size).await? else { return Ok(Kind::Other) };
    if BlobId::of(&bytes) != b.blob {
        return Ok(Kind::Other);
    }
    Ok(if json {
        Manifest::from_bytes(&bytes).map_or(Kind::Other, |m| Kind::Manifest(Box::new(m)))
    } else {
        Recipe::from_bytes(&bytes).map_or(Kind::Other, Kind::Recipe)
    })
}

/// The first `len` bytes of `blob`, none if it has fewer, or nothing if it is gone.
async fn read(store: &dyn BlobStore, blob: BlobId, len: u64) -> io::Result<Option<Vec<u8>>> {
    let buf = vec![0; usize::try_from(len).map_err(io::Error::other)?];
    match store.read_vectored(blob, vec![ReadReq { offset: 0, buf }]).await {
        Ok(mut done) => Ok(done.pop().map(|r| r.buf)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        // An empty blob has no first byte.
        Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => Ok(Some(Vec::new())),
        Err(e) => Err(e),
    }
}
