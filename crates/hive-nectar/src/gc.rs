//! Garbage collection for a [`PosixStore`]. A blob is kept when an image reachable from the roots
//! names it, or when it was written or used again after the cutoff, and every other blob goes.
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
use std::io::{self, Read};
use std::path::PathBuf;
use std::time::SystemTime;

use crate::cas::{self, Recipe};
use crate::image::Manifest;
use crate::store::{Listed, blocking};
use crate::{BlobId, BlobStore, PosixStore};

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
    /// Files a put left in the store's temporary directory before the cutoff.
    pub stale: Vec<PathBuf>,
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
pub async fn plan(store: &PosixStore, roots: &[BlobId], cutoff: SystemTime) -> io::Result<Plan> {
    let all = store.list().await?;
    let mut live = HashSet::new();
    let mut missing = Vec::new();
    for &root in roots {
        if !mark(store, root, &mut live).await? {
            missing.push(root);
        }
    }
    let mut young = 0;
    for b in &all {
        if b.modified >= cutoff
            && !live.contains(&b.blob)
            && young_root(store, b, &mut live).await?
        {
            young += 1;
        }
    }
    let (mut manifests, mut recipes, mut rest, mut kept) =
        (Vec::new(), Vec::new(), Vec::new(), (0, 0));
    for b in &all {
        if live.contains(&b.blob) || b.modified >= cutoff {
            kept = (kept.0 + 1, kept.1 + b.size);
            continue;
        }
        match kind(store, b).await? {
            Kind::Manifest(_) => manifests.push(*b),
            Kind::Recipe(_) => recipes.push(*b),
            Kind::Other => rest.push(*b),
        }
    }
    let tmp = store.tmp();
    let stale = blocking(move || {
        let mut stale = Vec::new();
        for f in std::fs::read_dir(tmp)? {
            let f = f?;
            if f.metadata().and_then(|m| m.modified()).is_ok_and(|t| t < cutoff) {
                stale.push(f.path());
            }
        }
        Ok(stale)
    })
    .await?;
    manifests.extend(recipes);
    manifests.extend(rest);
    let listed = all.iter().map(|b| (b.blob, b.modified)).collect();
    Ok(Plan { remove: manifests, kept, missing, young, stale, listed })
}

/// Removes what `plan` found, but for blobs written or used since, and what they name.
///
/// # Errors
///
/// The store cannot be listed, read or changed. What was removed by then stays removed.
pub async fn sweep(store: &PosixStore, plan: Plan) -> io::Result<Swept> {
    let mut live = HashSet::new();
    for b in store.list().await? {
        if plan.listed.get(&b.blob) == Some(&b.modified) {
            continue;
        }
        live.insert(b.blob);
        young_root(store, &b, &mut live).await?;
    }
    let mut swept = Swept::default();
    for b in plan.remove {
        if live.contains(&b.blob) {
            swept.spared += 1;
            continue;
        }
        store.delete(b.blob).await?;
        swept.removed = (swept.removed.0 + 1, swept.removed.1 + b.size);
    }
    for f in plan.stale {
        match tokio::fs::remove_file(&f).await {
            Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
            _ => swept.stale += 1,
        }
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
async fn mark(store: &PosixStore, id: BlobId, live: &mut HashSet<BlobId>) -> io::Result<bool> {
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
async fn names(store: &PosixStore, m: &Manifest, live: &mut HashSet<BlobId>) -> io::Result<()> {
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

/// If `b` is a manifest or a recipe, adds what it names to `live` and says so.
async fn young_root(
    store: &PosixStore,
    b: &Listed,
    live: &mut HashSet<BlobId>,
) -> io::Result<bool> {
    match kind(store, b).await? {
        Kind::Manifest(m) => names(store, &m, live).await?,
        Kind::Recipe(r) => live.extend(r.chunks()),
        Kind::Other => return Ok(false),
    }
    live.insert(b.blob);
    Ok(true)
}

/// What `b` is. Only blobs that start as JSON does and are small enough, or have a recipe's
/// length, are read whole, and one counts as a manifest or a recipe only when it parses as one and hashes to its name.
async fn kind(store: &PosixStore, b: &Listed) -> io::Result<Kind> {
    let (path, size) = (store.path(b.blob), b.size);
    let blob = b.blob;
    blocking(move || {
        let mut f = match std::fs::File::open(&path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Kind::Other),
            Err(e) => return Err(e),
        };
        let mut first = [0u8; 1];
        if f.read(&mut first)? == 0 {
            return Ok(Kind::Other);
        }
        let json = first[0] == b'{';
        if (json && size > MANIFEST_MAX) || (!json && !Recipe::fits(size)) {
            return Ok(Kind::Other);
        }
        let mut bytes = Vec::with_capacity(usize::try_from(size).unwrap_or(0));
        bytes.push(first[0]);
        f.read_to_end(&mut bytes)?;
        if BlobId::of(&bytes) != blob {
            return Ok(Kind::Other);
        }
        Ok(if json {
            Manifest::from_bytes(&bytes).map_or(Kind::Other, |m| Kind::Manifest(Box::new(m)))
        } else {
            Recipe::from_bytes(&bytes).map_or(Kind::Other, Kind::Recipe)
        })
    })
    .await
}
