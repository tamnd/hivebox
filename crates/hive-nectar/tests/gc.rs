//! Garbage collection on a store in a directory, with images imported in process.

use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::{Duration, SystemTime};

use hive_nectar::cas::{self, Chunked, Cuts};
use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Writer};
use hive_nectar::oci::{Imported, Importer, load_manifest};
use hive_nectar::{BlobId, BlobStore, Cache, Manifest, PosixStore, gc};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "hive-nectar-gc-{}-{}",
            std::process::id(),
            N.fetch_add(1, Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        Self(dir)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn noise(seed: u64, len: usize) -> Vec<u8> {
    let mut x = seed;
    (0..len)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

/// Imports a one layer image of `files`, each a name and its bytes, keeping data as chunks.
async fn import(s: &Scratch, store: &PosixStore, name: &str, files: &[(&str, &[u8])]) -> Imported {
    let path = s.0.join(format!("{name}.tar"));
    let mut b = tar::Builder::new(std::fs::File::create(&path).unwrap());
    for (file, data) in files {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_mtime(1_700_000_000);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, file, *data).unwrap();
    }
    b.finish().unwrap();
    let importer = Importer::new(Writer::new(DEFAULT_CHUNK_SIZE).unwrap(), s.0.join("work"))
        .unwrap()
        .chunked(Cuts::default());
    importer.import_tar(store, &path).await.unwrap()
}

/// Every blob `m` needs, its recipes' chunks included.
async fn needs(store: &PosixStore, m: &Manifest) -> HashSet<BlobId> {
    let mut all = HashSet::new();
    for l in &m.layers {
        all.extend(l.blobs().filter(|b| store.path(*b).exists()));
        for r in [l.data_chunks, l.relaid_chunks].into_iter().flatten() {
            all.extend(cas::load(store, r).await.unwrap().chunks());
        }
    }
    all
}

/// Makes every blob in the store at `root` look written `ago`.
fn age(root: &Path, ago: Duration) {
    let then = SystemTime::now() - ago;
    for d in std::fs::read_dir(root.join("blobs")).unwrap() {
        for f in std::fs::read_dir(d.unwrap().path()).unwrap() {
            let f = std::fs::File::open(f.unwrap().path()).unwrap();
            f.set_modified(then).unwrap();
        }
    }
}

/// Puts `bytes` into `store` as they are, the way an older importer would have.
async fn put(store: &PosixStore, dir: &Path, bytes: &[u8]) -> BlobId {
    let id = BlobId::of(bytes);
    let path = dir.join(format!("{id}.put"));
    std::fs::write(&path, bytes).unwrap();
    store.put(id, &path).await.unwrap();
    id
}

const DAY: Duration = Duration::from_secs(24 * 3600);

#[tokio::test(flavor = "multi_thread")]
async fn only_what_no_root_needs_goes() {
    let s = Scratch::new();
    let store = PosixStore::open(s.0.join("store")).unwrap();
    let lib = noise(1, 6 << 20);
    let a =
        import(&s, &store, "a", &[("app/main.py", &noise(2, 50_000)), ("usr/lib/big.so", &lib)])
            .await;
    let b = import(
        &s,
        &store,
        "b",
        &[
            ("app/main.py", &noise(3, 330_000)),
            ("app/util.py", &noise(4, 9_000)),
            ("usr/lib/big.so", &lib),
        ],
    )
    .await;
    let (need_a, need_b) = (needs(&store, &a.manifest).await, needs(&store, &b.manifest).await);
    assert!(need_a.intersection(&need_b).count() > 50, "the library's chunks are shared");
    age(&s.0.join("store"), 2 * DAY);
    // A stale file a put left behind.
    let stale = store.tmp().join("left.1.1");
    std::fs::write(&stale, b"half").unwrap();
    std::fs::File::open(&stale).unwrap().set_modified(SystemTime::now() - 2 * DAY).unwrap();

    let nothing = BlobId::of(b"no such image");
    let plan = gc::plan(&store, &[a.id, nothing], SystemTime::now() - DAY).await.unwrap();
    assert_eq!(plan.missing, [nothing]);
    assert_eq!(plan.young, 0);
    let gone: HashSet<BlobId> = plan.remove.iter().map(|b| b.blob).collect();
    assert!(gone.contains(&b.id));
    assert!(gone.is_disjoint(&need_a) && !gone.contains(&a.id));
    let only_b: HashSet<BlobId> = need_b.difference(&need_a).copied().collect();
    assert_eq!(gone, &only_b | &HashSet::from([b.id]));
    // The manifest goes first, then the recipe, then its chunks and the rest.
    let recipe = b.manifest.layers[0].data_chunks.unwrap();
    assert_eq!(plan.remove[0].blob, b.id);
    assert_eq!(plan.remove[1].blob, recipe);
    assert_eq!(plan.stale, std::slice::from_ref(&stale));

    // A blob used again after the plan stays.
    let meta = b.manifest.layers[0].meta;
    store.reuse(meta).await.unwrap();
    let swept = gc::sweep(&store, plan).await.unwrap();
    assert_eq!(swept.spared, 1);
    assert_eq!(swept.removed.0, gone.len() as u64 - 1);
    assert_eq!(swept.stale, 1);
    assert!(!stale.exists());
    assert!(store.stat(b.id).await.is_err() && store.stat(recipe).await.is_err());
    assert!(store.stat(meta).await.is_ok());

    // What is left of a reads back whole.
    assert_eq!(load_manifest(&store, a.id).await.unwrap(), a.manifest);
    let shared: Arc<dyn BlobStore> = Arc::new(PosixStore::open(s.0.join("store")).unwrap());
    let recipes = a.manifest.layers.iter().filter_map(|l| l.data_chunks);
    let chunked = Chunked::open(shared, recipes).await.unwrap();
    let cache = Cache::open(s.0.join("l1"), 1 << 30).unwrap();
    for l in &a.manifest.layers {
        assert_eq!(cache.get(&chunked, l.data).await.unwrap().size(), l.data_size);
        assert_eq!(cache.get(&chunked, l.meta).await.unwrap().size(), l.meta_size);
    }

    // Once swept there is nothing more to remove.
    let again = gc::plan(&store, &[a.id], SystemTime::now() - DAY).await.unwrap();
    assert!(again.remove.is_empty(), "{:?}", again.remove);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_young_image_keeps_what_it_names() {
    let s = Scratch::new();
    let store = PosixStore::open(s.0.join("store")).unwrap();
    let a = import(&s, &store, "a", &[("app/main.py", &noise(5, 2 << 20))]).await;
    let need_a = needs(&store, &a.manifest).await;
    age(&s.0.join("store"), 2 * DAY);
    // A new image over a's layers, its manifest put without touching them, as one written while a
    // collection plans.
    let mut c = a.manifest.clone();
    c.source = Some("c".into());
    let c = put(&store, &s.0, &c.to_bytes()).await;

    let plan = gc::plan(&store, &[], SystemTime::now() - DAY).await.unwrap();
    assert_eq!(plan.young, 1);
    let gone: Vec<BlobId> = plan.remove.iter().map(|b| b.blob).collect();
    assert_eq!(gone, [a.id], "only a's manifest goes");
    assert!(!gone.contains(&c));
    gc::sweep(&store, plan).await.unwrap();
    for b in need_a {
        assert!(store.stat(b).await.is_ok(), "{b} went");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_blob_put_or_named_again_gets_a_new_time() {
    let s = Scratch::new();
    let store = PosixStore::open(s.0.join("store")).unwrap();
    let a = import(&s, &store, "a", &[("app/main.py", &noise(6, 300_000))]).await;
    age(&s.0.join("store"), 2 * DAY);
    let time = |b: BlobId| std::fs::metadata(store.path(b)).unwrap().modified().unwrap();
    let old = SystemTime::now() - DAY;
    assert!(time(a.id) < old);
    // Imported again, every blob is found stored, and all of them are new again.
    let again = import(&s, &store, "a", &[("app/main.py", &noise(6, 300_000))]).await;
    assert_eq!(again.id, a.id);
    for b in needs(&store, &a.manifest).await.into_iter().chain([a.id]) {
        assert!(time(b) > old, "{b} kept its time");
    }
}
