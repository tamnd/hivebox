//! Imports with a real `mkfs.erofs`. They need `HIVE_MKFS_EROFS` pointing at one, and pass
//! without doing anything when it is not set. The OCI test also needs `HIVE_OCI_LAYOUT`, an image
//! layout such as `docker save` writes, unpacked.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Instant;

use hive_nectar::cas::{Chunked, Cuts};
use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Mkfs, superblock};
use hive_nectar::oci::{Importer, Platform, load_manifest};
use hive_nectar::{BlobStore, Cache, PosixStore};

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static N: AtomicU32 = AtomicU32::new(0);
        let dir = std::env::temp_dir().join(format!(
            "hive-nectar-it-{}-{}",
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

fn mkfs() -> Option<Mkfs> {
    let Some(program) = std::env::var_os("HIVE_MKFS_EROFS") else {
        eprintln!("skipped: set HIVE_MKFS_EROFS to run it");
        return None;
    };
    Some(Mkfs::new(program, DEFAULT_CHUNK_SIZE).unwrap())
}

/// A small root filesystem tar: a file big enough for a few chunks, a small one, a link, and an
/// OCI whiteout.
fn tar(path: &Path, mtime: u64) {
    let mut b = tar::Builder::new(std::fs::File::create(path).unwrap());
    let mut add = |name: &str, kind: tar::EntryType, mode: u32, data: &[u8], link: Option<&str>| {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(kind);
        h.set_mode(mode);
        h.set_mtime(mtime);
        h.set_uid(1000);
        h.set_gid(1000);
        h.set_size(data.len() as u64);
        if let Some(link) = link {
            h.set_link_name(link).unwrap();
        }
        b.append_data(&mut h, name, data).unwrap();
    };
    let big: Vec<u8> = (0..700_000u32).map(|i| (i % 253) as u8).collect();
    add("etc/", tar::EntryType::Directory, 0o755, &[], None);
    add("etc/hostname", tar::EntryType::Regular, 0o644, b"hive\n", None);
    add("usr/lib/big", tar::EntryType::Regular, 0o755, &big, None);
    add("usr/lib/link", tar::EntryType::Symlink, 0o777, &[], Some("big"));
    add("var/.wh.cache", tar::EntryType::Regular, 0o644, &[], None);
    b.finish().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_tar_becomes_the_same_layer_every_time() {
    let Some(mkfs) = mkfs() else { return };
    let s = Scratch::new();
    let src = s.0.join("rootfs.tar");
    tar(&src, 1_700_000_000);

    let mut ids = Vec::new();
    for i in 0..2 {
        let store = PosixStore::open(s.0.join(format!("store{i}"))).unwrap();
        let importer = Importer::new(mkfs.clone(), s.0.join(format!("work{i}"))).unwrap();
        let got = importer.import_tar(&store, &src).await.unwrap();
        let layer = &got.manifest.layers[0];
        let sb = superblock(&store.path(layer.meta)).unwrap();
        assert_eq!(u64::from(sb.data_blocks) * u64::from(sb.block_size), layer.data_size);
        assert_eq!(u64::from(sb.blocks) * u64::from(sb.block_size), layer.meta_size);
        // The 700 KB file fills three chunks, and the rest of the data is the small file.
        assert!(layer.data_size >= 700_000 && layer.data_size < 1 << 20, "{}", layer.data_size);
        ids.push(got.id);
        // A second build lands on the same blobs.
        std::thread::sleep(std::time::Duration::from_millis(1100));
    }
    assert_eq!(ids[0], ids[1]);

    // A different mtime is a different layer.
    tar(&src, 1_700_000_001);
    let store = PosixStore::open(s.0.join("store0")).unwrap();
    let importer = Importer::new(mkfs, s.0.join("work0")).unwrap();
    assert_ne!(importer.import_tar(&store, &src).await.unwrap().id, ids[0]);
}

#[tokio::test(flavor = "multi_thread")]
async fn an_oci_image_is_imported_once_and_fetched_whole() {
    let Some(mkfs) = mkfs() else { return };
    let Some(layout) = std::env::var_os("HIVE_OCI_LAYOUT") else {
        eprintln!("skipped: set HIVE_OCI_LAYOUT to run it");
        return;
    };
    let s = Scratch::new();
    let store = PosixStore::open(s.0.join("store")).unwrap();
    let importer = Importer::new(mkfs, s.0.join("work")).unwrap();

    let t = Instant::now();
    let first =
        importer.import_layout(&store, layout.as_ref(), &Platform::default()).await.unwrap();
    let cold = t.elapsed();
    let m = &first.manifest;
    assert!(first.built > 0 && first.reused == 0);
    assert!(!m.config.env.is_empty(), "the config came through");
    let meta: u64 = m.layers.iter().map(|l| l.meta_size).sum();
    let data: u64 = m.layers.iter().map(|l| l.data_size).sum();
    println!(
        "{} layers imported in {cold:.2?}: {:.1} MiB metadata, {:.1} MiB data, metadata is {:.2}%",
        m.layers.len(),
        meta as f64 / (1 << 20) as f64,
        data as f64 / (1 << 20) as f64,
        100.0 * meta as f64 / (meta + data) as f64
    );

    let t = Instant::now();
    let again =
        importer.import_layout(&store, layout.as_ref(), &Platform::default()).await.unwrap();
    println!("imported again in {:.2?}", t.elapsed());
    assert_eq!(again.id, first.id);
    assert_eq!((again.built, again.reused), (0, m.layers.len()));
    assert_eq!(load_manifest(&store, first.id).await.unwrap(), *m);

    let cache = Cache::open(s.0.join("l1"), 64 << 30).unwrap();
    let t = Instant::now();
    let held = futures::future::try_join_all(
        m.layers.iter().flat_map(|l| [l.meta, l.data]).map(|b| cache.get(&store, b)),
    )
    .await
    .unwrap();
    let took = t.elapsed();
    let bytes: u64 = held.iter().map(hive_nectar::Held::size).sum();
    println!(
        "fetched {} blobs, {:.1} MiB, in {took:.2?}, {:.0} MiB/s",
        held.len(),
        bytes as f64 / (1 << 20) as f64,
        bytes as f64 / (1 << 20) as f64 / took.as_secs_f64()
    );
    for h in &held {
        assert_eq!(
            std::fs::metadata(h.path()).unwrap().len(),
            store.stat(h.blob()).await.unwrap().size
        );
    }
    assert_eq!(cache.usage().pinned, held.len());
}

/// A tar of `files`, each a name and its bytes.
fn tar_of(path: &Path, files: &[(&str, &[u8])]) {
    let mut b = tar::Builder::new(std::fs::File::create(path).unwrap());
    for (name, data) in files {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_mtime(1_700_000_000);
        h.set_size(data.len() as u64);
        b.append_data(&mut h, name, *data).unwrap();
    }
    b.finish().unwrap();
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

#[tokio::test(flavor = "multi_thread")]
async fn layers_that_share_a_file_share_its_chunks() {
    let Some(mkfs) = mkfs() else { return };
    let s = Scratch::new();
    let store = PosixStore::open(s.0.join("store")).unwrap();
    let importer = Importer::new(mkfs, s.0.join("work")).unwrap().chunked(Cuts::default());
    let lib = noise(1, 6 << 20);
    let (a, b) = (s.0.join("a.tar"), s.0.join("b.tar"));
    tar_of(&a, &[("app/main.py", &noise(2, 50_000)), ("usr/lib/libbig.so", &lib)]);
    tar_of(
        &b,
        &[
            ("app/main.py", &noise(3, 330_000)),
            ("app/util.py", &noise(4, 9_000)),
            ("usr/lib/libbig.so", &lib),
        ],
    );
    let first = importer.import_tar(&store, &a).await.unwrap();
    let second = importer.import_tar(&store, &b).await.unwrap();
    let (la, lb) = (&first.manifest.layers[0], &second.manifest.layers[0]);
    assert_ne!(la.data, lb.data);
    assert_eq!(first.stored, la.data_size);
    // The library sits at another offset in the second layer, and only the chunks around the
    // files that differ are new.
    assert!(second.stored < lb.data_size / 3, "{} of {} new", second.stored, lb.data_size);
    assert!(store.stat(lb.data).await.is_err());

    // A whole fetch through the recipe gives the data blob back, checked against its name.
    let recipes = [la.data_chunks.unwrap(), lb.data_chunks.unwrap()];
    let store: std::sync::Arc<dyn BlobStore> = std::sync::Arc::new(store);
    let chunked = Chunked::open(store, recipes).await.unwrap();
    let cache = Cache::open(s.0.join("cache"), 1 << 30).unwrap();
    let held = cache.get(&chunked, lb.data).await.unwrap();
    assert_eq!(held.size(), lb.data_size);
}
