//! Mounting an imported image as it would be for container cells. It needs root, a kernel with
//! EROFS and loop devices, and `HIVE_OCI_LAYOUT` as the import tests do, and passes without doing
//! anything otherwise. Layers are built with `HIVE_MKFS_EROFS` when that is set. The lazy test also
//! needs the `nbd` module, and takes the image from the bucket at `HB_S3_URL` when that is set.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use hive_nectar::cas::Cuts;
use hive_nectar::erofs::{Builder, DEFAULT_CHUNK_SIZE, Mkfs, Writer};
use hive_nectar::mount::{IdMap, Layers, Lease};
use hive_nectar::oci::{Importer, Platform};
use hive_nectar::{BlobStore, Cache, Manifest, PosixStore, S3Config, S3Store};

const BASE: u32 = 1_000_000;

/// Loop devices backed by a file under `dir`.
fn loops_under(dir: &Path) -> usize {
    let Ok(entries) = std::fs::read_dir("/sys/block") else { return 0 };
    entries
        .filter_map(|e| std::fs::read_to_string(e.ok()?.path().join("loop/backing_file")).ok())
        .filter(|f| Path::new(f.trim()).starts_with(dir))
        .count()
}

fn mounts_under(dir: &Path) -> usize {
    let info = std::fs::read_to_string("/proc/self/mountinfo").unwrap();
    info.lines()
        .filter_map(|l| l.split(' ').nth(4))
        .filter(|p| Path::new(p).starts_with(dir))
        .count()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_image_mounts_once_idmapped_and_goes_away_cleanly() {
    let Some(layout) = std::env::var_os("HIVE_OCI_LAYOUT") else {
        eprintln!("skipped: set HIVE_OCI_LAYOUT to run it");
        return;
    };
    if !rustix::process::geteuid().is_root() {
        eprintln!("skipped: needs root");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-nectar-mount-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn BlobStore> = Arc::new(PosixStore::open(dir.join("store")).unwrap());
    let importer = Importer::new(builder(), dir.join("work")).unwrap();
    let image =
        importer.import_layout(&*store, layout.as_ref(), &Platform::default()).await.unwrap();
    let m = &image.manifest;
    let cache = Cache::open(dir.join("l1"), 64 << 30).unwrap();

    let t = Instant::now();
    let idmap = IdMap::new(BASE, 65536).unwrap();
    println!("user namespace for the id map made in {:.2?}", t.elapsed());
    let layers = Layers::new(dir.join("layers"), cache.clone(), Some(idmap)).unwrap();
    let t = Instant::now();
    let lowers = layers.mount(&store, m).await.unwrap();
    println!("{} layers fetched and mounted in {:.2?}", lowers.len(), t.elapsed());
    assert_eq!(lowers.len(), m.layers.len());
    assert_eq!(layers.len(), m.layers.len());

    let t = Instant::now();
    assert_eq!(layers.mount(&store, m).await.unwrap(), lowers);
    println!("mounted again in {:.2?}", t.elapsed());

    // Root owns everything in the image, and the cells' root owns it on the host.
    let root = std::fs::metadata(lowers.last().unwrap()).unwrap();
    assert_eq!((root.uid(), root.gid()), (BASE, BASE));

    // The overlay a container cell gets, with a write that copies a file up.
    let (upper, work, merged) = (dir.join("upper"), dir.join("ovl-work"), dir.join("merged"));
    for d in [&upper, &work, &merged] {
        std::fs::create_dir(d).unwrap();
    }
    let lowerdir: Vec<String> = lowers.iter().map(|p| p.display().to_string()).collect();
    let options = format!(
        "lowerdir={},upperdir={},workdir={},volatile",
        lowerdir.join(":"),
        upper.display(),
        work.display()
    );
    rustix::mount::mount(
        "overlay",
        &merged,
        "overlay",
        rustix::mount::MountFlags::empty(),
        std::ffi::CString::new(options).unwrap().as_c_str(),
    )
    .unwrap();
    let python = std::fs::read_link(merged.join("usr/local/bin/python3")).unwrap();
    println!("python3 is {}", python.display());
    let passwd = merged.join("etc/passwd");
    assert!(std::fs::read_to_string(&passwd).unwrap().starts_with("root:"));
    std::fs::write(&passwd, "root:x:0:0::/root:/bin/sh\n").unwrap();
    let up = std::fs::metadata(upper.join("etc/passwd")).unwrap();
    assert_eq!((up.uid(), up.gid()), (BASE, BASE), "the copied up file is the cells' root's");
    let lower_passwd = lowers.iter().map(|l| l.join("etc/passwd")).find(|p| p.exists()).unwrap();
    assert!(std::fs::read_to_string(lower_passwd).unwrap().len() > 30, "the layer is untouched");

    // A second node process with the blobs already cached only pays for the mounts.
    let other =
        Layers::new(dir.join("layers2"), cache.clone(), Some(IdMap::new(BASE, 65536).unwrap()))
            .unwrap();
    let t = Instant::now();
    other.mount(&store, m).await.unwrap();
    println!("{} layers mounted from a warm cache in {:.2?}", m.layers.len(), t.elapsed());
    drop(other);

    let cache_dir = dir.join("l1");
    assert!(loops_under(&cache_dir) > 0);
    rustix::mount::unmount(&merged, rustix::mount::UnmountFlags::empty()).unwrap();
    drop(layers);
    assert_eq!(mounts_under(&dir), 0);
    // Autoclear frees the devices once the last mount goes, which can lag the unmount a little.
    for _ in 0..100 {
        if loops_under(&cache_dir) == 0 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    assert_eq!(loops_under(&cache_dir), 0, "every loop device was freed");
    assert_eq!(cache.usage().pinned, 0);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A tar holding one file, `big`, of `size` bytes that do not repeat, made from `seed`.
fn one_file_tar(path: &Path, seed: u64, size: usize) {
    let mut x = seed | 1;
    let body: Vec<u8> = (0..size)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            x as u8
        })
        .collect();
    let mut b = tar::Builder::new(std::fs::File::create(path).unwrap());
    let mut h = tar::Header::new_gnu();
    h.set_mode(0o644);
    h.set_mtime(1_700_000_000);
    h.set_size(body.len() as u64);
    b.append_data(&mut h, "big", &body[..]).unwrap();
    b.finish().unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn layers_no_cell_holds_make_room_for_new_ones() {
    if !rustix::process::geteuid().is_root() {
        eprintln!("skipped: needs root");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-nectar-lease-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn BlobStore> = Arc::new(PosixStore::open(dir.join("store")).unwrap());
    let importer = Importer::new(builder(), dir.join("work")).unwrap();
    let mut images = Vec::new();
    let mut bodies = Vec::new();
    for seed in [1, 2] {
        let tar = dir.join(format!("{seed}.tar"));
        one_file_tar(&tar, seed, 8 << 20);
        let mut a = tar::Archive::new(std::fs::File::open(&tar).unwrap());
        let mut entry = a.entries().unwrap().next().unwrap().unwrap();
        let mut body = Vec::new();
        std::io::Read::read_to_end(&mut entry, &mut body).unwrap();
        bodies.push(body);
        images.push(importer.import_tar(&*store, &tar).await.unwrap().manifest);
    }
    let size = |m: &Manifest| m.layers.iter().map(|l| l.meta_size + l.data_size).sum::<u64>();
    // Room for one image, not both.
    let cache =
        Cache::open(dir.join("l1"), size(&images[0]).max(size(&images[1])) * 3 / 2).unwrap();
    let layers = Layers::new(dir.join("layers"), cache.clone(), None).unwrap();
    let read = |lease: &Lease| std::fs::read(lease.lowers()[0].join("big")).unwrap();

    let a = layers.lease(&store, &images[0]).await.unwrap();
    assert!(read(&a) == bodies[0]);
    let e = layers.lease(&store, &images[1]).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::StorageFull, "{e}");
    assert_eq!(layers.len(), 1);

    // Let go of, the layer stays mounted until the room is wanted.
    drop(a);
    assert_eq!((layers.len(), mounts_under(&dir.join("layers"))), (1, 1));
    let t = Instant::now();
    let b = layers.lease(&store, &images[1]).await.unwrap();
    println!("the idle layer went and the other came in {:.2?}", t.elapsed());
    assert!(read(&b) == bodies[1]);
    assert_eq!((layers.len(), mounts_under(&dir.join("layers"))), (1, 1));
    drop(b);
    let again = layers.lease(&store, &images[0]).await.unwrap();
    assert!(read(&again) == bodies[0]);
    drop(again);

    // What mount gives out stays, so nothing makes room for the other image.
    layers.mount(&store, &images[0]).await.unwrap();
    let e = layers.lease(&store, &images[1]).await.unwrap_err();
    assert_eq!(e.kind(), std::io::ErrorKind::StorageFull, "{e}");

    drop(layers);
    assert_eq!(mounts_under(&dir), 0);
    assert_eq!(cache.usage().pinned, 0);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// NBD devices with a server on them.
fn nbd_in_use() -> usize {
    let Ok(entries) = std::fs::read_dir("/sys/block") else { return 0 };
    entries
        .filter_map(Result::ok)
        .filter(|e| e.file_name().to_string_lossy().starts_with("nbd"))
        .filter(|e| {
            let size = std::fs::read_to_string(e.path().join("size")).unwrap_or_default();
            e.path().join("pid").exists() || !matches!(size.trim(), "0" | "")
        })
        .count()
}

/// Every file, link and directory under `dir`, with what each holds, and the bytes in files.
fn digest_tree(dir: &Path) -> (BTreeMap<PathBuf, String>, u64) {
    let mut out = BTreeMap::new();
    let mut bytes = 0;
    let mut todo = vec![dir.to_owned()];
    while let Some(d) = todo.pop() {
        for e in std::fs::read_dir(&d).unwrap() {
            let path = e.unwrap().path();
            let m = std::fs::symlink_metadata(&path).unwrap();
            let what = if m.is_dir() {
                todo.push(path.clone());
                "dir".to_owned()
            } else if m.is_symlink() {
                format!("-> {}", std::fs::read_link(&path).unwrap().display())
            } else if m.is_file() {
                let data = std::fs::read(&path).unwrap();
                bytes += data.len() as u64;
                blake3::hash(&data).to_string()
            } else {
                "other".to_owned()
            };
            out.insert(path.strip_prefix(dir).unwrap().to_owned(), what);
        }
    }
    (out, bytes)
}

#[tokio::test(flavor = "multi_thread")]
async fn an_image_mounts_lazily_and_reads_the_same_as_a_whole_fetch() {
    let Some(layout) = std::env::var_os("HIVE_OCI_LAYOUT") else {
        eprintln!("skipped: set HIVE_OCI_LAYOUT to run it");
        return;
    };
    if !rustix::process::geteuid().is_root() || !Path::new("/sys/block/nbd0").exists() {
        eprintln!("skipped: needs root and the nbd module");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-nectar-lazy-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn BlobStore> = match std::env::var("HB_S3_URL") {
        Ok(url) => Arc::new(S3Store::new(S3Config::from_url_and_env(&url).unwrap()).unwrap()),
        Err(_) => Arc::new(PosixStore::open(dir.join("store")).unwrap()),
    };
    let mut importer = Importer::new(builder(), dir.join("work")).unwrap();
    // With HIVE_DEDUP=chunks the data is kept as chunks, and both mounts read through recipes.
    let chunked = std::env::var("HIVE_DEDUP").is_ok_and(|d| d == "chunks");
    if chunked {
        importer = importer.chunked(Cuts::default());
    }
    let image =
        importer.import_layout(&*store, layout.as_ref(), &Platform::default()).await.unwrap();
    let m = &image.manifest;
    let data: u64 = m.layers.iter().map(|l| l.data_size).sum();
    assert!(m.layers.iter().all(|l| l.data_size == 0 || l.data_leaves.is_some()));
    for l in &m.layers {
        assert_eq!(l.data_chunks.is_some(), chunked);
        assert_eq!(store.stat(l.data).await.is_ok(), !chunked);
    }
    let in_use = nbd_in_use();

    let whole =
        Layers::new(dir.join("whole"), Cache::open(dir.join("l1-whole"), 64 << 30).unwrap(), None)
            .unwrap();
    let t = Instant::now();
    let eager = whole.mount(&store, m).await.unwrap();
    println!(
        "{} layers, {} MiB of data, fetched whole and mounted in {:.2?}",
        m.layers.len(),
        data >> 20,
        t.elapsed()
    );

    let cache = Cache::open(dir.join("l1-lazy"), 64 << 30).unwrap();
    let layers = Layers::new(dir.join("lazy"), cache.clone(), None).unwrap().lazily();
    let t = Instant::now();
    let lowers = layers.mount(&store, m).await.unwrap();
    let mounted = t.elapsed();
    let fetched = |layers: &Layers| layers.progress().iter().map(|(_, p)| p.bytes).sum::<u64>();
    println!("mounted lazily in {mounted:.2?}, {} KiB of data fetched", fetched(&layers) >> 10);
    assert_eq!(nbd_in_use(), in_use + m.layers.iter().filter(|l| l.data_size > 0).count());
    let t = Instant::now();
    let passwd = lowers.iter().map(|l| l.join("etc/passwd")).find(|p| p.exists()).unwrap();
    assert!(std::fs::read_to_string(&passwd).unwrap().starts_with("root:"));
    println!("first file read {:.2?} after the mount", t.elapsed());

    // When the background fill finishes, while the trees are read.
    let watch = {
        let progress = Arc::new(layers);
        let p = progress.clone();
        let since = t;
        (
            tokio::spawn(async move {
                while !p.progress().iter().all(|(_, g)| g.have == g.chunks) {
                    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                }
                (
                    since.elapsed() + mounted,
                    p.progress().iter().map(|(_, g)| g.requests).sum::<u64>(),
                )
            }),
            progress,
        )
    };
    let (filled, layers) = watch;

    let mut files = 0;
    let mut bytes = 0;
    let mut took = [std::time::Duration::ZERO; 2];
    for (a, b) in lowers.iter().zip(&eager) {
        let t = Instant::now();
        let (lazy_tree, n) = digest_tree(a);
        took[0] += t.elapsed();
        let t = Instant::now();
        let (whole_tree, _) = digest_tree(b);
        took[1] += t.elapsed();
        assert!(lazy_tree == whole_tree, "{} reads differently", a.display());
        files += lazy_tree.len();
        bytes += n;
    }
    println!(
        "{files} entries, {} MiB, read the same: lazily in {:.2?}, from the whole fetch in {:.2?}",
        bytes >> 20,
        took[0],
        took[1]
    );
    let (filled, requests) =
        tokio::time::timeout(std::time::Duration::from_secs(300), filled).await.unwrap().unwrap();
    println!(
        "the background fill finished {filled:.2?} after the mount began, {requests} requests"
    );
    let layers = Arc::try_unwrap(layers).unwrap();

    drop(layers);
    drop(whole);
    assert_eq!(mounts_under(&dir), 0);
    assert_eq!(nbd_in_use(), in_use, "every NBD device was freed");
    assert_eq!(cache.usage().pinned, 0);
    std::fs::remove_dir_all(&dir).unwrap();
}

fn builder() -> Builder {
    match std::env::var_os("HIVE_MKFS_EROFS") {
        Some(program) => Mkfs::new(program, DEFAULT_CHUNK_SIZE).unwrap().into(),
        None => Writer::new(DEFAULT_CHUNK_SIZE).unwrap().into(),
    }
}
