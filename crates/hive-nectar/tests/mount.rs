//! Mounting an imported image as it would be for container cells. It needs root, a kernel with
//! EROFS and loop devices, `HIVE_MKFS_EROFS` and `HIVE_OCI_LAYOUT` as the import tests do, and
//! passes without doing anything otherwise.

#![cfg(target_os = "linux")]

use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Instant;

use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Mkfs};
use hive_nectar::mount::{IdMap, Layers};
use hive_nectar::oci::{Importer, Platform};
use hive_nectar::{Cache, PosixStore};

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
    let (Some(program), Some(layout)) =
        (std::env::var_os("HIVE_MKFS_EROFS"), std::env::var_os("HIVE_OCI_LAYOUT"))
    else {
        eprintln!("skipped: set HIVE_MKFS_EROFS and HIVE_OCI_LAYOUT to run it");
        return;
    };
    if !rustix::process::geteuid().is_root() {
        eprintln!("skipped: needs root");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-nectar-mount-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store = PosixStore::open(dir.join("store")).unwrap();
    let importer =
        Importer::new(Mkfs::new(program, DEFAULT_CHUNK_SIZE).unwrap(), dir.join("work")).unwrap();
    let image =
        importer.import_layout(&store, layout.as_ref(), &Platform::default()).await.unwrap();
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
