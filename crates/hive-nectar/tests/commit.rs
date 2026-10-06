//! Committing a cell's changes on a real overlay: the image the commit makes must show what the
//! cell saw, less what scrubbing took out. It needs root, a kernel with EROFS, overlayfs and loop
//! devices, and `HIVE_MKFS_EROFS`, and passes without doing anything otherwise.

#![cfg(target_os = "linux")]

use std::collections::BTreeMap;
use std::os::unix::fs::{MetadataExt, lchown, symlink};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use hive_nectar::erofs::{DEFAULT_CHUNK_SIZE, Mkfs};
use hive_nectar::mount::{IdMap, Layers};
use hive_nectar::oci::{Commit, Importer};
use hive_nectar::upper::{Scrub, SecretsFound, Shift};
use hive_nectar::{BlobStore, Cache, PosixStore};

const BASE: u32 = 1_000_000;

/// The image the cell runs: a few directories, files and a link, all root's.
fn base_tar(path: &Path) {
    use tar::EntryType::{Directory, Regular, Symlink};
    let mut b = tar::Builder::new(std::fs::File::create(path).unwrap());
    let entries: &[(&str, tar::EntryType, u32, &str)] = &[
        ("etc", Directory, 0o755, ""),
        ("etc/a", Regular, 0o644, "one\n"),
        ("etc/keep", Regular, 0o644, "keep\n"),
        ("opt", Directory, 0o755, ""),
        ("opt/dir", Directory, 0o755, ""),
        ("opt/dir/x", Regular, 0o644, "x\n"),
        ("opt/dir/y", Regular, 0o600, "y\n"),
        ("root", Directory, 0o700, ""),
        ("usr/bin", Directory, 0o755, ""),
        ("usr/bin/tool", Regular, 0o755, "#!/bin/sh\n"),
        ("usr/bin/t", Symlink, 0o777, "tool"),
    ];
    for &(name, kind, mode, body) in entries {
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(kind);
        h.set_mode(mode);
        h.set_mtime(1_700_000_000);
        if kind == Symlink {
            h.set_size(0);
            b.append_link(&mut h, name, body).unwrap();
        } else {
            h.set_size(body.len() as u64);
            b.append_data(&mut h, name, body.as_bytes()).unwrap();
        }
    }
    b.finish().unwrap();
}

/// An overlay of `lowers` at `dir/name/merged`, with an upper when it is writable.
fn overlay(dir: &Path, name: &str, lowers: &[PathBuf], writable: bool) -> PathBuf {
    let at = dir.join(name);
    let merged = at.join("merged");
    std::fs::create_dir_all(&merged).unwrap();
    let lowerdir: Vec<String> = lowers.iter().map(|p| p.display().to_string()).collect();
    let mut options = format!("lowerdir={}", lowerdir.join(":"));
    if writable {
        for d in ["upper", "work"] {
            std::fs::create_dir(at.join(d)).unwrap();
        }
        options += &format!(
            ",upperdir={},workdir={},volatile",
            at.join("upper").display(),
            at.join("work").display()
        );
    }
    rustix::mount::mount(
        "overlay",
        &merged,
        "overlay",
        rustix::mount::MountFlags::empty(),
        std::ffi::CString::new(options).unwrap().as_c_str(),
    )
    .unwrap();
    merged
}

/// Unmounts the overlays when dropped, so a failed assert leaves nothing mounted.
struct Overlays(Vec<PathBuf>);

impl Drop for Overlays {
    fn drop(&mut self) {
        for m in self.0.iter().rev() {
            let _ = rustix::mount::unmount(m, rustix::mount::UnmountFlags::DETACH);
        }
    }
}

/// What a reader sees under `root`: each path's type, content or target, mode, owner and user
/// xattrs, and the link count of files.
fn tree(root: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut todo = vec![PathBuf::new()];
    while let Some(dir) = todo.pop() {
        for e in std::fs::read_dir(root.join(&dir)).unwrap() {
            let rel = dir.join(e.unwrap().file_name());
            let full = root.join(&rel);
            let m = std::fs::symlink_metadata(&full).unwrap();
            let what = if m.is_dir() {
                todo.push(rel.clone());
                "dir".to_owned()
            } else if m.file_type().is_symlink() {
                format!("link to {}", std::fs::read_link(&full).unwrap().display())
            } else {
                let body = std::fs::read(&full).unwrap();
                format!("file {:?} with {} links", String::from_utf8_lossy(&body), m.nlink())
            };
            let mut names = vec![0u8; 4096];
            let n = rustix::fs::llistxattr(&full, &mut names[..]).unwrap_or(0);
            let user: Vec<String> = names[..n]
                .split(|&b| b == 0)
                .filter(|n| n.starts_with(b"user."))
                .map(|name| {
                    let mut v = vec![0u8; 256];
                    let len = rustix::fs::lgetxattr(&full, name, &mut v[..]).unwrap();
                    let lossy = String::from_utf8_lossy;
                    format!("{}={}", lossy(name), lossy(&v[..len]))
                })
                .collect();
            let shown = format!(
                "{what}, mode {:o}, owner {}:{}, xattrs [{}]",
                m.mode() & 0o7777,
                m.uid(),
                m.gid(),
                user.join(",")
            );
            out.insert(rel.display().to_string(), shown);
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn a_commit_shows_what_the_cell_saw_less_its_secrets() {
    let Some(program) = std::env::var_os("HIVE_MKFS_EROFS") else {
        eprintln!("skipped: set HIVE_MKFS_EROFS to run it");
        return;
    };
    if !rustix::process::geteuid().is_root() {
        eprintln!("skipped: needs root");
        return;
    }
    let dir = std::env::temp_dir().join(format!("hive-nectar-commit-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let store: Arc<dyn BlobStore> = Arc::new(PosixStore::open(dir.join("store")).unwrap());
    let importer =
        Importer::new(Mkfs::new(program, DEFAULT_CHUNK_SIZE).unwrap(), dir.join("work")).unwrap();
    base_tar(&dir.join("base.tar"));
    let base = importer.import_tar(&*store, &dir.join("base.tar")).await.unwrap();
    let cache = Cache::open(dir.join("l1"), 1 << 30).unwrap();
    let idmap = IdMap::new(BASE, 65536).unwrap();
    let layers = Layers::new(dir.join("layers"), cache, Some(idmap)).unwrap();
    let lowers = layers.mount(&store, &base.manifest).await.unwrap();
    let cell = overlay(&dir, "cell", &lowers, true);
    let mut mounted = Overlays(vec![cell.clone()]);

    // What the cell does, as its root, which is BASE on the host.
    let at = |p: &str| cell.join(p);
    std::fs::write(at("etc/a"), "two\n").unwrap();
    std::fs::remove_file(at("etc/keep")).unwrap();
    std::fs::remove_dir_all(at("opt/dir")).unwrap();
    std::fs::create_dir(at("opt/dir")).unwrap();
    std::fs::write(at("opt/dir/z"), "z\n").unwrap();
    std::fs::create_dir_all(at("srv/app/.git")).unwrap();
    std::fs::create_dir_all(at("srv/app/tests")).unwrap();
    std::fs::write(at("srv/app/main.py"), "print(1)\n").unwrap();
    std::fs::hard_link(at("srv/app/main.py"), at("srv/app/same.py")).unwrap();
    symlink("main.py", at("srv/app/link")).unwrap();
    rustix::fs::setxattr(at("srv/app/main.py"), "user.hive", b"1", rustix::fs::XattrFlags::empty())
        .unwrap();
    std::fs::write(at("root/.bash_history"), "export TOKEN=x\n").unwrap();
    let config = "[remote \"origin\"]\n\turl = https://x:ghs_secret@github.com/o/r\n";
    std::fs::write(at("srv/app/.git/config"), config).unwrap();
    let pem = "-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----\n";
    std::fs::write(at("srv/app/tests/key.pem"), pem).unwrap();
    for p in [
        "opt/dir",
        "opt/dir/z",
        "srv",
        "srv/app",
        "srv/app/.git",
        "srv/app/.git/config",
        "srv/app/tests",
        "srv/app/tests/key.pem",
        "srv/app/link",
        "root/.bash_history",
    ] {
        lchown(at(p), Some(BASE), Some(BASE)).unwrap();
    }
    lchown(at("srv/app/main.py"), Some(BASE + 1000), Some(BASE + 1000)).unwrap();
    let seen = tree(&cell);

    let how = |scrub| Commit {
        upper: dir.join("cell/upper"),
        base: base.id,
        shift: Some(Shift { base: BASE, count: 65536 }),
        scrub,
        from: "cell-1".into(),
    };
    let err = importer.commit(&*store, how(Some(Scrub::default()))).await.unwrap_err();
    assert_eq!(err.kind(), std::io::ErrorKind::PermissionDenied);
    let found = &err.get_ref().unwrap().downcast_ref::<SecretsFound>().unwrap().0;
    let places: Vec<(&str, u64)> = found.iter().map(|f| (f.path.as_str(), f.line)).collect();
    assert_eq!(places, [("srv/app/tests/key.pem", 1)]);
    assert!(!err.to_string().contains("MIIB"), "the error never holds the secret");

    let t = Instant::now();
    let allow = Scrub { allow: vec!["srv/app/tests".into()] };
    let done = importer.commit(&*store, how(Some(allow))).await.unwrap();
    let w = &done.written;
    println!(
        "committed {} entries, {} bytes and {} whiteouts in {:.2?}",
        w.entries,
        w.bytes,
        w.whiteouts,
        t.elapsed()
    );
    assert_eq!(w.whiteouts, 2, "etc/keep and the opaque opt/dir");
    let p = done.manifest.provenance.as_ref().unwrap();
    assert_eq!((p.parent, p.from.as_str()), (base.id, "cell-1"));
    let s = p.scrubbed.as_ref().unwrap();
    assert_eq!(s.removed, ["root/.bash_history"]);
    assert_eq!(s.rewritten, ["srv/app/.git/config"]);
    assert_eq!(s.allowed.len(), 1);
    assert_eq!(done.manifest.layers.len(), 2);
    assert_eq!(done.manifest.layers[0], base.manifest.layers[0]);
    assert!(done.manifest.layers[1].diff_id.as_ref().is_some_and(|d| d.starts_with("sha256:")));

    // The committed image, mounted as a new cell would get it.
    let lowers = layers.mount(&store, &done.manifest).await.unwrap();
    let after = overlay(&dir, "after", &lowers, false);
    mounted.0.push(after.clone());
    let got = tree(&after);
    let mut want = seen.clone();
    want.remove("root/.bash_history");
    let cfg = want.get_mut("srv/app/.git/config").unwrap();
    *cfg = cfg.replace("x:ghs_secret@", "");
    assert_eq!(got, want);
    assert!(!got.contains_key("etc/keep") && !got.contains_key("opt/dir/x"));

    drop(mounted);
    drop(layers);
    std::fs::remove_dir_all(&dir).unwrap();
}
