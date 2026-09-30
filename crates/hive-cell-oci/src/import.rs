//! Unpacks a root filesystem for container cells, with every owner shifted into the cells' id
//! range. `hive-nectar` images are the usual way to get one onto a node now, and this stays for
//! tests and for nodes with no image store:
//!
//! ```text
//! docker export $(docker create python:3.12-slim) | hive-oci import /var/lib/hivebox/images/python
//! ```
//!
//! Root in a cell is `uid_base` on the host, so a file root owns in the image has to be owned by
//! `uid_base` on disk, or the cell sees it as owned by nobody. `hive-nectar` layers get the same
//! result from an idmapped mount, made once per layer rather than once per cell.

use std::io::Read;
use std::path::Path;

/// Unpacks the tar stream `from` into `dir`, which must not exist yet, adding `base` to every
/// owner. An owner at or past `count` becomes nobody. Returns how many entries it wrote.
///
/// # Errors
///
/// `dir` exists, the stream is not a tar, or an entry would land outside `dir`.
pub fn import(from: impl Read, dir: &Path, base: u32, count: u32) -> std::io::Result<u64> {
    std::fs::create_dir(dir)?;
    unpack(from, dir, base, count, false)
}

/// Applies one OCI image layer, an uncompressed tar stream, on top of what is in `dir` already,
/// adding `base` to every owner the way [`import`] does. Applying an image's layers in order, the
/// first into an empty `dir`, gives the same tree as unpacking the image flat, without the
/// copy through `docker export` that a flat import needs. Returns how many entries it wrote.
///
/// A whiteout, `.wh.NAME`, removes `NAME` from the layers below, and an opaque whiteout,
/// `.wh..wh..opq`, empties the directory it is in of what the layers below put there.
///
/// # Errors
///
/// The stream is not a tar or an entry would land outside `dir`.
pub fn import_layer(from: impl Read, dir: &Path, base: u32, count: u32) -> std::io::Result<u64> {
    std::fs::create_dir_all(dir)?;
    unpack(from, dir, base, count, true)
}

const WHITEOUT: &str = ".wh.";
const OPAQUE: &str = ".wh..wh..opq";

fn unpack(from: impl Read, dir: &Path, base: u32, count: u32, layer: bool) -> std::io::Result<u64> {
    let mut archive = tar::Archive::new(from);
    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(true);
    archive.set_unpack_xattrs(false);
    archive.set_overwrite(true);
    // What this layer wrote, which an opaque whiteout after it in the stream must keep.
    let mut written = std::collections::HashSet::new();
    let mut n = 0;
    for entry in archive.entries()? {
        let mut entry = entry?;
        // Rebuilt from its components, which drops the trailing slash a directory's name has, so a
        // lookup of the path finds a file of the same name.
        let rel: std::path::PathBuf = entry.path()?.components().collect();
        if layer {
            let name = rel.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if let Some(gone) = name.strip_prefix(WHITEOUT) {
                let parent = inside(dir, rel.parent().unwrap_or(Path::new("")))?;
                if name == OPAQUE {
                    empty(&parent, &written)?;
                } else {
                    remove(&parent.join(gone))?;
                }
                continue;
            }
        }
        let header = entry.header();
        let (uid, gid) = (header.uid()?, header.gid()?);
        let mode = header.mode()?;
        let kind = header.entry_type();
        let (uid, gid) = (shift(uid, base, count), shift(gid, base, count));
        let path = dir.join(&rel);
        if layer {
            // A lower layer may have something else at this path, which tar will not replace
            // with a directory, or a directory, which it will not replace with anything else.
            match std::fs::symlink_metadata(&path) {
                Ok(m) if m.is_dir() && !kind.is_dir() => std::fs::remove_dir_all(&path)?,
                Ok(m) if !m.is_dir() && kind.is_dir() => std::fs::remove_file(&path)?,
                _ => {}
            }
            written.insert(path.clone());
        }
        // `unpack_in` refuses paths that climb out of `dir`, and says so with false.
        if !entry.unpack_in(dir)? {
            continue;
        }
        rustix::fs::chownat(
            rustix::fs::CWD,
            &path,
            Some(rustix::fs::Uid::from_raw(uid)),
            Some(rustix::fs::Gid::from_raw(gid)),
            rustix::fs::AtFlags::SYMLINK_NOFOLLOW,
        )?;
        // A chown clears the setuid and setgid bits, so they go back on after.
        if !kind.is_symlink() && !kind.is_hard_link() && mode & 0o7000 != 0 {
            rustix::fs::chmod(&path, rustix::fs::Mode::from_raw_mode(mode & 0o7777))?;
        }
        n += 1;
    }
    Ok(n)
}

/// The kernel's overflow id, which a user namespace shows for an owner it has no mapping for.
const NOBODY: u32 = 65534;

/// `id` moved into the cells' range. An owner past the range, which images built on some hosts
/// have, becomes nobody, which is what the cell would see for it through an idmapped mount too.
fn shift(id: u64, base: u32, count: u32) -> u32 {
    let nobody = NOBODY.min(count.saturating_sub(1));
    base + u32::try_from(id).ok().filter(|id| *id < count).unwrap_or(nobody)
}

/// `rel` under `dir`, or an error if it would climb out.
fn inside(dir: &Path, rel: &Path) -> std::io::Result<std::path::PathBuf> {
    use std::path::Component;
    if rel.components().any(|c| !matches!(c, Component::Normal(_) | Component::CurDir)) {
        return Err(std::io::Error::other(format!("{} is outside the image", rel.display())));
    }
    Ok(dir.join(rel))
}

fn remove(path: &Path) -> std::io::Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(m) if m.is_dir() => std::fs::remove_dir_all(path),
        Ok(_) => std::fs::remove_file(path),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Removes everything under `dir` but what is in `keep`, going into the directories it keeps.
fn empty(dir: &Path, keep: &std::collections::HashSet<std::path::PathBuf>) -> std::io::Result<()> {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if !keep.contains(&path) {
            remove(&path)?;
        } else if entry.file_type()?.is_dir() {
            empty(&path, keep)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tar(entries: &[(&str, Option<&str>)]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (path, body) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_uid(0);
            h.set_gid(0);
            match body {
                Some(text) => {
                    h.set_entry_type(tar::EntryType::Regular);
                    h.set_mode(0o644);
                    h.set_size(text.len() as u64);
                    b.append_data(&mut h, path, text.as_bytes()).unwrap();
                }
                None => {
                    h.set_entry_type(tar::EntryType::Directory);
                    h.set_mode(0o755);
                    h.set_size(0);
                    b.append_data(&mut h, path, &[][..]).unwrap();
                }
            }
        }
        b.into_inner().unwrap()
    }

    fn tree(dir: &Path) -> Vec<String> {
        let mut out = Vec::new();
        let mut stack = vec![dir.to_path_buf()];
        while let Some(d) = stack.pop() {
            for e in std::fs::read_dir(&d).unwrap() {
                let p = e.unwrap().path();
                let rel = p.strip_prefix(dir).unwrap().display().to_string();
                if p.is_dir() {
                    out.push(format!("{rel}/"));
                    stack.push(p);
                } else {
                    out.push(format!("{rel}={}", std::fs::read_to_string(&p).unwrap()));
                }
            }
        }
        out.sort();
        out
    }

    #[test]
    fn layers_apply_in_order_with_whiteouts() {
        let dir = std::env::temp_dir().join(format!("hive-oci-layers-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        // Owners stay this process's own, so the test needs no privilege.
        let me = rustix::process::getuid().as_raw();
        let apply = |entries: &[(&str, Option<&str>)]| {
            import_layer(&tar(entries)[..], &dir, me, 1).unwrap();
        };
        apply(&[
            ("etc/", None),
            ("etc/a", Some("1")),
            ("etc/b", Some("1")),
            ("opt/", None),
            ("opt/old", Some("1")),
            ("opt/sub/", None),
            ("opt/sub/old", Some("1")),
            ("var/", None),
            ("var/x", Some("1")),
            ("swap", Some("file")),
        ]);
        apply(&[
            ("etc/", None),
            ("etc/a", Some("2")),
            ("etc/.wh.b", Some("")),
            ("opt/", None),
            ("opt/sub/", None),
            ("opt/.wh..wh..opq", Some("")),
            ("opt/new", Some("2")),
            (".wh.var", Some("")),
            ("swap/", None),
            ("swap/in", Some("2")),
        ]);
        assert_eq!(
            tree(&dir),
            ["etc/", "etc/a=2", "opt/", "opt/new=2", "opt/sub/", "swap/", "swap/in=2"],
            "whiteouts remove what is below, an opaque directory keeps only its own entries"
        );
        // The builder will not write a path that climbs, so the name goes into the header by hand.
        let mut h = tar::Header::new_old();
        h.as_old_mut().name[..10].copy_from_slice(b"../.wh.etc");
        h.set_entry_type(tar::EntryType::Regular);
        h.set_size(0);
        h.set_cksum();
        let mut b = tar::Builder::new(Vec::new());
        b.append(&h, &[][..]).unwrap();
        let climb = b.into_inner().unwrap();
        assert!(import_layer(&climb[..], &dir, me, 1).is_err());
        assert!(dir.join("etc/a").exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn an_owner_past_the_range_becomes_nobody() {
        assert_eq!(shift(0, 100_000, 65536), 100_000);
        assert_eq!(shift(1000, 100_000, 65536), 101_000);
        assert_eq!(shift(197_609, 100_000, 65536), 100_000 + NOBODY);
        assert_eq!(shift(u64::MAX, 100_000, 65536), 100_000 + NOBODY);
        assert_eq!(shift(5, 7, 1), 7, "a range too small for nobody falls back to its last id");
    }
}
