//! Unpacks a root filesystem for container cells, with every owner shifted into the cells' id
//! range. Until `hive-nectar` makes images, this is how one gets onto a node:
//!
//! ```text
//! docker export $(docker create python:3.12-slim) | hive-oci import /var/lib/hivebox/images/python
//! ```
//!
//! Root in a cell is `uid_base` on the host, so a file root owns in the image has to be owned by
//! `uid_base` on disk, or the cell sees it as owned by nobody. Shifting once here is cheaper than
//! an idmapped mount per cell.

use std::io::Read;
use std::path::Path;

/// Unpacks the tar stream `from` into `dir`, which must not exist yet, adding `base` to every
/// owner. Returns how many entries it wrote.
///
/// # Errors
///
/// `dir` exists, the stream is not a tar, an entry would land outside `dir`, or an owner is past
/// the end of the range.
pub fn import(from: impl Read, dir: &Path, base: u32, count: u32) -> std::io::Result<u64> {
    std::fs::create_dir(dir)?;
    let mut archive = tar::Archive::new(from);
    archive.set_preserve_permissions(true);
    archive.set_preserve_mtime(true);
    archive.set_unpack_xattrs(false);
    let mut n = 0;
    for entry in archive.entries()? {
        let mut entry = entry?;
        let header = entry.header();
        let (uid, gid) = (header.uid()?, header.gid()?);
        let mode = header.mode()?;
        let kind = header.entry_type();
        let shift = |id: u64| {
            u32::try_from(id).ok().filter(|id| *id < count).map(|id| id + base).ok_or_else(|| {
                std::io::Error::other(format!("owner {id} is past the cells' {count} ids"))
            })
        };
        let (uid, gid) = (shift(uid)?, shift(gid)?);
        let path = dir.join(entry.path()?);
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
