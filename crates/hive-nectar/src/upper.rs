//! A cell's changes as a layer: the overlay upper directory a container cell wrote into, written
//! out as an OCI layer tar that [`crate::oci::Importer::commit`] builds into a layer on top of the
//! image the cell ran.
//!
//! Overlay marks a deleted file with a character device 0:0 and a directory that hides everything
//! below it with the `overlay.opaque` xattr. A layer says the same with an empty `.wh.NAME` file
//! and a `.wh..wh..opq` file in the directory, which the layer builder turns back into the
//! overlay form, so the tar uses those. The upper holds files as the host sees them, so owners are
//! shifted back down by the cells' id base. A renamed directory (`overlay.redirect`) or a file
//! copied up without its data (`overlay.metacopy`) cannot be said in a layer, so they fail the
//! commit. Neither happens with the options cells are mounted with.
//!
//! Scrubbing keeps what the cell should not hand on out of the layer. It leaves out shell and REPL
//! histories anywhere, and well known credential files in home directories, takes credentials out
//! of the remote urls and extra headers in `.git/config`, and leaves out `.env` and `.npmrc` files
//! that hold a secret. Every other file up to [`SCAN_MAX`] is searched for secrets such as private
//! keys and cloud or API tokens, except under `site-packages`, `dist-packages` and `node_modules`,
//! which come from public registries. A secret found outside the paths the caller allows stops the
//! commit, and the error says where, but never what. So does one in a file a Debian package put
//! there, unless the file is still what the package shipped, by the md5sums dpkg keeps for it in
//! the upper: libgnutls, for one, holds the private keys of its self tests.

use std::collections::HashMap;
use std::ffi::OsString;
use std::fs::{self, File, Metadata};
use std::io::{self, Read, Write};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;

use md5::{Digest, Md5};
use regex::bytes::{Regex, RegexBuilder, RegexSet, RegexSetBuilder};

use crate::image::{Finding, Scrubbed};

/// Files bigger than this are not searched for secrets.
pub const SCAN_MAX: u64 = 8 << 20;

/// Shell and REPL histories, left out wherever they are.
const HISTORIES: &[&str] = &[
    ".bash_history",
    ".zsh_history",
    ".sh_history",
    ".ash_history",
    ".python_history",
    ".node_repl_history",
    ".mysql_history",
    ".psql_history",
    ".sqlite_history",
    ".lesshst",
    ".viminfo",
    ".wget-hsts",
];

/// Credential files in a home directory, `root` or `home/NAME`, left out.
const CREDENTIALS: &[&str] = &[
    ".git-credentials",
    ".netrc",
    ".pypirc",
    ".aws/credentials",
    ".docker/config.json",
    ".kube/config",
    ".config/gh/hosts.yml",
    ".cache/huggingface/token",
    ".huggingface/token",
    ".ssh/id_rsa",
    ".ssh/id_ecdsa",
    ".ssh/id_ed25519",
    ".ssh/id_dsa",
];

/// Directories whose files come from public registries and are not searched.
const NOT_SEARCHED: &[&str] = &["site-packages", "dist-packages", "node_modules"];

/// What a secret looks like, by the name a finding gives it.
const RULES: &[(&str, &str)] = &[
    (
        "private key",
        r"-----BEGIN (?:RSA |EC |DSA |OPENSSH |ENCRYPTED |PGP )?PRIVATE KEY(?: BLOCK)?-----",
    ),
    ("aws access key", r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
    ("github token", r"\bgh[pousr]_[A-Za-z0-9]{36,}\b|\bgithub_pat_[A-Za-z0-9_]{50,}\b"),
    ("gitlab token", r"\bglpat-[A-Za-z0-9_-]{20,}"),
    ("slack token", r"\bxox[abprs]-[A-Za-z0-9-]{10,}"),
    ("openai or anthropic key", r"\bsk-(?:ant-|proj-)?[A-Za-z0-9_-]{32,}"),
    ("hugging face token", r"\bhf_[A-Za-z0-9]{34,}\b"),
    ("google api key", r"\bAIza[0-9A-Za-z_-]{35}\b"),
    ("stripe key", r"\b[rs]k_live_[0-9A-Za-z]{24,}\b"),
    ("npm token", r"\bnpm_[A-Za-z0-9]{36}\b|_authToken\s*=\s*[^\s$]{8,}"),
];

static ANY: LazyLock<RegexSet> = LazyLock::new(|| {
    RegexSetBuilder::new(RULES.iter().map(|r| r.1))
        .unicode(false)
        .build()
        .expect("the secret rules compile")
});

static EACH: LazyLock<Vec<Regex>> = LazyLock::new(|| {
    RULES
        .iter()
        .map(|r| RegexBuilder::new(r.1).unicode(false).build().expect("a secret rule compiles"))
        .collect()
});

/// A remote url with a user or token in it, and an extra header, which carries a token.
static GIT_URL: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(r"(?m)^(\s*(?:url|pushurl)\s*=\s*[A-Za-z][A-Za-z0-9+.-]*://)[^@/\s]+@")
        .unicode(false)
        .build()
        .expect("the url rule compiles")
});
static GIT_HEADER: LazyLock<Regex> = LazyLock::new(|| {
    RegexBuilder::new(r"(?mi)^\s*extraheader\s*=.*\n?")
        .unicode(false)
        .build()
        .expect("the header rule compiles")
});

/// How the cells' ids sit on the host: the cells' root is `base`, and `count` ids follow it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Shift {
    /// The host id of the cells' root.
    pub base: u32,
    /// How many ids the cells have.
    pub count: u32,
}

impl Shift {
    /// The id inside the cell for the host id `id`, or `nobody` for one the cells do not have.
    #[must_use]
    pub fn back(self, id: u32) -> u64 {
        match id.checked_sub(self.base) {
            Some(n) if n < self.count => u64::from(n),
            _ => 65534,
        }
    }
}

/// What scrubbing lets through.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Scrub {
    /// Paths, relative to the root, whose secrets are let through: a file, or everything in a
    /// directory.
    pub allow: Vec<String>,
}

impl Scrub {
    fn allows(&self, path: &str) -> bool {
        self.allow.iter().any(|a| {
            let a = a.trim_matches('/');
            path == a || path.strip_prefix(a).is_some_and(|rest| rest.starts_with('/'))
        })
    }
}

/// What [`write_tar`] wrote.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Written {
    /// Entries in the tar, not counting whiteouts.
    pub entries: u64,
    /// Bytes of file content.
    pub bytes: u64,
    /// Files and directories the cell deleted, and directories it replaced.
    pub whiteouts: u64,
    /// What scrubbing did, when it ran.
    pub scrubbed: Option<Scrubbed>,
    /// Secrets found outside the allowed paths. A commit with any is refused.
    pub found: Vec<Finding>,
}

/// The error a commit gives when it finds secrets, which lists where they are.
#[derive(Debug)]
pub struct SecretsFound(pub Vec<Finding>);

impl std::fmt::Display for SecretsFound {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} secrets found, so the commit was refused:", self.0.len())?;
        for s in self.0.iter().take(10) {
            write!(f, " {}:{} ({})", s.path, s.line, s.rule)?;
        }
        if self.0.len() > 10 {
            write!(f, " and {} more", self.0.len() - 10)?;
        }
        Ok(())
    }
}

impl std::error::Error for SecretsFound {}

/// Writes the overlay upper directory `upper` to `out` as a layer tar, shifting owners back with
/// `shift` and scrubbing with `scrub` when it is given.
///
/// # Errors
///
/// `upper` cannot be read, it holds a renamed directory or a metadata only copy up, or `out`
/// fails. Secrets found are not an error here, they are in [`Written::found`].
pub fn write_tar<W: Write>(
    upper: &Path,
    shift: Option<Shift>,
    scrub: Option<&Scrub>,
    out: W,
) -> io::Result<(W, Written)> {
    let mut w = Walk {
        out: tar::Builder::new(out),
        upper,
        shift,
        scrub,
        links: HashMap::new(),
        sums: None,
        xbuf: vec![0; 1 << 16],
        written: Written { scrubbed: scrub.map(|_| Scrubbed::default()), ..Written::default() },
    };
    w.dir(Path::new(""))?;
    let out = w.out.into_inner()?;
    Ok((out, w.written))
}

struct Walk<'a, W: Write> {
    out: tar::Builder<W>,
    upper: &'a Path,
    shift: Option<Shift>,
    scrub: Option<&'a Scrub>,
    /// The first path of each file with more than one link, by device and inode.
    links: HashMap<(u64, u64), PathBuf>,
    /// The md5 of each file the packages installed in the upper shipped, by path, read the first
    /// time a secret is found.
    sums: Option<HashMap<Vec<u8>, [u8; 16]>>,
    xbuf: Vec<u8>,
    written: Written,
}

/// What a file's overlay xattrs say about it, and the xattrs it keeps.
#[derive(Default)]
struct Attrs {
    keep: Vec<(OsString, Vec<u8>)>,
    opaque: bool,
    whiteout: bool,
    redirect: bool,
    metacopy: bool,
}

impl<W: Write> Walk<'_, W> {
    /// Writes the entries in the directory `rel`, in name order.
    fn dir(&mut self, rel: &Path) -> io::Result<()> {
        let mut names: Vec<OsString> = fs::read_dir(self.upper.join(rel))?
            .map(|e| e.map(|e| e.file_name()))
            .collect::<io::Result<_>>()?;
        names.sort();
        for name in names {
            let rel = rel.join(&name);
            let meta = fs::symlink_metadata(self.upper.join(&rel))?;
            self.entry(&rel, &meta)?;
        }
        Ok(())
    }

    fn entry(&mut self, rel: &Path, meta: &Metadata) -> io::Result<()> {
        let full = self.upper.join(rel);
        let ft = meta.file_type();
        let attrs = self.attrs(&full)?;
        let shown = rel.display();
        if attrs.redirect {
            return Err(unsupported(format!("{shown} was renamed in the overlay")));
        }
        if attrs.metacopy {
            return Err(unsupported(format!("{shown} was copied up without its data")));
        }
        if (ft.is_char_device() && meta.rdev() == 0)
            || (ft.is_file() && meta.len() == 0 && attrs.whiteout)
        {
            return self.whiteout(rel);
        }
        let mut h = tar::Header::new_gnu();
        h.set_size(0);
        h.set_mode(meta.mode() & 0o7777);
        let (uid, gid) = match self.shift {
            Some(s) => (s.back(meta.uid()), s.back(meta.gid())),
            None => (u64::from(meta.uid()), u64::from(meta.gid())),
        };
        h.set_uid(uid);
        h.set_gid(gid);
        h.set_mtime(u64::try_from(meta.mtime()).unwrap_or(0));
        if ft.is_dir() {
            h.set_entry_type(tar::EntryType::Directory);
            self.xattrs(&attrs.keep)?;
            self.add(&mut h, rel, &[][..])?;
            if attrs.opaque {
                self.whiteout(&rel.join(".wh..wh..opq"))?;
            }
            return self.dir(rel);
        }
        if ft.is_symlink() {
            h.set_entry_type(tar::EntryType::Symlink);
            let target = fs::read_link(&full)?;
            self.xattrs(&attrs.keep)?;
            self.written.entries += 1;
            return self.out.append_link(&mut h, rel, target);
        }
        if ft.is_file() {
            return self.file(rel, meta, h, &attrs.keep);
        }
        let kind = if ft.is_char_device() {
            tar::EntryType::Char
        } else if ft.is_block_device() {
            tar::EntryType::Block
        } else if ft.is_fifo() {
            tar::EntryType::Fifo
        } else {
            // A socket means nothing once its process is gone.
            return Ok(());
        };
        h.set_entry_type(kind);
        if kind != tar::EntryType::Fifo {
            h.set_device_major(rustix::fs::major(meta.rdev()))?;
            h.set_device_minor(rustix::fs::minor(meta.rdev()))?;
        }
        self.xattrs(&attrs.keep)?;
        self.add(&mut h, rel, &[][..])
    }

    fn file(
        &mut self,
        rel: &Path,
        meta: &Metadata,
        mut h: tar::Header,
        keep: &[(OsString, Vec<u8>)],
    ) -> io::Result<()> {
        let key = (meta.dev(), meta.ino());
        if meta.nlink() > 1
            && let Some(first) = self.links.get(&key).cloned()
        {
            h.set_entry_type(tar::EntryType::Link);
            h.set_size(0);
            self.written.entries += 1;
            return self.out.append_link(&mut h, rel, first);
        }
        let full = self.upper.join(rel);
        let mut body: Option<Vec<u8>> = None;
        if let Some(scrub) = self.scrub {
            let path = rel.to_string_lossy().into_owned();
            let report = self.written.scrubbed.get_or_insert_with(Scrubbed::default);
            if left_out(&path) {
                report.removed.push(path);
                return Ok(());
            }
            if meta.len() > SCAN_MAX {
                if !not_searched(&path) {
                    report.unsearched.push(path);
                }
            } else {
                let mut bytes = fs::read(&full)?;
                if is_git_config(&path) {
                    let clean = git_config(&bytes);
                    if clean != bytes {
                        report.rewritten.push(path.clone());
                        bytes = clean;
                    }
                }
                let found = if not_searched(&path) { Vec::new() } else { secrets(&path, &bytes) };
                if !found.is_empty() {
                    if drops_with_secret(&path) {
                        report.removed.push(path);
                        return Ok(());
                    }
                    if scrub.allows(&path) {
                        report.allowed.extend(found);
                    } else if shipped(&mut self.sums, self.upper, &path, &bytes) {
                        report.shipped.extend(found);
                    } else {
                        self.written.found.extend(found);
                    }
                }
                body = Some(bytes);
            }
        }
        h.set_entry_type(tar::EntryType::Regular);
        self.xattrs(keep)?;
        match body {
            Some(bytes) => {
                h.set_size(bytes.len() as u64);
                self.written.bytes += bytes.len() as u64;
                self.add(&mut h, rel, &bytes[..])?;
            }
            None => {
                let f = File::open(&full)?;
                // What is read is what the header says, so a file that grows meanwhile cannot
                // spill into the next entry.
                h.set_size(meta.len());
                self.written.bytes += meta.len();
                self.add(&mut h, rel, Exact { inner: f, left: meta.len() })?;
            }
        }
        if meta.nlink() > 1 {
            self.links.insert(key, rel.to_path_buf());
        }
        Ok(())
    }

    fn add(&mut self, h: &mut tar::Header, rel: &Path, data: impl Read) -> io::Result<()> {
        self.written.entries += 1;
        self.out.append_data(h, rel, data)
    }

    /// An empty `.wh.NAME` file for `rel`, or `rel` itself when it is already the opaque marker.
    fn whiteout(&mut self, rel: &Path) -> io::Result<()> {
        let name = rel.file_name().unwrap_or_default();
        let path = if name == ".wh..wh..opq" {
            rel.to_path_buf()
        } else {
            let mut wh = OsString::from(".wh.");
            wh.push(name);
            rel.with_file_name(wh)
        };
        let mut h = tar::Header::new_gnu();
        h.set_entry_type(tar::EntryType::Regular);
        h.set_mode(0o644);
        h.set_size(0);
        self.written.whiteouts += 1;
        self.out.append_data(&mut h, path, &[][..])
    }

    /// A PAX header with the xattrs of the entry that follows, if it has any.
    fn xattrs(&mut self, keep: &[(OsString, Vec<u8>)]) -> io::Result<()> {
        if keep.is_empty() {
            return Ok(());
        }
        let mut data = Vec::new();
        for (name, value) in keep {
            let mut key = b"SCHILY.xattr.".to_vec();
            key.extend_from_slice(name.as_bytes());
            pax_record(&mut data, &key, value);
        }
        let mut h = tar::Header::new_ustar();
        h.set_entry_type(tar::EntryType::XHeader);
        h.set_mode(0o644);
        h.set_size(data.len() as u64);
        self.out.append_data(&mut h, "././@PaxHeader", &data[..])
    }

    fn attrs(&mut self, path: &Path) -> io::Result<Attrs> {
        let mut out = Attrs::default();
        let n = match rustix::fs::llistxattr(path, &mut self.xbuf[..]) {
            Ok(n) => n,
            Err(rustix::io::Errno::NOTSUP) => return Ok(out),
            Err(e) => return Err(e.into()),
        };
        let names: Vec<Vec<u8>> = self.xbuf[..n]
            .split(|&b| b == 0)
            .filter(|s| !s.is_empty())
            .map(<[u8]>::to_vec)
            .collect();
        for name in names {
            let len = match rustix::fs::lgetxattr(path, name.as_slice(), &mut self.xbuf[..]) {
                Ok(len) => len,
                Err(rustix::io::Errno::NODATA) => continue,
                Err(e) => return Err(e.into()),
            };
            let value = &self.xbuf[..len];
            let overlay = name
                .strip_prefix(b"trusted.overlay.")
                .or_else(|| name.strip_prefix(b"user.overlay."));
            match overlay {
                // `x` marks a directory with whiteouts in it, which it is not hiding.
                Some(b"opaque") => out.opaque = value == b"y",
                Some(b"whiteout") => out.whiteout = true,
                Some(b"redirect") => out.redirect = true,
                Some(b"metacopy") => out.metacopy = true,
                Some(_) => {}
                None if name == b"security.capability" => {
                    out.keep.push((OsString::from_vec(name), caps_v2(value)));
                }
                None => out.keep.push((OsString::from_vec(name), value.to_vec())),
            }
        }
        Ok(out)
    }
}

/// Reads exactly `left` bytes, zero filled if the file shrank.
struct Exact<R> {
    inner: R,
    left: u64,
}

impl<R: Read> Read for Exact<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if self.left == 0 {
            return Ok(0);
        }
        let want = buf.len().min(usize::try_from(self.left).unwrap_or(usize::MAX));
        let mut n = self.inner.read(&mut buf[..want])?;
        if n == 0 {
            buf[..want].fill(0);
            n = want;
        }
        self.left -= n as u64;
        Ok(n)
    }
}

fn unsupported(msg: String) -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, msg)
}

/// File capabilities as a namespaced writer stores them, with the root id the cells' namespace
/// gave them, become the plain form an image holds, which applies in any namespace.
fn caps_v2(value: &[u8]) -> Vec<u8> {
    if value.len() == 24 && value[3] == 3 {
        let mut v = value[..20].to_vec();
        v[3] = 2;
        v
    } else {
        value.to_vec()
    }
}

/// Adds `key=value` to a PAX header, as a record that starts with its own length.
fn pax_record(out: &mut Vec<u8>, key: &[u8], value: &[u8]) {
    let rest = key.len() + value.len() + 3;
    let mut len = rest + rest.to_string().len();
    if len.to_string().len() + rest > len {
        len += 1;
    }
    out.extend_from_slice(len.to_string().as_bytes());
    out.push(b' ');
    out.extend_from_slice(key);
    out.push(b'=');
    out.extend_from_slice(value);
    out.push(b'\n');
}

fn name_of(path: &str) -> &str {
    path.rsplit('/').next().unwrap_or(path)
}

/// Whether scrubbing leaves `path` out whatever it holds.
fn left_out(path: &str) -> bool {
    if HISTORIES.contains(&name_of(path)) {
        return true;
    }
    let in_home = path.strip_prefix("root/").or_else(|| {
        let rest = path.strip_prefix("home/")?;
        rest.split_once('/').map(|(_, r)| r)
    });
    in_home.is_some_and(|r| CREDENTIALS.contains(&r))
}

/// Whether scrubbing leaves `path` out when a secret is found in it.
fn drops_with_secret(path: &str) -> bool {
    let name = name_of(path);
    name == ".env" || name.starts_with(".env.") || name == ".npmrc"
}

fn is_git_config(path: &str) -> bool {
    path == ".git/config" || path.ends_with("/.git/config")
}

fn not_searched(path: &str) -> bool {
    path.split('/').any(|c| NOT_SEARCHED.contains(&c))
}

/// Whether `bytes` at `path` is what a Debian package installed in the upper shipped there, by the
/// md5sums dpkg keeps in the upper. With merged `/usr`, a package may list `lib/x` for what is at
/// `usr/lib/x`.
fn shipped(
    sums: &mut Option<HashMap<Vec<u8>, [u8; 16]>>,
    upper: &Path,
    path: &str,
    bytes: &[u8],
) -> bool {
    let sums = sums.get_or_insert_with(|| md5sums(upper));
    let listed = [Some(path), path.strip_prefix("usr/")]
        .into_iter()
        .flatten()
        .find_map(|p| sums.get(p.as_bytes()));
    listed.is_some_and(|sum| Md5::digest(bytes)[..] == sum[..])
}

/// The md5 of each file the packages in `upper/var/lib/dpkg/info` shipped, by path. The cell
/// wrote these lists, so a link is not followed and a list bigger than [`SCAN_MAX`] is not read.
fn md5sums(upper: &Path) -> HashMap<Vec<u8>, [u8; 16]> {
    let mut sums = HashMap::new();
    let mut info = upper.to_path_buf();
    for part in ["var", "lib", "dpkg", "info"] {
        info.push(part);
        if !fs::symlink_metadata(&info).is_ok_and(|m| m.is_dir()) {
            return sums;
        }
    }
    let Ok(lists) = fs::read_dir(&info) else { return sums };
    for list in lists.flatten() {
        if !list.file_name().as_bytes().ends_with(b".md5sums")
            || !fs::symlink_metadata(list.path()).is_ok_and(|m| m.is_file() && m.len() <= SCAN_MAX)
        {
            continue;
        }
        let Ok(text) = fs::read(list.path()) else { continue };
        for line in text.split(|&b| b == b'\n') {
            // `<32 hex digits>  <path>`, the path without its leading slash.
            if line.len() < 35 || &line[32..34] != b"  " {
                continue;
            }
            let mut sum = [0u8; 16];
            let hex = |b: u8| (b as char).to_digit(16);
            let ok = sum.iter_mut().zip(line[..32].chunks(2)).all(|(s, h)| {
                match (hex(h[0]), hex(h[1])) {
                    (Some(hi), Some(lo)) => {
                        *s = (hi * 16 + lo) as u8;
                        true
                    }
                    _ => false,
                }
            });
            if ok {
                sums.insert(line[34..].to_vec(), sum);
            }
        }
    }
    sums
}

/// A git config with the users and tokens taken out of remote urls, and extra headers dropped.
fn git_config(bytes: &[u8]) -> Vec<u8> {
    let urls = GIT_URL.replace_all(bytes, &b"${1}"[..]);
    GIT_HEADER.replace_all(&urls, &b""[..]).into_owned()
}

/// The first place each rule matches in `bytes`.
fn secrets(path: &str, bytes: &[u8]) -> Vec<Finding> {
    let hits = ANY.matches(bytes);
    if !hits.matched_any() {
        return Vec::new();
    }
    hits.iter()
        .filter_map(|i| {
            let m = EACH[i].find(bytes)?;
            let line = bytes[..m.start()].iter().filter(|&&b| b == b'\n').count() as u64 + 1;
            Some(Finding { path: path.to_owned(), line, rule: RULES[i].0.to_owned() })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ids_shift_back_and_strangers_become_nobody() {
        let s = Shift { base: 1_000_000, count: 65536 };
        assert_eq!(s.back(1_000_000), 0);
        assert_eq!(s.back(1_001_000), 1000);
        assert_eq!(s.back(0), 65534);
        assert_eq!(s.back(1_065_536), 65534);
    }

    #[test]
    fn pax_records_count_their_own_length() {
        for value in [&b"x"[..], &[7; 5], &[7; 89], &[7; 90], &[7; 994], &[7; 995]] {
            let mut out = Vec::new();
            pax_record(&mut out, b"SCHILY.xattr.user.a", value);
            let (len, _) = std::str::from_utf8(&out).unwrap().split_once(' ').unwrap();
            assert_eq!(len.parse::<usize>().unwrap(), out.len(), "{}", value.len());
        }
    }

    #[test]
    fn scrubbing_knows_histories_credentials_and_env_files() {
        for p in ["root/.bash_history", "testbed/.python_history", "home/dev/.git-credentials"] {
            assert!(left_out(p), "{p}");
        }
        for p in ["root/.ssh/id_ed25519", "home/a/.aws/credentials", "home/a/.netrc"] {
            assert!(left_out(p), "{p}");
        }
        for p in ["root/.ssh/id_ed25519.pub", "srv/.aws/credentials", "home/.netrc", "root/notes"] {
            assert!(!left_out(p), "{p}");
        }
        assert!(drops_with_secret("app/.env") && drops_with_secret("app/.env.local"));
        assert!(!drops_with_secret("app/env.py"));
        assert!(not_searched("usr/lib/python3/site-packages/x/y.py"));
        assert!(!not_searched("testbed/site_packages.py"));
        let allow = Scrub { allow: vec!["testbed/tests/".into(), "etc/key.pem".into()] };
        assert!(allow.allows("testbed/tests/fixtures/k") && allow.allows("etc/key.pem"));
        assert!(!allow.allows("testbed/tests2/k") && !allow.allows("etc/key.pem.bak"));
    }

    #[test]
    fn git_configs_lose_their_tokens() {
        let cfg = b"[core]\n\tbare = false\n[remote \"origin\"]\n\turl = https://x-access-token:ghs_abc@github.com/o/r.git\n\tfetch = +refs/heads/*:refs/remotes/origin/*\n[http \"https://github.com/\"]\n\textraheader = AUTHORIZATION: basic eC1hY2Nlc3M=\n[remote \"up\"]\n\turl = git@github.com:o/r.git\n";
        let out = String::from_utf8(git_config(cfg)).unwrap();
        assert!(out.contains("url = https://github.com/o/r.git"), "{out}");
        assert!(!out.contains("ghs_abc") && !out.contains("AUTHORIZATION"), "{out}");
        assert!(out.contains("url = git@github.com:o/r.git"), "scp style urls have no secret");
        assert!(out.contains("fetch = +refs"));
    }

    #[test]
    fn secrets_are_found_by_kind_and_line_and_lookalikes_are_not() {
        let text = format!(
            "a = 1\nkey = \"AKIA{}\"\n-----BEGIN OPENSSH PRIVATE KEY-----\ntoken: ghp_{}\nsk-{}\n",
            "Q".repeat(16),
            "a".repeat(36),
            "b".repeat(40)
        );
        let got: Vec<(u64, String)> =
            secrets("f", text.as_bytes()).into_iter().map(|f| (f.line, f.rule)).collect();
        assert_eq!(
            got,
            [
                (3, "private key".to_owned()),
                (2, "aws access key".to_owned()),
                (4, "github token".to_owned()),
                (5, "openai or anthropic key".to_owned()),
            ]
        );
        let fine = "task-0123456789abcdef0123456789abcdef01\nAKIA_NOT_A_KEY\nhf_short\nsk-learn\n";
        assert!(secrets("f", fine.as_bytes()).is_empty());
    }

    #[test]
    fn namespaced_file_caps_become_plain() {
        let mut v3 = vec![0u8; 24];
        v3[0] = 1;
        v3[3] = 3;
        v3[20..].copy_from_slice(&1_000_000u32.to_le_bytes());
        let v2 = caps_v2(&v3);
        assert_eq!((v2.len(), v2[0], v2[3]), (20, 1, 2));
        assert_eq!(caps_v2(&v2), v2);
    }

    #[test]
    fn an_upper_without_overlay_marks_writes_a_plain_tar() {
        let dir = std::env::temp_dir().join(format!("hive-upper-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("root")).unwrap();
        fs::create_dir_all(dir.join("app/.git")).unwrap();
        fs::write(dir.join("app/main.py"), "print(1)\n").unwrap();
        fs::write(dir.join("app/.env"), "OPENAI_API_KEY=sk-0123456789abcdef0123456789abcdef\n")
            .unwrap();
        fs::write(dir.join("app/.git/config"), "[remote \"o\"]\n\turl = https://u:p@h/r\n")
            .unwrap();
        fs::write(dir.join("root/.bash_history"), "ls\n").unwrap();
        fs::write(dir.join("root/key"), "-----BEGIN RSA PRIVATE KEY-----\n").unwrap();
        fs::hard_link(dir.join("app/main.py"), dir.join("app/same.py")).unwrap();
        std::os::unix::fs::symlink("main.py", dir.join("app/link")).unwrap();

        let (tar, w) = write_tar(&dir, None, Some(&Scrub::default()), Vec::new()).unwrap();
        let s = w.scrubbed.as_ref().unwrap();
        assert_eq!(s.removed, ["app/.env", "root/.bash_history"]);
        assert_eq!(s.rewritten, ["app/.git/config"]);
        assert_eq!(w.found.len(), 1);
        assert_eq!((w.found[0].path.as_str(), w.found[0].line), ("root/key", 1));

        let mut entries = Vec::new();
        for e in tar::Archive::new(&tar[..]).entries().unwrap() {
            let mut e = e.unwrap();
            let mut body = String::new();
            e.read_to_string(&mut body).unwrap();
            let path = e.path().unwrap().display().to_string();
            let link = e.link_name().unwrap().map(|l| l.display().to_string()).unwrap_or_default();
            entries.push((path, e.header().entry_type(), body, link));
        }
        let names: Vec<&str> = entries.iter().map(|e| e.0.as_str()).collect();
        assert_eq!(
            names,
            [
                "app",
                "app/.git",
                "app/.git/config",
                "app/link",
                "app/main.py",
                "app/same.py",
                "root",
                "root/key"
            ]
        );
        assert_eq!(entries[2].2, "[remote \"o\"]\n\turl = https://h/r\n");
        assert_eq!((entries[3].1, entries[3].3.as_str()), (tar::EntryType::Symlink, "main.py"));
        assert_eq!((entries[5].1, entries[5].3.as_str()), (tar::EntryType::Link, "app/main.py"));

        let allowed = Scrub { allow: vec!["root".into()] };
        let (_, w) = write_tar(&dir, None, Some(&allowed), Vec::new()).unwrap();
        assert!(w.found.is_empty());
        assert_eq!(w.scrubbed.unwrap().allowed.len(), 1);
        let (_, w) = write_tar(&dir, None, None, Vec::new()).unwrap();
        assert!(w.found.is_empty() && w.scrubbed.is_none());
        assert_eq!(w.entries, 10);
        fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn a_key_a_package_shipped_is_let_through_until_the_file_changes() {
        let dir = std::env::temp_dir().join(format!("hive-upper-pkg-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        let key = format!("\0\0-----BEGIN PRIVATE KEY-----\nMIIB{}\n\0", "A".repeat(60));
        let lib = "usr/lib/x86_64-linux-gnu/libtls.so.30";
        fs::create_dir_all(dir.join("var/lib/dpkg/info")).unwrap();
        fs::create_dir_all(dir.join("usr/lib/x86_64-linux-gnu")).unwrap();
        fs::write(dir.join(lib), &key).unwrap();
        fs::write(dir.join("usr/lib/x86_64-linux-gnu/libother.so"), &key).unwrap();
        let sum: String = Md5::digest(key.as_bytes()).iter().map(|b| format!("{b:02x}")).collect();
        let list = format!("{sum}  {lib}\n{sum}  lib/x86_64-linux-gnu/libother.so\nnot a line\n");
        fs::write(dir.join("var/lib/dpkg/info/libtls30:amd64.md5sums"), list).unwrap();

        let (_, w) = write_tar(&dir, None, Some(&Scrub::default()), Vec::new()).unwrap();
        assert!(w.found.is_empty(), "{:?}", w.found);
        let shipped: Vec<&str> =
            w.scrubbed.as_ref().unwrap().shipped.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(shipped, ["usr/lib/x86_64-linux-gnu/libother.so", lib]);

        // Changed after it was installed, it is the cell's file, and so is one no package lists.
        fs::write(dir.join(lib), format!("{key}x")).unwrap();
        fs::write(dir.join("usr/lib/x86_64-linux-gnu/libmine.so"), &key).unwrap();
        let (_, w) = write_tar(&dir, None, Some(&Scrub::default()), Vec::new()).unwrap();
        let found: Vec<&str> = w.found.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(found, ["usr/lib/x86_64-linux-gnu/libmine.so", lib]);
        fs::remove_dir_all(&dir).unwrap();
    }
}
