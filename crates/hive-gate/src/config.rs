//! The gate's config file.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;

use serde::Deserialize;

/// What the gate needs to run.
#[derive(Clone, Debug)]
pub struct Config {
    /// Where clients reach the API.
    pub listen: SocketAddr,
    /// Where `/metrics` is served, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// The scout to follow for the nodes and their addresses, like `http://10.0.0.5:7410`.
    pub scout: String,
    /// The project each API key opens, by the BLAKE3 hash of the key.
    pub keys: HashMap<[u8; 32], Arc<str>>,
    /// The keeper members to read the rest of the keys from, as `host:port`. Empty means only
    /// the keys here.
    pub keeper: Vec<String>,
    /// The name the gate takes its share of each project's quota under, the same across
    /// restarts. The hostname and the listen port by default.
    pub name: String,
    /// The E2B API, served when the file has an `[e2b]` table.
    pub e2b: Option<E2b>,
}

/// How E2B sandboxes become cells.
#[derive(Clone, Debug)]
pub struct E2b {
    /// The metadata key whose value is the image a sandbox runs, like `swe/image`.
    pub image_key: Option<String>,
    /// The image for each E2B template, for sandboxes whose metadata names none.
    pub templates: HashMap<String, String>,
    /// Resources by size name, which a sandbox picks with the `size` metadata key, or with
    /// `swe/size` when `image_key` is `swe/image`.
    pub sizes: HashMap<String, Size>,
    /// The backend sandboxes run on, `container` unless the file says otherwise.
    pub backend: hive_types::Backend,
}

impl Default for E2b {
    fn default() -> Self {
        Self {
            image_key: None,
            templates: HashMap::new(),
            sizes: HashMap::new(),
            backend: hive_types::Backend::Container,
        }
    }
}

impl E2b {
    /// The metadata key that picks a size.
    #[must_use]
    pub fn size_key(&self) -> String {
        match self.image_key.as_deref().and_then(|k| k.rsplit_once('/')) {
            Some((prefix, _)) => format!("{prefix}/size"),
            None => "size".into(),
        }
    }
}

/// The resources of one E2B size.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Size {
    /// Thousandths of a CPU.
    #[serde(default)]
    pub vcpu_milli: u32,
    /// Memory in MiB.
    #[serde(default)]
    pub mem_mib: u32,
    /// Disk in GiB.
    #[serde(default)]
    pub disk_gib: u32,
}

impl Config {
    /// Reads a config file. A key the gate does not know is an error, so a typo never passes
    /// for a setting.
    ///
    /// ```toml
    /// [gate]
    /// listen = "0.0.0.0:7400"
    /// metrics = "127.0.0.1:9467"
    /// scout = "http://10.0.0.5:7410"
    /// keeper = ["10.0.0.1:7430", "10.0.0.2:7430", "10.0.0.3:7430"]
    /// name = "gate-1"
    ///
    /// [[key]]
    /// project = "swe"
    /// blake3 = "5c1f...64 hex digits"
    ///
    /// [e2b]
    /// image_key = "swe/image"
    /// templates = { base = "docker.io/library/python:3.12" }
    /// sizes = { md = { vcpu_milli = 2000, mem_mib = 4096 } }
    /// backend = "container"
    /// ```
    ///
    /// With `keeper`, the gate takes the keys the keeper holds, and `[[key]]` is for keys that
    /// have to work when the keeper is down, and creates are held to the projects' quotas.
    /// Without it, only the keys here work and there are no quotas.
    /// `hive-gate key PROJECT` makes one and prints the lines to add here. With `[e2b]`, the
    /// gate serves the E2B REST API and the parts of envd that run commands and move files, so
    /// the E2B SDK works with `E2B_API_URL` and `E2B_SANDBOX_URL` both set to the gate.
    ///
    /// # Errors
    ///
    /// The file is not TOML, or holds a key or a value the gate does not take.
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let file: File = toml::from_str(text).map_err(|e| e.to_string())?;
        let g = file.gate;
        let listen = match g.listen {
            Some(t) => addr(&t, "gate.listen")?,
            None => SocketAddr::from(([0, 0, 0, 0], 7400)),
        };
        let metrics = g.metrics.map(|t| addr(&t, "gate.metrics")).transpose()?;
        let scout = g.scout.ok_or("gate.scout is needed, like \"http://10.0.0.5:7410\"")?;
        if !scout.starts_with("http://") && !scout.starts_with("https://") {
            return Err(format!("gate.scout = {scout:?} is not a URL like http://10.0.0.5:7410"));
        }
        let mut keys = HashMap::new();
        for k in file.key {
            if !hive_types::is_name(&k.project) {
                return Err(format!("key.project = {:?} is not a name", k.project));
            }
            let hash = hex32(&k.blake3)
                .ok_or_else(|| format!("key.blake3 for {} is not 64 hex digits", k.project))?;
            if keys.insert(hash, Arc::from(k.project.as_str())).is_some() {
                return Err(format!("the key for {} is there twice", k.project));
            }
        }
        for m in &g.keeper {
            if m.contains("://") || !m.contains(':') {
                return Err(format!("gate.keeper has {m:?}, which is not host:port"));
            }
        }
        if keys.is_empty() && g.keeper.is_empty() {
            return Err("no [[key]] and no gate.keeper, so nobody could call the gate".into());
        }
        let name = match g.name {
            Some(n) => n,
            None => {
                let host = std::fs::read_to_string("/proc/sys/kernel/hostname").unwrap_or_default();
                format!("{}/{}", host.trim(), listen.port())
            }
        };
        if !hive_types::is_name(&name) {
            return Err(format!("gate.name = {name:?} is not a name, so set one"));
        }
        let e2b = file
            .e2b
            .map(|e| {
                let backend = match e.backend.as_deref().unwrap_or("container") {
                    "container" => hive_types::Backend::Container,
                    "microvm" => hive_types::Backend::Microvm,
                    "fullvm" => hive_types::Backend::Fullvm,
                    b => {
                        return Err(format!(
                            "e2b.backend = {b:?} is not container, microvm or fullvm"
                        ));
                    }
                };
                Ok(E2b {
                    image_key: e.image_key.filter(|k| !k.is_empty()),
                    templates: e.templates,
                    sizes: e.sizes,
                    backend,
                })
            })
            .transpose()?;
        if let Some(e) = &e2b {
            if let Some(k) = e.image_key.as_deref().filter(|k| !hive_types::is_name(k)) {
                return Err(format!("e2b.image_key = {k:?} is not a name"));
            }
            if e.image_key.is_none() && e.templates.is_empty() {
                return Err("[e2b] needs image_key or templates, or no sandbox has an image".into());
            }
        }
        Ok(Self { listen, metrics, scout, keys, keeper: g.keeper, name, e2b })
    }
}

/// A fresh API key for `project`, and the lines that let the gate take it.
///
/// # Errors
///
/// The system has no randomness to give.
pub fn new_key(project: &str) -> Result<(String, String), String> {
    let mut raw = [0u8; 24];
    getrandom::fill(&mut raw).map_err(|e| e.to_string())?;
    let key = format!("hb_{}", hex(&raw));
    let lines = format!(
        "[[key]]\nproject = \"{project}\"\nblake3 = \"{}\"\n",
        hex(blake3::hash(key.as_bytes()).as_bytes())
    );
    Ok((key, lines))
}

fn addr(text: &str, what: &str) -> Result<SocketAddr, String> {
    text.parse().map_err(|_| format!("{what} = {text:?} is not an address like 10.0.0.5:7400"))
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write;
    bytes.iter().fold(String::with_capacity(bytes.len() * 2), |mut s, b| {
        let _ = write!(s, "{b:02x}");
        s
    })
}

fn hex32(text: &str) -> Option<[u8; 32]> {
    let digits = text.as_bytes();
    if digits.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (o, pair) in out.iter_mut().zip(digits.chunks(2)) {
        *o = u8::from_str_radix(std::str::from_utf8(pair).ok()?, 16).ok()?;
    }
    Some(out)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    gate: Gate,
    #[serde(default)]
    key: Vec<Key>,
    e2b: Option<E2bFile>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct E2bFile {
    image_key: Option<String>,
    backend: Option<String>,
    #[serde(default)]
    templates: HashMap<String, String>,
    #[serde(default)]
    sizes: HashMap<String, Size>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Gate {
    listen: Option<String>,
    metrics: Option<String>,
    scout: Option<String>,
    #[serde(default)]
    keeper: Vec<String>,
    name: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Key {
    project: String,
    blake3: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_new_key_is_one_the_config_takes() {
        let (key, lines) = new_key("swe").unwrap();
        assert!(key.starts_with("hb_") && key.len() == 51, "{key}");
        let c = Config::from_toml(&format!("[gate]\nscout = \"http://10.0.0.5:7410\"\n{lines}"))
            .unwrap();
        let hash = *blake3::hash(key.as_bytes()).as_bytes();
        assert_eq!(c.keys.get(&hash).map(|p| &**p), Some("swe"));
        assert_eq!(c.listen, "0.0.0.0:7400".parse().unwrap());
        assert_ne!(new_key("swe").unwrap().0, key);
    }

    #[test]
    fn a_keeper_is_enough_without_keys() {
        let c = Config::from_toml(
            "[gate]\nscout = \"http://s:7410\"\nkeeper = [\"k1:7430\", \"k2:7430\"]\n",
        )
        .unwrap();
        assert!(c.keys.is_empty());
        assert_eq!(c.keeper, ["k1:7430", "k2:7430"]);
    }

    #[test]
    fn e2b_takes_images_and_sizes() {
        let c = Config::from_toml(
            "[gate]\nscout = \"http://s:7410\"\nkeeper = [\"k:7430\"]\n[e2b]\n\
             image_key = \"swe/image\"\ntemplates = { base = \"python:3.12\" }\n\
             sizes = { md = { vcpu_milli = 2000, mem_mib = 4096 } }\n",
        )
        .unwrap();
        let e = c.e2b.unwrap();
        assert_eq!(e.image_key.as_deref(), Some("swe/image"));
        assert_eq!(e.size_key(), "swe/size");
        assert_eq!(e.templates["base"], "python:3.12");
        assert_eq!(e.sizes["md"], Size { vcpu_milli: 2000, mem_mib: 4096, disk_gib: 0 });
        assert_eq!(e.backend, hive_types::Backend::Container);
        let plain = E2b { image_key: Some("image".into()), ..E2b::default() };
        assert_eq!(plain.size_key(), "size");
    }

    #[test]
    fn mistakes_are_errors() {
        let key = "[[key]]\nproject = \"swe\"\nblake3 = \"".to_string() + &"ab".repeat(32) + "\"\n";
        let scout = "[gate]\nscout = \"http://s:7410\"\n";
        let cases = [
            (key.clone(), "gate.scout is needed"),
            (format!("[gate]\nscout = \"s:7410\"\n{key}"), "not a URL"),
            (format!("{scout}listen = \"7400\"\n{key}"), "not an address"),
            (format!("{scout}lisen = \"0.0.0.0:7400\"\n{key}"), "unknown field"),
            (scout.to_string(), "no [[key]]"),
            (format!("{scout}{}", key.replace("ab", "zz")), "not 64 hex digits"),
            (format!("{scout}{key}{key}"), "there twice"),
            (format!("{scout}{}", key.replace("swe", "a b")), "not a name"),
            (format!("{scout}keeper = [\"http://k:7430\"]\n"), "not host:port"),
            (format!("{scout}name = \"a b\"\n{key}"), "gate.name"),
            (format!("{scout}{key}[e2b]\n"), "needs image_key or templates"),
            (format!("{scout}{key}[e2b]\nimage_key = \"a b\"\n"), "e2b.image_key"),
            (format!("{scout}{key}[e2b]\nimage = \"x\"\n"), "unknown field"),
            (format!("{scout}{key}[e2b]\nimage_key = \"i\"\nbackend = \"auto\"\n"), "e2b.backend"),
        ];
        for (text, want) in cases {
            let e = Config::from_toml(&text).unwrap_err();
            assert!(e.contains(want), "{text}: {e}");
        }
    }
}
