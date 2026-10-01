//! The keeper's config file.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::time::Duration;

use serde::Deserialize;

/// What one keeper member needs to run.
#[derive(Clone, Debug)]
pub struct Config {
    /// This member's Raft id, one of the `[[member]]` ids.
    pub id: u64,
    /// Where gates, combs and the other members reach this one.
    pub listen: SocketAddr,
    /// The directory for the database.
    pub data: PathBuf,
    /// How long a comb's lease lasts.
    pub lease: Duration,
    /// Every member of the group, this one too, by id. The address is `host:port`.
    pub members: BTreeMap<u64, String>,
}

impl Config {
    /// Reads a config file. A key the keeper does not know is an error.
    ///
    /// ```toml
    /// [keeper]
    /// id = 1
    /// listen = "0.0.0.0:7430"
    /// data = "/var/lib/hivebox/keeper"
    /// lease_ms = 10000
    ///
    /// [[member]]
    /// id = 1
    /// addr = "10.0.0.1:7430"
    ///
    /// [[member]]
    /// id = 2
    /// addr = "10.0.0.2:7430"
    ///
    /// [[member]]
    /// id = 3
    /// addr = "10.0.0.3:7430"
    /// ```
    ///
    /// Every member gets the same list. The group forms the first time they start, and after
    /// that the list is read from the log, so changing it here does nothing.
    ///
    /// # Errors
    ///
    /// The file is not TOML, or holds a key or a value the keeper does not take.
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let file: File = toml::from_str(text).map_err(|e| e.to_string())?;
        let k = file.keeper;
        let id = k.id.ok_or("keeper.id is needed")?;
        let listen = match k.listen {
            Some(t) => t.parse().map_err(|_| {
                format!("keeper.listen = {t:?} is not an address like 0.0.0.0:7430")
            })?,
            None => SocketAddr::from(([0, 0, 0, 0], 7430)),
        };
        let data = k.data.map_or_else(|| PathBuf::from("/var/lib/hivebox/keeper"), PathBuf::from);
        let lease_ms = k.lease_ms.unwrap_or(10_000);
        if !(1000..=600_000).contains(&lease_ms) {
            return Err(format!("keeper.lease_ms = {lease_ms} is not between 1000 and 600000"));
        }
        let mut members = BTreeMap::new();
        for m in file.member {
            if m.id == 0 {
                return Err("member.id 0 is not an id, they start at 1".into());
            }
            if m.addr.contains("://") || !m.addr.contains(':') {
                return Err(format!("member.addr = {:?} is not host:port", m.addr));
            }
            if members.insert(m.id, m.addr).is_some() {
                return Err(format!("member {} is there twice", m.id));
            }
        }
        if !members.contains_key(&id) {
            return Err(format!("keeper.id = {id} is not one of the [[member]] ids"));
        }
        Ok(Self { id, listen, data, lease: Duration::from_millis(lease_ms), members })
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct File {
    #[serde(default)]
    keeper: Keeper,
    #[serde(default)]
    member: Vec<Member>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Keeper {
    id: Option<u64>,
    listen: Option<String>,
    data: Option<String>,
    lease_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Member {
    id: u64,
    addr: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    const THREE: &str = "[[member]]\nid = 1\naddr = \"10.0.0.1:7430\"\n\
        [[member]]\nid = 2\naddr = \"10.0.0.2:7430\"\n\
        [[member]]\nid = 3\naddr = \"10.0.0.3:7430\"\n";

    #[test]
    fn a_config_reads_with_its_defaults() {
        let c = Config::from_toml(&format!("[keeper]\nid = 2\n{THREE}")).unwrap();
        assert_eq!(c.id, 2);
        assert_eq!(c.listen, "0.0.0.0:7430".parse().unwrap());
        assert_eq!(c.lease, Duration::from_secs(10));
        assert_eq!(c.members.len(), 3);
    }

    #[test]
    fn a_bad_config_says_what_is_wrong() {
        for (text, says) in [
            (format!("[keeper]\nid = 4\n{THREE}"), "not one of"),
            (format!("[keeper]\n{THREE}"), "keeper.id is needed"),
            (format!("[keeper]\nid = 1\nlease_ms = 5\n{THREE}"), "lease_ms"),
            (format!("[keeper]\nid = 1\nleader = 1\n{THREE}"), "unknown field"),
            ("[keeper]\nid = 1\n[[member]]\nid = 1\naddr = \"http://a:1\"\n".into(), "host:port"),
        ] {
            let e = Config::from_toml(&text).unwrap_err();
            assert!(e.contains(says), "{e}");
        }
    }
}
