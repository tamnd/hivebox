//! The API keys the gate lets in: the ones in its config, and the ones the keeper holds.
//!
//! With a keeper, the gate asks it for every key once a second and swaps the whole set in, so a
//! new key works and a revoked one stops working within about a second. A keeper that cannot be
//! reached leaves the last set in place, so the gate goes on serving the keys it knew.

use std::collections::HashMap;
use std::sync::{Arc, PoisonError, RwLock};
use std::time::Duration;

use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// Projects by the BLAKE3 hash of the key that opens them.
pub type KeyMap = HashMap<[u8; 32], Arc<str>>;

/// How often the gate asks the keeper for the keys.
pub const EVERY: Duration = Duration::from_secs(1);

/// The keys the gate takes. Cheap to clone, and every clone sees the same set.
#[derive(Clone, Debug, Default)]
pub struct Keys {
    inner: Arc<Inner>,
}

#[derive(Debug, Default)]
struct Inner {
    fixed: KeyMap,
    live: RwLock<Arc<KeyMap>>,
}

impl Keys {
    /// The keys in `fixed`, with none from a keeper yet.
    #[must_use]
    pub fn new(fixed: KeyMap) -> Self {
        Self { inner: Arc::new(Inner { fixed, live: RwLock::default() }) }
    }

    /// The project the key with this hash opens. A key in the config wins over the keeper.
    #[must_use]
    pub fn project(&self, hash: &[u8; 32]) -> Option<Arc<str>> {
        if let Some(p) = self.inner.fixed.get(hash) {
            return Some(Arc::clone(p));
        }
        self.inner.live.read().unwrap_or_else(PoisonError::into_inner).get(hash).cloned()
    }

    /// Swaps in the keeper's keys.
    pub fn set_live(&self, keys: KeyMap) {
        *self.inner.live.write().unwrap_or_else(PoisonError::into_inner) = Arc::new(keys);
    }

    /// How many keys came from the keeper.
    #[must_use]
    pub fn live(&self) -> usize {
        self.inner.live.read().unwrap_or_else(PoisonError::into_inner).len()
    }
}

impl From<KeyMap> for Keys {
    fn from(fixed: KeyMap) -> Self {
        Self::new(fixed)
    }
}

/// The keys that work out of a keeper's list: the ones not revoked.
#[must_use]
pub fn usable(list: &[pb::KeyInfo]) -> KeyMap {
    list.iter()
        .filter(|k| k.revoked_ms == 0)
        .filter_map(|k| {
            Some((<[u8; 32]>::try_from(k.hash.as_slice()).ok()?, Arc::from(k.project.as_str())))
        })
        .collect()
}

/// Asks the keeper at `members`, one member at a time and the next when one fails, for the
/// keys every [`EVERY`] and puts them in `keys`, until `stop`.
///
/// # Errors
///
/// A member address does not parse.
pub fn follow(keys: Keys, members: &[String], stop: CancellationToken) -> Result<(), String> {
    let clients = members
        .iter()
        .map(|m| {
            let channel = Endpoint::from_shared(format!("http://{m}"))
                .map_err(|e| format!("keeper member {m}: {e}"))?
                .connect_timeout(Duration::from_secs(1))
                .timeout(Duration::from_secs(5))
                .connect_lazy();
            Ok((m.clone(), KeeperClient::new(channel)))
        })
        .collect::<Result<Vec<(String, KeeperClient<Channel>)>, String>>()?;
    if clients.is_empty() {
        return Err("the keeper has no members".into());
    }
    tokio::spawn(async move {
        let mut clients = clients;
        let n = clients.len();
        let (mut at, mut ok) = (0, None);
        let mut tick = tokio::time::interval(EVERY);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                () = stop.cancelled() => return,
                _ = tick.tick() => {}
            }
            let (member, client) = &mut clients[at];
            match client.list_keys(pb::ListKeysRequest { project: String::new() }).await {
                Ok(r) => {
                    let got = usable(&r.into_inner().keys);
                    // Only a change between answers and failures is logged, not every second of either.
                    if ok != Some(true) {
                        eprintln!("hive-gate: {} keys from the keeper at {member}", got.len());
                        ok = Some(true);
                    }
                    keys.set_live(got);
                }
                Err(e) => {
                    if ok != Some(false) {
                        eprintln!(
                            "hive-gate: reading keys from the keeper at {member}: {}, keeping the last {}",
                            e.message(),
                            keys.live()
                        );
                        ok = Some(false);
                    }
                    at = (at + 1) % n;
                }
            }
        }
    });
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn info(hash: u8, project: &str, revoked_ms: u64) -> pb::KeyInfo {
        pb::KeyInfo {
            prefix: "hb_00000".into(),
            project: project.into(),
            hash: vec![hash; 32],
            created_ms: 1,
            revoked_ms,
        }
    }

    #[test]
    fn a_revoked_key_or_a_bad_hash_is_left_out() {
        let mut short = info(4, "c", 0);
        short.hash.pop();
        let got = usable(&[info(1, "a", 0), info(2, "b", 5), short]);
        assert_eq!(got.len(), 1);
        assert_eq!(got.get(&[1; 32]).map(|p| &**p), Some("a"));
    }

    #[test]
    fn the_config_wins_and_the_keeper_set_is_swapped_whole() {
        let keys = Keys::new(HashMap::from([([1; 32], Arc::from("fixed"))]));
        keys.set_live(HashMap::from([([1; 32], Arc::from("live")), ([2; 32], Arc::from("b"))]));
        assert_eq!(keys.project(&[1; 32]).as_deref(), Some("fixed"));
        assert_eq!(keys.project(&[2; 32]).as_deref(), Some("b"));
        keys.set_live(HashMap::new());
        assert_eq!(keys.project(&[2; 32]), None);
        assert_eq!(keys.live(), 0);
    }
}
