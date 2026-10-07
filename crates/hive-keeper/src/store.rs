//! The Raft log and the state machine, in one redb file.
//!
//! The log, the vote and the state live in the same database, and each call that changes them
//! commits once, so a crash leaves the file as it was before the call or after it. The state is
//! also kept in memory for reads, and the rows on disk are the same state, written a row at a
//! time as commands change it. Only the log and the vote wait for the disk; the state rows can
//! always be made again from the log.

// openraft's storage traits return its StorageError, which is over 200 bytes, so every helper
// that feeds them returns it too.
#![allow(clippy::result_large_err)]

use std::fmt;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::Path;
use std::sync::{Arc, PoisonError, RwLock};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, OptionalSend, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership, Vote,
};
use redb::{Database, Durability, ReadableTable, TableDefinition};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::state::{AuditChain, Changed, Command, Key, Node, Project, Reply, State};

openraft::declare_raft_types!(
    /// The types the keeper's Raft group works with.
    pub TypeConfig:
        D = Command,
        R = Reply,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<TypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
);

/// A result with a Raft storage error.
pub type StoreResult<T> = Result<T, StorageError<u64>>;

const LOGS: TableDefinition<'_, u64, &[u8]> = TableDefinition::new("logs");
const META: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("meta");
const PROJECTS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("projects");
const KEYS: TableDefinition<'_, &[u8], &[u8]> = TableDefinition::new("keys");
const NODES: TableDefinition<'_, u16, &[u8]> = TableDefinition::new("nodes");
/// The sealed hours of the nodes' audit chains, by node name and hour.
const AUDIT_HOURS: TableDefinition<'_, (&str, &str), &[u8]> = TableDefinition::new("audit_hours");
/// The tips of the nodes' audit chains, by node name.
const AUDIT_TIPS: TableDefinition<'_, &str, &[u8]> = TableDefinition::new("audit_tips");

const VOTE: &str = "vote";
const PURGED: &str = "purged";
const APPLIED: &str = "applied";
const MEMBERSHIP: &str = "membership";
const SNAPSHOT: &str = "snapshot";
const ROOT: &str = "root";

/// The state machine as one value, for snapshots.
#[derive(Serialize, Deserialize)]
struct Image {
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
    projects: Vec<Project>,
    keys: Vec<([u8; 32], Key)>,
    nodes: Vec<Node>,
    #[serde(default)]
    root: Option<[u8; 32]>,
    #[serde(default)]
    audit: Vec<(String, AuditChain)>,
}

/// A snapshot as it is kept, the meta and the bytes.
#[derive(Serialize, Deserialize)]
struct Kept {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

/// The keeper's storage. Cloning it is cheap, and the clones share everything.
#[derive(Clone)]
pub struct Store {
    inner: Arc<Inner>,
}

struct Inner {
    db: Database,
    state: RwLock<Applied>,
}

/// The state and how far into the log it is.
#[derive(Default)]
struct Applied {
    state: State,
    applied: Option<LogId<u64>>,
    membership: StoredMembership<u64, BasicNode>,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let a = self.inner.state.read().unwrap_or_else(PoisonError::into_inner);
        f.debug_struct("Store").field("applied", &a.applied).finish_non_exhaustive()
    }
}

impl Store {
    /// Opens the store at `path`, making it if it is not there, and reads the state back.
    ///
    /// # Errors
    ///
    /// The file cannot be opened, or what is in it does not decode.
    pub async fn open(path: &Path) -> StoreResult<Self> {
        // redb takes the file for one handle. A member that just stopped in the same process can
        // still hold it while openraft's tasks and the connections it served wind down, which
        // takes seconds on a loaded machine, so wait that out without holding up the runtime
        // those tasks run on.
        let mut tries = 0;
        let db = loop {
            match Database::create(path) {
                Err(redb::DatabaseError::DatabaseAlreadyOpen) if tries < 300 => {
                    tries += 1;
                    tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                }
                r => break r.map_err(write)?,
            }
        };
        let tx = db.begin_write().map_err(write)?;
        for t in [META, PROJECTS] {
            tx.open_table(t).map_err(write)?;
        }
        tx.open_table(LOGS).map_err(write)?;
        tx.open_table(KEYS).map_err(write)?;
        tx.open_table(NODES).map_err(write)?;
        tx.open_table(AUDIT_HOURS).map_err(write)?;
        tx.open_table(AUDIT_TIPS).map_err(write)?;
        tx.commit().map_err(write)?;
        let store = Self { inner: Arc::new(Inner { db, state: RwLock::default() }) };
        let loaded = store.load()?;
        *store.inner.state.write().unwrap_or_else(PoisonError::into_inner) = loaded;
        Ok(store)
    }

    /// Reads `f` from the current state.
    pub fn read<T>(&self, f: impl FnOnce(&State) -> T) -> T {
        f(&self.inner.state.read().unwrap_or_else(PoisonError::into_inner).state)
    }

    /// The index of the last entry applied, or 0.
    #[must_use]
    pub fn applied(&self) -> u64 {
        let a = self.inner.state.read().unwrap_or_else(PoisonError::into_inner);
        a.applied.map_or(0, |l| l.index)
    }

    fn load(&self) -> StoreResult<Applied> {
        let tx = self.inner.db.begin_read().map_err(read)?;
        let meta = tx.open_table(META).map_err(read)?;
        let mut a = Applied {
            applied: get(&meta, APPLIED)?,
            membership: get(&meta, MEMBERSHIP)?.unwrap_or_default(),
            ..Applied::default()
        };
        a.state.root = get(&meta, ROOT)?;
        for row in tx.open_table(PROJECTS).map_err(read)?.iter().map_err(read)? {
            let (k, v) = row.map_err(read)?;
            a.state.projects.insert(k.value().to_owned(), decode(v.value())?);
        }
        for row in tx.open_table(KEYS).map_err(read)?.iter().map_err(read)? {
            let (k, v) = row.map_err(read)?;
            let hash: [u8; 32] =
                k.value().try_into().map_err(|_| bad("a key hash is not 32 bytes"))?;
            a.state.keys.insert(hash, decode(v.value())?);
        }
        for row in tx.open_table(NODES).map_err(read)?.iter().map_err(read)? {
            let (k, v) = row.map_err(read)?;
            a.state.nodes.insert(k.value(), decode(v.value())?);
        }
        for row in tx.open_table(AUDIT_HOURS).map_err(read)?.iter().map_err(read)? {
            let (k, v) = row.map_err(read)?;
            let (node, hour) = k.value();
            let chain = a.state.audit.entry(node.to_owned()).or_default();
            chain.hours.insert(hour.to_owned(), decode(v.value())?);
        }
        for row in tx.open_table(AUDIT_TIPS).map_err(read)?.iter().map_err(read)? {
            let (k, v) = row.map_err(read)?;
            a.state.audit.entry(k.value().to_owned()).or_default().tip = Some(decode(v.value())?);
        }
        Ok(a)
    }

    fn meta<T: DeserializeOwned>(&self, key: &str) -> StoreResult<Option<T>> {
        let tx = self.inner.db.begin_read().map_err(read)?;
        get(&tx.open_table(META).map_err(read)?, key)
    }

    fn put_meta<T: Serialize>(&self, key: &str, value: &T) -> StoreResult<()> {
        let tx = self.inner.db.begin_write().map_err(write)?;
        tx.open_table(META)
            .map_err(write)?
            .insert(key, encode(value)?.as_slice())
            .map_err(write)?;
        tx.commit().map_err(write)
    }

    fn image(&self) -> StoreResult<Vec<u8>> {
        let a = self.inner.state.read().unwrap_or_else(PoisonError::into_inner);
        let image = Image {
            applied: a.applied,
            membership: a.membership.clone(),
            projects: a.state.projects.values().cloned().collect(),
            keys: a.state.keys.iter().map(|(h, k)| (*h, k.clone())).collect(),
            nodes: a.state.nodes.values().cloned().collect(),
            root: a.state.root,
            audit: a.state.audit.iter().map(|(n, c)| (n.clone(), c.clone())).collect(),
        };
        encode(&image)
    }
}

impl RaftLogReader<TypeConfig> for Store {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + fmt::Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> StoreResult<Vec<Entry<TypeConfig>>> {
        let tx = self.inner.db.begin_read().map_err(read_logs)?;
        let logs = tx.open_table(LOGS).map_err(read_logs)?;
        let bounds = (range.start_bound().cloned(), range.end_bound().cloned());
        let mut out = Vec::new();
        for row in logs.range::<u64>(bounds).map_err(read_logs)? {
            let (_, v) = row.map_err(read_logs)?;
            out.push(serde_json::from_slice(v.value()).map_err(read_logs)?);
        }
        Ok(out)
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Store {
    async fn build_snapshot(&mut self) -> StoreResult<Snapshot<TypeConfig>> {
        let data = self.image()?;
        let (applied, membership) = {
            let a = self.inner.state.read().unwrap_or_else(PoisonError::into_inner);
            (a.applied, a.membership.clone())
        };
        let id =
            applied.map_or_else(|| "empty".to_owned(), |l| format!("{}-{}", l.leader_id, l.index));
        let meta =
            SnapshotMeta { last_log_id: applied, last_membership: membership, snapshot_id: id };
        self.put_meta(SNAPSHOT, &Kept { meta: meta.clone(), data: data.clone() })?;
        Ok(Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStorage<TypeConfig> for Store {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn get_log_state(&mut self) -> StoreResult<LogState<TypeConfig>> {
        let purged: Option<LogId<u64>> = self.meta(PURGED)?;
        let tx = self.inner.db.begin_read().map_err(read_logs)?;
        let logs = tx.open_table(LOGS).map_err(read_logs)?;
        let last = match logs.last().map_err(read_logs)? {
            Some((_, v)) => {
                let e: Entry<TypeConfig> = serde_json::from_slice(v.value()).map_err(read_logs)?;
                Some(e.log_id)
            }
            None => purged,
        };
        Ok(LogState { last_purged_log_id: purged, last_log_id: last })
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> StoreResult<()> {
        self.put_meta(VOTE, vote)
    }

    async fn read_vote(&mut self) -> StoreResult<Option<Vote<u64>>> {
        self.meta(VOTE)
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> StoreResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
    {
        let rows = entries
            .into_iter()
            .map(|e| Ok((e.log_id.index, serde_json::to_vec(&e).map_err(write_logs)?)))
            .collect::<StoreResult<Vec<_>>>()?;
        let inner = Arc::clone(&self.inner);
        blocking(move || {
            let tx = inner.db.begin_write().map_err(write_logs)?;
            {
                let mut logs = tx.open_table(LOGS).map_err(write_logs)?;
                for (index, bytes) in &rows {
                    logs.insert(*index, bytes.as_slice()).map_err(write_logs)?;
                }
            }
            tx.commit().map_err(write_logs)
        })
        .await
    }

    async fn delete_conflict_logs_since(&mut self, log_id: LogId<u64>) -> StoreResult<()> {
        let tx = self.inner.db.begin_write().map_err(write_logs)?;
        tx.open_table(LOGS)
            .map_err(write_logs)?
            .retain_in(log_id.index.., |_, _| false)
            .map_err(write_logs)?;
        tx.commit().map_err(write_logs)
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> StoreResult<()> {
        let tx = self.inner.db.begin_write().map_err(write_logs)?;
        tx.open_table(META)
            .map_err(write_logs)?
            .insert(PURGED, encode(&log_id)?.as_slice())
            .map_err(write_logs)?;
        tx.open_table(LOGS)
            .map_err(write_logs)?
            .retain_in(..=log_id.index, |_, _| false)
            .map_err(write_logs)?;
        tx.commit().map_err(write_logs)
    }

    async fn last_applied_state(
        &mut self,
    ) -> StoreResult<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>)> {
        let a = self.inner.state.read().unwrap_or_else(PoisonError::into_inner);
        Ok((a.applied, a.membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> StoreResult<Vec<Reply>> {
        let entries = entries.to_vec();
        let inner = Arc::clone(&self.inner);
        blocking(move || inner.apply(entries)).await
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StoreResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StoreResult<()> {
        let data = snapshot.into_inner();
        let image: Image = serde_json::from_slice(&data)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut a = self.inner.state.write().unwrap_or_else(PoisonError::into_inner);
        let tx = self.inner.db.begin_write().map_err(write_sm)?;
        {
            let mut projects = tx.open_table(PROJECTS).map_err(write_sm)?;
            projects.retain(|_, _| false).map_err(write_sm)?;
            for p in &image.projects {
                projects.insert(p.name.as_str(), encode(p)?.as_slice()).map_err(write_sm)?;
            }
            let mut keys = tx.open_table(KEYS).map_err(write_sm)?;
            keys.retain(|_, _| false).map_err(write_sm)?;
            for (h, k) in &image.keys {
                keys.insert(h.as_slice(), encode(k)?.as_slice()).map_err(write_sm)?;
            }
            let mut nodes = tx.open_table(NODES).map_err(write_sm)?;
            nodes.retain(|_, _| false).map_err(write_sm)?;
            for n in &image.nodes {
                nodes.insert(n.node, encode(n)?.as_slice()).map_err(write_sm)?;
            }
            let mut hours = tx.open_table(AUDIT_HOURS).map_err(write_sm)?;
            hours.retain(|_, _| false).map_err(write_sm)?;
            let mut tips = tx.open_table(AUDIT_TIPS).map_err(write_sm)?;
            tips.retain(|_, _| false).map_err(write_sm)?;
            for (node, chain) in &image.audit {
                for (hour, h) in &chain.hours {
                    hours
                        .insert((node.as_str(), hour.as_str()), encode(h)?.as_slice())
                        .map_err(write_sm)?;
                }
                if let Some(t) = &chain.tip {
                    tips.insert(node.as_str(), encode(t)?.as_slice()).map_err(write_sm)?;
                }
            }
            let mut m = tx.open_table(META).map_err(write_sm)?;
            m.insert(APPLIED, encode(&meta.last_log_id)?.as_slice()).map_err(write_sm)?;
            m.insert(MEMBERSHIP, encode(&meta.last_membership)?.as_slice()).map_err(write_sm)?;
            match &image.root {
                Some(r) => m.insert(ROOT, encode(r)?.as_slice()).map(drop).map_err(write_sm)?,
                None => m.remove(ROOT).map(drop).map_err(write_sm)?,
            }
            let kept = Kept { meta: meta.clone(), data: data.clone() };
            m.insert(SNAPSHOT, encode(&kept)?.as_slice()).map_err(write_sm)?;
        }
        tx.commit().map_err(write_sm)?;
        a.applied = meta.last_log_id;
        a.membership = meta.last_membership.clone();
        a.state = State {
            projects: image.projects.into_iter().map(|p| (p.name.clone(), p)).collect(),
            keys: image.keys.into_iter().collect(),
            nodes: image.nodes.into_iter().map(|n| (n.node, n)).collect(),
            root: image.root,
            audit: image.audit.into_iter().collect(),
        };
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> StoreResult<Option<Snapshot<TypeConfig>>> {
        let kept: Option<Kept> = self.meta(SNAPSHOT)?;
        Ok(kept.map(|k| Snapshot { meta: k.meta, snapshot: Box::new(Cursor::new(k.data)) }))
    }
}

impl Inner {
    /// Applies `entries` to the state in memory, then writes the rows they changed.
    ///
    /// The commit does not wait for the disk. Every entry is in the log already, which is
    /// written durably, so after a crash openraft applies again whatever these rows lost. The
    /// next durable commit, of the log or a snapshot, takes these rows to disk with it.
    fn apply(&self, entries: Vec<Entry<TypeConfig>>) -> StoreResult<Vec<Reply>> {
        let mut replies = Vec::with_capacity(entries.len());
        let mut a = self.state.write().unwrap_or_else(PoisonError::into_inner);
        let mut changed = Vec::new();
        let mut membership = false;
        for e in entries {
            a.applied = Some(e.log_id);
            replies.push(match e.payload {
                EntryPayload::Blank => Reply::None,
                EntryPayload::Normal(cmd) => a.state.apply_all(cmd, &mut changed),
                EntryPayload::Membership(m) => {
                    a.membership = StoredMembership::new(Some(e.log_id), m);
                    membership = true;
                    Reply::None
                }
            });
        }
        let mut tx = self.db.begin_write().map_err(write_sm)?;
        tx.set_durability(Durability::None);
        {
            let mut meta = tx.open_table(META).map_err(write_sm)?;
            let mut projects = tx.open_table(PROJECTS).map_err(write_sm)?;
            let mut keys = tx.open_table(KEYS).map_err(write_sm)?;
            let mut nodes = tx.open_table(NODES).map_err(write_sm)?;
            let mut hours = tx.open_table(AUDIT_HOURS).map_err(write_sm)?;
            let mut tips = tx.open_table(AUDIT_TIPS).map_err(write_sm)?;
            for c in changed {
                match c {
                    Changed::Project(name) => {
                        let p = encode(&a.state.projects[&name])?;
                        projects.insert(name.as_str(), p.as_slice()).map_err(write_sm)?;
                    }
                    Changed::Key(hash) => {
                        let k = encode(&a.state.keys[&hash])?;
                        keys.insert(hash.as_slice(), k.as_slice()).map_err(write_sm)?;
                    }
                    Changed::Root => {
                        if let Some(r) = &a.state.root {
                            meta.insert(ROOT, encode(r)?.as_slice()).map_err(write_sm)?;
                        }
                    }
                    Changed::Node(n) => {
                        nodes
                            .insert(n, encode(&a.state.nodes[&n])?.as_slice())
                            .map_err(write_sm)?;
                    }
                    Changed::Audit { node, added, gone, tip } => {
                        let chain = &a.state.audit[&node];
                        // A batch can add an hour and a later command in it drop it again.
                        for hour in &added {
                            if let Some(h) = chain.hours.get(hour) {
                                let row = encode(h)?;
                                hours
                                    .insert((node.as_str(), hour.as_str()), row.as_slice())
                                    .map_err(write_sm)?;
                            }
                        }
                        for hour in &gone {
                            hours.remove((node.as_str(), hour.as_str())).map_err(write_sm)?;
                        }
                        if let (true, Some(t)) = (tip, &chain.tip) {
                            tips.insert(node.as_str(), encode(t)?.as_slice()).map_err(write_sm)?;
                        }
                    }
                }
            }
            if membership {
                meta.insert(MEMBERSHIP, encode(&a.membership)?.as_slice()).map_err(write_sm)?;
            }
            meta.insert(APPLIED, encode(&a.applied)?.as_slice()).map_err(write_sm)?;
        }
        tx.commit().map_err(write_sm)?;
        Ok(replies)
    }
}

/// Runs `f` on tokio's blocking threads, so a commit waiting on the disk does not hold up the
/// heartbeats and the calls sharing a worker thread with it.
async fn blocking<T, F>(f: F) -> StoreResult<T>
where
    T: Send + 'static,
    F: FnOnce() -> StoreResult<T> + Send + 'static,
{
    tokio::task::spawn_blocking(f).await.map_err(write)?
}

fn get<T: DeserializeOwned>(
    t: &impl ReadableTable<&'static str, &'static [u8]>,
    key: &str,
) -> StoreResult<Option<T>> {
    t.get(key).map_err(read)?.map(|v| decode(v.value())).transpose()
}

fn encode<T: Serialize>(v: &T) -> StoreResult<Vec<u8>> {
    serde_json::to_vec(v).map_err(write)
}

fn decode<T: DeserializeOwned>(b: &[u8]) -> StoreResult<T> {
    serde_json::from_slice(b).map_err(read)
}

fn bad(what: &str) -> StorageError<u64> {
    read(std::io::Error::new(std::io::ErrorKind::InvalidData, what))
}

fn read<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
    StorageIOError::read(&e).into()
}

fn write<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
    StorageIOError::write(&e).into()
}

fn read_logs<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
    StorageIOError::read_logs(&e).into()
}

fn write_logs<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
    StorageIOError::write_logs(&e).into()
}

fn write_sm<E: std::error::Error + 'static>(e: E) -> StorageError<u64> {
    StorageIOError::write_state_machine(&e).into()
}

#[cfg(test)]
mod tests {
    use openraft::storage::Adaptor;
    use openraft::testing::{StoreBuilder, Suite};

    use super::*;

    struct Builder;

    type Half = Adaptor<TypeConfig, Store>;

    impl StoreBuilder<TypeConfig, Half, Half, Scratch> for Builder {
        async fn build(&self) -> StoreResult<(Scratch, Half, Half)> {
            let dir = Scratch::new();
            let store = Store::open(&dir.0.join("keeper.redb")).await?;
            let (log, sm) = Adaptor::new(store);
            Ok((dir, log, sm))
        }
    }

    /// A directory removed when dropped.
    struct Scratch(std::path::PathBuf);

    impl Scratch {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static N: AtomicU64 = AtomicU64::new(0);
            let n = N.fetch_add(1, Ordering::Relaxed);
            let p = std::env::temp_dir().join(format!("hive-keeper-{}-{n}", std::process::id()));
            let _ = std::fs::remove_dir_all(&p);
            std::fs::create_dir_all(&p).unwrap();
            Self(p)
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    fn the_store_passes_the_openraft_suite() {
        Suite::test_all(Builder).unwrap();
    }

    #[tokio::test]
    async fn the_state_comes_back_after_a_reopen() {
        let dir = Scratch::new();
        let path = dir.0.join("keeper.redb");
        let mut store = Store::open(&path).await.unwrap();
        let cmds = [
            Command::CreateProject { name: "swe".into(), quota: Default::default(), now_ms: 1 },
            Command::AddKey {
                hash: [7; 32],
                prefix: "hb_7777".into(),
                project: "swe".into(),
                now_ms: 2,
            },
            Command::Register {
                name: "box-1".into(),
                addr: "http://10.0.0.1:7420".into(),
                epoch: 0,
                now_ms: 3,
                ttl_ms: 30_000,
                wait: true,
            },
        ];
        let entries: Vec<Entry<TypeConfig>> = cmds
            .into_iter()
            .enumerate()
            .map(|(i, c)| Entry {
                log_id: openraft::testing::log_id(1, 1, i as u64 + 1),
                payload: EntryPayload::Normal(c),
            })
            .collect();
        let replies = store.apply_to_state_machine(&entries).await.unwrap();
        assert!(replies.iter().all(|r| !matches!(r, Reply::Refused(_))), "{replies:?}");
        let before = store.read(Clone::clone);
        drop(store);

        let store = Store::open(&path).await.unwrap();
        assert_eq!(store.read(Clone::clone), before);
        assert_eq!(store.applied(), 3);
        assert_eq!(store.read(|s| s.project_of(&[7; 32]).map(str::to_owned)), Some("swe".into()));
    }
}
