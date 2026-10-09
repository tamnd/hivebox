//! Following scout from another process. A gate or a placer keeps a [`Mirror`] of scout's
//! snapshot, built from the whole cluster once and then from the nodes that changed.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use hive_proto::internal as pb;
use hive_proto::internal::scout_client::ScoutClient;
use hive_waggle::NodeView;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;

use crate::wire::{BadReport, from_node_state};
use crate::{Mark, Snapshot};

/// The longest wait between tries to reach scout.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// Scout's snapshot, rebuilt from what `Watch` streams.
#[derive(Debug, Default)]
pub struct Mirror {
    version: u64,
    nodes: BTreeMap<u16, (NodeView, Arc<str>)>,
}

impl Mirror {
    /// A mirror of an empty cluster.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Brings the mirror up to `delta` and returns the snapshot it now holds.
    ///
    /// # Errors
    ///
    /// When the delta is malformed, older than what the mirror has, or names a node the mirror
    /// has never seen without sending its layer filter. The mirror is left as it was, and the
    /// caller should start over from a full delta.
    pub fn apply(&mut self, delta: pb::ClusterDelta) -> Result<Arc<Snapshot>, BadReport> {
        if !delta.full && delta.version <= self.version {
            return Err(BadReport(format!(
                "delta to version {} after version {}",
                delta.version, self.version
            )));
        }
        let mut removed = Vec::with_capacity(delta.removed.len());
        for n in delta.removed {
            removed
                .push(u16::try_from(n).map_err(|_| BadReport(format!("node {n} is past 65535")))?);
        }
        let mut changed = Vec::with_capacity(delta.nodes.len());
        for state in delta.nodes {
            let (mut view, addr, layers) = from_node_state(state)?;
            view.layers = match layers {
                Some(l) => l,
                None => self
                    .nodes
                    .get(&view.node)
                    .filter(|_| !delta.full)
                    .map(|(v, _)| v.layers.clone())
                    .ok_or_else(|| {
                        BadReport(format!("node {} came without its layer filter", view.node))
                    })?,
            };
            changed.push((view, addr));
        }
        if delta.full {
            self.nodes.clear();
        }
        for n in removed {
            self.nodes.remove(&n);
        }
        for (view, addr) in changed {
            self.nodes.insert(view.node, (view, addr));
        }
        self.version = delta.version;
        Ok(Arc::new(self.snapshot()))
    }

    /// The snapshot the mirror holds.
    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        Snapshot::assemble(
            self.version,
            self.nodes.values().map(|(v, a)| (v.clone(), a.clone(), Mark::default())),
        )
    }
}

/// Follows the scout at `endpoint`, for example `http://10.0.0.5:7410`, until `stop`. The
/// receiver holds an empty snapshot of version 0 until the first message arrives, and then
/// always the latest. When the stream breaks it keeps the last snapshot while the follower
/// reconnects, waiting a little longer each time up to 5 s, and logs only the first failure in a
/// row. Must be called within a tokio runtime.
#[must_use]
pub fn follow(endpoint: String, stop: CancellationToken) -> watch::Receiver<Arc<Snapshot>> {
    const FIRST: Duration = Duration::from_millis(200);
    let (tx, rx) = watch::channel(Arc::new(Snapshot::default()));
    tokio::spawn(async move {
        let (mut backoff, mut quiet) = (FIRST, false);
        loop {
            let started = Instant::now();
            let e = tokio::select! {
                () = stop.cancelled() => return,
                e = watch_once(&endpoint, &tx) => e,
            };
            if started.elapsed() > MAX_BACKOFF {
                (backoff, quiet) = (FIRST, false);
            }
            if !quiet {
                eprintln!("hive-scout: following {endpoint}: {e}");
                quiet = true;
            }
            tokio::select! {
                () = stop.cancelled() => return,
                () = tokio::time::sleep(backoff) => {}
            }
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    });
    rx
}

/// One stream, until it breaks. Returns why.
async fn watch_once(endpoint: &str, tx: &watch::Sender<Arc<Snapshot>>) -> String {
    let channel = match tonic::transport::Endpoint::from_shared(endpoint.to_owned()) {
        Ok(e) => e.connect_timeout(MAX_BACKOFF).tcp_nodelay(true).connect().await,
        Err(e) => return e.to_string(),
    };
    let mut client = match channel {
        // The first message holds every node with its 4 KiB filter, so about 4 MB for 1,000
        // nodes, which is past tonic's default limit.
        Ok(c) => ScoutClient::new(c).max_decoding_message_size(usize::MAX),
        Err(e) => return e.to_string(),
    };
    let mut stream = match client.watch(pb::WatchRequest {}).await {
        Ok(r) => r.into_inner(),
        Err(e) => return e.message().to_owned(),
    };
    let mut mirror = Mirror::new();
    loop {
        match stream.message().await {
            Ok(Some(delta)) => match mirror.apply(delta) {
                Ok(snap) => {
                    tx.send_replace(snap);
                }
                Err(e) => return e.to_string(),
            },
            Ok(None) => return "scout closed the stream".into(),
            Err(e) => return e.message().to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use hive_types::Backend;
    use hive_waggle::{BackendSet, LayerBloom};

    use super::*;
    use crate::wire::delta;
    use crate::{FORGET_AFTER, NodeReport, STALE_AFTER, Scout};

    fn report(node: u16, seq: u64, cells: u32) -> NodeReport {
        NodeReport {
            node,
            epoch: 1,
            seq,
            addr: Arc::from(format!("10.0.0.{node}:7400")),
            healthy: true,
            cloud: false,
            backends: BackendSet::of(&[Backend::Container]),
            cpu_milli: 64_000,
            cpu_committed_milli: 0,
            mem_admit_mib: 100_000,
            mem_committed_mib: u64::from(cells) * 512,
            cells,
            max_cells: 1000,
            pool_depth: 64,
            create_rate: 0.5,
            burst_cap: 300,
            layers: None,
            top_projects: vec![(7, cells)],
        }
    }

    fn bloom(name: &str) -> LayerBloom {
        let mut b = LayerBloom::default();
        b.insert(blake3::hash(name.as_bytes()).as_bytes());
        b
    }

    fn secs(s: u64) -> Duration {
        Duration::from_secs(s)
    }

    /// What a watcher can see of a snapshot, which is all of it but the marks.
    fn seen(s: &Snapshot) -> String {
        let mut projects: Vec<_> = s.projects.iter().collect();
        projects.sort();
        format!("{} {:?} {:?} {:?} {projects:?}", s.version, s.view.nodes, s.addrs, s.totals)
    }

    #[test]
    fn a_delta_holds_only_what_changed() {
        let mut scout = Scout::new();
        for node in 1..=3 {
            scout.apply(NodeReport { layers: Some(bloom("base")), ..report(node, 1, 4) }, secs(1));
        }
        let s1 = scout.tick(secs(1)).unwrap();
        let first = delta(None, &s1);
        assert!(first.full);
        assert_eq!(first.nodes.len(), 3);
        assert!(first.nodes.iter().all(|n| n.layers.len() == 4096));

        // Node 2 reports again with the same filter, as a comb does on a new stream.
        scout.apply(NodeReport { layers: Some(bloom("base")), ..report(2, 2, 5) }, secs(2));
        let s2 = scout.tick(secs(2)).unwrap();
        let d = delta(Some(&s1), &s2);
        assert!(!d.full);
        assert_eq!(d.nodes.iter().map(|n| (n.node, n.cells)).collect::<Vec<_>>(), vec![(2, 5)]);
        assert!(d.nodes[0].layers.is_empty(), "the filter did not change");
        assert!(d.removed.is_empty());

        scout.apply(NodeReport { layers: Some(bloom("python")), ..report(3, 2, 4) }, secs(3));
        let s3 = scout.tick(secs(3)).unwrap();
        let d = delta(Some(&s2), &s3);
        assert_eq!(d.nodes.len(), 1);
        assert_eq!((d.nodes[0].node, d.nodes[0].layers.len()), (3, 4096));
        // A watcher that missed s2 gets both changes at once.
        let d = delta(Some(&s1), &s3);
        assert_eq!(d.nodes.iter().map(|n| n.node).collect::<Vec<_>>(), vec![2, 3]);

        // Node 1 goes quiet: it is sent once as down, then as removed.
        scout.apply(report(2, 3, 5), secs(3) + STALE_AFTER);
        scout.apply(report(3, 3, 4), secs(3) + STALE_AFTER);
        let s4 = scout.tick(secs(3) + STALE_AFTER).unwrap();
        let d = delta(Some(&s3), &s4);
        let down: Vec<_> = d.nodes.iter().filter(|n| !n.healthy).map(|n| n.node).collect();
        assert_eq!(down, vec![1]);
        scout.apply(report(2, 4, 5), secs(1) + FORGET_AFTER);
        scout.apply(report(3, 4, 4), secs(1) + FORGET_AFTER);
        let s5 = scout.tick(secs(1) + FORGET_AFTER).unwrap();
        let d = delta(Some(&s4), &s5);
        assert_eq!(d.removed, vec![1]);
        assert_eq!(d.nodes.iter().map(|n| n.node).collect::<Vec<_>>(), vec![2, 3]);
    }

    #[test]
    fn a_mirror_fed_deltas_matches_scout() {
        let mut scout = Scout::new();
        let mut mirror = Mirror::new();
        let mut prev: Option<Arc<Snapshot>> = None;
        let mut skipped = 0;
        // A made up run: nodes join, report, change layers, restart, go quiet and are
        // forgotten, and the watcher misses some snapshots.
        let mut seq = [0u64; 40];
        for step in 0..400u64 {
            let now = secs(step);
            for i in 0..8 {
                let node = ((step * 7 + i * 13) % 40) as u16;
                if step > 200 && node.is_multiple_of(5) {
                    continue;
                }
                seq[usize::from(node)] += 1;
                let s = seq[usize::from(node)];
                let layers =
                    (s == 1 || s.is_multiple_of(17)).then(|| bloom(&format!("{node}/{}", s / 17)));
                let epoch = if step > 300 && node.is_multiple_of(7) { 2 } else { 1 };
                let cells = ((step + u64::from(node)) % 90) as u32;
                scout.apply(NodeReport { epoch, layers, ..report(node, s, cells) }, now);
            }
            let Some(snap) = scout.tick(now) else { continue };
            if step % 11 == 5 {
                skipped += 1;
                continue;
            }
            let got = mirror.apply(delta(prev.as_deref(), &snap)).unwrap();
            assert_eq!(seen(&got), seen(&snap), "at step {step}");
            prev = Some(snap);
        }
        assert!(skipped > 30);
        assert!(prev.unwrap().view.nodes.len() < 40, "some nodes were forgotten");
    }

    #[test]
    fn a_mirror_turns_away_what_it_cannot_follow() {
        let mut scout = Scout::new();
        scout.apply(NodeReport { layers: Some(bloom("base")), ..report(1, 1, 4) }, secs(1));
        let s1 = scout.tick(secs(1)).unwrap();
        scout.apply(report(1, 2, 6), secs(2));
        let s2 = scout.tick(secs(2)).unwrap();
        let mut mirror = Mirror::new();
        // A delta without the filter, to a mirror that never had it.
        let e = mirror.apply(delta(Some(&s1), &s2)).unwrap_err();
        assert!(e.0.contains("without its layer filter"), "{e}");
        mirror.apply(delta(None, &s2)).unwrap();
        // The same delta again is not news.
        let e = mirror.apply(delta(Some(&s1), &s2)).unwrap_err();
        assert!(e.0.contains("after version 2"), "{e}");
        assert_eq!(seen(&mirror.snapshot()), seen(&s2));
    }
}
