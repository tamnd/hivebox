//! Node reports and cluster deltas to and from `hivebox.internal.v1`.

use std::sync::Arc;

use hive_proto::internal as pb;
use hive_waggle::{BackendSet, LayerBloom, NodeView};

use crate::{NodeReport, Snapshot};

/// Why a report off the wire was refused.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BadReport(pub String);

impl std::fmt::Display for BadReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for BadReport {}

impl TryFrom<pb::NodeReport> for NodeReport {
    type Error = BadReport;

    fn try_from(r: pb::NodeReport) -> Result<Self, BadReport> {
        let node = u16::try_from(r.node)
            .map_err(|_| BadReport(format!("node {} is past 65535", r.node)))?;
        let epoch = u16::try_from(r.epoch)
            .map_err(|_| BadReport(format!("epoch {} is past 65535", r.epoch)))?;
        let layers = if r.layers.is_empty() {
            None
        } else {
            Some(LayerBloom::from_bytes(&r.layers).ok_or_else(|| {
                BadReport(format!("the layer filter is {} bytes, not 4096", r.layers.len()))
            })?)
        };
        Ok(Self {
            node,
            epoch,
            seq: r.seq,
            addr: Arc::from(r.addr),
            healthy: r.healthy,
            backends: BackendSet::from_bits(u8::try_from(r.backends & 0xff).unwrap_or(0)),
            cpu_milli: r.cpu_milli,
            cpu_committed_milli: r.cpu_committed_milli,
            mem_admit_mib: r.mem_admit_mib,
            mem_committed_mib: r.mem_committed_mib,
            cells: r.cells,
            max_cells: r.max_cells,
            pool_depth: r.pool_depth,
            create_rate: if r.create_rate.is_finite() { r.create_rate.max(0.0) } else { 0.0 },
            burst_cap: r.burst_cap,
            layers,
            top_projects: r.top_projects.into_iter().map(|p| (p.project, p.cells)).collect(),
        })
    }
}

impl From<&NodeReport> for pb::NodeReport {
    fn from(r: &NodeReport) -> Self {
        Self {
            node: u32::from(r.node),
            epoch: u32::from(r.epoch),
            seq: r.seq,
            addr: r.addr.to_string(),
            healthy: r.healthy,
            backends: u32::from(r.backends.bits()),
            cpu_milli: r.cpu_milli,
            cpu_committed_milli: r.cpu_committed_milli,
            mem_admit_mib: r.mem_admit_mib,
            mem_committed_mib: r.mem_committed_mib,
            cells: r.cells,
            max_cells: r.max_cells,
            pool_depth: r.pool_depth,
            create_rate: r.create_rate,
            burst_cap: r.burst_cap,
            layers: r.layers.as_ref().map(LayerBloom::to_bytes).unwrap_or_default(),
            top_projects: r
                .top_projects
                .iter()
                .map(|&(project, cells)| pb::ProjectCells { project, cells })
                .collect(),
        }
    }
}

/// What a reader holding `old` needs to have `new`: the nodes that changed since, with the
/// layer filter only where it changed too, and the nodes that went away. With no `old` it is the
/// whole cluster.
pub(crate) fn delta(old: Option<&Snapshot>, new: &Snapshot) -> pb::ClusterDelta {
    // Versions only go up within a scout, so an old snapshot that is not older is no base.
    let old = old.filter(|o| o.version < new.version);
    let since = old.map_or(0, |o| o.version);
    let full = old.is_none();
    let mut nodes = Vec::new();
    for ((view, (_, addr)), mark) in new.view.nodes.iter().zip(&new.addrs).zip(&new.marks) {
        if full || mark.view > since {
            nodes.push(node_state(view, addr, full || mark.layers > since));
        }
    }
    let mut removed = Vec::new();
    if let Some(old) = old {
        let mut have = new.addrs.iter().map(|(n, _)| *n).peekable();
        for &(n, _) in &old.addrs {
            while have.next_if(|&h| h < n).is_some() {}
            if have.peek() != Some(&n) {
                removed.push(u32::from(n));
            }
        }
    }
    pb::ClusterDelta { version: new.version, full, nodes, removed }
}

fn node_state(v: &NodeView, addr: &str, layers: bool) -> pb::NodeState {
    pb::NodeState {
        node: u32::from(v.node),
        report: v.report,
        addr: addr.to_owned(),
        healthy: v.healthy,
        backends: u32::from(v.backends.bits()),
        cpu_milli: v.cpu_milli,
        cpu_committed_milli: v.cpu_committed_milli,
        mem_admit_mib: v.mem_admit_mib,
        mem_committed_mib: v.mem_committed_mib,
        cells: v.cells,
        max_cells: v.max_cells,
        pool_depth: v.pool_depth,
        create_rate: v.create_rate,
        burst_cap: v.burst_cap,
        layers: if layers { v.layers.to_bytes() } else { Vec::new() },
        top_projects: v
            .top_projects
            .iter()
            .map(|&(project, cells)| pb::ProjectCells { project, cells })
            .collect(),
    }
}

/// A node off the wire, with its address and its layer filter when the message carried one.
pub(crate) fn from_node_state(
    s: pb::NodeState,
) -> Result<(NodeView, Arc<str>, Option<LayerBloom>), BadReport> {
    let node =
        u16::try_from(s.node).map_err(|_| BadReport(format!("node {} is past 65535", s.node)))?;
    let layers = if s.layers.is_empty() {
        None
    } else {
        Some(LayerBloom::from_bytes(&s.layers).ok_or_else(|| {
            BadReport(format!("the layer filter is {} bytes, not 4096", s.layers.len()))
        })?)
    };
    let view = NodeView {
        node,
        report: s.report,
        healthy: s.healthy,
        backends: BackendSet::from_bits(u8::try_from(s.backends & 0xff).unwrap_or(0)),
        cpu_milli: s.cpu_milli,
        cpu_committed_milli: s.cpu_committed_milli,
        mem_admit_mib: s.mem_admit_mib,
        mem_committed_mib: s.mem_committed_mib,
        cells: s.cells,
        max_cells: s.max_cells,
        pool_depth: s.pool_depth,
        create_rate: if s.create_rate.is_finite() { s.create_rate.max(0.0) } else { 0.0 },
        burst_cap: s.burst_cap,
        layers: LayerBloom::default(),
        top_projects: s.top_projects.into_iter().map(|p| (p.project, p.cells)).collect(),
    };
    Ok((view, Arc::from(s.addr), layers))
}
