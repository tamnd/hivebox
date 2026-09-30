//! Node reports to and from `hivebox.internal.v1`.

use std::sync::Arc;

use hive_proto::internal as pb;
use hive_waggle::{BackendSet, LayerBloom};

use crate::NodeReport;

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
