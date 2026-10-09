//! What waggle knows about the cluster: one [`NodeView`] per comb, built by scout from the reports
//! the combs push.

use std::sync::Arc;

use hive_types::Backend;

/// The cluster as scout last saw it.
#[derive(Debug, Clone, Default)]
pub struct ClusterView {
    /// The nodes, in any order.
    pub nodes: Vec<NodeView>,
}

impl ClusterView {
    /// How much of the admittable memory of the healthy on-prem nodes is committed, from 0 to 1.
    /// Below [`crate::PACK_BELOW`] placement packs cells for image locality, above it spreads
    /// them. Cloud nodes are left out, since they are mostly empty and would hide how full the
    /// rest is.
    #[must_use]
    pub fn utilization(&self) -> f64 {
        let (mut used, mut total) = (0u64, 0u64);
        for n in self.nodes.iter().filter(|n| n.healthy && !n.cloud) {
            used += n.mem_committed_mib;
            total += n.mem_admit_mib;
        }
        if total == 0 { 1.0 } else { (used as f64 / total as f64).min(1.0) }
    }
}

/// One comb, from its last report.
#[derive(Debug, Clone)]
pub struct NodeView {
    /// The comb's registered index, the `node` in every cell id it makes.
    pub node: u16,
    /// The comb's registration epoch, or 0 when it is not known. A cell id from an older epoch
    /// names a cell the comb no longer has.
    pub epoch: u16,
    /// Counts the comb's reports. An in-flight entry for this node is dropped once a report newer
    /// than the one it was placed against arrives, since that report counts the cells.
    pub report: u64,
    /// Whether the comb is taking creates.
    pub healthy: bool,
    /// Whether the node is a cloud VM that takes cells only past [`crate::BURST_ABOVE`], and
    /// only of the images it has staged.
    pub cloud: bool,
    /// The backends it can run.
    pub backends: BackendSet,
    /// CPU on the node, in thousandths of a core.
    pub cpu_milli: u64,
    /// CPU the node's cells were given, in thousandths of a core.
    pub cpu_committed_milli: u64,
    /// The memory the comb admits cells up to, its RAM times its overcommit, in MiB.
    pub mem_admit_mib: u64,
    /// Memory its cells were given, in MiB.
    pub mem_committed_mib: u64,
    /// Cells on the node now.
    pub cells: u32,
    /// Of those, the ones paused or with nothing using them lately, which the rebalancer may
    /// move.
    pub idle_cells: u32,
    /// Memory the idle cells were given, in MiB.
    pub idle_mem_mib: u64,
    /// Most cells the node takes.
    pub max_cells: u32,
    /// Cgroups and network namespaces ready in the comb's pools.
    pub pool_depth: u32,
    /// Creates a second the comb has been doing lately.
    pub create_rate: f64,
    /// Most creates the comb takes at once, its `create_concurrency`, so one big batch spreads
    /// over many nodes.
    pub burst_cap: u32,
    /// The image layers in the node's local cache, and on a cloud node the images it has staged.
    pub layers: LayerBloom,
    /// Cells of the projects with the most cells on the node, as (project, cells).
    pub top_projects: Vec<(u64, u32)>,
}

impl NodeView {
    /// A healthy node with nothing on it, for tests and simulations.
    #[must_use]
    pub fn empty(node: u16, cpu_milli: u64, mem_mib: u64) -> Self {
        Self {
            node,
            epoch: 0,
            report: 0,
            healthy: true,
            cloud: false,
            backends: BackendSet::of(&[Backend::Container, Backend::Microvm]),
            cpu_milli,
            cpu_committed_milli: 0,
            mem_admit_mib: mem_mib,
            mem_committed_mib: 0,
            cells: 0,
            idle_cells: 0,
            idle_mem_mib: 0,
            max_cells: 4096,
            pool_depth: 64,
            create_rate: 0.0,
            burst_cap: 300,
            layers: LayerBloom::default(),
            top_projects: Vec::new(),
        }
    }

    pub(crate) fn project_cells(&self, project: u64) -> u32 {
        self.top_projects.iter().find(|(p, _)| *p == project).map_or(0, |(_, n)| *n)
    }
}

/// A set of backends, as bits.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendSet(u8);

impl BackendSet {
    /// The set of `backends`.
    #[must_use]
    pub fn of(backends: &[Backend]) -> Self {
        Self(backends.iter().fold(0, |bits, b| bits | Self::bit(*b)))
    }

    /// Whether `backend` is in the set. `Auto` is in it when a container or a microVM is.
    #[must_use]
    pub fn has(self, backend: Backend) -> bool {
        self.0 & Self::bit(backend) != 0
    }

    /// The set as the bits a node report carries: 1 fncall, 2 container, 4 microVM, 8 full VM.
    #[must_use]
    pub fn bits(self) -> u8 {
        self.0
    }

    /// The set from a node report's bits. Bits it does not know are dropped.
    #[must_use]
    pub fn from_bits(bits: u8) -> Self {
        Self(bits & 0b1111)
    }

    fn bit(backend: Backend) -> u8 {
        match backend {
            Backend::Fncall => 1,
            Backend::Container => 2,
            Backend::Microvm => 4,
            Backend::Fullvm => 8,
            Backend::Auto => 2 | 4,
        }
    }
}

/// Which image layers a node has cached, as a 4 KiB bloom filter over their digests. A layer the
/// filter says is there almost always is, and one it says is not never is.
///
/// Clones share the bits until one of them inserts, so scout can hand out a view of a thousand
/// nodes without copying 4 MiB.
#[derive(Clone)]
pub struct LayerBloom(Arc<[u64; WORDS]>);

impl PartialEq for LayerBloom {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0) || self.0 == other.0
    }
}

impl Eq for LayerBloom {}

const WORDS: usize = 512;
const BITS: u32 = (WORDS * 64) as u32;

impl Default for LayerBloom {
    fn default() -> Self {
        Self(Arc::new([0; WORDS]))
    }
}

impl std::fmt::Debug for LayerBloom {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let set: u32 = self.0.iter().map(|w| w.count_ones()).sum();
        write!(f, "LayerBloom({set} of {BITS} bits set)")
    }
}

impl LayerBloom {
    /// Adds a layer by its digest.
    pub fn insert(&mut self, digest: &[u8; 32]) {
        let words = Arc::make_mut(&mut self.0);
        for bit in Self::bits(digest) {
            words[(bit / 64) as usize] |= 1 << (bit % 64);
        }
    }

    /// Whether the layer is probably cached.
    #[must_use]
    pub fn contains(&self, digest: &[u8; 32]) -> bool {
        Self::bits(digest).iter().all(|&bit| self.0[(bit / 64) as usize] & (1 << (bit % 64)) != 0)
    }

    /// Whether the node probably has the image named `name` staged.
    #[must_use]
    pub fn has_image(&self, name: &str) -> bool {
        self.contains(&image_digest(name))
    }

    /// The filter as the 4096 bytes a node report carries.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.iter().flat_map(|w| w.to_le_bytes()).collect()
    }

    /// The filter from a node report's bytes, or `None` when there are not 4096 of them.
    #[must_use]
    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        if bytes.len() != WORDS * 8 {
            return None;
        }
        let mut words = [0u64; WORDS];
        for (w, chunk) in words.iter_mut().zip(bytes.as_chunks::<8>().0) {
            *w = u64::from_le_bytes(*chunk);
        }
        Some(Self(Arc::new(words)))
    }

    /// Three bit positions from the digest, which is already a uniform hash.
    fn bits(digest: &[u8; 32]) -> [u32; 3] {
        let at =
            |i: usize| u32::from_le_bytes([digest[i], digest[i + 1], digest[i + 2], digest[i + 3]]);
        [at(0) % BITS, at(4) % BITS, at(8) % BITS]
    }
}

/// What a node puts in its layer filter for an image named `name` it has staged, so it never
/// looks like the digest of a layer.
#[must_use]
pub fn image_digest(name: &str) -> [u8; 32] {
    let mut h = blake3::Hasher::new();
    h.update(b"hivebox image name\0");
    h.update(name.as_bytes());
    *h.finalize().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_bloom_filter_finds_what_went_in_and_survives_bytes() {
        let mut b = LayerBloom::default();
        let a = *blake3::hash(b"a").as_bytes();
        let c = *blake3::hash(b"c").as_bytes();
        b.insert(&a);
        assert!(b.contains(&a));
        assert!(!b.contains(&c));
        let back = LayerBloom::from_bytes(&b.to_bytes()).unwrap();
        assert!(back.contains(&a) && !back.contains(&c));
        assert!(LayerBloom::from_bytes(&[0; 10]).is_none());
    }

    #[test]
    fn auto_runs_where_a_container_or_a_microvm_does() {
        let s = BackendSet::of(&[Backend::Container]);
        assert!(s.has(Backend::Container) && s.has(Backend::Auto));
        assert!(!s.has(Backend::Microvm) && !s.has(Backend::Fullvm));
    }
}
