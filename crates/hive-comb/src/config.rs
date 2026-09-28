use hive_types::Backend;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

/// How one comb runs. The defaults are the ones in `spec/08_node_agent.md`, section 11.
#[derive(Clone, Debug)]
pub struct Config {
    /// Where the WAL and every cell's own directory live.
    pub data_dir: PathBuf,
    /// The unit this node belongs to, which goes into every cell id.
    pub unit: u8,
    /// This node's number in its unit.
    pub node: u16,
    /// The epoch this node runs in. Fixed in standalone mode, and handed out by the keeper once
    /// there is one.
    pub epoch: u16,
    /// Memory the cells may commit before overcommit, in MiB. `None` means the machine's total
    /// less [`Config::reserved_mem_mib`].
    pub mem_mib: Option<u64>,
    /// Memory kept back for the host, the comb and the page cache, in MiB.
    pub reserved_mem_mib: u64,
    /// Most cells alive at once, whatever their size.
    pub max_cells: usize,
    /// Most creates in flight per backend. More wait their turn.
    pub create_limit: BTreeMap<Backend, usize>,
    /// Longest a create may take, from admission to the guest agent's handshake.
    pub create_deadline: Duration,
    /// How long a stop waits for the workload to end on its own before killing it.
    pub stop_grace: Duration,
    /// How long a stopped or failed cell stays visible before its record is dropped.
    pub keep_ended: Duration,
    /// The cgroup the comb puts its cells under, which it makes if it has to. `None` runs cells
    /// with no cgroup of their own, with no limits and no kill on stop, which only suits tests.
    pub cgroup_root: Option<PathBuf>,
    /// Empty cgroups kept ready per QoS class, so a create does not wait on `mkdir`.
    pub cgroup_depth: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/hivebox"),
            unit: 1,
            node: 1,
            epoch: 1,
            mem_mib: None,
            reserved_mem_mib: 4096,
            max_cells: 3200,
            create_limit: BTreeMap::from([
                (Backend::Fncall, 256),
                (Backend::Container, 128),
                (Backend::Microvm, 64),
                (Backend::Fullvm, 8),
            ]),
            create_deadline: Duration::from_secs(30),
            stop_grace: Duration::from_secs(10),
            keep_ended: Duration::from_secs(600),
            cgroup_root: Some(PathBuf::from("/sys/fs/cgroup/hive.slice")),
            cgroup_depth: 256,
        }
    }
}
