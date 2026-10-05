//! What a caller asks for when it creates a cell, from `spec/05_api_sdk.md`, section 1.1.
//!
//! The wire form lives in `hive-proto`. These are the checked, in-memory forms that the node agent
//! admits against, so every limit here has a floor and a ceiling and `validate` is the one place
//! that enforces them.

use std::collections::BTreeMap;
use std::fmt;
use std::time::Duration;

/// Which isolation backend runs the cell. The API does not hide the difference, so a caller picks.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Backend {
    /// WebAssembly or a forked worker process, for short trusted functions.
    Fncall,
    /// A container on the host kernel.
    Container,
    /// A Firecracker or Cloud Hypervisor microVM.
    Microvm,
    /// A QEMU virtual machine with real devices.
    Fullvm,
    /// Let the node pick between container and microVM from the project's isolation policy.
    Auto,
}

impl Backend {
    /// Every backend, in tier order with `Auto` last.
    pub const ALL: [Self; 5] =
        [Self::Fncall, Self::Container, Self::Microvm, Self::Fullvm, Self::Auto];

    /// The name used in config files, labels and metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Fncall => "fncall",
            Self::Container => "container",
            Self::Microvm => "microvm",
            Self::Fullvm => "fullvm",
            Self::Auto => "auto",
        }
    }

    /// The backend with this name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|b| b.as_str() == name)
    }
}

impl fmt::Display for Backend {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How a cell's CPU time is weighed against its neighbours', from `spec/08_node_agent.md`,
/// section 6.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Qos {
    /// Interactive work. A high CPU weight and no memory overcommit.
    Latency,
    /// The default.
    #[default]
    Standard,
    /// Runs on idle CPU only, and is the first to be reclaimed or killed under pressure.
    BestEffort,
}

impl Qos {
    /// Every class, highest priority first.
    pub const ALL: [Self; 3] = [Self::Latency, Self::Standard, Self::BestEffort];

    /// The name used on the wire and in config.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Latency => "latency",
            Self::Standard => "standard",
            Self::BestEffort => "best_effort",
        }
    }

    /// The name of the cgroup slice cells of this class live under.
    #[must_use]
    pub const fn slice(self) -> &'static str {
        match self {
            Self::Latency => "latency.slice",
            Self::Standard => "standard.slice",
            Self::BestEffort => "besteffort.slice",
        }
    }

    /// The `cpu.weight` of a cell in this class. Best effort cells also get `cpu.idle`, which is
    /// what actually keeps them off a busy core, so their weight is the cgroup minimum.
    #[must_use]
    pub const fn cpu_weight(self) -> u32 {
        match self {
            Self::Latency => 1000,
            Self::Standard => 100,
            Self::BestEffort => 1,
        }
    }

    /// How far memory may be committed past physical memory for cells of this class, in tenths,
    /// so 15 is 1.5 times. The defaults are from `spec/08_node_agent.md`, section 5.
    #[must_use]
    pub const fn overcommit_tenths(self) -> u64 {
        match self {
            Self::Latency => 10,
            Self::Standard => 15,
            Self::BestEffort => 30,
        }
    }
}

/// What a cell may use. CPU is in thousandths of a core, so a half core is 500.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Resources {
    /// CPU, in thousandths of a core.
    pub vcpu_milli: u32,
    /// Memory, in MiB.
    pub mem_mib: u32,
    /// Writable disk, in GiB.
    pub disk_gib: u32,
    /// Most processes and threads alive at once.
    pub pids: u32,
    /// Most open file descriptors per process.
    pub open_files: u32,
}

impl Default for Resources {
    /// [`Resources::DEFAULT`].
    fn default() -> Self {
        Self::DEFAULT
    }
}

impl Resources {
    /// One core, 2 GiB, 10 GiB of disk. Enough for a typical SWE task's test suite.
    pub const DEFAULT: Self =
        Self { vcpu_milli: 1000, mem_mib: 2048, disk_gib: 10, pids: 1024, open_files: 4096 };
    /// The smallest cell the node will make. Below this the guest agent alone does not fit.
    pub const MIN: Self =
        Self { vcpu_milli: 50, mem_mib: 64, disk_gib: 1, pids: 16, open_files: 64 };
    /// The largest single cell. A bigger one is a full VM on its own node.
    pub const MAX: Self = Self {
        vcpu_milli: 64_000,
        mem_mib: 256 * 1024,
        disk_gib: 2048,
        pids: 65_536,
        open_files: 1 << 20,
    };

    /// The `cpu.max` quota for this cell over `period_us`, with the burst factor in tenths, so 20
    /// allows twice the requested cores.
    #[must_use]
    pub const fn cpu_quota_us(&self, period_us: u64, burst_tenths: u64) -> u64 {
        self.vcpu_milli as u64 * period_us * burst_tenths / 10_000
    }

    /// Memory in bytes.
    #[must_use]
    pub const fn mem_bytes(&self) -> u64 {
        self.mem_mib as u64 * 1024 * 1024
    }

    /// Whether every field is within [`Resources::MIN`] and [`Resources::MAX`], with the name of
    /// the first one that is not.
    pub fn check(&self) -> Result<(), &'static str> {
        let fields = [
            ("vcpu_milli", self.vcpu_milli, Self::MIN.vcpu_milli, Self::MAX.vcpu_milli),
            ("mem_mib", self.mem_mib, Self::MIN.mem_mib, Self::MAX.mem_mib),
            ("disk_gib", self.disk_gib, Self::MIN.disk_gib, Self::MAX.disk_gib),
            ("pids", self.pids, Self::MIN.pids, Self::MAX.pids),
            ("open_files", self.open_files, Self::MIN.open_files, Self::MAX.open_files),
        ];
        match fields.into_iter().find(|&(_, v, lo, hi)| v < lo || v > hi) {
            Some((name, ..)) => Err(name),
            None => Ok(()),
        }
    }
}

/// What happens to a cell that has been idle for its idle time to live.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum IdleAction {
    /// Pause it, so the next request resumes it.
    #[default]
    Pause,
    /// Stop it.
    Stop,
}

/// Caps on what one request can make a cell produce or spend.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Limits {
    /// Most bytes of stdout plus stderr kept from one command. The head and tail are kept past it.
    pub output_bytes: u64,
    /// Longest one command may run before it is killed.
    pub wall_time: Duration,
}

impl Default for Limits {
    fn default() -> Self {
        Self { output_bytes: 1 << 20, wall_time: Duration::from_secs(600) }
    }
}

/// Where a cell's root filesystem comes from.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Source {
    /// A named template, which pins an image and a resource shape.
    Template(String),
    /// An image reference, either `hive://project/name@digest` or an OCI reference.
    Image(String),
    /// A snapshot id to restore from.
    Snapshot(String),
}

/// Everything a caller says about a cell when it asks for one.
#[derive(Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct CellSpec {
    /// The root filesystem.
    pub source: Source,
    /// The isolation backend.
    pub backend: Backend,
    /// What the cell may use.
    pub resources: Resources,
    /// Its CPU class.
    pub qos: Qos,
    /// The egress policy, `none` by default.
    pub network_profile: String,
    /// How long it may sit idle before `idle_action`. `None` means never.
    pub idle_ttl: Option<Duration>,
    /// What to do when idle for `idle_ttl`.
    pub idle_action: IdleAction,
    /// How long it may live at all. `None` means until stopped.
    pub hard_ttl: Option<Duration>,
    /// Caller labels, for selectors and for the rollout, group, task and step of an RL run.
    pub labels: BTreeMap<String, String>,
    /// Environment for every process started in the cell.
    pub env: BTreeMap<String, String>,
    /// Per request caps.
    pub limits: Limits,
    /// False means a container runs inside a shield VM.
    pub trusted_image: bool,
    /// How long from its start the cell may run on the node's setup boost, a higher CPU quota for
    /// the install and build before the real work. Marking the cell ready ends it sooner. `None`
    /// means no boost.
    #[cfg_attr(feature = "serde", serde(default))]
    pub burst_until_ready: Option<Duration>,
}

impl CellSpec {
    /// A spec for `source` with every other field at its default.
    #[must_use]
    pub fn new(source: Source, backend: Backend) -> Self {
        Self {
            source,
            backend,
            resources: Resources::default(),
            qos: Qos::default(),
            network_profile: "none".into(),
            idle_ttl: None,
            idle_action: IdleAction::default(),
            hard_ttl: None,
            labels: BTreeMap::new(),
            env: BTreeMap::new(),
            limits: Limits::default(),
            trusted_image: false,
            burst_until_ready: None,
        }
    }

    /// The checks the node runs before it admits a cell. Everything here is the caller's fault,
    /// so a failure is never an infra error.
    pub fn validate(&self) -> Result<(), SpecError> {
        self.resources.check().map_err(SpecError::Resource)?;
        if self.labels.len() > MAX_LABELS {
            return Err(SpecError::TooManyLabels);
        }
        for (k, v) in &self.labels {
            if !is_name(k) || v.len() > 256 {
                return Err(SpecError::Label(k.clone()));
            }
        }
        for (k, v) in &self.env {
            if k.is_empty() || k.contains('=') || k.contains('\0') || v.contains('\0') {
                return Err(SpecError::Env(k.clone()));
            }
        }
        if self.limits.output_bytes == 0 || self.limits.wall_time.is_zero() {
            return Err(SpecError::Limits);
        }
        if !is_name(&self.network_profile) {
            return Err(SpecError::NetworkProfile);
        }
        if self.burst_until_ready.is_some_and(|d| d.is_zero() || d > MAX_BOOST) {
            return Err(SpecError::Boost);
        }
        Ok(())
    }
}

/// Longest setup boost a cell may ask for. Setup that takes longer is the real work.
pub const MAX_BOOST: Duration = Duration::from_secs(3600);

/// Most labels on one cell. Labels are indexed on every node, so they are not free.
pub const MAX_LABELS: usize = 64;

/// A short name: 1 to 63 bytes of letters, digits, `-`, `_`, `.` and `/`.
#[must_use]
pub fn is_name(k: &str) -> bool {
    !k.is_empty()
        && k.len() <= 63
        && k.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'/'))
}

/// Why a spec was refused.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SpecError {
    /// A resource field is out of range. The field is named.
    Resource(&'static str),
    /// More than [`MAX_LABELS`] labels.
    TooManyLabels,
    /// A label key that is not a short name, or a value over 256 bytes.
    Label(String),
    /// An environment variable that cannot be set.
    Env(String),
    /// A zero output cap or wall time.
    Limits,
    /// A network profile name that is not a short name.
    NetworkProfile,
    /// A setup boost of zero or longer than [`MAX_BOOST`].
    Boost,
}

impl fmt::Display for SpecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Resource(field) => write!(f, "resources.{field} is out of range"),
            Self::TooManyLabels => write!(f, "more than {MAX_LABELS} labels"),
            Self::Label(k) => write!(f, "label {k:?} has a bad key or a value over 256 bytes"),
            Self::Env(k) => write!(f, "environment variable {k:?} cannot be set"),
            Self::Limits => f.write_str("output and wall time limits must be above zero"),
            Self::NetworkProfile => f.write_str("the network profile is not a valid name"),
            Self::Boost => {
                write!(f, "burst_until_ready must be above zero and at most {MAX_BOOST:?}")
            }
        }
    }
}

impl std::error::Error for SpecError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec() -> CellSpec {
        CellSpec::new(Source::Image("docker.io/library/python:3.12".into()), Backend::Container)
    }

    #[test]
    fn the_default_spec_is_admissible() {
        assert_eq!(spec().validate(), Ok(()));
    }

    #[test]
    fn resources_out_of_range_name_the_field() {
        let mut s = spec();
        s.resources.mem_mib = 1;
        assert_eq!(s.validate(), Err(SpecError::Resource("mem_mib")));
        s.resources.mem_mib = 512;
        s.resources.pids = u32::MAX;
        assert_eq!(s.validate(), Err(SpecError::Resource("pids")));
    }

    #[test]
    fn labels_and_env_are_checked() {
        let mut s = spec();
        s.labels.insert("step".into(), "412".into());
        s.labels.insert("rollout/id".into(), "r-1".into());
        assert_eq!(s.validate(), Ok(()));
        s.labels.insert("has space".into(), "x".into());
        assert!(matches!(s.validate(), Err(SpecError::Label(_))));

        let mut s = spec();
        s.env.insert("A=B".into(), "x".into());
        assert!(matches!(s.validate(), Err(SpecError::Env(_))));

        let mut s = spec();
        s.env.insert("PATH".into(), "a\0b".into());
        assert!(matches!(s.validate(), Err(SpecError::Env(_))));

        let mut s = spec();
        for i in 0..=MAX_LABELS {
            s.labels.insert(format!("k{i}"), String::new());
        }
        assert_eq!(s.validate(), Err(SpecError::TooManyLabels));
    }

    #[test]
    fn a_setup_boost_is_bounded() {
        let mut s = spec();
        s.burst_until_ready = Some(Duration::from_secs(120));
        assert_eq!(s.validate(), Ok(()));
        s.burst_until_ready = Some(Duration::ZERO);
        assert_eq!(s.validate(), Err(SpecError::Boost));
        s.burst_until_ready = Some(MAX_BOOST + Duration::from_secs(1));
        assert_eq!(s.validate(), Err(SpecError::Boost));
    }

    #[test]
    fn cpu_quota_follows_the_burst_factor() {
        let r = Resources { vcpu_milli: 1500, ..Resources::default() };
        assert_eq!(r.cpu_quota_us(100_000, 10), 150_000);
        assert_eq!(r.cpu_quota_us(100_000, 20), 300_000);
    }

    #[test]
    fn names_round_trip() {
        for b in Backend::ALL {
            assert_eq!(Backend::from_name(b.as_str()), Some(b));
        }
        assert_eq!(Backend::from_name("docker"), None);
        assert_eq!(Qos::ALL.map(Qos::as_str), ["latency", "standard", "best_effort"]);
    }
}
