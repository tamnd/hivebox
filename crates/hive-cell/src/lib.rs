//! The contract between the node agent and an isolation backend.
//!
//! A cell is one sandbox. It might be a WebAssembly instance, a container, a Firecracker microVM
//! or a QEMU guest, and the node agent does not care which. This crate holds the isolation tiers,
//! the [`CellDriver`] trait every backend implements, and the [`DriverRegistry`] the node agent
//! keeps them in. The design is in `spec/07_backends_snapshots.md`.

// Unsafe is denied everywhere but the one `unshare` call in `netns`, which is how a network
// namespace is made.
#![deny(unsafe_code)]

pub mod cgroup;
mod driver;
#[cfg(target_os = "linux")]
pub mod netns;

use std::fmt;

pub use driver::{
    CellDriver, CellHandle, CellMetrics, DriverCaps, DriverRegistry, ExitInfo, GuestChannel,
    Liveness, NodeFit, PauseMode, Result, RootfsPlan, Slot, SnapshotCaps,
};
pub use hive_types::{CellId, CellState};

/// How strongly a cell is isolated from the host and from its neighbours. A higher tier is a
/// stronger boundary and a more expensive cell.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Tier {
    /// WebAssembly or a forked process. For trusted verifiers and reward functions.
    T0,
    /// A container sharing the host kernel.
    T1,
    /// A microVM with its own kernel. The default for untrusted code.
    T2,
    /// A full virtual machine with real devices.
    T3,
}

impl Tier {
    /// Whether code nobody has reviewed may run at this tier without a shield VM around it.
    #[must_use]
    pub const fn holds_untrusted_code(self) -> bool {
        matches!(self, Self::T2 | Self::T3)
    }
}

impl fmt::Display for Tier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let name = match self {
            Self::T0 => "T0",
            Self::T1 => "T1",
            Self::T2 => "T2",
            Self::T3 => "T3",
        };
        f.write_str(name)
    }
}

#[cfg(test)]
mod tests {
    use super::Tier;

    #[test]
    fn only_virtual_machines_hold_untrusted_code_alone() {
        assert!(!Tier::T0.holds_untrusted_code());
        assert!(!Tier::T1.holds_untrusted_code());
        assert!(Tier::T2.holds_untrusted_code());
        assert!(Tier::T3.holds_untrusted_code());
        assert!(Tier::T0 < Tier::T3);
    }
}
