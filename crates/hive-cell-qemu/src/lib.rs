//! Full virtual machines through QEMU, for workloads that need a kernel of their own.
//!
//! Each cell is a QEMU guest booted straight into the drone from an initramfs, with its root
//! filesystem the same overlay a container gets, served from the host over 9p, and the drone
//! reached over vsock. QEMU runs in the cell's cgroup and network namespace, in a user namespace
//! of its own as a host uid no other cell has. The design is in `spec/07_backends_snapshots.md`,
//! section 3.5.

#![deny(unsafe_code)]

#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod initramfs;
// Moving QEMU into its namespaces happens between fork and exec, which only `pre_exec` allows.
#[cfg(target_os = "linux")]
#[allow(unsafe_code)]
mod jail;

#[cfg(target_os = "linux")]
pub use driver::{Config, DRONE_PORT, QemuDriver};
