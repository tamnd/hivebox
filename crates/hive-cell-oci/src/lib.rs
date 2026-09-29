//! Container cells through youki's libcontainer, in process rather than through a runtime binary,
//! because a fork and exec of `runc` per cell is a cost the create path cannot afford.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 3.3. A cell is a container with its
//! own user, PID, mount, IPC, UTS and cgroup namespaces, in the network namespace and the cgroup
//! the comb made for it. The host's user namespace keeps owning the network namespace, so root in
//! the cell cannot change its own routes or firewall. Root in the cell is `uid_base` on the host, and every cell shares
//! that one range, so images are shifted to it once when they are imported (see [`import`]).
//! The root filesystem is an overlay of the image's layers under a scratch upper.
//!
//! The container's first process is `hive-drone --init`, bind mounted read only from the host.
//! The drone's socket is bound on the host by the worker that makes the container and passed in
//! as descriptor 3, so it never shows up in the cell's filesystem, and the drone reads its secret
//! from stdin. It then hardens itself with seccomp before it serves anything.
//!
//! libcontainer clones, so the containers are made by worker processes with one thread each (see
//! [`worker`]). The driver keeps a few running and gives each a create at a time.

#![forbid(unsafe_code)]

#[cfg(target_os = "linux")]
mod driver;
#[cfg(target_os = "linux")]
mod import;
#[cfg(target_os = "linux")]
mod spec;
#[cfg(target_os = "linux")]
pub mod worker;

#[cfg(target_os = "linux")]
pub use driver::{Config, OciDriver};
#[cfg(target_os = "linux")]
pub use import::import;
