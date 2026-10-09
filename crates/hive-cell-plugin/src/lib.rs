//! Driver plugins: a hivebox isolation backend that runs as a process of its own and speaks gRPC
//! over a Unix socket, so a third party backend does not need to be linked into the node agent.
//!
//! A plugin implements [`hive_cell::CellDriver`] as a built in driver would and hands it to
//! [`serve`]. The node agent connects with [`PluginDriver::connect`] and adds what it gets to its
//! [`hive_cell::DriverRegistry`], where it replaces a built in driver for the same backend. The
//! wire API is `hivebox.plugin.v1.Driver` in `crates/hive-proto/proto/hivebox/plugin/v1`, so a
//! plugin can be written in any language with gRPC.
//!
//! ```no_run
//! # async fn run(driver: std::sync::Arc<dyn hive_cell::CellDriver>) -> std::io::Result<()> {
//! let listener = hive_cell_plugin::bind("/run/hivebox/plugins/qemu.sock".as_ref())?;
//! hive_cell_plugin::serve(driver, "qemu-plugin 0.3.1", listener, std::future::pending()).await
//! # }
//! ```
//!
//! What a driver gets and gives is plain data: paths, ids and a handle the node agent stores. The
//! node agent does admission, cgroups, network namespaces, root filesystems and its WAL itself, so
//! a plugin restart loses nothing a later call needs. A cell's metrics are read from its cgroup on
//! the node agent's side, so they cost no call.

#![forbid(unsafe_code)]

mod client;
mod server;
mod wire;

pub use client::PluginDriver;
pub use server::{bind, serve};

/// The version of the plugin API this build speaks. A node agent uses no plugin that speaks
/// another one, so a change that is not an addition gets a new version.
pub const API_VERSION: u32 = 1;
