//! Block devices served from user space, so that a layer or a microVM root disk can be filled
//! lazily.
//!
//! The design is in `spec/06_storage_images.md`, section 5.1, which prefers ublk. The kernels the
//! nodes run today ship ublk in a package they do not have installed, so the first device here is
//! NBD, which every distribution kernel has: [`nbd::Device`] serves a read only [`Source`] on
//! `/dev/nbdN`. ublk and copy on write come later.

#![allow(unsafe_code)]

#[cfg(target_os = "linux")]
pub mod nbd;
#[cfg(target_os = "linux")]
pub use nbd::{Device, Source};
