//! Firecracker microVMs, started fresh or restored from a snapshot, always under the jailer. This is the default for untrusted code.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
