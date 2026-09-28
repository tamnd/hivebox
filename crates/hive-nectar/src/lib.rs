//! Everything about getting bytes onto a node: OCI to EROFS conversion, the blob store implementations, the L1 cache and the lazy filler. A cell cannot start faster than its root filesystem appears, so this crate decides the create latency more than any other.
//!
//! The design is in `spec/06_storage_images.md`. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
