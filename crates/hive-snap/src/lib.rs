//! Snapshots form a tree, and a fork is a new branch of it. This crate keeps the tree, computes diffs between snapshots and coordinates a fork across the driver, the storage layer and the page server.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 4. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
