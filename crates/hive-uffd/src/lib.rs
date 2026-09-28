//! A restored microVM starts before its memory has arrived. This server answers its page faults from the snapshot and prefetches the pages the last run touched first.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 3. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
