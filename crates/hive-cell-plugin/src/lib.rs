//! Out of process drivers speak gRPC over a Unix socket through this bridge, so that a third party backend does not need to be linked into the node agent to be used by it.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 5. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
