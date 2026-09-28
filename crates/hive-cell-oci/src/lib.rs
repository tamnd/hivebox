//! Containers through youki's libcontainer, in process rather than through a shim, because a fork and exec per cell is a cost the create path cannot afford at the target rate.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
