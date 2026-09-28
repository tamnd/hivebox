//! Placement. Given a batch of cell specs and the cluster state from scout it picks nodes, and it has to do that for thousands of cells a second without becoming the bottleneck.
//!
//! The design is in `spec/04_control_plane.md`, section 5. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
