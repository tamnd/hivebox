//! The small amount of state that has to be consistent: projects, principals, keys, quotas, templates and the image registry. It is a Raft group of three or five and it is not on the path of an exec or a file read.
//!
//! The design is in `spec/04_control_plane.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
