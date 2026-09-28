//! Every comb pushes a report once a second. Scout folds those into one view of the cluster and pushes it to the gate and to waggle.
//!
//! The design is in `spec/04_control_plane.md`, section 4. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
