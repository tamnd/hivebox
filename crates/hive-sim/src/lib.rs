//! Runs the control plane and the node agents in one process, on simulated time and a simulated network, with failures injected from a seed. A failure found here is a failure that can be replayed.
//!
//! The design is in `spec/13_observability_testing_bench.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
