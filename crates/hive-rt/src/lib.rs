//! Every service reads time, draws random numbers and opens connections through the traits in this crate. The production implementation is tokio. The simulation implementation is driven by `hive-sim`, and the reason this crate exists is that the second one has to be a drop-in for the first.
//!
//! The design is in `spec/13_observability_testing_bench.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
