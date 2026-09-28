//! One place that sets up tracing, OpenTelemetry export and metrics for every daemon. It also owns the cardinality guard, because a cell id used as a metric label takes down a Prometheus server long before it takes down anything else.
//!
//! The design is in `spec/13_observability_testing_bench.md`, section 1. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
