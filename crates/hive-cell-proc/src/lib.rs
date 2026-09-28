//! Process isolation with namespaces, seccomp and Landlock, forked from a warm zygote. Fast and weak, and it is only offered where the workload is trusted.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 2. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
