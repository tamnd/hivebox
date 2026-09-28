//! Network policy for cells. The eBPF programs enforce the egress rules on the node and the DNS proxy decides which names resolve at all.
//!
//! The design is in `spec/12_networking.md`. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
