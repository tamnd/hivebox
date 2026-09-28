//! A small static binary that runs inside each cell, as PID 1 in a microVM or as a normal process in a container. It speaks a framed protocol over a Unix socket or vsock, and it assumes the code it runs is hostile.
//!
//! The design is in `spec/09_guest_agent.md`. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
