//! One comb per machine. It owns every cell on that machine and every state transition of those cells, writes each transition to its WAL before acting on it, and survives its own restart without taking the cells down with it.
//!
//! The design is in `spec/08_node_agent.md`. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
