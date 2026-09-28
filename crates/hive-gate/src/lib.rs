//! The only component with a foot on both the trusted and the untrusted network. It authenticates, sheds load, places batches and routes every per cell request to the comb that owns the cell by decoding the cell id.
//!
//! The design is in `spec/04_control_plane.md`, section 3. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
