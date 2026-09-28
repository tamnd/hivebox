//! Block devices served from user space through ublk, with OverlayBD compatible copy on write, so that a microVM root disk can be filled lazily.
//!
//! The design is in `spec/06_storage_images.md`, section 4. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
