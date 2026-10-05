//! A restored microVM starts before its memory has arrived. This server answers its page faults from the snapshot's memory file, and fills in the pages the last restore touched before the VM asks for them.
//!
//! The design is in `spec/07_backends_snapshots.md`, section 3.4. A VMM restoring with the userfaultfd backend connects to a Unix socket and sends its guest memory regions as JSON, with the userfaultfd that covers them attached. [`Session::accept`] takes both, [`Session::prefetch`] fills in the pages of a [`Trace`] saved from an earlier restore, as REAP does, and [`Session::serve`] answers each fault with a copy of the page from the [`Memory`] file, which every VM restored from the same snapshot shares through the page cache. Memory the guest gives back, as the balloon does, comes back as zero pages. [`Guest`] plays the VMM's side, so all of this runs and is measured without a VM.
//!
//! Minor fault mode, where clean pages are mapped from shared memory and not copied, and streaming a remote snapshot in chunks come later.

#![allow(unsafe_code)]

#[cfg(target_os = "linux")]
mod server;
#[cfg(target_os = "linux")]
mod sys;

#[cfg(target_os = "linux")]
pub use server::{Guest, Memory, Region, Session, Stats, Trace};
