//! The generated `hivebox.v1` messages, clients and servers.
//!
//! The protos under `proto/hivebox/v1` are the source of truth and carry the field comments. The
//! services for M0 are Cells, Exec and Files. Snapshots, Images and Verify are drafts whose shape
//! is fixed early so clients can plan for them. Version 1 only ever grows: fields and methods are
//! added, never renamed, renumbered or removed.

#![allow(missing_docs, missing_debug_implementations, unreachable_pub, unused_qualifications)]
#![allow(clippy::all, clippy::pedantic, clippy::missing_panics_doc)]

tonic::include_proto!("hivebox.v1");
