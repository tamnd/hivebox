//! A service that turns OCI images into what `hive-nectar` serves: EROFS layers, deduplicated chunks and a file layout ordered by what the first seconds of a run read.
//!
//! The design is in `spec/06_storage_images.md`, section 5. Nothing here is implemented yet, and the milestone issues say when it will be.

#![forbid(unsafe_code)]
