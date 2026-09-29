//! The command line client, for operators and for anyone poking at a cluster by hand.
//!
//! The design is in `spec/05_api_sdk.md`. The commands are in [`cli`], built on `hive-sdk`.

#![forbid(unsafe_code)]

pub mod cli;
