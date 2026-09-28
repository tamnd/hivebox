# Changelog

Notable changes, newest first. The minor version is the number of milestones finished, so 0.1.0 is the release where M0's exit criterion passes. The milestones are the issues at https://github.com/tamnd/hivebox/issues.

## Unreleased

## 0.0.2

hive-drone v1, the guest agent that runs inside every cell.

- `process.run`, `process.start` and `health`, with a typed client for the node side. Running `/bin/true` through the drone costs about the same as spawning it straight from tokio, around 0.7 to 0.9 ms p50 on the test box (#17).
- Persistent shell sessions that keep `cd` and variables across calls, with end of command detection by a sentinel. A shell builtin in a session takes about 150 us p50. Channel streams split into halves so a caller can write and read at once (#18).
- `fs.read`, `fs.write`, `fs.stat`, `fs.list`, `fs.mkdir`, `fs.remove`, `fs.rename` and `fs.chmod`. Every path resolves with `openat2` inside the drone's configured roots, and writes are all or nothing. `hive-types` gains the `FILE_ERROR` reason and an `errno` field on errors (#19).
- `fs.upload` and `fs.download` move whole trees as tar archives, and hostile archives stay inside the destination. `fs.watch` streams changes under a directory through inotify, recursively if asked. The drone crate now builds only on Linux (#20).

## 0.0.1

The first pieces of M0.

- `hive-types` gains the cell spec with its resource bounds and validation, the QoS classes, the stable error reasons with their gRPC codes and infra flags, and the stop causes (#11).
- `hive-proto` has the drone channel: a 9 byte frame header, a mutual handshake with keyed BLAKE3, and multiplexed streams with per stream credit. It moves about 3 to 4 GiB/s over a Unix socket on the test box (#12).
- `hive-rt` has clock, randomness and network traits, each with a tokio implementation and a deterministic simulated one (#13).
- `hive-telemetry` has a metrics registry with a label allowlist and a series cap, exponential histograms, a `/metrics` endpoint and JSON log setup. An observation costs 12 to 19 ns (#14).
- `hive-proto` has the first draft of `hivebox.v1`, built without protoc, and the conversions to `hive-types` including the ErrorInfo error model (#15).
- `cargo xtask bump` moves the workspace, the internal pins and this file to a new version (#10).

The workspace itself: every crate from `spec/03_architecture.md` as a skeleton with a rank in `xtask/layers.toml`, the cell id codec and the cell state machine in `hive-types`, and the isolation tiers in `hive-cell`. CI runs formatting, the layer rule, the prose rules, clippy, tests on Linux and macOS, documentation, the msrv floor, cargo-deny, typos and zizmor on every commit.
