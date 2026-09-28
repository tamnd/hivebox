# Changelog

Notable changes, newest first. The minor version is the number of milestones finished, so 0.1.0 is the release where M0's exit criterion passes. The milestones are the issues at https://github.com/tamnd/hivebox/issues.

## Unreleased

The workspace, with every crate from `spec/03_architecture.md` as a skeleton and a rank in `xtask/layers.toml`. `hive-types` has the cell id codec and the cell state machine, and `hive-cell` has the isolation tiers. CI runs formatting, the layer rule, the prose rules, clippy, tests on Linux and macOS, documentation, the msrv floor, cargo-deny, typos and zizmor on every commit.
