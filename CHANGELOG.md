# Changelog

Notable changes, newest first. The minor version is the number of milestones finished, so 0.1.0 is the release where M0's exit criterion passes. The milestones are the issues at https://github.com/tamnd/hivebox/issues.

## Unreleased

- `hive-nectar` v0 stores images as EROFS layers. `PosixStore` keeps blobs by BLAKE3 name on a local or shared filesystem, `oci::Importer` turns an OCI image layout into one metadata blob and one data blob per layer by streaming each layer into `mkfs.erofs` through a pipe, and `Cache` is the node's L1 that fetches whole blobs in 256 KiB chunks, resumes after a crash, checks every blob it fetches and evicts the least recently used. Layers build the same bytes every time, so the same layer is stored once. On server3, python:3.12-slim imports in 19 to 41 s depending on how busy the disk is, with 0.7 MiB of metadata for 122.5 MiB of data, and an import of an image already there takes about 25 ms.
- `hive-comb` runs container cells on `hive-nectar` images. A file in `data_dir/images` holding an image id, which is what `hive-nectar import-oci` prints, names an image in the store set by `[images]` in the config. The first cell to use it fetches its blobs into the node cache and mounts each layer once: EROFS through the new mount API on read only loop devices with direct I/O and autoclear, idmapped to the cells' id range, so layers keep the owners the image has. Mounts left by an earlier run are detached at start, and cells still running on them keep them. On server3 at load about 30, with python:3.12-slim, a create through the comb takes 174 ms at p50 against 164 ms on an unpacked image, 64 at once are all running in 2.64 s against 2.48 s, mounting the 4 layers from a warm cache takes 198 ms and later cells pay nothing for it.

## 0.0.5

The first real backend: container cells through youki's libcontainer, with a hardened drone inside.

- `hive-drone` can run as a container's first process with `--init`, which reaps orphans and passes stop signals on, and can lock itself down with `--harden`: Landlock makes the `--protect` paths read only, a seccomp allowlist of 282 calls returns ENOSYS for the rest, and 41 calls such as mount, unshare, setns, bpf and io_uring return EPERM. The secret can come from a file the drone deletes after reading it, and `--env` sets the environment commands get. On server3 the filters add about 100 to 200 ns to each syscall and nothing measurable to running a command (#29).
- `hive-cell-oci` runs container cells through youki's libcontainer, in process. Each cell has its own user, PID, mount, IPC, UTS and cgroup namespaces, root in the cell is uid 1000000 on the host, the root filesystem is an overlay over an image unpacked by `hive-oci import`, and the first process is a hardened `hive-drone --init` whose socket is bound on the host and passed in as descriptor 3. Containers are made by single threaded worker processes, which `hive-comb` starts as `hive-comb --oci-worker`, and the comb registers the backend when the node passes its probe (`[backends.container]` in the config). On server3 at load 22 to 45 on 8 cores, a create through the comb takes 112 ms at p50 with a python image, 64 at once are all running in 2.7 s, and an idle cell uses about 680 KiB (#30).

## 0.0.4

hive-comb gets a network namespace per cell and runs on its own with a local API.

- Every cell gets its own network namespace with loopback up, taken from a pool of 400 spares the comb refills in the background. Namespaces are made on up to 4 threads, which took server2 from about 230 to about 430 a second, and removed up to 64 at a time, about 5,700 a second there. Both pools now retry a failed refill after a backoff from 10ms to 5s, and every connection to a guest agent has a 2s limit (#26).
- `hive-comb` runs on its own with a local gRPC API on a unix socket that only its owner can open, and reads `/etc/hivebox/comb.toml` or `--config`. It serves Cells (create with a count up to 1024, get, list with paging and label selectors, watch, pause, resume and stop by id or labels) and Exec (run, start with stdin and signals, and shell sessions), scoped to the project named in the `x-hive-project` header. On server2 with a fake driver, a Get costs 144 us p50, running `true` through the API costs about 1ms more at p50 than calling the drone directly, one caller makes and stops 480 cells a second, 16 callers make and stop 1,318 a second, and one create call with count 64 takes 18ms (#27).

## 0.0.3

The start of hive-comb, the node agent, in standalone mode.

- `hive-cell` has the `CellDriver` trait every backend implements, with plain data handles the node can store and give back after a restart, and a `DriverRegistry` (#22).
- `hive-comb` has its WAL, admission and the lifecycle actors. The WAL groups concurrent writes into one fsync, so 256 writers get about 32,600 writes/s on a VPS disk where one fsync takes 2.4 ms. A restarted comb takes back every cell it knew, reconnects running cells to their guest agent and cleans up half made ones. Create then stop runs about 1,650 cells/s with 256 callers on the same box, with a fake driver (#23).
- Every cell gets its own cgroup under `hive.slice/<class>.slice`, taken from a pool of spares that already have the default limits, which costs under 1 us. Stopping a cell kills everything in its cgroup, and a restarted comb sweeps any cgroup no cell claims (#24).

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
