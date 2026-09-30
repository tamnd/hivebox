# Changelog

Notable changes, newest first. The minor version is the number of milestones finished, so 0.1.0 is the release where M0's exit criterion passes. The milestones are the issues at https://github.com/tamnd/hivebox/issues.

## Unreleased

- `hive-waggle` places a batch of cells: it samples `clamp(2n, 8, nodes)` nodes weighted by room, fills them from a heap a share at a time, packs for cached layers below 60% memory use and spreads above it, and keeps an overlay of what it placed until the node's report counts it. On server3 a single cell on 1,000 nodes takes 40.9 us of CPU at p50, and 32,000 cells on 1,000 nodes take 1.18 ms.
- `hive-scout` folds node reports into a versioned snapshot for waggle and the gate. It drops late reports and ones from a replaced comb, carries the 4 KiB layer filter over when a report leaves it out, shows a node silent for 3 s as down and forgets it after 60 s, and flags a report as urgent when a node is new, came back, changed health or moved its room by more than 5%. On server3 taking a report costs about 1 us of CPU and a snapshot of 1,000 nodes 311 us at p50.

## 0.0.10

Fixes the SWE-bench Verified run and the 1,000 cell test found, and timing for stops.

- `hive-oci import` no longer refuses a file owned by an id past the cell's range. That owner becomes nobody, which is what an idmapped mount shows. The matplotlib images in SWE-bench Verified have files owned by uid 197609, so none of those tasks could start before.
- Every container cell gets its own writable `/etc/hosts` and `/etc/hostname`. Images made with docker ship an empty hosts file, so `localhost` did not resolve in a cell, and test suites that bind to it failed before running a test. On server3, matplotlib-13989 failed this way before the fix. After it, its 1.74 GB image in 10 layers imported in 1000 s at load 40 to 60, and the gold patch resolved in a 280 s eval.
- Fixed: a node could not hold 1,000 cells, and a stop that failed could leave the cell running. The comb ran with a soft limit of 1024 open files and holds a connection to every running cell, so on server3 it ran out at 966 cells, and then writing `cgroup.kill` failed the same way and about 1,070 cells kept running with no record left. The comb now raises the soft limit to the hard one at startup, and retries a cgroup removal that fails for about 100 s before leaving it for the next comb. On server3 at load 57 to 75, 1,000 cells with 512 MiB each ran at once, all answered, and all stopped with nothing left running.
- `hive_stop_seconds{backend,stage}` times each stop by stage (wal, driver, release and total), and the unmount and delete of a stopped cell now run on the blocking pool instead of the runtime's threads. A single stop on server3 takes 104 ms at p50 over 20 stops. A bulk stop still gets through about 30 cells a second.

## 0.0.9

What the first node benchmarks turned up: create stage metrics, a fairer create deadline, and images imported layer by layer.

- `hive-oci import --layer` applies one OCI image layer on top of what the directory has already, with its whiteouts and opaque directories, so an image can go from a registry into a node's image directory one layer at a time without docker in between. The SWE-bench runner in hivebox-bench uses it now, which keeps one copy of each image on disk where going through docker kept two.
- A create that waits its turn behind a burst now gets the whole `create_deadline` again once it starts, instead of what was left of it. Before, with 1000 creates asked for at once on server3, 169 got a turn with almost no time left and failed as `DRONE_UNREACHABLE`, which blamed the guest agent for the queue. A create that waits past the deadline for its turn still fails with `CAPACITY_UNAVAILABLE`, and is now counted in `hive_create_total`.
- `hive-comb` counts what it does and serves it on `/metrics` when `[node] metrics` names an address. `hive_create_seconds{backend,stage}` times each create stage by stage (admit, pool, rootfs, prepare, wal, start, handshake and total), `hive_create_total{backend,result}` counts how creates ended, and `hive_exec_seconds{op}` times exec calls from the comb's side. On server3 at load 14, 50 container creates at 10 a second took 93 ms at p50, of which 66 ms was the OCI worker starting the container, 8 ms the drone handshake, 6 ms the two WAL writes and 4 ms preparing the bundle.

## 0.0.8

Clients: the Python SDK, the Rust SDK and hivectl, on a local API that now serves Files too.

- The Python SDK in `sdk/python`, the primary client. `AsyncHive` talks to a comb's socket or a gate with grpcio's asyncio API. Cells are made one at a time or as a group that stops every cell on the way out, and each cell has `run`, streamed `start`, sessions and `files`. Every failure raises a class named after its reason with `is_infra_error` set, a missing file is also a `FileNotFoundError`, and calls that are safe to repeat are retried on infra errors. The stubs are checked in, CI checks they match the protos, and `cargo xtask bump` now moves the package version too. On server3 against a real comb, 8 cells are made in 71 to 91 ms.
- `hive-sdk` is the Rust client: `Client` on a comb's socket or an HTTP endpoint, cells made one at a time or in batches with an idempotency key, `run`, streamed `start` and sessions, file read, write, list, watch and tar upload, and bulk pause, resume and stop by id or labels. Errors come back as `hive_types::Error` with the reason the node gave. `hivectl` is the CLI on top of it. On server3 against a real comb with python:3.12-slim, `hivectl run ID -- true` takes 13 to 180 ms end to end at load 27, and 100 MB goes into a cell in 1.0 to 1.3 s and comes out in 0.4 to 1.2 s at load 3. Through the SDK, 16 cells are made in 337 ms, run Python in all of them in 201 ms and stop by label in 106 ms.
- Fixed: a streamed command that reads its stdin and prints nothing no longer stalls until its timeout once 16 chunks of input are waiting. The drone now wakes when the command's pipe has room again. On server3, 100 MB piped into `cat > file` in a cell takes about 1 s where it used to wait out the 10 minute default.
- The local API in `hive-comb` serves Files: read, write, stat, list, remove and watch go to the drone in the cell, and `Apply` unpacks a tar. Small writes go in one message and bigger ones are streamed, and `ListDirResponse` now says when a listing stopped at the cell's limit.

## 0.0.7

Cells get a network: hive-guard on a veth per cell, and a DNS proxy that only answers the names a profile lists.

- `hive-guard` has the egress program every cell's traffic goes through, written in plain C, built by clang and loaded with aya on tc ingress of the host side of the cell's interface as a tcx link. An interface with no cell passes nothing, a cell may only send from its own address and MAC, ARP only for its own address, no IPv6, and IPv4 only to what a rule in its profile allows or what the DNS proxy resolved for it until the answer expires. Drops are counted by reason and reported in a ring. The maps and links are pinned in `/sys/fs/bpf/hive/guard-v1`, so the policy holds while the node restarts, and attaching again replaces a link with no gap. The built-in `none` profile reaches only the DNS proxy and `mirrors` adds the mirror proxy. On server3 a packet costs about 300 ns when a rule allows it and 550 ns when DNS does, against 80 ns for an empty program, and a cell is put on its interface with one map write of 2 to 17 us.
- Cells in `hive-comb` have a network. Every pooled namespace gets a veth pair named `hv<n>` with the guard attached to the host end, a /32 from the node's range (`[network] cells`, 100.64.0.0/20 by default), a default route through 169.254.77.1 and static neighbours on both ends, all made over one netlink socket in a batch. A create then only writes the cell's entry in the guard's maps, with its sequence number, address, MAC and profile, and the OCI driver mounts a read only `/etc/resolv.conf` that points at 169.254.77.53. The `mirrors` profile is accepted along with `none`, a restarted comb finds each cell's interface again from its namespace, and if the guard cannot load the comb says so and cells get loopback only as before. On server3 at load 34 to 38, a veth takes about 65 ms to make against 138 ms for `ip link add` at the same time, a pool of 64 fills in 8 to 11 s against 2 to 2.5 s without interfaces, and a create one at a time takes 170 to 237 ms at p50 against 261 to 314 ms without, so the guard adds nothing measurable there.
- The DNS proxy from `hive-guard::dns` runs in `hive-comb` on 169.254.77.53. A cell may only look up the names its profile lists, so `none` and `mirrors` get NXDOMAIN for everything, and profiles of the node's own come from `[network.profiles.<name>] domains = ["pypi.org", "*.pythonhosted.org"]` and reach the proxy and what it resolved for them. Only A queries are forwarded: AAAA, HTTPS and SVCB get an empty answer and TXT and the rest are refused. Before an answer goes back, private, shared, loopback and other special addresses are taken out of it and the cell is allowed each address that is left for the answer's TTL, held between 30 s and 10 min. Long or random looking labels under a wildcard get NXDOMAIN, each cell may ask 50 a second with bursts of 100, and the resolvers asked are the host's own unless `[network] upstream` says otherwise. On server2 against a resolver on loopback, the proxy answers 17,000 to 18,000 queries a second from 64 clients against 25,000 to 33,000 for the resolver alone, and adds about 28 us at p50 for one client. On server3 through systemd-resolved, pypi.org, files.pythonhosted.org, github.com and example.com resolve to the same addresses through the proxy as without it.

## 0.0.6

Images as EROFS layers with hive-nectar, and container cells running on them.

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
