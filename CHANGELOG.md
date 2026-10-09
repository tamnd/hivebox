# Changelog

Notable changes, newest first. The minor version is the number of milestones finished, so 0.1.0 is the release where M0's exit criterion passes. The milestones are the issues at https://github.com/tamnd/hivebox/issues.

## Unreleased

- Lists, pauses, resumes and stops by label, and watches through a gate with `[units]` cover the cells of every unit, the gate's own first, so a client of one gate sees the whole fleet. A list of 10 cells over two units took 3.5 ms at the median through one gate on a loaded host, against 2.2 ms for the 4 cells of one unit.
- A gate can front one unit of several. `unit` and `peer_listen` in `[gate]` and a `[units]` table naming the other units' gates send a get, stop, exec, file call or verify about a cell of another unit through that unit's gate, so a client needs only one gate. Creates still go to the gate's own unit.
- `Verify.Run` leaves `conftest.py`, `pytest.ini`, `.pytest.ini`, `tox.ini`, `setup.cfg` and installed package metadata out of the subject's diff even when the request names no protected paths, and reports them in `tampered`, so a `conftest.py` hook that marks every test passed no longer earns a reward. `[verify] protected_paths` in the comb's config changes the list, and `[]` turns it off.

## 0.0.35

Git squashing for snapshots, a static shell the cell cannot overwrite, and disk quotas for container cells.

- `[backends.container] disk_quota` holds what each container cell writes to its `disk_gib`, and its files to 65,536 a GiB, with a project quota on its upper, so `yes > f` in one cell fills its own share and not the node's disk. It needs project quotas on the filesystem under `data_dir`. `hivectl create` takes `--disk GIB`.
- `[backends.container] shell` names a static shell on the host that the drone runs every command and session with, mounted read-only at `/.hive/sh`. A cell that overwrites its own `/bin/sh`, `/bin/bash` or libc can no longer fake the exit codes of later commands. With a dynamically linked shell the node runs no container cells, and its log says why.
- A snapshot can squash git repositories first with `squash_git` in the API, `hivectl snapshot --squash-git DIR` and the SDKs. Each one is rebuilt in the cell with a single commit holding what `HEAD` holds, so a testbed cloned from upstream no longer carries the fix in a later commit, a branch, a tag, the reflog, a stash or a pack. The Rust SDK's `snapshot` takes the paths as a new argument.

## 0.0.34

Verify judged by a JUnit report, ioctl filtering in the drone, and scrubbed commits that pass packaged keys.

- The drone's seccomp filter looks at `ioctl` requests. Terminal, socket, file flag and reflink requests pass, `TIOCSTI` and the ext4 extent swaps fail with EPERM, and the rest, `XFS_IOC_SWAPEXT` among them, fail with ENOTTY. The three filters are now one program, so an allowed `ioctl` costs 149 to 185 ns more than with no filter on server3, not 795 to 1,006 ns. `/proc/kpagecount` is masked in containers, as `kpagecgroup` and `kpageflags` were.
- A verify can name a JUnit report with `report`, and tests that have to pass with `must_pass`, in the API, `hivectl verify --report PATH --must-pass TEST`, the SDK and pollen tasks. A run then passes only when it exited with 0 and left the report with no test failed or errored and each test in `must_pass` passed, so a `sys.exit(0)` slipped into the code under test no longer passes. The report is removed before each run, its counts replace the ones read from the output, and the tests that did not pass come back in `not_passed`.
- A scrubbed commit no longer fails on a secret in a file that is still what a Debian package shipped, by the md5sums dpkg keeps. libgnutls holds the private keys of its self tests, so any image with git installed by apt could not be committed without `--allow usr/lib/`. Such finds are listed as `shipped` in the provenance.

## 0.0.33

Cell quarantine, and security events sent to a SIEM.

- Security events go to a SIEM with `[siem] sink` in the comb and gate configs: a file, syslog over UDP or TCP, or HTTP. The comb sends the packets the guard dropped and the names the DNS proxy refused, each put down to its cell, along with calls refused for want of a right and quarantines. The gate sends callers with bad keys. Repeats are counted into one line a window, and drops the guard's ring had no room for are counted as `net.unreported`, so nothing goes unsaid. On server3, 400,000 packets flooded from one cell in about 2 s were all counted, in 4 lines, and a scan of 3,000 ports came as 17 lines. The first line for a flood, a scan, a spoof or a lookup reached the listener 139 to 1,417 ms after the cell began, 550 ms or less in 11 of 12.
- `hivectl quarantine ID... | -l KEY=VALUE... [--reason TEXT]`, `Quarantine` on the API and the gate, and `quarantine` in the Python SDK freeze a cell, cut it off the network, not even DNS, and keep an unscrubbed disk snapshot of it. It stays paused and cut off until it is stopped, across comb restarts too, and resume, exec and file calls on it are refused. On server3 the freeze and the cut took 8 to 319 ms, 40 ms or less in 6 of 10, the snapshot of a cell that had written 64 MiB took 2.5 to 4.9 s, and the cell could no longer resolve names or reach the address it had resolved before.

## 0.0.32

An audit log on every node, with every API call in it and its roots held by the keeper.

- An audit log in `hive_telemetry::audit`: a node's events as a hash chain cut into hourly files, each sealed with its count and root when the hour turns, written in batches with one sync each. `hivectl audit verify DIR` says where a chain breaks. On server3 at load 80, 8 threads recorded 32,000 to 42,000 events a second, and verifying ran at 71,000 to 171,000 a second.
- The comb records every API call in its audit log: cells, exec, files, snapshots, verify and the LLM gateway, with the principal the gate stamps in `x-hive-principal` (`local` on the comb's own socket), a hash of the arguments, the result and the W3C trace id. It is on by default under the data directory and set with `[audit] dir`, and an empty `dir` turns it off. Syncs are at least `[audit] sync_gap` apart, 1 s unless set, since syncing every batch slowed file writes from cells by about 40 percent on server3. With the gap, at load 84 to 108, `exec.run` through the python SDK ran at 469 to 568 calls a second with the log on against 459 to 553 off, and file writes at 532 to 569 against 508 to 720.
- The comb publishes its audit chain's sealed hour roots and its tip to the keeper with each lease renewal, in the same Raft entry. The keeper takes them only if they follow on from the ones it holds, keeps 30 days of hours per node, and serves them over `GetAuditChain`. `hivectl audit verify DIR --keeper HOST:PORT` checks a chain on disk against them, so a chain rewritten and hashed again from some line on is caught. On server3 with 1,000 combs renewing once a second, each with a new tip, renewals took 103 to 147 ms at p50 against 88 to 95 ms without roots, with none failing.

## 0.0.31

A `hive-uffd` that survives its worker dying, and a prefetch trace that learns from every restore.

- `hive-uffd` merges the prefetch trace from every restore, not just the first. A page joins the trace once two restores faulted on it. On server3, with a working set that shifted from run to run, the merged trace left out 32 to 480 touched pages a run against about 5,000 for the first trace, and the working set was in after a median of 291.9 ms against 513.5 ms.
- `hive-uffd` runs under a watchdog. The process that listens keeps a copy of each VM's regions and userfaultfd and hands the VM to a worker process, and when the worker dies it starts another and hands it every VM still running. A fault the dead worker read and never answered is raised again, and memory the guest gave back still reads as zeros. On server3 the first page a VM touched after its worker was killed came in 2.9 to 6.3 ms in 7 of 10 runs, and in 21.0 to 62.1 ms in the other three.

## 0.0.30

Layers built in process, and VMs served from shared template memory.

- `hive-uffd` serves VMs in minor fault mode. When the snapshot's memory file is on tmpfs or hugetlbfs, or loaded into memory with `Memory::load`, a VMM that maps it privately and registers for minor faults gets each page mapped in from the page cache and not copied, so the VMs restored from one template share their clean pages and a page is copied only when its VM writes to it. On server3, with a 1024 MiB memory file and a 259 MiB working set, each restored VM added no private memory against 259 MiB with copies, and a prefetched restore had its working set in after 180 to 358 ms against 489 to 1124 ms.
- Layers are built in process. `hive-nectar` has its own EROFS writer that reads the tar as it streams in and writes the metadata and data blobs in the same format as `mkfs.erofs --tar=f --blobdev`, so imports and snapshots no longer need erofs-utils on the node. `mkfs.erofs` can still be used with `[images] mkfs` in the comb config or `--mkfs` on `hive-nectar`. On server3, the 11 layers of python:3.12 and python:3.12-slim mount as the same trees from both builders, down to modes, owners, times, links, devices, xattrs and contents, and `fsck.erofs` passes on every one. The data blobs come out byte for byte the same. Importing the largest python:3.12 layer, 642.5 MiB of data, took 10.8 to 13.9 s from a plain tar against 20.3 to 26.4 s with `mkfs.erofs`, and 10.3 to 17.7 s from the gzipped tar against 27.8 to 32.8 s, with the host at a load of 62 to 81.

## 0.0.29

Idle trimming that backs off for cells that need their page cache back.

- Idle trimming backs off for cells that read back what it dropped. A running cell that had to read back at least half of what its last trim gave away waits twice as long before the next one, up to 16 times `trim_idle`, and a trim it did not read back halves the wait again. The comb reads this from `workingset_refault_file` in the cell's cgroup.

## 0.0.28

Faster snapshots and restores of cells that wrote a lot.

- Faster snapshots and restores of cells that wrote a lot. A CAS put stores all the chunks a layer is missing in one batch, checked on a few threads at once, and flushes the filesystem twice for the whole batch instead of a file and a directory per chunk, and a fetch into the layer cache flushes every 64 MiB instead of every 16 MiB batch. A chunk still only gets its name once its bytes are on disk. On server3 a 1000 MiB snapshot went from 159 and 231 s to 118 and 141 s, and its restore from 26.6 and 52.5 s to 20.6 and 24.7 s.

## 0.0.27

Idle container cells give back their cold page cache.

- Idle container cells give back their cold page cache. A running cell idle for `[density] trim_idle` (30 s by default, 0 turns it off) has the comb ask `memory.reclaim` for its inactive file pages, again every 30 s while it stays idle. Its own memory stays, since running cells have no swap, and latency cells are left alone. `hive_memory_trimmed_bytes_total` counts it. On server3, six idle python cells went from 419 to 491 MiB to 235 to 275 MiB, and the next python start in a trimmed cell took 613 to 893 ms against 282 to 329 ms warm.

## 0.0.26

Snapshots of container cells in the comb, and committing a container's changes as a new image.

- Snapshots of container cells in the comb. `Snapshots.Snapshot` freezes a running cell only while what it wrote is read out, then builds that into one more layer on the cell's image while the cell runs on, and returns the new image's id. A create with that id as its source restores it, and `Commit` names a scrubbed snapshot as an image of the project. `hivectl snapshot`, `hivectl commit` and `hivectl create snapshot:ID` use it, and so do `snapshot` and `commit` in the Rust and Python SDKs. Set `[images] mkfs` when `mkfs.erofs` is not on the path. On server3, a cell that wrote 32 MiB was frozen for 184 to 197 ms.
- Committing a container's changes as a new image. `hive-nectar commit UPPER --base IMAGE` and `hive-nectar run --commit on` turn an overlay upper into an OCI layer, with whiteouts, opaque dirs, hard links, xattrs and owners shifted back from the cell's uid range, build it like any import and store a manifest of the base plus that layer with its provenance. Scrubbing is on by default: histories and home credential files are left out, `.git/config` URLs lose their passwords, and a private key or token anywhere else refuses the commit and says where, unless the path is allowed. On server3, committing `pip install numpy requests` in python:3.12-slim took 3.49 s for 74.0 MiB.

## 0.0.25

Content addressed chunks for layer data, and CPU caps with a burst factor.

- Content addressed chunks for layer data. `hive-nectar import-oci` and `import-tar` with `--dedup chunks` keep each data blob as content defined chunks of about 64 KiB (`--cas-avg` to change it) plus a recipe, and write only the chunks the store lacks, so layers that share files share them in the store. On four python images and six django checkouts the store is 495 MiB against 693 MiB with whole blobs, and a django point release adds 4 to 9 MiB of its 55 MiB. Relayout keeps its copies as chunks too, and mounts, `fetch` and `copy` read through the recipe. Imports print how much of the data was new to the store.
- CPU caps with a burst factor. A `standard` or `best_effort` cell's `cpu.max` is its requested cores times `[density] cpu_burst` (default 2.0), so it can use idle cores up to that, while a `latency` cell stays at its request. Spare cgroup leaves in the pool are made with the cap of their class.

## 0.0.24

The LLM gateway for `llm` cells, with token capture and holds, and the gate routing it to every node.

- The gate routes the `Llm` service. `SetRoute` and `Hold` go to every node, leaving out nodes with no gateway, and `Hold` adds up the calls left in flight. `Turns` for a rollout gathers the turns from every node in order and lists the nodes it could not reach in `unreached`, and `Turns` for a cell goes to the cell's node. Tokens can be held to the new `llm` call.
- The LLM gateway. Cells with the `llm` network profile reach `http://llm.hive.internal` (169.254.77.81), and the comb forwards their OpenAI and Anthropic style calls to the project's inference engine with the trainer's key, which the cell never sees. For chat and completions calls it asks the engine for the token ids, and log probabilities when `[network.llm] logprobs` is set, keeps them by the cell's `rollout_id` label within `keep_mib`, and takes them out of the answer, streamed or not, unless the cell asked for them. The new `Llm` service sets a project's route, holds its calls with 503 and Retry-After around a weight sync, and returns the turns. The Python SDK has it as `hive.llm` with `route`, `hold`, `release` and `turns`.

## 0.0.23

The Python SDK verifies, and adapters for verl and slime.

- `hivebox.slime`, grading for slime's coding agent example in `Verify.Run`. Its `run_evaluation` takes the same arguments as slime's and grades the same way, with the pre commands, the diff and the tests in one call to a verifier cell with no network, and `install(swe)` swaps it in for scaleswe tasks with `eval_cmd` or `f2p_script`. A check hivebox could not do raises, so slime aborts the sample instead of training on a 0.
- `hivebox.verl`, tools and an agent loop for verl 0.9. `BashTool`, `EditorTool` and `SubmitTool` work in one hivebox cell per trajectory, made on the first tool call, with a shell that keeps its directory and variables. `HiveAgentLoop` (`hive_agent`) checks each trajectory with `Verify.Run` when it ends, sets the sample's reward score, puts the verdict in its extra fields and stops the cell. A failure in hivebox gives a reward of 0 with `hive_infra_error` set, so the trainer can mask the sample.
- The Python SDK has `hive.verify`, which runs `Verify.Run` and returns a `VerifyResult` with the verdict, the tampered paths, the scores and an error that says whether to mask the sample. `AsyncHive` takes `http://host:port` for a gate without TLS and reads the token from `$HIVE_TOKEN`.

## 0.0.22

The rollout worker, and `Verify.Run` through the gate.

- The gate routes `Verify.Run`: to the comb that owns the subject cell, or with no subject to the node waggle picks for the verifier cell, trying up to two more nodes when one has no room. Tokens have a new `verify` op, and the verifier cell counts against the project's quota. `hive-pollen` sends `$HIVE_TOKEN` so it can run through a gate. On server3 the same 16 samples with eight cells in flight took 25.8 s straight on the comb socket and 26.5 s through the gate.
- `hive-pollen`, the rollout worker. It reads tasks as lines of JSON, makes each task's sample cells in one batched call, keeps at most `--max-inflight` sample cells alive, runs a script of commands in each, checks each sample with `Verify.Run` and writes a trajectory with a reward (1, 0, or masked when hivebox failed) as a line of JSON as soon as the sample is done. On server3, 16 samples of swe-requests-2317 with three verifier runs each took 99.9 s with one cell in flight, 47.0 s with four and 34.9 s with eight, and all 48 rewards were right.

## 0.0.21

A setup boost that lifts a cell's CPU quota until it is ready, and the first cut of the verifier service.

- A setup boost for cells. `burst_until_ready` on a cell spec, `hivectl create --boost`, gives the cell `[density] setup_boost` (default 4) times its CPU quota until the caller marks it ready with `UpdatePolicy` (`hivectl ready`, `Cell.ready()`) or the boost runs out. On server3 at load 85, a stdlib compile with four workers in a half core cell went from a median of 9.5 s to 4.6 s with it. `UpdatePolicy` is served for `ready` only so far.
- `Verify.Run` on the comb, `hivectl verify` and `Client.verify()`. It takes a subject cell's git diff, leaves out and reports changes to protected paths, applies the rest in a fresh cell with no network, adds hidden files and runs the tests as many times as asked. On server3 with the swe-requests-2317 image, the unfixed subject failed, the gold fix passed and edits to protected test files were caught, at a median of 6.1 s per verify for three test runs.

## 0.0.20

Core scheduling cookies per CPU class, and `Qos` in the SDK so the cpu-qos bench can set a cell's class.

- On a host with SMT, the comb gives every cell a core scheduling cookie for its CPU class, so the two threads of a core never run a latency cell next to a best effort one. On a host without SMT it says so at start and runs as before. The SDK now exports `Qos` and `IdleAction`, so callers can set a cell's class.

## 0.0.19

Two more pieces of M2: the pressure brake now pauses and reclaims idle cells, and traced layers can be relaid so a lazy mount fetches the trace in a few long reads.

- While the memory pressure brake is on, the comb pauses cells that have been idle for 30 s and reclaims paused cells' memory at once instead of after 10 minutes. Latency cells and cells whose idle action is to stop are left running. The wait is `density.pressure_idle`, and `density.psi_source` names the pressure file the brake reads.
- `hive-nectar relayout IMAGE` stores a copy of each traced layer's data with the traced chunks first, and a lazy mount of the new image fetches the trace from that copy in a few long reads. On server3 against MinIO, the 160 traced chunks of a python image were in after 1.9 to 4.1 s from the relaid copy in 19 to 29 reads, against 3.5 to 5.5 s and 51 to 60 reads with the trace leading and 5.5 to 7.9 s and 56 to 60 reads in order.

## 0.0.18

The first two pieces of M2: a memory pressure brake on the comb, and a userfaultfd page server with trace prefetch.

- `hive-uffd` serves a restored VM's memory. It takes Firecracker's region list and userfaultfd over a socket, answers faults from the snapshot's memory file, gives zero pages for memory the guest gave back, and records the order pages were first touched. Later restores prefetch that trace before the VM asks, in copies of up to 2 MiB. `examples/restore.rs` measures it.
- The comb has a memory pressure brake. Once its cells stall on memory more than 20% of the last ten seconds (`some avg10` in the cgroup's `memory.pressure`), it takes no new cells, tells scout it has no room, and asks the kernel for an eighth of the best effort slice's memory back every second. Admits come back once the stall falls under 10%. The limit is `density.psi_stop_admit`, and 0 turns the brake off. Pausing idle cells under pressure is not in yet.

## 0.0.17

A deterministic cluster simulation, and the lease and keyed create bugs it found.

- `hive-sim` runs a whole cluster in one thread from a seed: a keeper, scout, three gates and five combs with a spare, clients that make and end keyed and unkeyed cells for three projects, and faults on a schedule (crashes, power loss, cuts, lost and repeated messages, clock drift, a keeper that goes down, a machine replaced by the spare). The same seed gives the same run, digest and all. It checks that no cell id is used twice, every cell ends exactly once, no comb serves a node another comb serves, no comb keeps cells from an older epoch, a key runs at most one cell unless the gates saw the key's nodes differently, and a project's live cells stay within its quota plus the bound the shares allow. `hive-sim --seeds N` sweeps seeds, and `--trace` prints the messages of one.
- A keyed create no longer makes a second cell when its home is full. A comb that turned a key away for room now turns it away again for five to ten minutes, so a create sent again walks past it to the node that made the cell, and only when every node turns it away does the gate walk once more with `x-hive-anyway`, which the first node with room takes. The simulation is what found the second cells.
- The gate now answers `CELL_LOST` for a cell on a node whose lease ran out, or on a node that came back in a newer epoch, as the spec says. It reads the keeper's node list once a second for this. Before, a cell on a killed node came back as not found once scout forgot the node.
- A comb can no longer keep running cells while a new comb serves its node. The keeper now holds a node's epoch while its lease is live, so a comb that comes up without the old epoch, such as a spare taking the name of a machine that is still serving, waits until the old lease runs out instead of bumping the epoch at once. The comb also writes the end of its lease to its lease file on every renew, and if it cannot reach the keeper before that time it stops its cells and exits. The simulation found both gaps, in seeds 311 and 374.

## 0.0.16

Lazy layer mounts with prefetch traces, memory given back by paused cells, and the E2B API on the gate.

- E2B compatibility. With an `[e2b]` table in its config the gate serves the E2B REST API (create, get, list, kill, pause, resume, connect, timeout) and the envd process and file calls the SDK makes, on the same address, with the sandbox of each envd call named in the `E2b-Sandbox-Id` header. The image comes from a metadata key such as slime's `swe/image`. A user named in an envd call is looked up in the cell's own `/etc/passwd`, and a command run as that user gets its `HOME`, `USER` and `LOGNAME`. On server3 under load 49 to 58, slime's `sandbox.py` on the E2B Python SDK 2.52.0 ran 8 sandboxes of a SWE-bench `requests` image at once, all 8 passing with pytest at 32 passed each, in 39.35 s. Create took 1.22 s at p50 and 17.41 s at worst, a command as `agent` 2.00 s at p50 and the test run 7.52 s.
- Paused cells give their memory back. A paused cell stays frozen with its memory in place for `reclaim_after` (10 minutes by default), and then the comb lets its cgroup swap and pushes out what the kernel can, so a resume pages it back in. A cell paused for `pause_ttl` (24 hours by default) is stopped. `ExtendTtl` is served, and `hivectl extend` and `hivectl create --on-idle` call it. With a real Python image on server3, which has no swap, a cell went from 179 MiB to 100 MiB once reclaimed, a freeze took 63 ms, and Python started in 294 ms after a reclaim.
- Prefetch traces. A layer can name a trace, the chunks of its data that a run of the image read in the order it first read them, and a lazy mount fills those chunks first. `hive-nectar run IMAGE --mode trace` mounts an image lazily, runs a command chrooted into it, stores what the command read as traces and prints the name of the traced image, and `--mode whole` and `--mode lazy` time the same run without. On server3 under load 34 to 46, starting Python 3.12 and importing 12 stdlib modules from an image in MinIO took 14.0 to 26.5 s in all with a whole fetch, whose mount took 5.6 to 9.7 s, 11.5 to 20.3 s lazily with a mount of 2.0 to 2.9 s, and 11.5 to 14.4 s lazily with a trace of 160 chunks and a mount of 1.0 to 1.8 s. The host was loaded enough that the import alone took 8 to 17 s, so these three runs of each show the spread more than the gap.
- Layers can be mounted before their data is in. `hive-blockd` serves a read only block device from user space through the kernel's NBD driver, since the node kernels ship ublk in a package they do not have, and `Layers::lazily` puts each layer's data blob on one, filled from a lazy fill, with the rest fetched in the background. The comb turns it on with `images.lazy = true`, and `images.store` can now be a bucket URL. On server3 under load 44 to 56, with a real Python 3.12 image of 5 layers and 179 MiB of data: from a local store the whole fetch and mount took 6.6 to 15.7 s and the lazy mount 2.05 to 2.18 s, the first file read 39 to 311 ms after it, and the fill finished 4.5 to 7.8 s after the mount began. From MinIO the whole fetch took 13.1 to 22.7 s and the lazy mount 1.64 to 2.87 s, with the first file read 211 ms after it and the fill done 6.4 s after the mount began, in 55 requests. All 6656 entries read back the same as from the whole fetch, and once small reads skipped the blocking thread a full read of the tree took 74.7 s lazily against 74.9 s from loop devices, and from MinIO 45.6 s lazily against 37.4 s.

## 0.0.15

Nectar blobs from S3, filled lazily chunk by chunk.

- `hive-nectar` can fill a blob lazily. The importer now stores a layer's leaves, the BLAKE3 chaining value of each 256 KiB chunk, as a blob of its own and names it in the manifest, and `Cache::lazy` opens a blob from them. Reads fetch only the chunks they touch, 1 MiB at a time from the first missing one, check each chunk against its leaf as it lands, and `fill_rest` fetches the rest in the background in the store's ideal read size, in a given order first. The leaves merge up to the blob's name, so a wrong list is refused before any chunk is trusted. Chunks are readable once written, the map is synced at most 50 ms later after the part file, and a restart keeps what the map has. When the last chunk is in, the blob is renamed into the cache as a whole fetch would. On server3 against MinIO with a 1 GiB blob, under load 33 to 44: opening took 0.21 to 1.04 s, the first 4 KiB read 0.36 to 1.31 s from cold, 200 random 4 KiB reads had p50 99 to 108 ms and p99 555 to 788 ms, and the rest filled at 21 to 36 MiB/s, in 483 to 486 requests for the whole GiB. A whole fetch of the same blob took 96 to 142 s on that run.
- `hive-nectar` keeps blobs in an S3 bucket as well as a directory, with `S3Store`, which signs its own requests with Signature Version 4 on the hyper client the tree already has. A batch of reads becomes ranged GETs, with reads that follow on from each other merged into one, 16 in flight. Blobs up to 64 MiB go up in one PUT and bigger ones as a multipart upload in 16 MiB parts, four at a time, and a failed upload is aborted. Requests that fail with a 5xx, a dropped connection or a 60 s timeout are tried three times. Only `http://` endpoints work until the tree has a TLS stack. The CLI takes `--s3 URL` in place of `--store DIR`, and `copy IMAGE --store DIR --to-s3 URL` puts an image and its layers in a bucket, manifest last. On server3 at a load of 43 to 65 against MinIO on the same machine, with 8 blobs of 64 MiB, filling an empty L1 cache ran at 45 to 77 MiB/s from the bucket and 49 to 70 MiB/s from a directory, both held back by the cache's synced writes. A random 4 KiB read took 27.8 ms at p50 from the bucket and 0.75 ms from the directory, and a 1 MiB read 87.6 ms and 4.5 ms.

## 0.0.14

The gate takes API keys and quota shares from the keeper, comb leases, and biscuit tokens.

- Biscuit tokens. `hivebox.v1.Tokens/Mint` on the gate takes an API key and gives back a token for the key's project that lasts at most an hour, optionally held to some cells and some calls. The keeper signs it with an Ed25519 key it makes on the first token and keeps in the Raft log, and the gate checks tokens with the public key it reads along with the API keys, so a call with a token never asks the keeper. Anyone holding a token can narrow it down further with `hive_auth::narrow`, offline, and a narrowed token can never do more than the one it came from. A token stops working within a second of its API key being revoked. The gate checks a token held to some cells against the cell a call is about, including Exec and Files calls it forwards without decoding. On server3, checking a token's signatures took 168 us at p50 and asking what it allows took 40 us, and the gate keeps checked tokens so the signatures are checked once. Through a live gate and comb, running `true` in a cell took 5.1 and 6.4 ms at p50 with the key and 9.3 and 6.6 ms with a token held to that cell, over 300 runs each in turn on a machine at a load of 20, and the same token on another cell was refused with `PERMISSION_DENIED`.
- Gates hold creates to each project's quota of live cells and creates a second, spending shares the keeper hands them so a create never waits on the keeper. A gate asks for about one and a half times what the project asked of it lately, and asks again when half the share is spent or half its 30 s life is gone. When a gate gets less than it asked for, every gate asks again within a second and the keeper splits the quota evenly, so a busy gate cannot starve the others. A create past the quota gets `QUOTA_EXCEEDED`. While the keeper is out of reach a gate keeps spending the share it had. On server3 with two gates making cells at 40 a second against a quota of 150 cells, 152 were made and the rest refused. Against a quota of 20 creates a second at 60 a second, the gates made 48 and 41 in the first two seconds while the shares settled, then 20 a second, with the refusals split 160 and 191 between the gates.
- The gate takes API keys from the keeper. With `gate.keeper` set it reads every key once a second and swaps the set in, keeps the last set while the keeper is out of reach, and still takes the `[[key]]` entries in its config, which win over the keeper's. On server3 a key made in the keeper worked at the gate after 925 to 1,048 ms, a revoked one was refused after 853 to 993 ms, the gate went on taking calls with the keeper leader stopped and with every keeper stopped, and 100 cells made, run and stopped through the gate with a keeper key had no failures.
- A comb with a `[keeper]` section registers with the keeper before it opens and takes its node index and epoch from it, then renews its lease a third of the way through each lease. It keeps the last lease in `lease` in its data directory and asks for that epoch back, so a restart within the lease keeps its cells, and a restart after it ran out comes back in a new epoch and stops the cells from the old one. A comb whose lease is gone, or that could not reach the keeper before its lease ran out, stops and leaves its cells for the next start to fence. The keeper never hands a comb an epoch at or below the one it asks for, so a keeper that lost its data still fences old cells. On server3 against three keepers, a comb kept its epoch and its 5 running cells through a leader stop and a restart, came back in the next epoch and stopped all 5 after a restart 13 s later, and exited 8.8 s after every keeper stopped with a 10 s lease.

## 0.0.13

The keeper, Connect on the gate, and cell ids fenced by epoch.

- `hive-keeper` keeps the cluster's projects, API keys and comb leases in a three member Raft group, on openraft with the log and the state in one redb file per member. It serves `hivebox.internal.v1.Keeper` for projects, keys, comb registration, lease renewal and the node list, and the Raft messages between members go over gRPC on the same port. A comb that registers gets a node index and an epoch, keeps the epoch while it renews its lease in time, and gets a new one when the lease ran out. Writes that arrive together go in one log entry, and a follower passes writes to the leader. On server3 with three members on one machine, 1,000 combs renewing once a second got 999 renewals a second for 60 s with none failing, 148 ms at p50 and 506 ms at p99, and each member used 18 to 32 MB of memory. With the leader stopped partway through, 4 of 40,930 renewals failed and the rest went on at 994 a second.
- The gate takes Connect calls as well as gRPC, on the same port, over HTTP/1.1 or HTTP/2. Unary methods take `application/json` or `application/proto`, streaming ones take `application/connect+json` or `application/connect+proto`, and errors come back as Connect error JSON with the hivebox reason in `details`. The gate turns each call into gRPC and routes it the same way, so a client needs nothing but an HTTP library, and `curl` works. On server3 through the gate to one comb, a Python client using only the standard library made 300 cells in 3 calls of 100 in 13.8, 7.8 and 8.8 s against 9.3, 8.4 and 12.3 s over gRPC, and ran `true` in each cell one call at a time at a p50 of 13.8 to 15.3 ms. The gate used 11 MB of memory.
- A cell id from an older epoch of its node is lost, not missing. Scout carries each comb's epoch, and the gate answers `CELL_LOST` for an id from before it without asking the comb. A comb that opens in a new epoch fails the cells it still had from older ones with cause `NODE_LOST`, and answers `CELL_LOST` for their ids too.
- A gate forwarding an Exec or Files call whose body knows its exact size no longer panics. The gate's own size hint set the lower bound past the upper one.

## 0.0.12

The gate: one endpoint for a cluster of combs, with scout feeding it the nodes and waggle placing its batches.

- `hive-gate` serves the Cells, Exec and Files APIs for a whole cluster. It checks an API key from `authorization: Bearer`, keeps only the key's BLAKE3 hash in its config (`hive-gate key PROJECT` makes one), follows scout for the nodes, places batches of up to 32,768 cells with waggle and sends them to the combs 1,024 at a time, and places again what a comb turns away. Calls about one cell go to the node in the cell id, and Exec and Files calls pass through as bytes after the gate reads the cell id from the first message. List pages across the nodes, and watch, pause, resume and stop by labels ask every node and merge the answers. On server3 through the gate to one comb, 300 cells took 22.9 and 24.7 s against 31.0 and 26.3 s straight to the comb, and the gate used 8 MB of memory and under a second of CPU for 600 creates and 600 runs.
- Waggle's burst cap is soft now. A batch goes to nodes up to their burst caps first, and what is left then goes wherever there is room, since a comb queues creates past its cap. Before, a gate in front of one comb turned away all but 128 of a 300 cell batch; on server3 it now makes all 300, as the comb does on its own.
- A comb can serve its API on TCP for gates as well as on its Unix socket. Set `node.listen`, and the comb advertises `http://ADDR` to scout unless `scout.advertise` says otherwise. A listen address of 0.0.0.0 needs `scout.advertise` to name the address gates should use.
- Gates and placers can follow scout. `hivebox.internal.v1.Scout/Watch` sends the whole cluster once and then, after each snapshot, only the nodes that changed, with a node's 4 KiB layer filter only when it changed and the nodes scout forgot. `hive_scout::follow` keeps a `Mirror` of scout's snapshot from that stream and reconnects when it breaks. On server3 with 1,000 nodes reporting once a second, a follower got 95 KB a second where the whole cluster at every snapshot would be 34 MB a second, and seven more followers added 0.3 to 0.9 s of scout CPU over a 70 s run.

## 0.0.11

The first pieces of the cluster: placement, and scout collecting what every comb reports.

- `hive-waggle` places a batch of cells: it samples `clamp(2n, 8, nodes)` nodes weighted by room, fills them from a heap a share at a time, packs for cached layers below 60% memory use and spreads above it, and keeps an overlay of what it placed until the node's report counts it. On server3 a single cell on 1,000 nodes takes 40.9 us of CPU at p50, and 32,000 cells on 1,000 nodes take 1.18 ms.
- `hive-scout` folds node reports into a versioned snapshot for waggle and the gate. It drops late reports and ones from a replaced comb, carries the 4 KiB layer filter over when a report leaves it out, shows a node silent for 3 s as down and forgets it after 60 s, and flags a report as urgent when a node is new, came back, changed health or moved its room by more than 5%. On server3 taking a report costs about 1 us of CPU and a snapshot of 1,000 nodes 311 us at p50.
- Combs report to scout over gRPC. `hive-scout` is now a binary that takes reports on `hivebox.internal.v1.Scout/Report` and publishes a snapshot every 100 ms, or within 10 ms of an urgent report, and a comb with a `[scout] endpoint` in its config keeps a stream open and reports once a second and at once when its cells or memory move by more than 5%. On server3 at load 21 to 30, scout kept 250 streaming nodes with every report answered, 3.3 ms at p50, on 4.5% of a core, and 1,000 nodes on 12% of a core. Over four scout restarts the comb was back in the snapshot 352 to 1,180 ms after scout started again.

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
