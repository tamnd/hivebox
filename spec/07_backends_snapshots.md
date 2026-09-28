# Isolation backends, snapshots and fork (`hive-cell`)

> Crates: `hive-cell` (trait + registry), `hive-cell-wasm`, `hive-cell-proc`, `hive-cell-oci`, `hive-cell-fc` (Firecracker), `hive-cell-ch` (Cloud Hypervisor), `hive-cell-qemu`, `hive-uffd` (page-fault server), `hive-snap` (snapshot tree).
> A cell is one sandbox instance, regardless of backend.

## 1. Backend tiers

| Tier | Driver | Isolation | Start (target) | Density target / node | Use |
|---|---|---|---|---|---|
| T0 `fncall.wasm` | Wasmtime embedded (pooling allocator, `InstancePre`, CoW memory images) | Wasm SFI + WASI caps | ≤1 ms (instantiate ≈5 µs) | 10K+ concurrent | verifiers, pure tools, OJ in wasm-compilable langs |
| T0 `fncall.proc` | Pre-forked warm worker processes in a hardened container (per-tenant VM host) | ns + seccomp + Landlock + cgroup | ≤5 ms (warm) | 1K workers | OJ C++/Py/Java, compile jobs, GPU kernels (MIG) |
| T1 `container` | `libcontainer` (youki) in-process, or `crun` exec fallback | ns + cgroup v2 + seccomp + Landlock + AppArmor/eBPF; inside a per-node "shield VM" in untrusted mode | ≤150 ms p50 / ≤400 ms p99 | 3,200 | SWE, tool use, terminal tasks |
| T2 `microvm` | Firecracker (jailer) by default; Cloud Hypervisor when virtio-fs/hotplug/VFIO is needed | KVM + minimal virtio + jailer + seccomp | ≤150 ms p50 restore-from-template / ≤300 ms p99; ≤250 ms cold boot | 800 | security tasks, untrusted tenants, docker-in-sandbox |
| T3 `fullvm` | QEMU via QMP (libvirt optional) | KVM, full devices, virtio-gpu | seconds (snapshot restore) | tens | Android, Windows, GUI/computer-use, COTS OS |

Rules (from DSec section 2.2, plus our additions):

- The API does not hide tier differences. Callers pick `backend`. A `backend: auto` hint lets the scheduler choose T1 or T2 based on project policy (`min_isolation`).
- Untrusted mode: T1 and T0-proc never run directly on bare metal alongside other tenants. As in DSec, they run inside a shield VM (one per node or per tenant, QEMU/CH with large memory). Trusted or single-tenant clusters may disable shield VMs for density.
- T2 runs on bare metal (no nested virt). On clouds without nested virt, T2 is unavailable. PVM is a future option.

## 2. The `CellDriver` trait

```rust
/// One implementation per backend. Object-safe; registered in DriverRegistry at startup.
#[async_trait]
pub trait CellDriver: Send + Sync + 'static {
    fn kind(&self) -> BackendKind;
    fn caps(&self) -> DriverCaps;           // snapshot: None|Disk|DiskMem, fork: bool, gpu, pause, max_mem...
    async fn probe(&self) -> Result<NodeFit>;  // kernel features, KVM, capacity hints

    async fn prepare(&self, spec: &CellSpec, rootfs: &RootfsPlan) -> Result<Prepared>; // no CPU yet
    async fn start(&self, p: Prepared) -> Result<CellHandle>;
    async fn pause(&self, h: &CellHandle, mode: PauseMode) -> Result<()>;  // Freeze | Reclaim | SnapshotKill
    async fn resume(&self, h: &CellHandle) -> Result<()>;
    async fn snapshot(&self, h: &CellHandle, kind: SnapKind) -> Result<SnapshotRef>; // Disk | DiskMem
    async fn restore(&self, snap: &SnapshotRef, spec: &CellSpec) -> Result<CellHandle>;
    async fn fork(&self, h: &CellHandle, n: u32, spec: &ForkSpec) -> Result<Vec<CellHandle>>;
    async fn resize(&self, h: &CellHandle, r: &Resources) -> Result<()>;   // mem hotplug / cgroup
    async fn stop(&self, h: &CellHandle, grace: Duration) -> Result<ExitInfo>;
    fn guest_channel(&self, h: &CellHandle) -> GuestChannel;  // UDS | vsock | in-proc (wasm)
    fn metrics(&self, h: &CellHandle) -> CellMetrics;         // cgroup/VMM stats, cheap
}
```

Third-party drivers come in two forms. They are either (a) Rust crates compiled in behind a cargo feature, or (b) out-of-process drivers that speak the same trait over a gRPC `DriverService` on a UDS, in the style of a containerd shim. The second form lets people plug in gVisor (`runsc`), Kata, Hyperlight or macOS Virtualization.framework without touching core.

`DriverCaps` drives admission and API validation. For example, `fork` on T1 is rejected unless the driver is CRIU-capable.

## 3. Driver designs

### 3.1 T0 `fncall.wasm`

- One `wasmtime::Engine` per node, with a pooling allocator sized for the maximum number of concurrent instances. The `InstancePre` cache is keyed by component digest. Memory is initialized from CoW images (memfd + `madvise` reset), so instantiation takes ≈5 µs.
- WASI 0.2/0.3 component model. Capabilities are a preopened virtual dir (tmpfs-backed) and stdout/stderr capture. There is no network unless policy grants `wasi:sockets`.
- Limits: fuel or epoch interruption for CPU time, `StoreLimits` for memory, and a wall-clock deadline.
- Sandboxed languages: Python via componentized CPython (`componentize-py`), JS (StarlingMonkey), and Rust/C/C++/Go (tinygo) targets.

### 3.2 T0 `fncall.proc`

- A warm pool of zygote workers per (runtime image, language) runs inside a hardened container. Each invocation does `fork()` from the zygote, then `unshare`, then seccomp + Landlock, then execs the task, captures output, kills the process group and resets tmpfs. This is the Catalyzer "sfork" idea applied at the process level.
- GPU FnCall uses MIG-partitioned exclusive instances or shared mode. Code compiles on CPU FnCall and executes on GPU FnCall, so the GPU is never held during compilation (DSec).
- Python interpreters with numpy/torch already imported are forked from the zygote, which skips the import cost.

### 3.3 T1 `container`

- The OCI runtime is youki `libcontainer` running in-process, so there is no fork/exec of a runtime binary per create. The fallback is the `crun` binary. We benchmark both. The youki README reports crun at 47 ms vs youki at 112 ms for create to delete; re-measure on current versions.
- Rootfs: overlayfs over EROFS lowers (06), with the upper on XFS with project quota.
- cgroup v2 per cell: `cpu.max`/`cpu.weight`/`cpu.idle`, `memory.max`/`memory.high`/`memory.swap.max`, `pids.max`, `io.max`, and `memory.oom.group=1`.
- Security: a user namespace (root in the cell is not root on the host), a seccomp allowlist generated with `seccompiler`, Landlock ABI ≥4, and an AppArmor profile that protects the agent socket and logs even from in-cell root (a DSec lesson). Also `no_new_privs`, masked `/proc` paths including `/proc/kpagecgroup`, `/proc/kpageflags`, `/proc/kcore` and `/sys/kernel/debug` (after the DSec kernel-crash incident), and a read-only `/sys`.
- The guest agent `hive-drone` is bind-mounted read-only from the host and launched either as a PID 1 wrapper (tini-like) or as a sidecar process. The channel is a UDS.
- Docker-in-sandbox is not supported in T1. Use T2.

### 3.4 T2 `microvm` (Firecracker)

- Each cell gets one Firecracker process under the jailer (chroot, cgroup, netns, uid drop). The driver controls it over its REST-on-UDS API with a `hyper` UDS client (`hive-cell-fc::api`).
- Devices: virtio-blk (ublk-backed writable disk), virtio-pmem (EROFS base/toolkits, DAX, shared within a trust domain; see 06 section 6), virtio-net (tap), vsock (drone channel), balloon (stats + free-page-reporting), and optionally virtio-mem.
- Guest kernel: a minimal 6.x config (virtio, EROFS, overlayfs, ext4, DAMON, vsock, cgroup v2, no modules), with boot args `quiet` and serial off. The guest init is `hive-drone` (a static Rust binary) as PID 1. It mounts the overlay, then spawns sessions.
- Creation is a restore, not a boot (the E2B model):
  1. For each (kernel, base-layer-set, vCPU/mem shape), a template snapshot is produced on first use: boot, wait for the drone to be ready, quiesce, then `snapshot/create` (Full).
  2. The template snapshot memory file is stored in L1 (local) and L2 (3FS). A cell is created by restoring with the `Uffd` backend, served by `hive-uffd`.
  3. After restore, the drone receives the per-cell identity via vsock (not MMDS, which is not in snapshots), re-seeds the RNG (VMGenID on ≥5.18 guests), attaches the cell's writable disk and workspace pmem, remounts the overlay, and sets hostname, env and user.
- `hive-uffd` (Rust, `userfaultfd` crate) is one supervised process per node. Firecracker hangs if the handler dies, so it runs under a watchdog with per-VM fds, and a crash only affects the VMs it served. A restart re-attaches using fds stored via SCM_RIGHTS.
  - It serves faults from a shared MAP_PRIVATE-style page source of the template. There is one host copy of clean pages across all clones, either via `UFFDIO_COPY` from an mmap'd file in page cache, or via `UFFDIO_CONTINUE` in shmem/hugetlbfs "minor fault" mode so clean pages are mapped rather than copied.
  - REAP/FaaSnap prefetch: record the working set of the first N restores of a template, and have later restores pre-install that set in a batch. REAP reports a 3.7× cold-start reduction, and FaaSnap gets within 3.5% of warm.
  - It handles `UFFD_EVENT_REMOVE` from the balloon by serving zero pages.
  - Remote templates stream chunks from L2 in 256 KiB to 2 MiB units.
- Cloud Hypervisor path: the same UFFD handler. CH ≥v52 has native demand-paged restore, and the driver uses it when available.
- Pause: `PauseMode::Freeze` (vCPU pause, memory kept) is for short suspensions. `SnapshotKill` (snapshot mem+state, kill FC, resume by restoring) is for preemption (DSec section 6.3). Pausing a 4 GiB VM should take ≤2 s (memory written to local NVMe, dirty pages only when diff snapshots are enabled).

### 3.5 T3 `fullvm`

- QEMU is driven via QMP (`qapi` crate), with no required libvirt dependency (libvirt is an optional driver). Disks are OverlayBD over ublk. Snapshots use QEMU migration-to-file plus a disk snapshot. GPU is virtio-gpu (venus/virgl) or vGPU. Android uses Cuttlefish/goldfish images.
- This tier is used rarely. Exec and files keep API parity through `hive-drone` (Linux/Android) or a Windows service port of the drone.

## 4. Snapshot model (`hive-snap`)

```
SnapshotRef { id, kind: Disk | DiskMem, backend, parent: Option<SnapshotId>,
              layers: Vec<LayerRef> /*disk*/, mem: Option<MemImageRef>, vmstate: Option<BlobId>,
              created_from: CellId, project, labels, ttl }
```

- Disk snapshot (all backends except wasm): seal the writable upper into an immutable layer. For containers that means `mkfs.erofs` of the upper dir including whiteouts. For microVMs it means sealing the OverlayBD upper and starting a new upper. The cost target is O(dirty bytes), and ≤200 ms for ≤100 MiB dirty.
- DiskMem snapshot (T2/T3; T1 optionally via CRIU): the disk snapshot plus a memory image (diff when supported) plus VMM state.
- Snapshots form a tree through parent pointers. They are stored as layers in the CAS, so siblings dedup against each other.
- `pack_diff` (DSec) is a Disk snapshot plus a scrub policy, registered as a new image manifest (11 section 4).

## 5. Fork (beyond DSec)

Fork is for tree search, GRPO-tree and best-of-N from a shared prefix. One API, `fork(cell, n, placement)`, covers two mechanisms:

| Mode | Mechanism | Target latency | Scope |
|---|---|---|---|
| `vm` (coarse) | T2: pause → diff/full mem snapshot (local) → N restores via `hive-uffd` sharing clean pages; disks via reflink (XFS `FICLONE`) or OverlayBD seal+N uppers | ≤300 ms for N≤16 on same node; cross-node = snapshot upload + restore (≤2 s) | full machine state incl. running processes |
| `proc` (fine, in-guest) | DeltaBox-style: drone freezes session process tree, CRIU incremental dump / template-process fork; overlay upper cloned via reflink | ≤20 ms | processes of a session + FS; not kernel state |

Published reference points: DeltaBox (arXiv 2605.22781) reports checkpoint in 14.6 ms and restore in 5.1 ms, at ≈11 MB per fork. Morph Infinibranch claims <250 ms (vendor figure). E2B supports up to 100 clones per call.

Fork children get a fresh identity (VMGenID, hostname, cell id, network addresses) and inherit the parent's network policy.

The scheduler (04) co-locates children with the parent unless `spread=true`, because fork locality means page sharing.

## 6. Memory density (T2 focus)

| Mechanism | Use | Expected effect (DSec) |
|---|---|---|
| virtio-pmem + DAX for RO layers | trust-domain sharing | −40% peak host mem |
| Free page reporting (order-9) + guest DAMON reclaim (`DAMON_RECLAIM`, min_age e.g. 60 s) | all VMs | −21% time-integrated |
| Shared template pages via UFFD minor faults | restored VMs | clean pages shared across clones |
| Balloon inflate on `pause(Reclaim)` | idle/parked cells | reclaim before snapshot |
| KSM: OFF across tenants (side channels, CVE-2024-0564); optional within a trust domain | n/a | n/a |
| Hugepages (2 MiB hugetlbfs) for template memory | optional | fewer faults, less TLB |

Containers use `memory.high` soft limits and `memory.reclaim` on pause. Swap is enabled only on pause (`memory.swap.max`), and resume issues a `MADV_WILLNEED` prefetch (DSec).

## 7. CPU QoS

- Each cell has `qos: latency | standard | best_effort` (an API field, default `standard`).
- `best_effort` maps to cgroup `cpu.idle=1` (SCHED_IDLE semantics). `latency` maps to a higher `cpu.weight` plus a core-scheduling cookie per QoS class (`prctl(PR_SCHED_CORE, ...)` applied to all threads, including vCPU threads), so best-effort work never runs on the SMT sibling of latency-sensitive work. DSec saw the slowdown drop from +45.2% to +17.3% with this.
- Hard caps: `cpu.max` = requested cores × burst factor (default 2×) for `standard` and `best_effort`.
- Setup-phase boost: the optional `burst_until_ready` gives temporary quota during install/build (the CPU-heavy phase), then drops to the steady quota.
- vCPU threads of T2 are placed in the cell cgroup. FC VMM and API threads go in a separate low-priority system cgroup.

## 8. Backend conformance suite

Each driver must pass `hive-cell-conformance`, which runs in CI on real KVM hosts. It covers create/exec/files/stream/pause/resume/snapshot/restore/fork/stop, resource limits (OOM, pids, disk), network policy, escape probes (10 section 6), a 1K-cell soak, and chaos tests (kill the driver mid-operation and check for no leaked mounts, cgroups, taps or uffd).
