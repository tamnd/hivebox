# Node agent: hive-comb

> There is one `hive-comb` per node, and it is authoritative for its cells. Targets: 150 creates/s burst (microVM), 300/s (container), 2,500 to 3,200 cells per node, at least 50K exec/s, and restart without touching cells.

## 1. Internal structure

```
                      ┌──────────────────────────── hive-comb ─────────────────────────────┐
 gate (mTLS h2) ────▶ │ api (tonic)  ── admission ── lifecycle actor shards (by cell hash) │
 scout ◀── reports ── │ reporter                        │                                   │
 keeper ◀─ lease ──── │ registrar(epoch)                ▼                                   │
                      │  ┌──────────┐ ┌───────────┐ ┌──────────┐ ┌──────────┐ ┌─────────┐   │
                      │  │ pools    │ │ nectar    │ │ drivers  │ │ guard    │ │ density │   │
                      │  │ netns/tap│ │ mount/    │ │ registry │ │ eBPF +   │ │ mem/cpu │   │
                      │  │ cgroup/IP│ │ attach/L1 │ │ (07)     │ │ DNS (12) │ │ ctl     │   │
                      │  └──────────┘ └───────────┘ └──────────┘ └──────────┘ └─────────┘   │
                      │  drone mux (UDS/vsock) · WAL (redb) · telemetry/audit shipper        │
                      └──────────────────────────────────────────────────────────────────────┘
```

- Concurrency model: comb uses the tokio multi-thread runtime. Cells are sharded into `S = 64` lifecycle actors (bounded mpsc), which serializes per-cell transitions without global locks. Data-plane exec traffic bypasses the actors and goes through a direct drone mux lookup in `scc::HashMap<CellId, CellHandle>`.
- Privilege split: privileged ops (netlink, mount, cgroup create, ublk ctrl, bpf) go through a small `comb-priv` helper task with a narrow typed command enum. The API-facing code runs under a restricted seccomp profile where feasible.

## 2. Admission and create pipeline

```
admit -> reserve(pool slot, ip, cgroup, cpu class) -> rootfs(nectar attach, prefetch start)
      -> driver.prepare -> driver.start|restore -> drone.hello(nonce, version) -> policy attach
      -> RUNNING (WAL) -> ack
```

- Each backend has its own create semaphore. This is configurable, and the defaults are 64 in flight for microVM and 128 for container.
- Each stage has a deadline and a rollback. Reservation objects are `Drop`-guarded and return their slots to the pools.
- A group create (`count>1` with the same spec) amortizes the work: one layer attach, one prefetch trace, and parallel starts.

## 3. Resource pools

Pools are the real throughput lever.

| Pool | Pre-created unit | Refill | Recycle |
|---|---|---|---|
| netns | netns + veth pair (container) or tap (microVM) wired to node bridge / eBPF, IP assigned | background, rate-limited to avoid RTNL storms (≤200/s), target depth = 2 s of burst | flush conntrack, reset eBPF map entries, return |
| cgroup | pre-made `hive.slice/cell-<slot>` leaf with controllers enabled | batch mkdir | `cgroup.kill`, wait empty, reset limits |
| IP | /20 per node (4,094 addrs) IPv4 + ULA IPv6 | static | quarantine 30 s before reuse |
| template VMs | per hot template: pre-restored paused Firecracker (warm pool) | per template demand EWMA | never recycled (destroy after use) |
| uffd mappings | shared template memory file mapped once | n/a | n/a |
| drone | image-embedded; no pool | n/a | n/a |

Pools are sized from the per-template demand forecast that scout publishes. Comb reports pool depth in NodeReport, so waggle can prefer nodes with depth.

## 4. State and crash recovery

- WAL: a redb table `cells` holds the current record, and an append log `events` (rkyv) holds history. Group commit happens every 1 ms or 256 records, whichever comes first, with fsync on NVMe.
- Record: `{id, spec_hash, state, driver_handle{pid, api_sock, vsock_cid, cgroup, netns, rootfs devs}, ttl, labels, idem_key, created_at, last_activity}`.
- Restart: comb loads the table, then verifies liveness for each non-terminal cell (pidfd open, cgroup populated, VMM API ping). It then re-opens drone channels (the drone accepts a reconnect with the same session secret), rebuilds eBPF maps from pinned bpffs, and resumes timers. Unknown processes in `hive.slice` without a record are orphans and get killed.
- Cells are not children of comb. VMMs and containers are spawned via `systemd-run --scope`, or by double-fork with `PR_SET_CHILD_SUBREAPER` handled by a tiny `hive-shim` per cell. A comb restart therefore does not SIGKILL cells.
- Epoch fencing: if the lease is lost for longer than the TTL, comb stops accepting ops, re-registers and gets a new epoch. If keeper declared the old epoch dead, comb kills the old cells.

## 5. Memory density controller

Inputs are per-cell `memory.stat`, PSI (`memory.pressure`), node MemAvailable and DAMON regions.

| Mechanism | Scope | Setting |
|---|---|---|
| Shared read-only rootfs page cache | container: same EROFS image/fscache domain; microVM: virtio-pmem + DAX from host file (trust-domain scoped) | DSec −40.2% peak |
| Template memory sharing | microVM restored from same snapshot file `MAP_PRIVATE`, CoW | E2B/AgentENV |
| Free page reporting | virtio-balloon `free_page_reporting=on` in guest | DSec |
| Proactive reclaim | DAMON_RECLAIM in guest (cold ≥30 s) + host `memory.reclaim` on idle containers | DSec −21.2% |
| Swap/zswap | host zswap (zstd) for container cells; microVM guest memory via host swap on NVMe | overcommit headroom |
| Overcommit ceiling | `commit ≤ M × f(qos)`: latency 1.0, standard 1.5, best-effort 3.0 | tunable |
| Emergency | PSI some avg10 > 20%: stop admits, reclaim best-effort, pause idle cells (snapshot-kill if needed) | never kernel OOM random cells |

OOM policy: each cell has `memory.max` and `memory.oom.group=1`. Comb sets `oom_score_adj` so best-effort cells die first. An OOM moves the cell to `STOPPED(oom)`, which is not an infra error.

## 6. CPU QoS

- The cgroup tree:
  ```
  hive.slice/
    sys.slice          (comb, uffd, blockd, dns)       cpu.weight=10000, reserved cores 0-3
    latency.slice      cpu.weight=1000
    standard.slice     cpu.weight=100
    besteffort.slice   cpu.idle=1   (SCHED_IDLE)
  ```
- Each cell gets `cpu.max = vcpu × period` as a hard cap, and `cpu.weight` from its class.
- On SMT hosts, core scheduling (`PR_SCHED_CORE`) is applied per trust domain. Untrusted containers get cookies per project, so sibling threads never co-run different tenants. This also removes cross-HT side channels. DSec reduced latency inflation from 45.2% to 17.3% with SCHED_IDLE plus core scheduling.
- microVM vCPU threads are placed in the cell cgroup. VMM and IO threads go in the same cgroup so they are accounted.
- An optional `sched_ext` policy (v2) can do burst-aware scheduling.

## 7. Disk and I/O limits

- The upper (writable) layer is a per-cell sparse file on XFS with project quotas (prjquota), or a ublk CoW device with a size cap. There is also an inode limit.
- Each cell gets `io.max` (read/write bps and iops) and `io.weight` by class. Protection against `yes > file` comes from the quota plus output caps.
- Stdout/stderr ring buffer caps in the drone (09) protect memory and the gate.

## 8. Pause and resume

| Mode | Container | microVM | Resume |
|---|---|---|---|
| Freeze | `cgroup.freeze=1` | FC `PATCH /vm Paused` | ≤10 ms |
| Reclaim | freeze + `memory.reclaim` to swap/zswap | pause + balloon inflate / host madvise(PAGEOUT) | ≤150 ms (lazy fault-in) |
| SnapshotKill | CRIU (optional) or disk-only snapshot + stop | FC snapshot (diff) to nectar, kill VMM, free memory | restore anywhere via uffd, ≤300 ms |

Policy: a cell idle for at least `idle_ttl` gets Freeze. Reclaim follows after 10 min or more paused, or under memory pressure. After 1 h, or on node drain, it gets SnapshotKill. Bulk pause by selector is used for trainer weight-sync windows.

## 9. Exec data path

- Gate to comb: long-lived h2 streams, one per gate per comb. Frames carry `cell_id`, which comb uses to look up the drone mux.
- Comb to drone: one multiplexed channel per cell. Containers use UDS, bind-mounted from a comb-owned directory *outside* cell-writable paths. VMs use vsock.
- Zero-copy where possible: `bytes::Bytes` passthrough and no re-encoding of output frames.
- There is a per-cell exec concurrency limit (default 64) and a per-node global limit. Backpressure uses h2 flow control.

## 10. Node lifecycle

`hivectl node drain <n>` stops admits, SnapshotKills idle cells, waits (with a timeout), then reports. `cordon` and `uncordon` are also available, and kernel upgrades go through drain plus reboot. Node labels cover kernel version, backends ready (KVM present, ublk, fanotify pre-content), CPU flags and GPU.

## 11. Config

Config is TOML, and a subset of it is hot-reloadable.

```toml
[node]      unit = "u1"; reserved_cores = "0-3"; data_dir = "/var/lib/hivebox"
[pools]     netns_depth = 400; cgroup_depth = 800; refill_rate = 200
[density]   overcommit = { latency = 1.0, standard = 1.5, best_effort = 3.0 }; psi_stop_admit = 0.20
[backends]  enabled = ["container", "microvm", "fncall"]; microvm.vmm = "firecracker"
[cache]     l1_path = "/nvme/hive/l1"; l1_max = "6TiB"; pin = ["base:*"]
```
