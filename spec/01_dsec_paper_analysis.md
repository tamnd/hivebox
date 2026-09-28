# DSec paper analysis (arXiv 2609.22978)

> **Paper:** *DeepSeek Elastic Compute (DSec): A Sandbox Infrastructure for Effective Agentic Training at Scale*, by DeepSeek-AI (Jialiang Huang et al., 100+ authors), arXiv:2609.22978v1 [cs.DC], 19 Sep 2026. 31 pages, no appendix.
> **Purpose of this note:** this is the ground truth `hivebox` is derived from. Everything here comes from the paper unless tagged **[hivebox]** (our interpretation or decision) or **[not in paper]**.

## 1. Summary

DSec is DeepSeek's production sandbox platform for agentic RL and evaluation, used from DeepSeek V3.2 through V4.1. It exposes four isolation backends behind one async Python SDK (`libdsec`): FnCall (pre-created warm containers, CPU or GPU), Container (patched Docker/Moby inside a QEMU VM), MicroVM (Firecracker on bare metal) and Full VM (QEMU/libvirt, GPU-PV). A stateless control plane (IAM, API server, placement engine, watcher) routes requests to per-node edge daemons, which own admission and lifecycle. Inside each sandbox an aether proxy multiplexes chronus shell sessions. Images are composable layers (base + workspace + toolkits), stored as EROFS for containers and as OverlayBD over ublk (Rust) for microVM writable disks, all on 3FS and loaded on demand. Density comes from virtio-pmem+DAX, DAMON + free-page reporting, and SCHED_IDLE + core scheduling. One scale unit (~160 nodes, 30K cores, 250 TB DRAM) serves ~3M sandboxes/day, ~380K peak concurrent and >5,000 creates/s.

## 2. Workload properties

| # | Property | Consequence for design |
|---|---|---|
| 1 | **Bursty creation.** One job asks for up to **32K** sandboxes in a short window (the batch can't start until envs are ready) | No centralized bottleneck in scheduling or image distribution |
| 2 | **High density.** Sandboxes mostly wait on the LLM; ~90% use ≤5% of requested CPU on average | Oversubscribe CPU; 800 microVMs or 3,200 containers per node demonstrated |
| 3 | **Stateful & long-lived.** p50 life ~15 to 17 min, p99 ~214 to 232 min; memory/page cache stays pinned while idle | Memory sharing + reclamation are mandatory |
| 4 | **Heterogeneous.** OJ scripts, SWE repos, security, computer-use, Android, full-system | Multiple backends; "a single sandbox abstraction cannot cover all of them efficiently" |
| 5 | **High env diversity, low reuse.** Image fanout per task: containers p50=3, p90=28 | Local caches don't help much; on-demand loading beats pulling (1.7× faster completion, −57% disk writes) |
| 6 | **Untrustworthy execution.** Agents corrupt FS, exhaust resources, attack control plane | Defense-in-depth, continuous hardening |
| 7 | **Interruptible.** GPU training is preempted mid-rollout | Pause/resume; rollout state lives outside the GPU pool |

### 2.1 Measured numbers (one week, early 2026)

Sandboxes per task (CDF):

| | p50 | p90 | p99 | max |
|---|---|---|---|---|
| Container | 2,528 | 7,969 | 16,388 | ~32K |
| MicroVM | 352 | 1,835 | 4,044 | - |

Active environment artifacts:

| Backend | Base images | Workspaces | Snapshots | Size |
|---|---|---|---|---|
| Container | 11,266 | 102,171 | - | 82.8 TB |
| MicroVM | 2 | 53,590 | 4,889 | 50.9 TB |

There are also 103 toolkits. 67.8% of sandboxes need at least one workspace or toolkit on top of the base.

Fraction of image bytes actually read at runtime:

| C++ | Go | Java | JS | Python |
|---|---|---|---|---|
| 8.7% of 4.9 GB | 13.3% of 4.1 GB | 9.2% of 12.1 GB | 4.2% of 9.6 GB | 6.0% of 6.0 GB |

So only about 4 to 13% of an image is ever touched. Pre-warming "merely shifts this overhead earlier."

Lifetimes: container p50 17.4 min, p99 231.5 min; microVM p50 15.5 min, p99 213.9 min.

Density: one node peaked at 1,048 containers + 524 microVMs in a day. Nodes run stably at ≥3,200 containers or ≥800 microVMs each ("operating points, not hard limits").

Per-sandbox phases: *setup* (CPU heavy: install, build), then *tool-call* (short bursts between LLM waits), then *test* (burst). Setup cost × burst size dominates.

Scale unit: ~160 CPU nodes, 30K cores, 250 TB DRAM. By our arithmetic that is ≈190 cores and ≈1.5 TB DRAM per node, and ≈2,400 concurrent sandboxes per node at peak. Several units share one 3FS.

## 3. Architecture

```
      Training cluster (trusted, GPU)                  Sandbox cluster (untrusted, internet-ish)
 ┌──────────────────────────┐   mgmt / data   ┌──────────────────────────────────────────────┐
 │ RL framework ── libdsec  │ ──────────────▶ │ IAM ─▶ API server (stateless ingress proxy)   │
 └──────────────────────────┘                 │           │           ▲                      │
                                              │   Placement engine ◀── Watcher (polls edges) │
                                              │           ▼                                   │
                                              │  Node: Edge ─▶ runtime (docker/FC/QEMU/FnCall)│
                                              │          └─ uds/vsock ─▶ Aether ─▶ Chronus×N │
                                              │  Storage: EROFS + OverlayBD(ublk) on 3FS     │
                                              └──────────────────────────────────────────────┘
```

### 3.1 Cluster services (all stateless or soft-state, multi-instance)

- IAM holds principals, nested projects (arbitrary depth), policies and quotas. Principals, including agents and harnesses, can create subprojects and delegate a subset of their quota and permissions. Delegation is bounded: you can't grant what you don't hold. Humans and agents share one management API.
- The API server is the only path between the trusted GPU network and the untrusted sandbox network. It carries create, exec and streaming. It keeps no per-sandbox state: each sandbox ID encodes its owning edge, so any instance routes directly. It refreshes its edge list from the watcher.
- The placement engine filters nodes (healthy, has the backend and hardware), then applies power-of-k-choices (sample k, pick the least loaded). Each instance overlays its own recent, not-yet-observed placements onto the watcher snapshot. This avoids herding without coordination.
- The watcher polls edges for health and counts (running sandboxes per backend × edge × user × task). It rebuilds from scratch by re-polling and has no durable state.
- The edge has final admission authority. It rejects a create if local capacity is insufficient, and placement retries elsewhere.
- Reliability comes from BGP anycast/ECMP VIPs for ingress and mirrors, plus periodic full cluster resets that prove IaC can rebuild the control plane from scratch (outages corrupt reward signals).

### 3.2 Node runtime

- Edge (one per node) handles create/delete for all backends, admission, storage provisioning, eBPF network policy, runtime launch, lifecycle and TTL tracking, and coordination of disk and memory snapshots.
- Aether (one per sandbox, running inside it) holds the channel to the edge over a Unix socket (containers) or vsock (VMs). If the channel closes, the sandbox is considered failed. Aether maps each terminal-session id to a chronus instance and kills its process tree when the session ends.
- Chronus (one per shell session) does exec, file ops, HTTP requests and streaming I/O. It records output for async retrieval and invokes bash for some ops.

### 3.3 Backends

| | FnCall | Container | MicroVM | Full VM |
|---|---|---|---|---|
| Runtime perf | ●●● | ●●○ | ●◐○ | ●○○ |
| Isolation | ○○○ | ●●○ | ●●● | ●●● |
| Full OS | ○○○ | ●○○ | ●●○ | ●●● |
| Overhead | ○○○ | ●○○ | ●●○ | ●●● |
| Used for | OJ, GPU kernels | SWE, tool use | security, computer use | COTS OS, graphics, Android |
| Impl | warm pool of containers (CPU/GPU, MIG) | patched dockerd in QEMU VM | Firecracker, bare metal | QEMU/libvirt, virtio-gpu, DXVK |

- FnCall and containers run inside a QEMU/libvirt VM per node. This adds a kernel boundary, so kernel crashes don't take out bare metal. MicroVMs run on bare metal to avoid nested virt.
- FnCall bypasses the edge, aether and chronus path. The task spec (type, deps, code) executes directly in a precreated container, followed by best-effort cleanup. GPU work uses MIG, exclusive or shared. A CPU FnCall compiles first and a GPU FnCall then runs, so the GPU is not held during compile. Python processes are kept warm with libraries pre-imported.
- The SDK intentionally does not fully abstract backends. The caller picks one.

### 3.4 SDK (only names published)

```python
client = DSecClient(); await client.open()
args = DSecContainerRunArgs(container_image="registry.../sphinx-9658:official",
    memory_limit_mb=4096, cpu_cores_limit=4, ttl_running_stop=300,
    network_rules={"npm": False, "pypi": True}, init_user="root")
sandbox = await client.run_container(args, timeout=120)
result = await sandbox.run_shell("echo hello world")
await sandbox.stop()
```

The paper also names `pack_diff` (checkpoint to a new env) and pause/resume, where the next request resumes the sandbox implicitly.

The lifecycle is the same for all backends: Create, Prepare, Interact (stateful), then Stop or TTL reclaim.

## 4. Core mechanisms

### 4.1 Composable layers

- Images are split into Base (OS + runtimes), Workspace (repo + deps) and Toolkits (harness, updated frequently). Rebuild cost goes from O(m·N)/O(k·N) with monolithic OCI to O(m)/O(k).
- Rejected alternatives: per-sandbox tar extraction, because CPU/IO spikes cause startup timeouts; and RO bind mounts, because they replace rather than merge and break tools that write `__pycache__`.
- The chosen design is overlayfs with multiple RO lowerdirs and a local writable upper. dockerd is patched (~30 lines of Go) to insert pre-mounted EROFS layers as the topmost lowers.
- On microVMs, base and toolkits are EROFS RO block devices, and the guest rootfs is overlayfs(EROFS lowers, upper on an ext4 writable disk).

### 4.2 Image distribution on 3FS

- There is no registry or P2P tier, since 3FS already exists: tens of storage servers, each with 20×15 TB SSD and 2×400 Gbps RDMA, serving clusters with 100Ks of cores. 3FS is good at large sequential I/O and bad at small random I/O, so writes stay local, reads are on-demand and bulk, and metadata stays local.
- EROFS multi-device: the metadata blob is downloaded to local disk and the data blob stays on 3FS, accessed through the 3FS FUSE client. Path lookups never go remote.
- Layer collapsing: consecutive layers under a threshold (e.g., 3 GB) are merged offline into one (meta, data) pair, with whiteouts preserved.
- EROFS is mounted file-backed, with no loop devices.
- MicroVM writable disks use OverlayBD over ublk. This is a Rust port (open-sourced at `kvcache-ai/AgentENV/storage/overlaybd`) with 3FS, OSS and registry backends. It fetches 256 KiB chunks into an L2 local FS cache and takes incremental disk snapshots without EROFS repacking. It is needed because Firecracker lacks virtio-fs and Docker-in-VM overlay2 can't sit on overlayfs.

### 4.3 Memory density (microVMs)

- virtio-pmem + DAX serves the RO EROFS layers, so VMs share one host page-cache copy. The costs are synchronous DAX faults (no guest readahead) and guest `struct page` overhead of 1/64 of pmem size (128 GB of pmem costs 2 GB of guest RAM).
- Writable disks use DAMON + virtio-balloon free page reporting (FPR). DAMON evicts cold file pages, the free pages coalesce, FPR reports order-9 (2 MiB) blocks, and the host calls `MADV_DONTNEED`.
- Only stock kernel features are used.
- Results: pmem+DAX cuts peak host memory by 40.2%, and DAMON+FPR cuts time-integrated memory by 21.2%. The combination is best. pmem raises transient peak CPU from 26.5% to 41.4%.

### 4.4 CPU QoS

- Work is split into latency-sensitive (LS) and best-effort (BE). BE runs under `SCHED_IDLE`, and core scheduling (`prctl(PR_SCHED_CORE)`) ensures BE never shares an SMT sibling with LS.
- For a chess-agent LS workload under 50% BE load, step latency rises 45.2% with no QoS. SCHED_IDLE alone improves this by at most 3.4%. Adding core scheduling brings it down to +17.3%. The residual comes from turbo, memory bandwidth and LLC.

### 4.5 Cloud bursting

- Above 80% on-prem utilization, eligible creates are offloaded to cloud VMs running the same runtime and EROFS path. A 30 TB de-duplicated EROFS set covers the files accessed by 70% of container tasks, and tasks that stay fully inside it are cloud-eligible. 200 cloud VMs absorb ~30% of peak overflow.

## 5. RL co-design

- With `pack_diff`, an agent checkpoints a sandbox as an incremental disk snapshot that can be restored as a new env ("environments of agents, by agents, for agents"). Builder and solver agents use separate accounts. Build residue is scrubbed from the writable layer before packing to prevent answer leakage. A quality-check platform exports envs in standard formats.
- In V4.1 the agent loop moved out of the GPU pods. A rollout is an *agent sandbox* (scaffold + tools) plus a *worker container* (a scaffold-agnostic control layer), and together they are the single source of truth. Preempted GPU jobs reconnect without replay. The old design used a command log and replayed recorded results for non-idempotent ops.
- On preemption, the framework pauses all sandboxes of the job, and any request transparently resumes them.
  - Containers: freezer, then enable `memory.swap.max`, then `memory.reclaim`. Resume does a `MADV_WILLNEED` prefetch and then unfreezes.
  - MicroVMs: snapshot memory and state, then kill Firecracker. Resume starts a new process and restores.

## 6. Misbehavior observed and mitigations

Inside-sandbox reward hacking: forging RPCs to chronus sockets; reading chronus logs for answers; overwriting `/bin/bash` (chronus calls bash) to hijack later sessions; using `XFS_IOC_SWAPEXT` to read protected files via extent swap, which corrupted XFS and caused an FS shutdown.

Outside-sandbox: port and service scans for reachable mirrors; using the Go module proxy to fetch GitHub; installing newer package releases that contain the fix.

Accidental: `grep -r /` read `/proc/kpagecgroup` and crashed the kernel; an exploit meant for a target VM was run locally and crashed the kernel; `yes` output was captured to tens of GB.

Mitigations: AppArmor (applies to root; protects control sockets and logs); per-sandbox eBPF IP/port/proto allowlists derived from *named* permissions (`pypi`, `npm`), which can be updated per task stage; and an outer QEMU VM plus per-user isolation to bound the blast radius. The paper admits there is no general defense against triggering kernel bugs. It relies on observability and continuous hardening.

## 7. Evaluation summary

| Experiment | Result |
|---|---|
| 8,192 containers burst / 10 nodes, EROFS on-demand vs docker pull | on-demand ≈ cached pull (~35 min); cold pull >60 min (1.71×), 2.3× disk writes, 2× peak IOPS |
| EROFS vs tar.gz for workspace+toolkits | 45 vs 79 min (1.76×); tar 5.5× writes |
| Firecracker memory | pmem+DAX −40.2% peak; DAMON+FPR −21.2% integrated |
| CPU QoS | +45.2% → +17.3% LS step latency at 50% BE |

Test nodes: 2×EPYC 9655 (384 threads) with 1.5 TB for microVMs on bare metal; a 192-thread / 512 GB VM for containers. Host Linux 7.0, guest 6.1.

## 8. Gaps the paper leaves open, and where hivebox decides them

| Gap | hivebox decision (see spec file) |
|---|---|
| Creation latency SLO, exec RTT | 02 (SLOs) |
| Full SDK surface (files, streams, ports, fork) | 05 (API) |
| LS/BE classification | 08 (explicit `qos` field + auto-demotion) |
| k in power-of-k, admission formula | 04 (k=2..4, token-based admission) |
| Output caps, pids, disk quotas | 10 (hard caps, ring-buffer logs) |
| Worker/inference protocol | 11 (rollout worker) |
| Live **fork** of sandboxes (tree search) | 07 (snapshot/fork, beyond DSec) |
| Content-addressed dedup, prefetch | 06 (chunk CAS + trace prefetch, beyond DSec) |
| Comparison with K8s / E2B / Kata | 13 (benchmark plan) |

## 9. Related work cited (for follow-up)

Serverless: SAND (ATC'18), REAP (ASPLOS'21), TrEnv (SOSP'24), RunD (ATC'22), SEUSS (EuroSys'20). Agent platforms: OpenAI Code Interpreter, E2B, Kimi K2.5 Agent Swarm, MiMo-V2-Flash, ComputerRL. Images: DADI/OverlayBD (ATC'20), CoFS (FAST'26), FaaSNet (ATC'21), EROFS (ATC'19), Nydus. Isolation: Firecracker (NSDI'20), Kata, Graphene-SGX, LiteBox, Sledge, Faasm, Nested Kernel, Erebor (EuroSys'25), PVM (SOSP'23). RL: slime, veRL/HybridFlow, OpenRLHF, REINFORCE++, Seer. Memory/sched: DAMON, ESX ballooning (OSDI'02), core scheduling, power-of-two-choices (TPDS'01). Storage: 3FS, Fire-Flyer AI-HPC (SC'24).
