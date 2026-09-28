# Requirements, workload model and SLOs

> Derived from DSec (01) and the agentic-RL systems survey (15). Numbers tagged **[target]** are hivebox design targets. Everything else is cited.

## 1. Users and workloads

| Persona | Workload | Backend | Shape |
|---|---|---|---|
| **RL trainer** (verl, slime, AReaL, ROLL, SkyRL, OpenRLHF, NeMo-RL, custom) | Rollouts of coding/terminal/security/computer-use agents | container, microvm, fullvm | bursts of 1K to 32K cells per step; GRPO groups of 8 to 16 per task; 15 to 20 min median life, >3 h p99 |
| **Verifier/reward** | Hidden tests, OJ judges, kernel benchmarks | fncall, container (verifier cell) | short, stateless, high QPS; repeats for flakiness |
| **Evaluator** | SWE-bench/Terminal-Bench/OSWorld/tau-bench | all | periodic, same path as training |
| **Env builder (human or agent)** | Build repo envs, docker-in-sandbox, `pack_diff`/commit | microvm (net on) | long, CPU-heavy, needs registry push |
| **Interactive/agent product** | Code interpreter, computer-use | container, microvm | E2B-style sessions, latency-sensitive |

Reference workload (DSec, one scale unit): ~3M cells/day, ~380K peak concurrent, >5,000 creates/s, ~160 nodes (30K cores, 250 TB DRAM). 90% of cells use ≤5% of requested CPU. 67.8% need workspace or toolkit layers. Each week sees >10K base images and >100K workspaces. Only 4 to 13% of image bytes are touched.

Other anchors: Anyrun (Cursor) does >500 pod creates/s per cluster with 100Ks of pods. AgentENV (Kimi) reports <50 ms boot/resume, <100 ms pause and incremental snapshot, fork ≤16, 9.6× memory overcommit and 1.5M images. Qwen MegaFlow runs 10K concurrent ECS with 25 TB of images. Kimi K2 runs >10K concurrent sandboxes.

## 2. Functional requirements

### F1 Lifecycle

- F1.1 Create single cells and batches (`n` cells from one template, for GRPO groups) with an idempotency key.
- F1.2 States: `PENDING → PREPARING → STARTING → RUNNING ⇄ PAUSED → STOPPING → STOPPED | FAILED | EXPIRED` (05 section 3).
- F1.3 Idle TTL (auto-pause or stop) plus a hard TTL; `ExtendTTL`.
- F1.4 Pause/resume, with implicit resume on any data-plane request (as in DSec).
- F1.5 Bulk ops by label selector: `PauseAll`, `ResumeAll`, `StopAll` (for weight-sync windows and preemption).
- F1.6 Every terminal state carries a failure classification, `cause ∈ {completed, agent_exit, task_timeout, idle_timeout, oom, disk_quota, pids_limit, output_limit, policy_violation, infra_node_lost, infra_image, infra_runtime, infra_internal, cancelled}`, plus `is_infra_error`, so trainers mask infra failures rather than score them 0.

### F2 Execution (data plane)

- F2.1 One-shot `exec` with caps (stdout/stderr bytes, timeout) and truncation flags.
- F2.2 Streaming processes (stdin/stdout/stderr, signals, process-group kill), PTY.
- F2.3 Persistent shell sessions with end-of-command detection, exit codes and interactive expect (SWE-ReX/chronus semantics). Many sessions per cell.
- F2.4 Files: read/write/list/stat/remove/watch/upload/download (streamed), `git_diff`/patch extraction.
- F2.5 Ports: expose in-cell ports through the gateway (Jupyter, browser, VS Code), authenticated.
- F2.6 Outbound LLM route: in-cell harnesses reach an inference gateway (with token-in/token-out capture) without general internet access.

### F3 State

- F3.1 Disk snapshot (`pack_diff`), disk+mem snapshot, restore to a new cell.
- F3.2 Fork `n ≤ 16` children (same node preferred) for tree search and TVCACHE-style prefix reuse.
- F3.3 Commit to a new image manifest (for env-building agents), with scrubbing and provenance.
- F3.4 Auto-checkpoint policy (timer or tool-call boundary) for crash recovery.

### F4 Images

- F4.1 Composable layers (base/workspace/toolkits), ≥10⁶ distinct manifests.
- F4.2 Import OCI images, Dockerfiles and SWE task specs; lazy loading; dedup.
- F4.3 Image-build cells (docker-in-cell, network on).

### F5 Verification

- F5.1 `RunVerifier` runs in a separate cell derived from the agent cell's snapshot or diff. Tests never enter the agent cell before evaluation.
- F5.2 Repeats (`k`) and a flakiness flag; anti-tamper test harness (11 section 5).

### F6 Policy and tenancy

- F6.1 Nested projects, bounded delegation, quotas (cells, cores, memory, creates/s), usable by agents (as in DSec IAM).
- F6.2 Default-deny egress with named allowlists (`pypi`, `npm`, `github`...) that can be updated mid-life per stage.
- F6.3 Per-cell limits: CPU, memory, pids, disk, inodes, output bytes, open files, wall time.
- F6.4 Tamper-evident out-of-cell audit log of every data-plane call.

### F7 Compatibility adapters

- F7.1 E2B API compatibility (REST control + envd Connect-RPC data plane), so slime, the E2B SDKs and OpenHands-E2B work unchanged.
- F7.2 SWE-ReX `RemoteRuntime`, Harbor environment provider, OpenEnv `reset/step/state`, SandboxFusion `/run_code`, and an MCP server for tools.

## 3. Non-functional requirements and SLOs [target]

| Metric | Target | Reference |
|---|---|---|
| Cluster create throughput (sustained) | **≥5,000/s** per scale unit; burst 32K accepted in ≤2 s, started in ≤60 s | DSec |
| Concurrent cells per scale unit | **≥400K** (design to 1M across units) | DSec 380K |
| Density per node (1.5 TB, 192 cores) | **3,200 containers or 800 microVMs**; stretch 4× mem overcommit | DSec; AgentENV 9.6× |
| Create latency, warm (template/layers cached) | container p50 ≤150 ms, p99 ≤400 ms; microVM p50 ≤150 ms, p99 ≤300 ms; fncall p99 ≤10 ms | AgentENV <50 ms, Daytona <90 ms, E2B ~150 ms |
| Create latency, cold (layers remote) | p99 ≤5 s single; burst-completion ≤ cached-pull time | DSec on-demand = fully cached |
| Exec RTT (no-op) | p50 ≤5 ms in-cluster, p99 ≤25 ms | ProRL 420 ms (pty) |
| Exec throughput per node | ≥50K exec/s | - |
| Pause / resume (microVM 4 GiB) | ≤500 ms / ≤150 ms (lazy) | AgentENV <100 ms |
| Incremental disk snapshot (≤100 MiB dirty) | ≤200 ms | AgentENV <100 ms |
| Fork n=8 same node (microVM) | ≤300 ms | Morph <250 ms |
| Infra failure rate | ≤0.1% of cells | Daytona 1/768 |
| Control-plane availability | 99.95%; no single point of failure; cells survive control-plane loss | - |
| Node agent restart | cells keep running; re-attach ≤5 s | - |
| Placement decision latency | p99 ≤2 ms per cell | - |
| Memory overhead per cell | container ≤8 MiB host; microVM ≤12 MiB host (VMM + page tables) excl. guest | FC spec ≤5 MiB |

## 4. Scale model (per scale unit, used by the capacity planner)

```
nodes N = 160, cores/node C = 192, mem/node M = 1.5 TB
peak concurrent P = 400K  ⇒ 2,500 cells/node
creates/s R = 5,000        ⇒ 31/s/node (avg), 150/s/node burst (5× skew)
avg requested: 2 vCPU, 4 GiB  ⇒ nominal 5,000 vCPU & 10 TB/node requested
actual: ≤5% CPU ⇒ 250 vCPU busy (≈1.3× cores; QoS + SCHED_IDLE absorbs)
memory: resident ≈ 350 to 600 MiB/cell after sharing ⇒ 0.9 to 1.5 TB/node  ← binding constraint
```

Memory is the binding resource. The scheduler packs on measured resident memory plus reserved headroom, not on requests.

## 5. Constraints and assumptions

- Linux hosts, kernel ≥6.12 (7.x LTS recommended), cgroup v2 only, KVM on bare metal for the microVM tier.
- x86_64 is primary. aarch64 is supported for container and microVM (no DAX-pmem writes).
- Storage: 3FS preferred; S3-compatible storage and local NVMe are supported.
- Network: L3 fabric; optional BGP anycast for ingress VIPs.
- The trusted (trainer) network is separated from the untrusted (cell) network, and the gateway is the only bridge (as in DSec).

## 6. Out of scope (v1)

- Windows/macOS guests (T3 exists for Linux/Android first).
- Live migration across nodes (fork/snapshot + restore instead).
- Multi-region federation (per-unit control planes; SDK-level sharding).
