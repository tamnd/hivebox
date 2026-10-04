# Observability, Testing and Benchmarking

> "Real data, real cluster" means every performance claim in this spec needs a reproducible harness. It also means correctness under failure is tested before production, not discovered in it.

## 1. Observability

### 1.1 Metrics (Prometheus / OTLP metrics)

- There is never a `cell_id` label. At 400K cells and 3M/day, per-cell series would blow up TSDB cardinality. Allowed labels are `unit, node, backend, template_class, project (top-K, rest=other), qos, cause, op`.
- Main series:
  - `hive_create_total{backend,result}`, `hive_create_seconds_bucket{backend,path=warm|cold|restore}` with a stage breakdown (`admit, pool, rootfs, start, handshake`).
  - `hive_exec_seconds_bucket{op}`, `hive_exec_inflight`, `hive_drone_rtt_seconds`.
  - `hive_cells{state,backend}`, `hive_mem_{committed,resident,shared,reclaimed}_bytes`, `hive_psi_*`.
  - `hive_pool_depth{pool}`, `hive_pool_refill_seconds`.
  - `hive_nectar_{l1_hit_ratio,l2_bytes,fill_latency_seconds}`, `hive_prefetch_useful_ratio`.
  - `hive_guard_denied_total{reason}`, `hive_dns_nxdomain_total`.
  - `hive_placement_seconds`, `hive_admission_reject_total{reason}`, `hive_placement_retries_total`.
  - `hive_infra_error_ratio` (SLO), `hive_quota_slice_{granted,returned}`.
- Histograms use native/exponential buckets. Exemplars carry `trace_id` so you can jump to a trace.

### 1.2 Per-cell telemetry to a columnar store

- The comb emits a `CellRecord` on each terminal state and a sampled `CellSample` every 10 s:
  - `CellRecord`: id, project, template, backend, node, timings per stage, cause, is_infra, peak/avg CPU/mem/io/net, output bytes, exec count, deny counts.
  - `CellSample`: cpu, rss, pss, io, net.
- Records are shipped as Arrow batches to ClickHouse or to Parquet on object store. This supports DSec-style analyses (CPU utilization CDFs, lifetime distributions, image access fractions) and the capacity planner's `mem_est` model.

### 1.3 Tracing

- `tracing` plus `tracing-opentelemetry` export to OTLP. Tail sampling keeps 100% of errors and slow creates (>p99) and 0.1% of normal ones. Context propagates from gate to waggle to comb to drone (via frame metadata).
- The OTel Rust SDK is still pre-1.0 (0.33), so it is pinned and wrapped in `hive-telemetry` to contain the API churn.

### 1.4 Profiling and logs

- Nodes run continuous eBPF profiling (Parca/Pyroscope agents). `tokio-console` is available behind a feature flag in staging.
- Logs are structured JSON with a rate-limited per-cell log sink. Workload stdout is not logged. It is only returned to the caller or retained in audit digests.

### 1.5 SLO dashboards and alerts

Multi-window burn-rate alerts cover create success, create p99 by path, exec p99, infra_error_ratio, node PSI, pool depth exhaustion, 3FS latency, and keeper leader churn.

## 2. Testing strategy

| Layer | Tooling | Scope |
|---|---|---|
| Unit | `cargo nextest`, `proptest` | state machine transitions, resource math, ID codec, policy eval, manifest canonicalization |
| Model checking | `loom` (lock-free pools, WAL group commit), `kani` for ID/frame parsers where cheap | concurrency |
| Deterministic simulation | `hive-sim`, one event queue on simulated time with a simulated network and a seeded rng, running the keeper state machine, waggle's placer and the gate's quota share as they are | full control plane plus fake combs/drivers: partitions, message loss, clock skew, comb crash/restart, keeper leader loss, duplicate requests. Invariants: no cell double-owned, quota overshoot ≤ slice bound, epoch fencing kills stale cells, every create gets exactly one terminal outcome, idempotency holds. Thousands of seeds per CI run; failing seeds are replayable. |
| Fuzz | `cargo-fuzz` (libFuzzer) + `arbitrary` | drone framing, proto decoding, EROFS/manifest parsers, E2B JSON, DNS proxy |
| Driver conformance | `hive-cell` conformance suite (07) | every `CellDriver` including plugins: lifecycle, pause/resume, snapshot/restore/fork, limits, identity refresh |
| Security regression | adversarial workload corpus | DSec incidents reproduced: socket forgery, `/bin/bash` overwrite, XFS_IOC_SWAPEXT, `/proc/kpagecgroup`, `yes` flood, fork bomb, DNS tunnel, metadata IP, reverse shell, conftest monkeypatch, `sys.exit(0)`, git-history leak. Each must be blocked or classified correctly. |
| Integration | real nodes (bare-metal CI pool, 3 nodes with KVM) | full create/exec/snapshot/fork on all tiers; kernel matrix (6.12 LTS, 6.18 LTS, 7.x) |
| Chaos | chaos jobs in staging unit | kill comb/gate/keeper, drop 3FS, fill disks, PSI storms, netem on fabric, node power-off |
| Compatibility | E2B SDK test suites, SWE-ReX tests, Harbor task runs | adapters |

## 3. Benchmark plan

The benchmark harness (the `hive-bench` load generator, trace replay, node and cluster benchmarks, and the real dataset runs) lives in the companion repository tamnd/hivebox-bench.

### 3.1 Microbenchmarks (criterion, per PR)

ID codec, placement (1K nodes, n=32K), policy eval, frame encode/decode, WAL append, EROFS lookup, chunk cache hit path.

### 3.2 Node benchmarks (single node, bare metal)

| Bench | Metric | Target |
|---|---|---|
| create-storm | creates/s sustained until p99 breaks, per backend | container ≥300/s, microVM ≥150/s |
| density | max cells at p99 exec ≤25 ms with the DSec CPU distribution (90% ≤5% CPU) | 3,200 container / 800 microVM (1.5 TB, 192 cores) |
| exec | no-op exec RTT and throughput at 2,500 cells | p50 ≤5 ms, ≥50K/s |
| snapshot | pause/resume, disk snapshot at dirty 10/100/1000 MiB, fork n=1..16 | 02 section 3 |
| cold image | first create from an uncached 10 GiB image, with and without trace prefetch | p99 ≤5 s |
| memory | peak and time-integrated memory with/without pmem-DAX, FPR, DAMON | reproduce DSec −40.2% / −21.2% |
| cpu QoS | latency inflation of latency-class cells under best-effort saturation | ≤+20% (DSec 17.3%) |

### 3.3 Cluster benchmarks (scale unit)

- Trace replay: `hive-bench replay` drives synthetic workloads fit to the DSec distributions:
  - lifetimes: median 15 to 20 min, p99 >3 h;
  - bursts: 1K to 32K per step every N minutes;
  - GRPO groups of 8 to 16;
  - exec rates, image popularity (Zipf over 10K bases / 100K workspaces), 4-13% of bytes touched, and 67.8% layered.
- Headline test: 5,000 creates/s sustained for 30 min plus 400K concurrent. It measures create p50/p99, infra error ratio, placement retries, 3FS throughput, and keeper write rate, which must stay flat.
- Failure under load: kill 5% of nodes mid-burst and verify masking and recovery.
- Scaling curve: 16, then 64, then 160, then 500 nodes. Beyond the available hardware, nodes are simulated with the `hive-sim` fake-comb mode.

### 3.4 Real-data end-to-end

- Datasets: SWE-bench Verified (500), SWE-Gym (~2.4K), R2E-Gym, SWE-smith (50K+ tasks), SWE-rebench, Multi-SWE-bench, Terminal-Bench 2.
- Runs:
  - (a) Import all images and measure dedup ratio and storage.
  - (b) Gold-patch validation over the full dataset: every task's gold patch passes and the empty patch fails. This measures correctness and flakiness of the platform itself.
  - (c) Agent rollouts with an open model (for example Qwen3-Coder or DeepSeek-V3.x) via mini-SWE-agent/OpenHands, measuring the share of rollout wall time spent in the sandbox versus the LLM.
  - (d) A verl or slime GRPO training job for N steps on SWE-Gym, comparing throughput with a Docker/K8s baseline.

### 3.5 Baselines to compare

Kubernetes + containerd (+ Kata), self-hosted E2B infra, Daytona OSS, plain Docker (SWE-bench harness), and published numbers (DSec, AgentENV, Anyrun, Modal, Dirigent). Reports use the same hardware and warm/cold labels, and any vendor-published claim is tagged as such.

## 4. Release gates

A release must show no SLO regression >5% in node benches, pass all DST seeds (N=10K), keep the security corpus 100% blocked, pass conformance on all drivers, and have clean cargo-deny/vet.
