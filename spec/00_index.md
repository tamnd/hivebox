# hivebox: sandbox fabric for agentic RL (index)

> **Repo:** `github.com/tamnd/hivebox` · **Language:** Rust (2024 edition) · **Status:** design spec v0.1 (2026-09-28)
> **Seed paper:** DSec, DeepSeek's sandbox platform for agentic RL, [arXiv 2609.22978](https://arxiv.org/abs/2609.22978)

## 1. What hivebox is

hivebox is an open-source platform that runs hundreds of thousands of isolated, short-lived execution environments ("cells") for agentic reinforcement learning, evaluation and agent products. It targets DSec-class scale on commodity Linux clusters:

- ≥5,000 creates/s and ≥400K concurrent cells per scale unit of ~160 nodes.
- 3,200 containers or 800 microVMs per node.
- Warm create p50 ≤150 ms, exec RTT p50 ≤5 ms, fork of 8 ≤300 ms.
- ≤0.1% infrastructure failures, all classified so trainers can mask them.

It keeps DSec's core ideas:

- node-authoritative lifecycle;
- a stateless API with routing IDs;
- power-of-k placement with an overlay;
- composable lazily loaded EROFS layers on 3FS;
- memory density via pmem-DAX, FPR and DAMON;
- CPU QoS;
- four isolation tiers.

It adds what DSec leaves open:

- explicit QoS classes and fork/branching;
- chunk-level dedup and access-trace prefetch;
- a hardened guest channel;
- separate-cell verification against reward hacking;
- infra-error classification;
- E2B/SWE-ReX/Harbor compatibility;
- deterministic simulation testing;
- a public, versioned API.

## 2. Design principles

1. No consensus on the hot path. Per-cell state lives on the node that runs it.
2. Pool everything the kernel is slow at: netns, taps, cgroups, template VMs.
3. Measure, don't request. Pack on observed memory, isolate with QoS, and keep an escape valve (pause or snapshot-kill).
4. Assume the workload is an adversary optimizing against you.
5. Put traits at every seam (driver, store, clock, transport) for modularity, plugins and simulation.
6. Real data or it didn't happen. Every target has a benchmark and a run on a real dataset.

## 3. Component glossary

| Component | Crate | Role |
|---|---|---|
| **gate** | `hive-gate` | stateless API ingress: authn/z, quota slices, ID routing, E2B compat |
| **waggle** | `hive-waggle` | placement: batch power-of-k + in-flight overlay, pack/spread hybrid |
| **scout** | `hive-scout` | aggregates pushed node reports into a cluster view |
| **keeper** | `hive-keeper` | Raft (openraft+redb): IAM, nested projects, quotas, templates, image registry |
| **comb** | `hive-comb` | node agent: admission, pools, lifecycle WAL, density & QoS control |
| **cell** | `hive-cell*` | `CellDriver` trait + drivers: wasm, proc, oci (youki), fc, ch, qemu, plugin |
| **drone** | `hive-drone` | in-cell agent: channel, processes, shell sessions, files, quiesce |
| **nectar** | `hive-nectar` | layers, manifests, CAS, BlobStore (3FS/S3/posix), L1 cache, lazy fill |
| **blockd** | `hive-blockd` | ublk block targets (OverlayBD-compatible CoW) |
| **imaged** | `hive-imaged` | import/build/relayout/commit pipeline |
| **uffd** | `hive-uffd` | userfaultfd page server for snapshot restore & fork |
| **snap** | `hive-snap` | snapshot tree, diffs, fork orchestration |
| **guard** | `hive-guard` | aya eBPF policy datapath, DNS allowlist proxy, mirror/LLM VIPs |
| **pollen** | `hive-pollen` | RL rollout worker, verifier runner, trainer adapters |
| **sim / bench** | `hive-sim`, `hive-bench` | deterministic simulation, load generator & trace replay |

## 4. File map

| File | Contents |
|---|---|
| [01_dsec_paper_analysis.md](01_dsec_paper_analysis.md) | Analysis of the DSec paper: workload, architecture, mechanisms, evaluation, gaps |
| [02_requirements_slos.md](02_requirements_slos.md) | Personas, functional requirements F1 to F7, SLOs, scale model, constraints |
| [03_architecture.md](03_architecture.md) | System diagram, design decisions D1 to D13, cargo workspace, process model, flows, failure domains |
| [04_control_plane.md](04_control_plane.md) | IDs, keeper/IAM/quota leasing, gate, scout, waggle placement, admission, scaling math |
| [05_api_sdk.md](05_api_sdk.md) | `hivebox.v1` protobuf services, errors, state machine, Python/Rust SDK, E2B & other adapters |
| [06_storage_images.md](06_storage_images.md) | Layer model, EROFS/OverlayBD/CAS, BlobStore, node cache & lazy fill, prefetch, build pipeline |
| [07_backends_snapshots.md](07_backends_snapshots.md) | Isolation tiers, `CellDriver`, per-driver designs, UFFD restore, snapshots, fork, memory/CPU density |
| [08_node_agent.md](08_node_agent.md) | comb internals: pipeline, pools, WAL & recovery, memory/CPU/disk control, pause modes |
| [09_guest_agent.md](09_guest_agent.md) | drone: channel, handshake, sessions (sentinel protocol), output limits, hardening, restore awareness |
| [10_security.md](10_security.md) | Threat model, per-tier hardening, network/control-plane security, reward-hacking mitigations, audit, supply chain |
| [11_rl_integration.md](11_rl_integration.md) | Integration patterns, pollen worker, trainer adapters, control hooks, verifier, LLM route, masking |
| [12_networking.md](12_networking.md) | L3 topology, aya eBPF datapath & maps, policy profiles, DNS proxy, mirrors, ingress, pools |
| [13_observability_testing_bench.md](13_observability_testing_bench.md) | Metrics without per-cell labels, columnar telemetry, DST, fuzz, security corpus, benchmarks, real-data runs |
| [14_roadmap.md](14_roadmap.md) | Milestones M0 to M3 with exit criteria, staffing, risks |
| [15_related_work.md](15_related_work.md) | Bibliography: platforms, RL systems, datasets, reward hacking, virtualization, storage, scheduling, security |

## 5. Reading order

- Newcomers: 01, then 02, 03 and 05.
- Implementers: 03, then the component file (04, 06, 07, 08, 09 or 12), then 13.
- Security reviewers: 10, then 09, 12 and the tiers section of 07.
- RL users: 11, then 05 and 02.

## 6. Open questions (tracked as ADRs)

1. Should the default untrusted container tier be a shield VM, or just microVMs (drop T1-untrusted)? The deciding factors are density versus operational simplicity. Decide after the M2 density benchmarks.
2. How far can fanotify pre-content lazy fill replace ublk for containers across the kernel fleet?
3. Is `proc`-mode fork (DeltaBox-style) worth productizing, or should we ship VM fork only? A files only fork of container cells is in (07 section 5) and freezes the parent for under 200 ms on a loaded host, but carrying process state needs CRIU or VM snapshots.
4. Keeper: is openraft+redb enough at 5 voters for multi-unit, or should there be a FoundationDB option?
5. Should the LLM gateway be part of hivebox or remain an external dependency with a stable contract?
