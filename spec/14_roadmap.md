# Roadmap and Milestones

> Build the node first and measure it on real hardware. Then add the control plane, then scale. Each milestone ends with a benchmark report checked into `docs/bench/`. The benchmark harness itself lives in the companion repository tamnd/hivebox-bench.

## M0: Single-node core (weeks 0 to 8)

- Workspace skeleton, `hive-types`, `hive-proto` (v1 draft), `hive-rt` traits, `hive-telemetry`.
- `hive-comb` standalone mode (local API, no gate): WAL, lifecycle actors, admission, pools (cgroup, netns).
- `hive-drone` v1: channel with nonce handshake, Process, Session (sentinel protocol), Fs, output rings.
- Drivers: `hive-cell-oci` (youki libcontainer, rootless, seccomp/Landlock) and `hive-cell-fc` (Firecracker + jailer, cold boot first, then snapshot restore with plain file mmap).
- `hive-nectar` v0: OCI import to EROFS (meta/data split), local posix BlobStore, L1 cache. No lazy fill yet.
- `hive-guard` v0: aya egress program with `none`/`mirrors` profiles plus the DNS proxy.
- Python SDK (create/run/session/files), `hivectl`.
- Exit criteria:
  - container create p50 ≤300 ms and microVM restore p50 ≤250 ms on one node;
  - 1,000 concurrent cells;
  - SWE-bench Verified gold-patch validation passes ≥99% on the container tier.

## M1: Cluster MVP (weeks 8 to 16)

- `hive-gate` (gRPC + Connect), `hive-waggle` (power-of-k + overlay), `hive-scout` (push reports), `hive-keeper` (openraft + redb; projects, keys, biscuit, quota slices).
- Cell ID routing with epoch fencing; comb registration and leases; re-attach on restart.
- `hive-nectar` v1: 3FS (usrbio) and S3 BlobStores, chunked lazy fill (ublk path first), prefetch traces.
- Pause/resume (Freeze/Reclaim), idle TTL, bulk ops by selector.
- `hive-sim` DST harness with the core invariants.
- E2B compatibility (REST + envd subset).
- Exit criteria:
  - 16 nodes; 1,000 creates/s sustained; 50K concurrent;
  - the slime SWE example runs unmodified via E2B compat;
  - DST 10K seeds clean.

## M2: Scale and density (weeks 16 to 28)

- `hive-uffd` page server with REAP-style prefetch, template warm pools, fork (`vm` mode) ≤300 ms.
- Density: virtio-pmem+DAX (trust-domain scoped), free-page reporting, DAMON, zswap, PSI controller, CPU QoS classes + core scheduling.
- fanotify pre-content lazy fill for containers (kernel ≥6.14), where available.
- `hive-imaged`: composable layers, CAS dedup, relayout by access trace, commit/pack_diff with scrubbing.
- Verifier service (`Verify.Run`) and `hive-pollen` rollout worker; verl and slime adapters; LLM route with token capture.
- Shield-VM mode for untrusted containers.
- Exit criteria:
  - 160 nodes; 5,000 creates/s for 30 min; 400K concurrent;
  - density targets (3,200 containers / 800 microVMs per node);
  - infra error ≤0.1% under trace replay.

## M3: Production hardening (weeks 28 to 40)

- Full security corpus, audit hash chain, SIEM export, quarantine tooling.
- Multi-unit deployment, cloud bursting (pre-staged image subset, 80% threshold), offline rebalancer.
- T3 fullvm driver (QEMU/QMP), experimental `proc` fork mode (DeltaBox-style), WASM T0 + reward plugins.
- Stabilized out-of-process driver plugin API; WIT policy plugins.
- Adapters: SWE-ReX, Harbor, OpenEnv, SandboxFusion, MCP; dataset importers.
- Docs, runbooks, SLO dashboards, upgrade/drain automation.
- Exit criteria:
  - end-to-end GRPO training run on SWE-Gym/SWE-smith for ≥1 week with infra-masked ≤0.1%;
  - third-party security review done.

## Staffing assumption

4 to 6 senior Rust/systems engineers:

- node/runtime ×2
- storage ×1
- control plane ×1
- network/security ×1
- RL integration/SDK ×1

## Main risks

| Risk | Mitigation |
|---|---|
| Kernel feature availability (fanotify pre-content, ublk zero-copy, Landlock ABI) | Capability probing, per-feature fallbacks, node labels in placement |
| 3FS operational complexity | S3 + NVMe cache path is first-class. 3FS is an optimization. |
| Firecracker CVEs / VMM churn | Pin versions, canary pool, CH as a second VMM behind the same trait |
| Memory overcommit incidents | Conservative defaults (1.5×), PSI-driven admission, SnapshotKill escape valve |
| RL framework churn | Adapters are thin. The E2B compatibility layer is a stable fallback. |
