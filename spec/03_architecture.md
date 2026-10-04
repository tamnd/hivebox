# Architecture and repository layout

> Repo: **`github.com/tamnd/hivebox`**. Rust 2024 edition, MSRV 1.90, Apache-2.0.
> In one line: *a DSec-class sandbox fabric for agentic RL, running hundreds of thousands of isolated cells and thousands of creates per second on a few hundred Linux nodes.*

## 1. Vocabulary (hivebox names and their DSec equivalents)

| hivebox | Meaning | DSec equivalent |
|---|---|---|
| **cell** | one sandbox instance (any backend) | sandbox |
| **comb** | node agent daemon (1/node) | edge |
| **drone** | in-cell guest agent (channel + sessions) | aether + chronus |
| **gate** | stateless API ingress / router | API server |
| **waggle** | placement engine (bees' waggle dance = telling others where to go) | placement engine |
| **scout** | cluster-state aggregator (node reports → snapshots) | watcher |
| **keeper** | metadata & IAM service (Raft) | IAM (+ config) |
| **nectar** | image/layer/snapshot storage library & chunk cache | EROFS/OverlayBD/3FS layer |
| **imaged** | image build/convert/relayout service | offline conversion |
| **guard** | network policy datapath (eBPF) + DNS/mirror proxy | eBPF filters |
| **pollen** | RL integration: rollout worker, verifier runner, trainer adapters | worker container |
| **unit** | one scale unit (control plane + ≤~500 nodes) | scale unit |

## 2. System diagram

```
 TRUSTED NETWORK (trainers, GPUs)                 │ UNTRUSTED NETWORK (cells)
                                                  │
  RL framework ─ hivebox SDK (py/rs) ──┐          │
  E2B SDK / SWE-ReX / Harbor ──────────┤ gRPC/    │
  hivectl / Web UI ────────────────────┤ Connect  │
                                       ▼          │
                              ┌─────────────────┐ │
                              │ hive-gate  (N)  │─┼──────────────────────────────┐
                              │ authn, route by │ │                              │
                              │ cell-id prefix  │ │                              │
                              └──┬───────┬──────┘ │                              │
                         create  │       │ authz/quota lease                     │ data plane
                                 ▼       ▼        │                              │ (exec/files/stream)
                     ┌───────────────┐ ┌──────────────────┐                      │
                     │ hive-waggle(N)│ │ hive-keeper (3/5)│ Raft: tenants,       │
                     │ placement     │ │ IAM, quotas,     │ projects, policies,  │
                     └──────┬────────┘ │ templates, imgs  │ image manifests      │
                            │ snapshot └──────────────────┘                      │
                     ┌──────┴────────┐          ▲ leases                         │
                     │ hive-scout (N)│◀─ push ──┼───────────────┐                │
                     └───────────────┘          │               │                │
 ─────────────────────────────────────────────────────────────────────────────── │
                                  NODE (×160-500)               │                │
                     ┌──────────────────────────────────────────┴───────┐        │
                     │ hive-comb: admission · lifecycle WAL · pools ·   │◀───────┘
                     │ rootfs (nectar) · guard (aya eBPF, DNS proxy) ·  │
                     │ CellDriver registry · uffd · blockd (ublk) ·     │
                     │ metrics/trace/audit shipper                       │
                     └───┬──────────┬──────────┬──────────┬─────────────┘
                         │          │          │          │
                     T0 wasm/  T1 containers  T2 Firecracker  T3 QEMU
                     proc pool (in shield VM)  microVMs        full VMs
                         └── hive-drone (UDS / vsock) in every cell ──┘
                                        │
                           L1 NVMe chunk cache ─── L2 3FS / S3 (nectar)
```

## 3. Core design decisions and rationale

| # | Decision | Rationale |
|---|---|---|
| D1 | **No consensus on the per-cell path.** The node (comb) is authoritative for its cells (in-memory + local WAL). | DSec, Dirigent: persistent writes on the critical path cap K8s at ~100 to 300 pods/s; etcd would hit ~25K writes/s at 5K creates/s. |
| D2 | **Cell ID encodes owner**: `cell_id = base32(unit:8 | node:16 | epoch:16 | seq:40 | mac:48)` | Any gate routes with zero lookup (DSec); epoch fences zombie nodes; MAC (truncated HMAC) makes IDs unguessable. |
| D3 | **Placement uses stateless replicas with batch power-of-k and an in-flight overlay. comb has final admission.** | DSec section 7; Sparrow batch sampling for 32K bursts; Omega-style optimism without a shared store. |
| D4 | **Quota by leased slices** from keeper to gates/combs. | Removes per-create consensus; bounded overshoot. |
| D5 | **Composable EROFS layers, metadata local, data on 3FS/S3, lazy fill, trace prefetch.** | DSec 1.71×/1.76× wins; 4 to 13% touched. |
| D6 | **Four backend tiers behind `CellDriver`; the API exposes the tier.** | DSec Table 1; heterogeneity. |
| D7 | **MicroVM create is a restore from a template snapshot via our own UFFD server.** | E2B/AgentENV ≤150 ms; REAP/FaaSnap prefetch. |
| D8 | **Pool everything kernel-side** (netns, veth/tap, cgroups, IPs, template VMs). | RTNL & cgroup contention are the real node bottleneck (Dirigent ~1,750/s cap, LPC'24). |
| D9 | **Default-deny egress via aya eBPF + DNS/mirror proxy; policies mutable mid-life.** | DSec misbehavior; ROCK crypto-mining/reverse-SSH. |
| D10 | **Drone channel hardened** (outside cell-writable namespace, per-boot nonce, LSM-protected). | DSec chronus socket forgery, `/bin/bash` overwrite. |
| D11 | **Verifier runs in a separate cell** from snapshot/diff. | Harbor, Composer, reward-hack literature. |
| D12 | **Everything sits behind traits (`Clock`, `Rng`, `Transport`, `CellDriver`, `BlobStore`) so we can run deterministic simulation.** | FDB/DSQL-style DST catches control-plane races. |
| D13 | **E2B-compatible surface** in addition to native gRPC. | Ecosystem gravity (slime, OpenHands, AgentENV). |

## 4. Cargo workspace

```
hivebox/
├── Cargo.toml                    # workspace, [workspace.dependencies] pinned
├── rust-toolchain.toml           # stable 1.9x
├── deny.toml  supply-chain/      # cargo-deny, cargo-vet
├── proto/hivebox/v1/*.proto      # public API (buf-managed)
├── proto/hivebox/internal/*.proto# gate↔comb, comb↔drone, scout, keeper
├── crates/
│   ├── hive-types/        # ids, specs, states, errors, resource math (no_std-friendly core)
│   ├── hive-proto/        # prost/tonic generated + conversions
│   ├── hive-rt/           # Clock/Rng/Transport traits, tokio impl, sim impl (turmoil/madsim)
│   ├── hive-auth/         # API keys, biscuit tokens, mTLS (SPIFFE) helpers
│   ├── hive-gate/         # bin: ingress, routing, E2B compat, Connect/REST, WS
│   ├── hive-waggle/       # lib+bin: placement engine
│   ├── hive-scout/        # lib+bin: cluster state aggregation
│   ├── hive-keeper/       # bin: openraft + redb, IAM, quotas, templates, image registry
│   ├── hive-comb/         # bin: node agent
│   ├── hive-cell/         # CellDriver trait, registry, conformance harness
│   ├── hive-cell-wasm/    # T0 wasmtime
│   ├── hive-cell-proc/    # T0 zygote pool
│   ├── hive-cell-oci/     # T1 youki libcontainer / crun
│   ├── hive-cell-fc/      # T2 Firecracker (+jailer)
│   ├── hive-cell-ch/      # T2 Cloud Hypervisor
│   ├── hive-cell-qemu/    # T3 QMP
│   ├── hive-cell-plugin/  # out-of-process driver bridge (gRPC over UDS)
│   ├── hive-uffd/         # userfaultfd page server
│   ├── hive-snap/         # SnapshotRef tree, diff/merge, fork orchestration (07)
│   ├── hive-nectar/       # layers, manifests, CAS, BlobStore impls, L1 cache, filler
│   ├── hive-blockd/       # ublk targets (OverlayBD-compatible CoW)
│   ├── hive-imaged/       # bin: build/convert/relayout/verify pipeline
│   ├── hive-guard/        # aya eBPF programs (+ -ebpf crate) + userspace policy + DNS proxy
│   ├── hive-drone/        # bin (static, musl): guest agent, PID1 mode for VMs
│   ├── hive-pollen/       # rollout worker, verifier runner, trainer adapters
│   ├── hive-sdk/          # Rust client SDK
│   ├── hive-telemetry/    # tracing/otel/metrics setup, cardinality guard, audit log
│   ├── hive-sim/          # deterministic simulation harness & scenarios
│   └── hivectl/           # bin: CLI
├── sdk/python/hivebox/    # asyncio SDK (grpcio or pure h2), pyo3 fast path optional
├── guest/                 # guest kernel configs, template rootfs recipes
├── deploy/                # systemd units, Ansible, Nix, Helm (control plane only), Terraform
└── docs/                  # mdBook; ADRs in docs/adr/
```

The load generator, trace replay and the benchmark suites live in a separate repository, [tamnd/hivebox-bench](https://github.com/tamnd/hivebox-bench), so that the numbers are measured against released code and a benchmark change never needs a change here.

### 4.1 Dependency choices (pinned in the workspace)

| Area | Crate |
|---|---|
| async | `tokio` 1.5x (multi-thread for services; per-core current-thread groups in comb exec fan-out) |
| RPC | `tonic` 0.14 + `prost` 0.14; `connect-rust` for Connect; `axum` 0.8 for REST/admin; `ttrpc`-style framing for drone (own crate, prost-encoded) |
| TLS | `rustls` 0.23 (aws-lc-rs) |
| state | `openraft` 0.9 + `redb` (keeper); `redb` WAL (comb) |
| maps | `papaya` (read-heavy routing), `scc` (write-heavy cell tables) |
| syscalls | `rustix` 1.x (prefer), `nix` where needed; `rtnetlink`; `cgroups-rs` or direct cgroupfs |
| isolation | `libcontainer`/`oci-spec` (youki), `seccompiler`, `landlock`, `userfaultfd`, `kvm-ioctls` (probing) |
| eBPF | `aya` 0.14 |
| storage | `libublk`, `io-uring`, `blake3`, `fastcdc`, `object_store`/`opendal`, `usrbio` (3FS FFI), `erofs-rs` (reader) |
| wasm | `wasmtime` + `wasmtime-wasi` |
| auth | `biscuit-auth` 6 |
| telemetry | `tracing`, `opentelemetry` 0.33 (pinned), `metrics`/`prometheus-client` |
| alloc | `tikv-jemallocator` (daemons), `mimalloc` (drone) |
| DST | `turmoil` (net) + `madsim`-style clock/rng shims |
| serde | `serde`, `rkyv` (WAL records, snapshot metadata) |

## 5. Process model per node

| Process | Privilege | Notes |
|---|---|---|
| `hive-comb` | root (CAP_SYS_ADMIN, NET_ADMIN, BPF); **minimal code in privileged paths**, split helpers | main daemon; restart-safe (cells survive) |
| `hive-uffd` | dedicated uid, CAP_SYS_PTRACE-free (receives uffd via SCM_RIGHTS) | supervised; per-VM fds |
| `hive-blockd` | root for ublk ctrl, then drops | may run in-proc with comb (feature flag) |
| `hive-guard-dns` | unprivileged | DNS allowlist proxy + mirror reverse proxies |
| `firecracker` ×N | jailed per cell | one per microVM |
| shield VM(s) | QEMU/CH | hosts T1/T0-proc in untrusted mode; runs its own `hive-comb --shield` |

## 6. Request flows

### 6.1 Create (fast path, target p50 ≤150 ms warm)

1. The SDK sends `CreateCells{template, n, labels, idem_key}` to a gate.
2. The gate does authn (biscuit or API key), then authz (cached policy), then a quota lease check against its local slice. The slice is refilled asynchronously from keeper.
3. The gate calls waggle `Place{spec, n}`, which returns `[(node, count)]` (batch power-of-k with overlay). A single cell with an idempotency key and no affinity goes first to its key's home, the healthy node with the highest rendezvous hash of the project, the key and the node, so a retry through any gate reaches the comb that has the key. If the home is full it turns the cell away, and the next try takes the next node in the key's order, so every gate tries the same nodes in the same order. A comb remembers a key it turned away for room for five to ten minutes and turns it away again even once it has room, so a retry walks past it to the node that made the cell. When every node in the walk turned the cell away the gate walks it once more with the `x-hive-anyway` header, and the first node with room makes it. A comb that restarts forgets the keys it turned away. A keyed create is never sent to another node when the comb can't be reached, because the comb may already have it, so the caller gets an error and retries with the same key. A key can still make two cells when its home is alive but out of the gate's view, or when two gates see the nodes above the cell's node in the key's order differently. If the home is cut off from the keeper as well, it loses its lease and stops its copy, and if it is cut off from scout only, both copies run until one ends.
4. The gate sends `Admit+Create{spec, count}` to each chosen comb, batched as one RPC per node.
5. comb runs admission (hard limits), reserves a pooled slot (netns/tap/cgroup/IP), mounts or attaches through `nectar` (with trace prefetch), calls the driver's `prepare/start` (or does a UFFD restore), completes the drone handshake and marks the cell `RUNNING`. It appends to the WAL with group commit (≤1 ms).
6. comb returns cell IDs, which are minted in step 5 with node and epoch. The gate streams results back as each node completes. Partial success is allowed, and rejected cells are re-placed at most 2 times.

### 6.2 Exec (data path, target p50 ≤5 ms)

The SDK calls a gate, which routes by ID prefix over a persistent h2 connection to the comb. The comb forwards to the drone over UDS or vsock using multiplexed frames, and the drone hands the call to the session. Output frames stream back with backpressure.

### 6.3 Pause/resume, snapshot, fork

See 07 and 11.

## 7. Failure domains

| Failure | Effect | Recovery |
|---|---|---|
| gate instance | in-flight streams drop | SDK retries on another gate (anycast/DNS); exec is at-most-once unless it has an idempotency key |
| waggle/scout | placement uses a stale snapshot | stateless restart; rebuild from node pushes in ≤2 s |
| keeper leader | quota refills pause (leases cover ≥30 s) | Raft election ≤1 s |
| comb crash | no new ops on that node | cells keep running; comb restarts, replays the WAL, re-attaches drones/VMMs (≤5 s) |
| node loss | its cells go to `FAILED(infra_node_lost)` after lease TTL, and a comb that loses its lease stops its own cells the same way | trainer masks; optional auto-restore from the last checkpoint elsewhere |
| 3FS degraded | cold creates slow | L1 cache pins keep warm bases working; admission throttles cold starts |
