# hivebox

A sandbox fabric for agentic reinforcement learning, written in Rust.

hivebox runs hundreds of thousands of short lived, isolated execution environments on an ordinary Linux cluster. Each one is called a cell. A cell can be a WebAssembly instance, a container, a Firecracker microVM or a full QEMU guest, behind one API. An RL trainer creates cells by the thousand, runs an agent's shell commands and tests inside them, scores the result and throws them away. Evaluation harnesses and agent products do the same thing more slowly.

The design follows DeepSeek's DSec ([arXiv 2609.22978](https://arxiv.org/abs/2609.22978)), which reports one scale unit of about 160 nodes serving around three million sandboxes a day, 380K of them concurrently at peak, with more than 5,000 creates a second. hivebox takes that as the bar and tries to reach it in the open, with the parts DSec leaves unspecified written down and measured.

This is early. What exists is the workspace, the layer rule that keeps it modular, the cell id codec and state machine that everything else will share, and CI. The full technical design is in [`spec/`](spec/) and the milestones that build it are tracked as issues.

## The targets

Per scale unit of about 160 nodes:

- 5,000 creates a second, sustained for 30 minutes.
- 400K concurrent cells.
- 3,200 containers or 800 microVMs on one node.
- A warm create at p50 of 150 ms or less, an exec round trip at p50 of 5 ms or less, and a fork into eight branches in 300 ms or less.
- Infrastructure failures at 0.1% or less, every one of them classified so that a trainer can mask it out of the reward instead of learning from it.

Each of these is a number in [`spec/02_requirements_slos.md`](spec/02_requirements_slos.md) that the benchmark harness in [tamnd/hivebox-bench](https://github.com/tamnd/hivebox-bench) either reproduces or does not.

## Design in one page

**No consensus on the hot path.** A cell's state lives on the node that runs it. The node agent, `hive-comb`, owns every transition and writes each one to a local WAL before acting on it. The metadata service is a small Raft group that holds projects, keys, quotas and templates, and it is never asked anything during an exec or a file read.

**The id is the route.** A cell id carries the node that owns it and that node's registration epoch, plus a 48 bit tag keyed per unit so that ids cannot be guessed. The gateway routes a request by decoding the id, without a lookup, and a request for a cell on a node that has since re-registered fails fast with `CELL_LOST`.

**Four isolation tiers behind one driver trait.** T0 is WebAssembly or a forked process, T1 is a container through youki, T2 is a Firecracker or Cloud Hypervisor microVM, and T3 is QEMU. Untrusted code runs at T2 or inside a shield VM. A new backend is a crate that passes the conformance suite in `hive-cell`, and it can run out of process through the plugin bridge.

**Bytes arrive lazily.** Images are composable EROFS layers served from a content addressed store on 3FS or S3, filled on demand and prefetched in the order the last run read them. MicroVM disks are copy on write block devices served through ublk. A restored microVM starts before its memory has arrived and a userfaultfd page server fills it.

**Pack on what is used, not on what is requested.** Density comes from virtio-pmem with DAX, free page reporting, DAMON and zswap, with CPU QoS classes and core scheduling keeping latency sensitive work ahead of batch. When a node runs out anyway, idle cells are paused or snapshotted and killed rather than refused.

**Deterministic simulation from the start.** Every service reads time, randomness and the network through `hive-rt`, so that `hive-sim` can run the whole control plane in one process with failures injected from a seed. A failure found there can be replayed.

## The workspace

| Crate | What it is |
|---|---|
| `hive-types` | ids, specs, the cell state machine, resource arithmetic |
| `hive-proto` | the `hivebox.v1` API and the internal services |
| `hive-rt` | clock, randomness and transport traits for simulation |
| `hive-telemetry` | tracing, metrics and the audit log |
| `hive-auth` | API keys, biscuit tokens, mTLS |
| `hive-gate` | the gateway: authentication, admission, routing, E2B compatibility |
| `hive-waggle` | placement |
| `hive-scout` | cluster state |
| `hive-keeper` | metadata on openraft and redb |
| `hive-comb` | the node agent |
| `hive-cell` | the driver trait and conformance suite |
| `hive-cell-wasm`, `hive-cell-proc`, `hive-cell-oci`, `hive-cell-fc`, `hive-cell-ch`, `hive-cell-qemu`, `hive-cell-plugin` | the drivers |
| `hive-uffd`, `hive-snap` | snapshot restore, the snapshot tree and fork |
| `hive-nectar`, `hive-blockd`, `hive-imaged` | images, block devices and the image pipeline |
| `hive-guard` | egress policy in eBPF and the DNS allowlist proxy |
| `hive-drone` | the guest agent inside every cell |
| `hive-pollen` | the rollout worker and trainer adapters |
| `hive-sdk`, `hivectl` | the Rust client and the CLI |
| `hive-sim` | deterministic simulation |

Every crate has a rank in `xtask/layers.toml` and may depend only on crates of lower rank. `cargo xtask layers` checks it on every commit.

## Building

```
cargo build --workspace
cargo xtask ci
```

The toolchain is pinned in `rust-toolchain.toml` and the oldest supported Rust is in `Cargo.toml`. The node side needs Linux 6.12 or newer with cgroup v2 only, and KVM on bare metal for the microVM tier, per `spec/02_requirements_slos.md`. The client crates build anywhere Rust does.

## Related repositories

- [tamnd/hivebox-bench](https://github.com/tamnd/hivebox-bench) is the benchmark harness: the load generator, DSec trace replay, node and cluster suites, and the runs on real datasets such as SWE-bench Verified.

## License

Apache-2.0. See [LICENSE-APACHE](LICENSE-APACHE).
