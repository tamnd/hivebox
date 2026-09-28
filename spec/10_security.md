# Security

> hivebox runs adversarial code by design. RL policies are optimized to maximize reward and will find any shortcut. The threat model therefore treats every cell as an active attacker. That covers escape, lateral movement, resource abuse and reward tampering. It also covers the host's own control channels.

## 1. Assets and adversaries

| Asset | Threat |
|---|---|
| Host kernel / node | cell escape (container → host, guest → VMM → host) |
| Other cells | cross-tenant read/write, side channels, noisy neighbor |
| Control plane & trainers | lateral movement from untrusted network to trusted network |
| Reward signal | tampering with tests, graders, timing, exit codes |
| Data (images, snapshots, datasets) | poisoning of shared images, exfiltration of held-out tests |
| Infrastructure | crypto-mining, DDoS from cells, reverse shells (ROCK/ROLL report) |

There are four adversaries:
- (A1) the RL policy under training, which is unintentionally adversarial and highly creative;
- (A2) malicious tenant or user code;
- (A3) a compromised dependency inside a cell image;
- (A4) an insider with project credentials.

## 2. Isolation tiers and when to use them

| Tier | Boundary | Allowed for |
|---|---|---|
| T0 wasm | wasmtime (fuel/epoch, no WASI network) | pure-function verifiers, untrusted |
| T0 proc | seccomp + Landlock + userns + cgroup | trusted code only (own graders) |
| T1 container (trusted image mode) | namespaces + seccomp + LSM, host kernel | internal, curated images, `trusted_image=true`, project policy permits |
| T1 container (untrusted mode) | container inside a shield VM (DSec runs containers in a patched Docker inside QEMU) | default for RL rollouts |
| T2 microVM | KVM + Firecracker + jailer + seccomp | default for network-enabled, env-building, docker-in-cell |
| T3 fullvm | KVM + QEMU (sandboxed with seccomp, `-sandbox on`, run as unprivileged user) | GUI/Android/custom kernels |

Project policy sets `max_isolation_floor`. For example, `rl-*` projects cannot request T1 trusted mode. The reason is that LSMs are not a boundary. The runc CVE series (2024 CVE-2024-21626 "Leaky Vessels", the 2025 /proc-write and maskedPaths races) shows that container runtimes keep failing under hostile workloads. Unprivileged user namespaces featured in about 44% of kCTF submissions, so they are disabled inside cells for T1.

## 3. Hardening checklist per tier

### 3.1 Containers (T1)

- Containers run rootless in a userns, with a host uid shift range per cell. They get no capabilities except those in the template allowlist, and the default allowlist is empty.
- seccomp uses a default-deny allowlist based on the Docker default, with the following denied:
  - `unshare`, `clone3` with namespace flags, `setns`, `mount`, `umount2`, `pivot_root`
  - `bpf`, `perf_event_open`, `io_uring_*`, `userfaultfd`, `keyctl`, `add_key`, `request_key`
  - `kexec_*`, `init_module`, `ptrace` (except own children when `ptrace` capability is requested), `process_vm_*`
  - `open_by_handle_at`, `name_to_handle_at`, `fanotify_init`, `lookup_dcookie`, `move_mount`, `fsopen`, `fspick`, `fsconfig`, `open_tree`
- ioctls are restricted by an allowlist (seccomp arg filter): TTY ioctls, `FIONREAD`, `FIOCLEX`, and a few file ioctls. XFS_IOC_SWAPEXT and filesystem-specific ioctls are denied. DSec observed XFS_IOC_SWAPEXT being used as a path to tamper with shared files.
- /proc and /sys masking:
  - mask `/proc/kpagecgroup`, `/proc/kpageflags`, `/proc/kpagecount` (DSec kernel crash), `/proc/kcore`, `/proc/sched_debug`, `/proc/timer_list`, `/proc/sysrq-trigger`, `/proc/keys`, `/sys/firmware`, `/sys/kernel/debug`;
  - mount `/sys` read-only;
  - `hidepid=2`.
- Landlock is a defense-in-depth layer in the drone worker. It needs ABI 4 or later for network rules, 6 or later for scoping signals and abstract UDS, and 7 for audit logging. The workload can write only in the workspace and `/tmp`. Abstract UDS and signals are scoped so the workload cannot reach the drone or other sockets.
- An AppArmor or SELinux profile (the standard container profile) applies on top of the above.
- Limits: `pids.max` (default 1,024), `memory.max`, `memory.swap.max`, `cpu.max`, `io.max`, rlimits (`NOFILE` 65,536, `CORE` 0), and a project quota on the upper layer.
- There are no host-path mounts. `/dev` is a minimal tmpfs (null, zero, full, random, urandom, tty, ptmx, pts, shm).
- There is no docker socket. Docker-in-cell requires T2.

### 3.2 microVMs (T2)

- Firecracker runs under jailer, with a chroot, a unique uid/gid, its own netns and cgroup, and the FC seccomp filters at the default strict level.
- We track Firecracker security advisories: CVE-2026-5747 (PCI transport; keep the legacy MMIO transport unless PCI is needed) and CVE-2026-1386 (jailer). Updates are pinned and tested through a canary node pool.
- Virtio devices are kept to a minimum: block, net, vsock, balloon, pmem, entropy. There is no virtio-fs unless required. If it is required, virtiofsd runs sandboxed and read-only.
- The guest kernel uses our own minimal config with no module loading (`CONFIG_MODULES=n`), lockdown=integrity, and KASLR. Unprivileged userns stays enabled inside the guest, because the guest is its own boundary.
- On the host side, UFFD and the snapshot files are readable only by the jailer uid of that cell and by hive-uffd.
- Mitigations: the host kernel runs with retbleed/SRSO/ITS mitigations on. For untrusted tenants, SMT is off or core scheduling is applied per project.

### 3.3 Shield VM mode (T1 untrusted)

- There is one QEMU or Cloud Hypervisor VM per trust domain (per project or group). It runs its own mini-comb, and containers run inside it.
- The shield isolates the host kernel from container escapes. An escape stays confined to the shield, which holds only one trust domain's cells.
- Page-cache sharing via virtio-pmem+DAX happens only inside one shield or trust domain. Physical pages are never shared across tenants.

## 4. Network security

- Cells sit on an untrusted network. The only reachable services are the gate data-plane endpoints (for port exposure), the node DNS proxy, mirrors and the LLM gateway, all on link-local VIPs. eBPF denies everything else by default (see 12).
- Trusted and untrusted networks are separated with different VRFs/VLANs. Only the gate spans both.
- Cell-to-cell traffic is denied by default. It is allowed only within an explicit `group` with `allow_intra_group`.
- Egress policy can change during a cell's life. For example, allow `pypi` during env setup, then set `none` for the rollout (the DSec stage-specific policies).
- Metadata endpoints (`169.254.169.254`, cloud IMDS) are hard-denied.
- Rate and connection limits per cell stop DDoS and scanning.

## 5. Control plane security

- All hive components talk mTLS to each other, with SPIFFE IDs (`spiffe://hivebox/unit/u1/comb/node-17`). Certs are short-lived (24 h) and issued by a small internal CA (or SPIRE).
- User auth: API keys (argon2id-hashed, displayed once) are exchanged for biscuit tokens (Ed25519, attenuable, valid for 1 h or less). Per-cell access tokens are attenuated biscuits bound to `cell_id`.
- Authorization is evaluated at the gate against cached keeper policy (allow/deny rules plus bounded delegation). Deny takes precedence.
- Comb trusts only the gate SPIFFE identity for data-plane ops and the keeper for policy. The drone trusts only comb, via nonce plus HMAC.
- The audit log (section 7) covers every control op.

## 6. Reward-hacking mitigations (platform level)

| Attack (observed in literature/DSec) | Mitigation |
|---|---|
| Edit or delete tests, `conftest.py` monkeypatching, `pytest` plugins, `sys.exit(0)` | The verifier runs in a separate cell. It is built from the base image plus the agent's *source diff only*, with tests copied in after the diff is applied. Protected paths (tests, conftest, CI configs, `setup.cfg [tool:pytest]`) are reset from the pristine source. Diffs that touch protected paths are flagged. |
| Reading hidden tests or answers | Tests are never in the agent cell. Datasets are stored outside the cell network. Git history is scrubbed (`.git` rebuilt with a single squashed commit, no future refs, no packed objects from the upstream fix). Egress is denied to github.com and similar during rollout. |
| Overwriting the harness (`/bin/bash`, python, the test runner) | The verifier uses its own toolchain layer (read-only) and fresh processes. Session control uses the drone's bash (09 section 4). |
| Timing / exit-code spoofing | The verifier parses structured results (JUnit XML written to a path only the verifier can write) instead of relying on stdout or exit code alone. Hard timeouts come from the host. |
| Environment probing (detecting eval versus train) | Identical templates and network profiles for train and eval. The drone exposes no mode flags. |
| Resource exhaustion (fork bombs, `yes > f`, 10s of GB) | pids, disk and output caps. Classified as the agent's fault (`output_limit`, `disk_quota`), not infra. |
| Kernel poking (`/proc/kpage*`, exotic ioctls) | Masks and the ioctl allowlist. Such attempts are logged as `policy_violation` security events and can feed a penalty signal. |

The platform also emits per-cell tamper signals in the rollout metadata: protected-path writes, denied syscalls, denied egress, and drone-channel anomalies. The trainer can penalize these or filter the samples.

## 7. Audit

- Every API call, exec (argv, cwd, exit, bytes), file write, policy change, and snapshot/commit is recorded as an `AuditEvent{ts, principal, project, cell, op, args_digest, result, node, trace_id}`.
- Transport: comb ships batches to an append-only log in the object store. The log is hash-chained per node per hour, and the chain root is published to keeper. From there events land in a queryable columnar store (ClickHouse/Parquet).
- Audit data is stored out of cell reach. Retention is configurable, with a default of 30 d for full events and 1 y for digests.

## 8. Workspace and data hygiene

- Commit/pack_diff scrubbing removes `.bash_history`, `~/.cache` credentials, `.env` files matching secret patterns, and `.git/config` credentials. A trufflehog-style regex pass also runs, and any hit blocks the commit unless overridden.
- Images are content-addressed and signed at build (Sigstore/cosign or an internal Ed25519 key). Their provenance (who or what agent built them, and from which cell) is recorded in the manifest.
- Shared bases are immutable. Workspace and toolkit layers are owned per project.

## 9. Supply chain (hivebox itself)

We use `cargo-deny` (licenses, advisories, bans, sources), `cargo-vet` (audits imported from Mozilla/Google/Bytecode Alliance), `cargo-auditable` in release binaries, a CycloneDX SBOM, reproducible builds (Nix), signed releases, and minimal `unsafe`. Every `unsafe` block needs a `// SAFETY:` comment and lives in `*-sys`/FFI modules. Miri runs on the data-structure crates. Fuzzing (cargo-fuzz) covers the drone frame parser, the proto decoders, the EROFS/manifest parsers and the API JSON surface.

## 10. Incident runbook hooks

- `hivectl cell quarantine <id>` freezes the cell, takes a snapshot for forensics, and cuts the network.
- `hivectl project freeze <p>`.
- Security events go to a SIEM sink.
- On a kernel crash, the node is removed and its cells are marked `infra_node_lost` (masked). Crash dumps are collected, and the triggering cell spec is fingerprinted and blocked.
