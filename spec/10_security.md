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
- What is in now: the chain itself, in `hive_telemetry::audit`. A node's events go one JSON line each into a file per UTC hour, and each line holds a sequence number, the event with its arguments as a BLAKE3 digest, the hash of the line before it and its own hash. When the hour turns, a seal next to the old file records its first sequence number, its count and its root, the hash of its last line, and the first line of the new hour points back at that root. A thread of its own writes whatever has queued up as one batch with one sync. On open, a last line cut short by a crash is cut off, an hour left unsealed by a crash is sealed, and any other damage stops the log from opening. `hivectl audit verify DIR` checks every hash, link, sequence number and seal and names the line where the chain breaks. On server3 at load 80, in a release build, 8 threads recorded a million events in 23.6 to 31.3 s, 32,000 to 42,000 a second, with about 4,080 events a sync and 433 bytes an event. The writer spent 9.0 to 10.5 us of CPU an event and 6.2 to 6.9 s waiting for a CPU, and a `record` call took 0.20 us at the median and 0.55 to 0.71 ms at p99. Verifying ran at 71,000 to 171,000 events a second. One event on its own took 5.5 to 9.7 ms at the median to reach the disk. Not in yet: shipping the hours to the object store and the columnar store.
- The comb records every call on its API into its node's chain: cells, exec, files, snapshots, verify and the LLM gateway, one event a call, and one a cell for a call on many cells. The log goes in `audit` under the data directory, or where `[audit] dir` says, and an empty `dir` turns it off. The principal is what the gate put in `x-hive-principal`, `key:` or `token:` and the first 16 hex digits of the key's hash, which the gate sets over anything the caller sent, and `local` for a call on the comb's own socket. An empty or overlong principal is refused. The arguments go in as one BLAKE3 hash with each argument's length in front, so file contents, stdin, env values and API keys are never in the log. The result is `ok`, with counts such as `bytes=` or `session=` where there are some, the exit code or signal, a timeout and the output sizes for a command, or the gRPC code of an error. A streamed exec is recorded when it ends, or as `Cancelled` when the caller goes away. The trace id comes from the W3C `traceparent` header. Syncs are at least `[audit] sync_gap` apart, 1 s unless set. An event after a quiet spell is synced at once, a flush or shutdown does not wait, and the writer sleeps out the gap in one go and does not wake for each event. Syncing every batch, which is what the first version did, made every sync commit the ext4 journal on the disk the cells write to. On server3 that cut file writes through the comb to 363 to 456 a second against 590 to 760 with the log off, and with the log on tmpfs they ran at 654 to 684 against 677 to 703. With the 1 s gap, in a release build at load 84 to 108, 32 clients of the python SDK on 8 cells ran `exec.run` at 469 to 568 calls a second with the log on against 459 to 553 off, and 4 KiB file writes at 532 to 569 against 508 to 720. The writer spent 6 to 12 us of CPU an event, an event took 416 bytes, and every chain verified.
- Each comb publishes its chain's roots to the keeper with its lease renewal. A renewal carries up to 64 hours sealed since the last one the keeper holds, each with its first sequence number, its count, the root it follows on from and its own root, and the chain's tip: the open hour, the events so far and the hash of the last one. The keeper writes the renewal and the roots as one Raft entry, and the renewal goes through even when the roots are refused. It takes a new hour only if it starts where the last one held ends and points back at its root, an hour it already holds only if it is the same, and a tip only if its count never goes back, it has the same root as the tip held when the count is the same, and it agrees with every hour it is checked against. Only the comb holding the node's live lease, in its epoch, can publish, and a refused command changes nothing. A refusal comes back on the lease as `audit_refused` and the comb logs it once. The keeper keeps the last 720 hours, 30 days, of each node. The comb only reads the seals again when an hour has been sealed or the keeper's last hour has moved, and does not send a tip the keeper has already taken, so a quiet node's renewals are as small as before. On server3 at load 69 to 88, in a release build, with three members on one machine and 1,000 combs renewing once a second, each renewal carrying a new tip and every tenth one a sealed hour, renewals ran at 990 to 999 a second with none failing and no roots refused, at 103 to 147 ms at p50 and 0.75 to 0.96 s at p99, against 88 to 95 ms and 0.92 to 1.12 s without roots. The leader used 14 to 18 s of CPU in each 60 s run either way. `hivectl audit verify DIR --keeper HOST:PORT` reads the node's roots from the keeper and checks that the chain on disk ends each sealed hour, and the events up to the tip, in the roots published for them, so a chain rewritten from some line on and hashed again from there is caught at the line where it was changed, and a chain cut short is caught too. Two gaps are left. The open hour is pinned only at the last tip published, so a node that rewrites events in it and moves its tip on is not caught until the hour is sealed and published. And a node that loses its audit dir has every publish refused, since its new chain does not follow on from the one held, until an operator clears the chain at the keeper, which has no command yet.

## 8. Workspace and data hygiene

- Commit/pack_diff scrubbing removes `.bash_history`, `~/.cache` credentials, `.env` files matching secret patterns, and `.git/config` credentials. A trufflehog-style regex pass also runs, and any hit blocks the commit unless overridden.
  - What is in now: scrubbing is on by default in `hive-nectar commit`. It leaves out shell and REPL histories anywhere and credential files under a home (`.git-credentials`, `.netrc`, `.pypirc`, `.aws/credentials`, `.docker/config.json`, `.kube/config`, the gh and Hugging Face tokens and `.ssh/id_*`), takes the user and password out of URLs in `.git/config`, and drops `.env`, `.env.*` and `.npmrc` files that hold a secret. Every other file up to 8 MiB, outside `site-packages`, `dist-packages` and `node_modules`, is searched for private keys and AWS, GitHub, GitLab, Slack, OpenAI, Anthropic, Hugging Face, Google, Stripe and npm tokens. A hit refuses the commit and names the file, line and rule, never the secret, unless the path is under `--allow`. The manifest's provenance lists what was left out, rewritten, allowed or too big to search.
- Images are content-addressed and signed at build (Sigstore/cosign or an internal Ed25519 key). Their provenance (who or what agent built them, and from which cell) is recorded in the manifest.
- Shared bases are immutable. Workspace and toolkit layers are owned per project.

## 9. Supply chain (hivebox itself)

We use `cargo-deny` (licenses, advisories, bans, sources), `cargo-vet` (audits imported from Mozilla/Google/Bytecode Alliance), `cargo-auditable` in release binaries, a CycloneDX SBOM, reproducible builds (Nix), signed releases, and minimal `unsafe`. Every `unsafe` block needs a `// SAFETY:` comment and lives in `*-sys`/FFI modules. Miri runs on the data-structure crates. Fuzzing (cargo-fuzz) covers the drone frame parser, the proto decoders, the EROFS/manifest parsers and the API JSON surface.

## 10. Incident runbook hooks

- `hivectl cell quarantine <id>` freezes the cell, takes a snapshot for forensics, and cuts the network.
- `hivectl project freeze <p>`.
- Security events go to a SIEM sink.
- On a kernel crash, the node is removed and its cells are marked `infra_node_lost` (masked). Crash dumps are collected, and the triggering cell spec is fingerprinted and blocked.
