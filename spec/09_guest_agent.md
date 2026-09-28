# Guest agent: hive-drone

> DSec splits the in-sandbox agent into aether (a proxy on UDS/vsock) and chronus (shell sessions). hivebox merges them into one static binary, `hive-drone`, with two roles. The *channel* role runs privileged and outside the agent's reach. The *worker* roles are unprivileged and per-session. The design follows from real incidents DSec observed: chronus socket forgery and the `/bin/bash` overwrite.

## 1. Packaging

- The drone is a static musl binary with a size target of 4 MiB or less, using the `mimalloc` allocator. It lives in a read-only drone layer (EROFS) mounted at `/.hive/`, owned by root and immutable from inside the cell.
- In VMs (T2/T3) it is PID 1 (`--init` mode). It mounts filesystems, sets up the network from the kernel cmdline or MMDS, reaps zombies, and launches the entrypoint.
- In containers (T1) it runs as the container's PID 1 with a separate uid (`hive-drone`, not root in the userns if possible). The agent's workload runs as `uid 1000` (configurable).
- The drone layer is versioned independently of images, so upgrades need no image rebuild. Comb picks the drone layer version per cell.

## 2. Channel

| Tier | Transport | Endpoint |
|---|---|---|
| T0 proc | socketpair inherited from zygote | n/a |
| T1 container | UDS | host path `/run/hive/cells/<id>/drone.sock`. It is bind-mounted only into the drone's own mount namespace; the workload sees nothing. |
| T2/T3 | vsock | guest CID assigned by comb, port 1024 (drone listens) |

- Framing: length-prefixed frames of the form `{stream_id: u32, kind: u8, flags: u8, len: u24, payload}`. The maximum frame size is 64 KiB. Control messages are prost-encoded (ttrpc-like). Stream data is raw bytes.
- Multiplexing: up to 1,024 concurrent streams, with per-stream credit-based flow control (initial window 256 KiB).
- Handshake: the drone sends `Hello{proto_versions:[3,2,1], drone_build, caps[]}` and comb answers with `Welcome{chosen, session_secret, clock}`.
  - Comb supplies a per-boot nonce through a channel the workload cannot read: the kernel cmdline (scrubbed after read), or MMDS, or the first vsock message.
  - The drone must echo `HMAC(nonce, transcript)` before any command is accepted. A process that forges a socket therefore cannot impersonate either side.
- Version policy: comb supports drone protocol N, N-1 and N-2. Capability flags (`pty`, `watch`, `diff-git`, `envd-compat`, `snapshot-quiesce`) are negotiated.

## 3. Services inside the drone

| Service | Semantics |
|---|---|
| `Process` | spawn with argv/shell, cwd, env, uid, rlimits and its own cgroup (sub-cgroup via delegation in VMs), stdin streaming, signals to the process group, `PR_SET_PDEATHSIG`, pidfd tracking |
| `Pty` | openpty, resize, raw mode |
| `Session` | persistent bash with end-of-command detection (see section 4) |
| `Fs` | read/write/stat/list/remove/rename/chmod, streamed upload/download (tar), inotify watch, `O_NOFOLLOW` + `openat2(RESOLVE_BENEATH)` relative to allowed roots |
| `Diff` | `git diff` / `git status` helpers; layer-diff hints (changed paths) for pack_diff |
| `Quiesce` | freeze/thaw FS (`FIFREEZE`) + `sync` before snapshots; re-seed RNG and fix clock after restore (`vmgenid`, `CLOCK_REALTIME` step) |
| `Health` | liveness, load, mem, disk, fd counts |
| `Envd` | optional E2B envd-compatible Connect server on port 49983 (for pure E2B clients reaching cells via gate) |

## 4. Shell sessions (chronus semantics)

- Each session is `bash --noprofile --norc` or a shell the user picks. It runs from the drone layer's own copy of bash (`/.hive/bin/bash`), which is read-only. Overwriting `/bin/bash` in the workspace cannot break session control.
- Command framing: for each `SessionRun(cmd)`, the drone writes:
  ```
  { <cmd>
  } ; __hive_rc=$? ; printf '\x1e%s:%d\x1e' "$__HIVE_TOKEN_<n>" "$__hive_rc"
  ```
  The token is a random 128-bit value per command. The drone scans output for the sentinel carrying the token, which gives both the exit code and an exact end-of-output boundary. Tokens are never exposed in the environment before use, so a workload can't spoof completion.
- Fallback: if the shell dies, or the sentinel isn't seen within the timeout, the result is `timed_out=true`. The process group gets SIGKILL and the session restarts.
- Interactive mode (`SessionInteract`) is expect-style. It sends input, waits for a regex match or an idle quiet period (`quiet_ms`), and returns the buffered output. This covers `python`, `gdb`, `vim` and similar tools.
- Background jobs: `nohup … &` inside the session is allowed. Long-running processes should use `Process` instead.

## 5. Output handling

- Each stream has a ring buffer (default 1 MiB, capped by `max_output_bytes`). On overflow the drone keeps the head (64 KiB) and the tail, and sets `truncated`. The buffer never grows without bound, which handles the DSec `yes` incident.
- There is also a per-cell aggregate output budget in bytes per minute. Output past the budget is dropped and counted.
- Decoding is left to the client, which gets raw bytes. An optional `utf8_lossy` flag is available.

## 6. Hardening the drone itself

- The channel role runs with a separate uid. In containers it also has `no_new_privs`, a seccomp filter and no network namespace access.
- The workload cannot `ptrace` the drone. In VMs `ptrace_scope` is at least 1. In containers the uid differs and the drone is non-dumpable via `PR_SET_DUMPABLE=0`.
- In containers, `/proc/<drone pid>` is hidden by mounting `/proc` with `hidepid=2`. In VMs the workload runs in a sub-mount-namespace with `hidepid`.
- The drone never follows workload-controlled paths for its own config. Config comes only over the channel.
- At startup the drone checks its own binary against an fs-verity digest. This is optional in VM mode, where the rootfs is already verified by dm-verity/EROFS.

## 7. Restore and fork awareness

After a memory restore (Firecracker snapshot, UFFD), the drone receives a `Restored{new_identity}` message. It then:
1. reseeds the kernel RNG (`RNDADDENTROPY`) and userspace-visible random sources;
2. steps the clock;
3. updates the hostname, IP, MAC and cell id;
4. re-keys the channel secret;
5. notifies sessions via an optional hook script `/etc/hive/on-restore.d/*`.

Forked children get distinct identities before any user command runs.

## 8. Resource footprint targets

Idle RSS is 3 MiB or less. An exec round trip inside the guest takes 200 µs or less. The drone handles at least 5K small exec/s per cell and sustains 1 GB/s file streaming over vsock.
