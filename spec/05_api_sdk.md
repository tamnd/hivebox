# Public API, state machine, SDKs and compatibility

> Package `hivebox.v1` (protobuf, buf-linted, breaking-change checked in CI). It is served over gRPC (h2), Connect (JSON/proto over HTTP/1.1 or h2), and a thin REST mapping. Stability: `v1` is additive only, and the guest protocol supports N-2.

## 1. Services

```proto
syntax = "proto3";
package hivebox.v1;

service Cells {
  rpc Create(CreateRequest) returns (stream CreateEvent);          // batch n ≥ 1, streamed per cell
  rpc Get(GetCellRequest) returns (Cell);
  rpc List(ListCellsRequest) returns (ListCellsResponse);          // by project + label selector, paginated
  rpc Watch(WatchCellsRequest) returns (stream CellEvent);         // state transitions
  rpc Pause(CellSelector) returns (BulkResult);                    // single id or label selector
  rpc Resume(CellSelector) returns (BulkResult);
  rpc Stop(StopRequest) returns (BulkResult);
  rpc ExtendTtl(ExtendTtlRequest) returns (Cell);
  rpc UpdatePolicy(UpdatePolicyRequest) returns (Cell);            // network profile / limits mid-life
  rpc ExposePort(ExposePortRequest) returns (PortEndpoint);
}

service Exec {
  rpc Run(RunRequest) returns (RunResult);                         // one-shot, capped output
  rpc Start(stream ProcessInput) returns (stream ProcessOutput);    // streaming process / PTY
  rpc Signal(SignalRequest) returns (Empty);
  rpc SessionCreate(SessionCreateRequest) returns (Session);        // persistent bash (chronus semantics)
  rpc SessionRun(SessionRunRequest) returns (SessionRunResult);     // end-of-command detection, exit code
  rpc SessionInteract(stream SessionInput) returns (stream SessionOutput); // expect-style
  rpc SessionClose(SessionRef) returns (Empty);
}

service Files {
  rpc Read(ReadFileRequest) returns (stream Chunk);
  rpc Write(stream WriteFileChunk) returns (FileInfo);
  rpc Stat(PathRequest) returns (FileInfo);
  rpc List(ListDirRequest) returns (ListDirResponse);
  rpc Remove(PathRequest) returns (Empty);
  rpc Watch(WatchDirRequest) returns (stream FsEvent);
  rpc Diff(DiffRequest) returns (DiffResult);                       // git-diff or layer-diff (pack_diff)
  rpc Apply(ApplyRequest) returns (Empty);                          // patch / tar
}

service Snapshots {
  rpc Snapshot(SnapshotRequest) returns (SnapshotRef);             // kind: DISK | DISK_MEM | PROC
  rpc Restore(RestoreRequest) returns (stream CreateEvent);        // n children from one snapshot
  rpc Fork(ForkRequest) returns (stream CreateEvent);              // live fork, n ≤ 16
  rpc Commit(CommitRequest) returns (ImageRef);                    // snapshot -> image manifest
  rpc Delete(SnapshotRef) returns (Empty);
}

service Images {
  rpc Import(ImportRequest) returns (stream BuildEvent);           // OCI ref / tar / Dockerfile / SWE task
  rpc Get(ImageRef) returns (ImageManifest);
  rpc Compose(ComposeRequest) returns (ImageRef);                  // base + workspace + toolkits
  rpc Prefetch(PrefetchRequest) returns (Empty);                   // warm nodes ahead of a burst
}

service Verify {
  rpc Run(VerifyRequest) returns (VerifyResult);                   // separate verifier cell (11 section 5)
}

service Projects { /* CreateProject, Grant, SetQuota, CreateKey, MintToken, Usage, AuditQuery */ }
```

### 1.1 Key messages

```proto
message CellSpec {
  oneof source { string template = 1; ImageRef image = 2; SnapshotRef snapshot = 3; }
  Backend backend = 4;              // FNCALL | CONTAINER | MICROVM | FULLVM | AUTO
  Resources resources = 5;          // vcpu (milli), mem_mib, disk_gib, pids, open_files
  Qos qos = 6;                      // LATENCY | STANDARD | BEST_EFFORT
  string network_profile = 7;       // "none" (default) | "mirrors" | "llm" | custom
  Duration idle_ttl = 8;            // auto action on idle
  IdleAction idle_action = 9;       // PAUSE | STOP
  Duration hard_ttl = 10;
  map<string,string> labels = 11;   // rollout_id, group_id, task_id, step...
  map<string,string> env = 12;
  Limits limits = 13;               // output_bytes, wall_time, ...
  bool trusted_image = 14;          // false ⇒ container runs inside shield VM
  CheckpointPolicy checkpoint = 15; // none | every(N s) | on_session_idle
}
message CreateRequest { CellSpec spec = 1; uint32 count = 2; string idempotency_key = 3;
                        Placement placement = 4; /* affinity cell id, spread labels */ }
message CreateEvent  { uint32 index = 1; oneof r { Cell cell = 2; Error error = 3; } }

message RunRequest   { string cell_id = 1; repeated string argv = 2; string shell = 3; string cwd = 4;
                       map<string,string> env = 5; bytes stdin = 6; Duration timeout = 7;
                       uint64 max_output_bytes = 8; string user = 9; string idempotency_key = 10; }
message RunResult    { int32 exit_code = 1; bytes stdout = 2; bytes stderr = 3; bool truncated = 4;
                       bool timed_out = 5; Duration wall = 6; ResourceUsage usage = 7; }
```

### 1.2 Local API

A comb in standalone mode serves Cells, Exec and Files itself, on a Unix socket (`/run/hivebox/comb.sock` by default, mode 0600), so one node is usable with no gate in front of it. The calls and messages are the same as through a gate, with these differences:

- There is no auth. Whoever can open the socket is trusted, the same as with the Docker socket. The caller names its project in the `x-hive-project` header, `local` when it names none, and sees only that project's cells.
- Files goes to the drone in the cell. `Diff` and `Apply` with a patch are not served yet, and `Apply` with a tar unpacks it under the path it names.
- `Create` with a count makes the cells at once and streams each one as it is ready. A count over 1 with an idempotency key gives cell `i` the key `<key>/<i>`, so a retry of the whole batch gets the same cells back. At most 1,024 cells per call.
- `Watch` by id ends once the cell has ended. A watcher that falls more than 4,096 changes behind gets the current state of every cell that moved since it last heard, instead of the changes it missed.
- `Pause`, `Resume` and `Stop` by labels pick only the cells the call can act on (running, paused and not yet ended), so `matched` counts those.
- `Exec.Signal` reaches processes started with `Exec.Start` on the same comb. `user` is a uid or `uid:gid`.
- `ExtendTtl` sets the hard TTL to run out a given time from now and replaces the idle TTL, leaving either alone when it is unset.
- Not served yet: `UpdatePolicy`, `SessionInteract`, terminals on `Start`, idempotency keys on `Run`, and snapshots on `Stop`. `ExposePort` needs a gate and will not be served here.

## 2. Error model

Errors use the gRPC status plus `google.rpc.ErrorInfo{reason, domain:"hivebox.dev", metadata}`. The `reason` codes are stable:

| reason | gRPC | retry? | infra? |
|---|---|---|---|
| `QUOTA_EXCEEDED` | RESOURCE_EXHAUSTED | after backoff | no |
| `CAPACITY_UNAVAILABLE` | UNAVAILABLE | yes | yes |
| `CELL_NOT_FOUND` / `CELL_LOST` | NOT_FOUND | no | lost=yes |
| `CELL_NOT_RUNNING` | FAILED_PRECONDITION | after resume | no |
| `EXEC_TIMEOUT` | DEADLINE_EXCEEDED | caller's choice | no |
| `OUTPUT_LIMIT` | OK with `truncated` | n/a | no |
| `POLICY_DENIED` | PERMISSION_DENIED | no | no |
| `IMAGE_UNAVAILABLE` | UNAVAILABLE | yes | yes |
| `FILE_ERROR` | FAILED_PRECONDITION | no | no |
| `DRONE_UNREACHABLE` | UNAVAILABLE | yes (≤3) | yes |
| `INTERNAL` | INTERNAL | yes | yes |

Every error carries `is_infra_error` in its metadata so trainers can mask samples uniformly (11 section 7). A `FILE_ERROR` also carries `errno`, the Linux name of the cause such as `ENOENT` or `EISDIR`, so SDKs can raise the error their language uses for it.

## 3. Cell state machine

```
            create
              │
          PENDING ──(placement fail ×3)──▶ FAILED(infra_capacity)
              │ admitted
          PREPARING  (rootfs attach, pool slot, prefetch)
              │
          STARTING   (driver start / restore, drone handshake)
              │
   ┌────▶ RUNNING ◀───────────── resume (explicit or implicit on request)
   │          │  pause / idle_ttl
   │       PAUSING ─▶ PAUSED ──(pause TTL)──▶ STOPPING
   │          │                    │ snapshot-kill mode keeps SnapshotRef
   │   stop / hard_ttl / exit / oom / policy
   │          ▼
   │      STOPPING ─▶ STOPPED{cause}   │  FAILED{cause}  │  EXPIRED
   └─ (fork/restore creates new cells; they enter at STARTING)
```

A pause that does not take goes from PAUSING back to RUNNING, since the cell never stopped.

A paused cell starts frozen with its memory in place. After `reclaim_after` (default 10 min) the comb reclaims it, swapping the cell's memory out so a resume pages it back in, and after `pause_ttl` (default 24 h) it stops the cell with cause `IDLE`.

The comb owns all transitions. Each one is a WAL record `{cell, from, to, cause, ts, seq}`.

Terminal states are retained for 24 h (queryable), then compacted to the audit store.

Resume can be implicit. Any Exec or Files call on a `PAUSED` cell triggers a resume and blocks for up to `resume_timeout` (default 5 s).

## 4. Python SDK (asyncio, primary)

```python
from hivebox import AsyncHive, Spec, QoS

hive = AsyncHive(endpoint="hive.unit1.internal:443", token=os.environ["HIVE_TOKEN"])

async with hive.cells.create_many(
    Spec(template="swe-py311", qos=QoS.STANDARD, network_profile="mirrors",
         idle_ttl="10m", labels={"step": "412", "group": gid}),
    count=16, idempotency_key=f"{gid}",
) as group:                                     # stops all on exit
    for cell in group:
        r = await cell.run("pytest -x -q", timeout=300, max_output_bytes=1 << 20)
        s = await cell.session()                # persistent bash
        await s.run("cd /repo && git status")
    snap = await group[0].snapshot(kind="disk")
    kids = await hive.snapshots.fork(group[0], n=8)
    diff = await group[0].files.diff(mode="git")

# bulk ops for weight-sync windows
await hive.cells.pause(selector={"step": "412"})
```

The transport is `grpcio` asyncio by default. An optional `hivebox[fast]` pyo3 wheel wraps the Rust client. It uses one h2 connection pool per process and avoids GIL-bound protobuf for 10K+ concurrent cells.

The SDK retries automatically only when `is_infra_error && idempotent`.

## 5. Rust SDK

```rust
let hive = hive_sdk::Client::connect("unix:/run/hivebox/comb.sock").await?.project("rl")?;
let spec = CellSpec::new(Source::Image("python".into()), Backend::Container);
let cells = hive.create_many(&spec, 16, Some("batch-7")).await?;
let cell = cells.into_iter().next().unwrap()?;
let out = cell.run(Command::new(["bash", "-lc", "pytest -q"]).timeout(Duration::from_secs(300))).await?;
let s = cell.session().await?;
s.run("git diff").await?;
cell.write("/work/patch.diff", patch).await?;
hive.stop(&Selector::Labels(labels)).await?;
```

`Client::connect` takes `unix:PATH` for a comb's socket or an `http://` URL, and `token` sets the bearer token a gate wants. `create_many` returns one result per cell, so a batch where some cells failed still hands back the rest. `Cell::start` gives a `Process` that takes stdin and signals and yields stdout, stderr and the exit as they come.

`hivectl` is built on it. It talks to `$HIVE_SOCKET` or `--endpoint`, and has commands to create, list, get, pause, resume, stop and watch cells, `run` and `sh` to run commands, and `cat`, `cp`, `files` and `rm` for files. `hivectl run` exits with the command's exit code, 128 plus the signal when a signal killed it, and 124 when it timed out.

## 6. E2B compatibility layer (in the gate, with an `[e2b]` table)

The E2B SDK talks to two places: the E2B API, which makes and ends sandboxes, and envd, a daemon in each sandbox that runs commands and moves files. With `E2B_API_URL` and `E2B_SANDBOX_URL` both set to the gate, the SDK sends both to the gate, and names the sandbox of every envd call in the `E2b-Sandbox-Id` header, so one address serves everything and no wildcard DNS is needed. A sandbox is a cell and its id is the cell id.

| E2B surface | hivebox mapping |
|---|---|
| `POST /sandboxes`, `POST /v2/sandboxes` with `templateID`, `timeout`, `metadata`, `envVars` | `Cells.Create`, with the image from the metadata key `image_key` names or else from `templates`, resources from the size the metadata picks, `hard_ttl` from `timeout` and the metadata as labels |
| `GET /sandboxes/{id}`, `GET /sandboxes`, `GET /v2/sandboxes` | `Cells.Get`, `Cells.List` with the `metadata` query as a label selector |
| `DELETE /sandboxes/{id}`, `POST /sandboxes/{id}/pause`, `/resume`, `/connect` | `Cells.Stop`, `Cells.Pause`, `Cells.Resume` |
| `POST /sandboxes/{id}/timeout`, `/refreshes` | `Cells.ExtendTtl` |
| envd `process.Process/Start`, `SendInput`, `CloseStdin`, `SendSignal` | `Exec.Start` and `Exec.Signal` on the comb that owns the cell |
| envd `filesystem.Filesystem/Stat`, `ListDir`, `Remove`, `MakeDir`, `Move` | `Files.Stat`, `Files.List`, `Files.Remove`, and `Exec.Run` of `mkdir -p` and `mv` |
| envd `GET /files`, `POST /files`, raw or multipart | `Files.Read`, `Files.Write` with the file owned by the caller's user |
| `X-API-Key`, envd `X-Access-Token` | the same key or biscuit token the gate takes as a bearer |

Create gives back the key or token it was called with as `envdAccessToken`, which is what the SDK then sends to envd, so the gate checks envd calls the same way it checks every other call. The gate says it is envd 0.5.7, which turns on raw uploads and closing a process's input in the SDK, and leaves out file metadata. A user named in an envd call is looked up in the cell's `/etc/passwd` by the comb, and relative paths are taken from that user's home. Input to a process goes through the gate that started it, since that gate holds the process's stream to its comb, so a gate fleet behind a balancer needs sticky sessions by sandbox for processes started with their input open.

Not served yet: templates, snapshots, metrics and logs, terminals, watching directories, `process.Process/List` and `Connect`, and gzip uploads.

A gate config that serves the slime SWE examples:

```toml
[e2b]
image_key = "swe/image"
sizes = { md = { vcpu_milli = 2000, mem_mib = 4096 } }
```

## 7. Other adapters (crate `hive-pollen`, Python `hivebox.adapters`)

- SWE-ReX: `AbstractRuntime` (`create_session`, `run_in_session`, `execute`, `read_file`, `write_file`, `upload`) maps to Exec and Files.
- Harbor: a `BaseEnvironment` provider (`start`, `exec`, `upload`, `stop`). This lets Terminal-Bench and SWE-bench datasets run unchanged.
- OpenEnv: a `reset/step/state` HTTP server template.
- SandboxFusion: `/run_code` (fncall tier, per-language runners).
- MCP: a server exposing `create_cell`, `run`, `read_file`, `write_file` and `diff`. It is for agent-driven environment building, in the DSec style where agents use the API.
