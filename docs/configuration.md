# Configuration

The comb, gate and keeper each read one TOML file, `/etc/hivebox/comb.toml`, `gate.toml` and `keeper.toml` by default, or the one given with `--config PATH`. A key not in the file keeps its default, and a key the service does not know is an error, so a typo stops the service at start and does not go unnoticed. Scout takes flags only.

Durations are strings: a whole number with `ms`, `s`, `m` or `h`, such as `"500ms"`, `"30s"` or `"2h"`. Addresses are `ip:port`.

## comb.toml

With no file at all, the comb runs on every default below. The only key with a rule of its own is `node.node`, which must not be 0.

### [node]

| key | default | meaning |
|---|---|---|
| `data_dir` | `/var/lib/hivebox` | where the WAL and each cell's directory are |
| `unit` | 1 | the unit the node belongs to, which goes into every cell id |
| `node` | 1 | the node's number in its unit. With `[keeper]`, the keeper sets it |
| `epoch` | 1 | the node's epoch. With `[keeper]`, the keeper sets it |
| `socket` | `/run/hivebox/comb.sock` | the Unix socket the local API listens on |
| `listen` | none | where gates reach the API over TCP. Whoever can connect is trusted, so bind it to a network only gates are on |
| `metrics` | none | where `/metrics` is served |
| `mem_mib` | the machine's memory less `reserved_mem_mib` | memory cells may commit before the node counts as overcommitted |
| `reserved_mem_mib` | 4096 | memory kept for the host, the comb and the page cache |
| `max_cells` | 3200 | the most cells alive at once |
| `cloud` | false | marks a cloud VM. Placement sends it only images already staged on it, and only once the on-prem nodes are nearly full |

### [pools]

| key | default | meaning |
|---|---|---|
| `cgroup_root` | `/sys/fs/cgroup/hive.slice` | the parent cgroup of all cells. `""` turns cgroups off, which leaves cells with no limits and is only for tests |
| `cgroup_depth` | 256 | empty cgroups kept ready for each QoS class |
| `netns_dir` | `/run/hivebox/netns` | where the cells' network namespaces are kept. `""` puts cells on the host network, only for tests |
| `netns_depth` | 400 | network namespaces kept ready |

### [density]

| key | default | meaning |
|---|---|---|
| `psi_stop_admit` | 0.20 | the share of the last 10 s cells may stall on memory before the node takes no new cells. It takes them again below half of that. 0 turns the brake off |
| `psi_source` | none | the pressure file to read. By default `memory.pressure` of `cgroup_root`, or `/proc/pressure/memory` without one |
| `pressure_idle` | `"30s"` | while the brake is on, running cells idle this long are paused, if their idle action is pause and they are not latency class. 0 leaves them alone |
| `trim_idle` | `"30s"` | a running cell idle this long gives back page cache it has not used lately. 0 turns it off |
| `core_scheduling` | true | one core scheduling cookie per CPU class. Does nothing without SMT |
| `setup_boost` | 4 | CPU quota multiplier from create until the cell is ready or the boost runs out. 0 or 1 turns it off |
| `cpu_burst` | 2.0 | how far standard and best effort cells may burst past their CPU when cores are idle, from 1 to 16 |

### [lifecycle]

| key | default | meaning |
|---|---|---|
| `create_deadline` | `"30s"` | the longest a create waits for its turn, and then the longest from its turn to the drone's handshake. Past it the create fails with `CAPACITY_UNAVAILABLE` |
| `stop_grace` | `"10s"` | how long a stop waits for the workload to end before killing it |
| `keep_ended` | `"10m"` | how long a stopped or failed cell stays visible |
| `reclaim_after` | `"10m"` | how long a frozen cell keeps its memory before it is swapped out |
| `pause_ttl` | `"24h"` | how long a cell may stay paused before it is stopped |
| `resume_timeout` | `"5s"` | the longest an exec or file call on a paused cell waits for the resume it causes |

### [audit]

| key | default | meaning |
|---|---|---|
| `dir` | `data_dir/audit` | where the audit chain is written. `""` turns it off |
| `sync_gap` | `"1s"` | the least time between two syncs of the log |

### [verify]

| key | default | meaning |
|---|---|---|
| `protected_paths` | `**/conftest.py`, `**/pytest.ini`, `**/.pytest.ini`, `**/tox.ini`, `**/setup.cfg`, `**/*.egg-info/**`, `**/*.dist-info/**` | globs always left out of the diff a verify applies. Setting it replaces the whole list |
| `graders` | `data_dir/graders` | the directory of reward plugins, as `NAME.wasm` |
| `grader_timeout` | `"10s"` | how long a grader may run. Must not be 0 |
| `grader_mem_mib` | 256 | memory for a grader, 1 to 4096 |

### [policy]

| key | default | meaning |
|---|---|---|
| `plugins` | `[]` | policy plugins every create goes through, in order. The first to refuse stops the create |
| `dir` | `data_dir/policies` | where the plugins are, as `NAME.wasm` |
| `timeout` | `"100ms"` | a policy that runs longer refuses the cell. Must not be 0 |
| `mem_mib` | 64 | memory for a policy, 1 to 4096 |

### [backends]

`plugins` is a list of absolute paths to driver plugin sockets, empty by default. Each plugin takes the place of the built in driver for its backend.

`[backends.create_limit]` caps the creates in flight for each backend. The defaults are `fncall = 256`, `container = 128`, `microvm = 64` and `fullvm = 8`.

### [backends.container]

| key | default | meaning |
|---|---|---|
| `enabled` | true | false leaves the backend out |
| `drone` | `/usr/lib/hivebox/hive-drone` | the static drone binary put in every container |
| `shell` | none | a static POSIX shell mounted read only at `/.hive/sh`. It must not run its own applets in place of the image's tools |
| `disk_quota` | false | puts a project quota on each cell's upper layer. Needs XFS, or ext4 with project quotas, mounted with `prjquota`. Without that the node runs no container cells |
| `state_dir` | `/run/hivebox/oci` | each cell's bundle, state and log |
| `workers` | 8 | processes kept to make containers, so how many creates run at once. Must not be 0 |
| `uid_base` | 1000000 | the first host id that root in a cell maps to |
| `uid_count` | 65536 | how many ids each cell gets |

### [backends.fncall]

| key | default | meaning |
|---|---|---|
| `enabled` | false | turns the wasm backend on |
| `modules` | `data_dir/wasm` | the programs, as `NAME.wasm` |
| `instances` | 1000 | programs running at once across all wasm cells. Must not be 0 |
| `max_mem_mib` | 4096 | the most memory one program may have, 1 to 4096 |

`[backends.fncall.mounts]` maps guest paths to host directories every wasm cell sees read only, as `"/data" = "/srv/data"`. Both must be absolute, and the guest path cannot be `/`.

### [images]

| key | default | meaning |
|---|---|---|
| `store` | none | the blob store, a directory or `http://host:port/bucket/prefix` signed with `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` |
| `cache_dir` | `data_dir/cache` | where fetched blobs are kept |
| `cache_bytes` | 64 GiB | how big the cache may grow |
| `layers_dir` | `/run/hivebox/layers` | where layers are mounted |
| `lazy` | false | mount layers before all their data is in. Needs the `nbd` module |
| `mkfs` | none | the `mkfs.erofs` (1.7 or newer) used to make snapshot layers. If it is set and cannot run, snapshots are off |

### [network]

| key | default | meaning |
|---|---|---|
| `guard` | true | the eBPF guard that holds each cell to its network profile. Off gives cells loopback only, and so does a guard that cannot load, which the comb logs |
| `cells` | `100.64.0.0/20` | the range cell addresses come from, a prefix of 8 to 30 bits |
| `pin_dir` | `/sys/fs/bpf/hive/guard-v1` | where the guard's maps are pinned |
| `upstream` | `[]` | DNS servers for cells, as `ip` or `ip:port`. Empty uses the host's `/etc/resolv.conf` |

`[network.profiles.NAME]` adds a profile with `domains`, the names its cells may look up. The built in profiles cannot be redefined.

`[network.llm]` sets up the gateway for cells on the `llm` profile:

| key | default | meaning |
|---|---|---|
| `upstream` | none | the inference server, plain `http://` only. Without it calls get 503 |
| `api_key_file` | none | a file holding the key sent upstream as a bearer token |
| `label` | `rollout_id` | the cell label that names a call's rollout |
| `logprobs` | false | asks the engine for each token's log probability as well, and takes it out of the answer when the cell did not ask for it |
| `keep_mib` | 256 | memory for calls not collected yet |
| `timeout` | `"10m"` | how long one call may take. Must not be 0 |

### [scout]

| key | default | meaning |
|---|---|---|
| `endpoint` | none | scout's URL. Without it the node is standalone |
| `advertise` | from `node.listen` | the address gates reach this node at, `http://` and `node.listen`, or `unix:` and the socket without it. Required when `node.listen` is `0.0.0.0` |

### [keeper]

| key | default | meaning |
|---|---|---|
| `members` | `[]` | the keeper members, as `host:port`. With them the keeper sets `node.node` and `node.epoch` and holds the node's lease |
| `name` | the host name | the name the node keeps across restarts, so it gets the same number back |

### [siem]

| key | default | meaning |
|---|---|---|
| `sink` | none | where security events go: a path, `file:///path`, `udp://host:port`, `tcp://host:port` or `http://host:port/path`. https is not taken |
| `window` | `"10s"` | the window over which repeats of an event are counted |
| `header_file` | none | a file with one header line added to each HTTP request |

## gate.toml

`hive-gate key PROJECT` makes a new key for a project and prints the `[[key]]` block for it.

### [gate]

| key | default | meaning |
|---|---|---|
| `listen` | `0.0.0.0:7400` | where clients reach the API |
| `metrics` | none | where `/metrics` is served |
| `scout` | required | scout's URL, which the gate follows for the nodes and their addresses |
| `keeper` | `[]` | keeper members to read keys from. With them, creates are held to each project's quota |
| `name` | `HOSTNAME/PORT` | the name the gate takes its share of each project's quota under |
| `unit` | none | the unit this gate serves. Without it, it serves the cells of every unit |
| `peer_listen` | none | where gates of other units reach this one. Needs `unit` |
| `burst_above` | 0.8 | the share of on-prem memory in use past which cells may go to cloud nodes. Above 1 means never |

### [[key]]

Each `[[key]]` has a `project` and a `blake3`, the hash of the key in 64 hex digits. The gate needs at least one `[[key]]` or `gate.keeper`, or nobody could call it.

### [e2b]

Its presence turns on the E2B compatible API.

| key | default | meaning |
|---|---|---|
| `image_key` | none | the metadata key whose value is the image, such as `"swe/image"` |
| `templates` | none | the image for each template, for sandboxes whose metadata names none |
| `sizes` | none | a map from size name to `{vcpu_milli, mem_mib, disk_gib}` |
| `backend` | `container` | the backend sandboxes run on: `container`, `microvm` or `fullvm` |

It needs `image_key` or `templates`. The size is read from the metadata key `PREFIX/size` when `image_key` is `PREFIX/...`, and from `size` otherwise.

### [units]

Maps other unit numbers to their gates' URLs, as `"2" = "http://gate2:7402"`. Needs `gate.unit`, and cannot name the gate's own unit.

### [siem]

The same as the comb's.

## keeper.toml

### [keeper]

| key | default | meaning |
|---|---|---|
| `id` | required | this member's id, one of the ids in `[[member]]` |
| `listen` | `0.0.0.0:7430` | where gates, combs and the other members reach this one |
| `data` | `/var/lib/hivebox/keeper` | the database directory |
| `lease_ms` | 10000 | how long a comb's lease lasts, from 1000 to 600000 |

### [[member]]

Each `[[member]]` has an `id`, which is not 0 and differs from the others, and an `addr` as `host:port`. Every member gets the same list. It is only read when the group first forms, after which the group's own log has the members.

## hive-scout

| flag | default | meaning |
|---|---|---|
| `--listen ADDR` | `0.0.0.0:7410` | where combs send their reports and gates follow |
| `--metrics ADDR` | none | where `/metrics` is served |
| `--rebalance SECS` | 60 | how often to plan a rebalance and log the plan. 0 turns it off |
| `--burst-above SHARE` | 0.8 | the same as the gates' `burst_above`, and should match it |

Under systemd, the flags go in `SCOUT_ARGS` in `/etc/hivebox/scout.env`.

## hivectl and the SDKs

`hivectl` talks to one comb. It takes `--endpoint URL`, or `--socket PATH`, or reads `HIVE_ENDPOINT` and then `HIVE_SOCKET`, and falls back to `/run/hivebox/comb.sock`. The project is `--project NAME` or `HIVE_PROJECT`, and `local` without either.

The Python SDK reads the same `HIVE_ENDPOINT`, `HIVE_SOCKET` and `HIVE_PROJECT`, and `HIVE_TOKEN` for the key a gate checks.
