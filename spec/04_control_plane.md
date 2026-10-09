# Control plane

> Goal: ≥5,000 creates/s and ≥400K live cells per unit with zero consensus writes on the per-cell path, p99 placement ≤2 ms, and cells that survive total control-plane loss.

The control plane has four parts: gate, waggle, scout and keeper.

## 1. Identifiers

```text
CellId      = "c" + base32( unit:8 | node:16 | epoch:16 | seq:40 | tag:48 )      // 128 bits
              tag = trunc48(HMAC(unit_key, unit|node|epoch|seq))  → unguessable, verifiable at gate
SnapshotId  = "s" + base32(blake3(manifest)[..16])
ImageId     = "i" + base32(blake3(manifest)[..16])
ProjectId   = "p" + ulid ; PrincipalId = "u"/"a" + ulid (human / agent)
```

`node` is the comb's registered index. `epoch` bumps on every comb registration or re-registration after lease loss, so requests to stale epochs are rejected with `CELL_LOST`.

The gate validates `tag` before routing. The check is cheap and stops ID-guessing scans.

## 2. hive-keeper (metadata, IAM, quotas)

Store: openraft 0.9 with a redb state machine, 3 or 5 voters, and snapshots to object store. The write rate is at most hundreds per second, by design.

Logical schema:

```
Principal{id, kind: Human|Service|Agent, parent_project, keys[], created_by}
Project{id, parent, depth, policy: Policy, quota: Quota, labels}      // arbitrary nesting
Policy{allow: [Rule{action, resource_selector, condition}], deny: [...], max_isolation_floor,
       network_profiles: [name], share_pagecache: bool}
Quota{cells, vcpu, mem_gib, creates_per_s, snapshots_gib, egress_gbps, gpu}
Template{id, image_ref, backend, resources, network_profile, qos, ttl, drone_caps, prefetch}
ImageManifest (see 06) ; NetworkProfile{name, allow: [Mirror|Domain|CIDR:port/proto]}
NodeRecord{node, epoch, lease_expiry, capacity, labels(zone, rack, gpu, kernel, backends)}
```

Bounded delegation follows DSec. A principal may create subprojects and grant `quota' ≤ own_remaining` and `perms' ⊆ own_perms`. Agents use the same API. The keeper enforces this inside its transaction.

### Quota leasing

- The keeper grants slices per (project, gate), for example `cells: 2,000, creates: 500/s, ttl 30 s`. Slices are sized from recent usage (EWMA × 1.5).
- The gate decrements locally, asks for a refill at 50% consumption, and returns unused quota on expiry.
- Quotas are hierarchical. A create charges the project and all its ancestors, so slices are granted from the most constraining ancestor. Maximum overshoot is the sum of outstanding slices, which is bounded and tunable.
- Live counts are reconciled every 10 s from scout aggregates. Authoritative usage is the sum over combs.

### Tokens

API keys are stored hashed with a prefix id. They are exchanged for biscuit tokens (≤1 h) that carry project, perms and caveats. Holders can attenuate a token offline. For example, a rollout worker mints a token scoped to `cell=X, ops=[exec,files], exp=30m` and hands it to a harness. The gate verifies tokens with the keeper public key, with no RPC.

## 3. hive-gate (stateless ingress)

- Protocols: gRPC (tonic, h2), Connect (HTTP/1.1+JSON / h2), REST (axum) for E2B compatibility, and WebSocket for PTY.
- Routing: the gate decodes `CellId` into `(node, epoch)` and sends the request over a persistent multiplexed h2 connection pool to that comb (mTLS SPIFFE). The node address table comes from scout (a `papaya` map, refreshed on push).
- Create path: authn, then authz (policy cached, invalidated by keeper watch), then quota slice, then waggle `Place` (an in-process library call when co-deployed, otherwise RPC), then fan-out to combs, then stream results.
- Batch create: `n` up to 32K in one call. The gate splits the batch per node and streams responses. Rejected cells are re-placed (≤2 retries) with an `exclude` set.
- Idempotency: the result for `(principal, idem_key)` is cached for 10 min in gate and comb. The comb is authoritative because it stores idem_key in its WAL.
- Admission shedding: per-principal token buckets (creates/s, exec/s), global concurrency caps, and priority classes (eval > train > build) under overload.
- Units: a gate with `unit = N` in `[gate]` serves that unit's cells, and sends a call about a cell of another unit to the gate `[units]` names for it, at that gate's `peer_listen`. The peer listener takes the project and principal the first gate stamped and checks no key, like a comb's TCP listener, so it belongs on the network only gates reach. It never sends a call on to a third unit, so two gates pointed at each other cannot loop, and it charges no quota, since the first gate did. Exec and Files calls pass through as bytes, as they do to a comb. A list, a pause, resume or stop by label, and a watch by label cover every unit: the gate asks its own nodes and each other unit's gate, its own unit first, and a page token that starts with `u` names the unit a list goes on with. A unit whose gate cannot be reached fails a list, shows as a failure of a call by label, and is watched again once it is back. Creates and the LLM gateway's routes stay within the gate's own unit for now.
- Egress separation: the gate is the only component with interfaces on both trusted and untrusted networks (DSec). It sits behind an anycast VIP via BGP/ECMP, and a failed health check withdraws the route.

## 4. hive-scout (cluster state)

Combs push a `NodeReport` every 1 s (delta-encoded), plus an immediate push on a significant change (>5% capacity delta or a health change):

```
NodeReport{node, epoch, health, backends_ready, cpu{cores, busy_ewma, ls_busy},
           mem{total, committed, resident, reclaimable}, cells{by_backend, by_state},
           pools{netns, tap, template_vms}, disk{l1_free, upper_free}, io{l2_bytes_s},
           layers_bloom: [u8; 4096] /* resident base/toolkit layers */, per_project_counts(top-K)}
```

Scout merges reports into a versioned `ClusterView` and serves it to waggle and gate through a streaming subscription (snapshot plus deltas). It is stateless and rebuilds within one report interval.

Scout also computes per-project live usage for quota reconciliation and exports aggregates to metrics.

## 5. hive-waggle (placement)

### 5.1 Algorithm

```
fn place(req: PlaceReq{spec, n, affinity, anti_affinity, exclude}) -> Vec<(Node, u32)> {
  let view = scout.snapshot() ⊕ self.inflight_overlay;          // DSec overlay trick
  let feasible = view.nodes.filter(|x| healthy && has_backend && has_hw && !exclude && policy_ok);
  // equivalence class: all n cells share one spec -> score once per node
  let k = clamp(2*n, 8, feasible.len());                         // Sparrow batch sampling, d=2
  let cand = sample_weighted(feasible, k, by=headroom);
  let scored = cand.map(|x| (x, score(x, spec)));
  greedy_fill(scored, n, per_node_cap = min(headroom(x, spec), burst_cap(x)))  // water-filling
}
score(x) = w1*mem_headroom_ratio + w2*cpu_headroom + w3*layer_locality(bloom, spec.layers)
         + w4*pool_depth(x) - w5*recent_create_rate(x) - w6*project_concentration(x)
```

- Pack vs spread (Hermes hybrid): below 60% cluster utilization, locality (`w3`) gets a high weight, which keeps L1 warm and uses fewer active nodes. Above 60%, headroom gets the weight and cells spread out.
- Fork and restore affinity: children prefer the parent's node because they share pages. Snapshot restores prefer nodes that hold the snapshot in L1.
- Burst cap per node: at most the comb's advertised `create_concurrency` (≈150/s for microVM, 300/s for container), so one 32K burst spreads over ≥100 nodes. The cap is soft: once every node has had its cap, the cells left go where there is room and the combs queue them, so a small cluster still takes a big batch, only slower.
- `inflight_overlay` entries expire when the next NodeReport reflects them, or after 3 s.
- Cloud bursting: a comb with `cloud = true` in `[node]` is a cloud VM. It reports that, and puts a digest of each image name staged under its `data_dir/images` in its layer filter, read again every 10 s. Placement leaves cloud nodes out until the healthy on-prem nodes have `burst_above` of their admittable memory given out, counting the in-flight overlay so a burst crosses the line before the reports do. The gate sets this in `[gate]`, 0.8 by default, and over 1 turns bursting off. Past it, a cloud node that has the cell's image staged joins the candidates and is scored like any other, so the emptiest take the most. Cells from a snapshot, of an image no cloud node has, or with an idempotency key stay on-prem, the last so a create sent again always walks the same nodes. The 60% pack or spread line counts on-prem nodes only.
- Randomized sampling and a per-replica overlay avoid herding between waggle replicas. Conflicts show up as a comb admission reject, and the request is retried.

### 5.2 Offline rebalancer (optional, v2)

Every 60 s the rebalancer runs an MCMF/ILP plan over the snapshot. It recommends pause+restore migrations of idle cells away from hot nodes, and cloud-burst offload when utilization stays above 80% for cloud-eligible templates. DSec reports that a 30 TB image subset covers 70% of tasks.

## 6. comb admission (final authority)

```
admit(spec, count) -> Ok(k ≤ count) | Reject{reason, headroom}
  hard checks: mem_committed + k*mem_est ≤ mem_total*overcommit(qos)   // mem_est = learned resident per template
               cells_by_backend + k ≤ max_cells(backend)
               pools have k slots (or refill rate allows within 200 ms)
               create_semaphore permits (per backend)
               l2 cold-read budget (bytes/s) not exceeded for cold images
               disk upper space, pids headroom
```

Partial admits are allowed and return k. A reject includes fresh headroom, so waggle updates its overlay immediately.

`mem_est` per template is the p90 of observed resident memory after 60 s (EWMA). It bootstraps at request × 0.5. This follows the Resource Central approach.

## 7. Scalability math

| Component | Load at 5K creates/s + 400K cells | Instances |
|---|---|---|
| gate | 5K creates/s + ~200K exec/s (0.5 exec/s/cell) | 8-16 (each ~20K rps) |
| waggle | 5K placements/s (≈µs each) | 3 (co-located in gate as lib) |
| scout | 160-500 reports/s | 3 |
| keeper | ≤200 writes/s (slices, policy edits) | 3-5 |
| comb | 31/s avg, 150/s burst creates; ~1.2K exec/s | 1/node |

## 8. Failure semantics

- Total control-plane outage: running cells are unaffected. Data-plane requests fail while the gate is down, which multiple gates mitigate. Comb TTL enforcement continues locally.
- Comb lease expiry (node partition > 30 s): the keeper bumps the node epoch and cells on that node are reported to clients as `LOST`. If the node comes back with the old epoch, the comb must kill those cells (fencing) unless the lease was renewed. A comb asking for the node with an older epoch than the keeper's, such as a replacement machine under the same name or one that lost its disk, waits until the live lease runs out, so the comb holding it has stopped its cells before the node starts again in a new epoch. A comb keeps in its `lease` file when its lease runs out by its own clock, and if it restarts and cannot register again before then, it stops the cells it has and starts over.
- Split brain on quota is bounded by slice sizes.
