# Runbooks

One section for each alert in `deploy/prometheus/hivebox-rules.yml`, which links here, and then the failures that have no alert. The commands assume a shell on the node in question. To work from elsewhere, point `hivectl --endpoint http://NODE:PORT` at the comb's `node.listen`.

Two things hold for every runbook here:

- **The cells outlive the comb.** Stopping or restarting `hive-comb` leaves the cells running, and the next comb takes them back from its WAL. Restarting the comb is safe, and is often the fix. In a cluster, the comb has to be back within its keeper lease (`lease_ms`, 10 s by default), or it stops its cells when it does come back.
- **Infra failures spend the error budget.** The SLO is that at most 0.1% of cells end with an infra cause and at most 0.1% of creates fail for an infra reason. The infra causes are `start_failed`, `drone_lost`, `node_lost`, `recovery` and `drained`. The infra reasons are `CAPACITY_UNAVAILABLE`, `CELL_LOST`, `IMAGE_UNAVAILABLE`, `DRONE_UNREACHABLE` and `INTERNAL`.

## HiveCellInfraFailuresFast

Cells are ending with infra causes fast enough to spend a month's budget in a few days. This pages.

First find the cause and where it comes from:

```
sum by (instance, backend, cause) (increase(hive_cells_ended_total{cause=~"start_failed|drone_lost|node_lost|recovery|drained"}[1h]))
```

If one node has most of them, cordon it with `hivectl node cordon` so new cells go elsewhere, then look at it. If they are spread over every node, look at what changed: a release, an image, a kernel, the store.

Then by cause:

- **`start_failed`**: the cell was admitted and failed to start, because there was no pool slot, the image could not be had, or the driver refused. The failed creates also show in `hive_create_total` by `result`, see [HiveCreateInfraFailuresFast](#hivecreateinfrafailuresfast). The comb's log says why for each one: `journalctl -u hive-comb --since -1h`.
- **`drone_lost`**: the drone in the cell never finished its handshake or stopped answering. A cell whose memory limit killed the drone is the usual case, so look for OOM kills with `journalctl -k --since -1h | grep -i oom`. A node so loaded that drones miss the handshake inside `create_deadline` shows up too, so check the load and [HiveMemoryStall](#hivememorystall). If every drone fails on one node, check the drone binary at `backends.container.drone` is the one that came with this comb.
- **`node_lost`**: a node went away, or its comb lost its keeper lease and its cells were fenced. The comb logs `lost the lease, stopped N cells` when that happens. See [A comb lost its lease](#a-comb-lost-its-lease).
- **`recovery`**: a comb that started found cells it could not take back. See [HiveCombLostCells](#hivecomblostcells).
- **`drained`**: drains stopped cells. Someone is draining nodes with a short timeout or `--stop-rest`. During planned work this is expected, so silence the alert for the work. Otherwise find the drain in the comb's log and talk to whoever ran it.

To see single cells, `hivectl ls --state failed` lists the failed ones on a node and `hivectl get ID` shows each one's cause. Failed cells stay listed for `lifecycle.keep_ended`, ten minutes by default.

## HiveCellInfraFailuresSlow

The same failures as [HiveCellInfraFailuresFast](#hivecellinfrafailuresfast), at a rate that is too low to page but still spends the budget before the month ends. This is a ticket. Follow the same steps, over a longer range such as `[1d]`. A slow burn is often a single node or a single image, so group by `instance` first.

## HiveCreateInfraFailuresFast

Creates through the gates are failing for infra reasons, many times faster than the SLO allows. This pages. The gate counts each create as `created`, `infra_error` or `caller_error` in `hive_gate_cells_created_total`. The combs count each create by its reason in `hive_create_total`:

```
sum by (instance, backend, result) (increase(hive_create_total{result!="ok"}[1h]))
```

By reason:

- **`CAPACITY_UNAVAILABLE`**: no node could take the cell, or the create waited past `lifecycle.create_deadline`. Check room: `hive_scout_mem_admit_mib` and `hive_scout_mem_committed_mib` on scout, `hive_cells` on the combs, and whether the pressure brake is holding admits on any node ([HiveMemoryStall](#hivememorystall)). Check whether nodes are cordoned with `hive_node_cordoned`. A burst bigger than `backends.create_limit` for its backend waits in line, and runs into the deadline if the line is long. Add nodes, uncordon the ones that are done, or raise the limit if the node has room.
- **`IMAGE_UNAVAILABLE`**: the image is not on the node, or its blobs could not be fetched. Check that the name exists in `data_dir/images` on every node, or that the project committed it. Check that the store at `images.store` answers from the node, and that `images.cache_dir` is not on a full disk.
- **`DRONE_UNREACHABLE`**: the comb lost the drone in a cell during the call, the same as `drone_lost` above.
- **`CELL_LOST`**: the cell's node was lost while the call was on it, see [HiveScoutNodesDown](#hivescoutnodesdown).
- **`INTERNAL`**: a bug. The comb's log has the message. Note the cell ids and the release, and open an issue.

If the gates count infra errors and the combs show none, the gates are failing before they reach a comb. Check that scout is up and that the gates can reach the combs' `node.listen`.

## HiveCreateInfraFailuresSlow

The slow burn version of [HiveCreateInfraFailuresFast](#hivecreateinfrafailuresfast). This is a ticket. Use the same queries over `[1d]`.

## HiveCreateSlow

A node's create p99 has been over the target, 400 ms for containers and 300 ms for microVMs, over both the last 5 minutes and the last hour. The targets are for warm creates, of images already in the node's cache.

The comb times each stage of a create in `hive_create_seconds`, so find the stage that grew:

```
histogram_quantile(0.99, rate(hive_create_seconds_bucket{instance="NODE"}[15m]))
```

Take quantiles node by node and never sum buckets across nodes. A node writes only the buckets that hold something, so the buckets of two nodes do not line up.

- **`admit`** is the wait for a turn. It grows when more creates come at once than `backends.create_limit` lets through, or when the pressure brake holds admits.
- **`pool`** is taking a ready cgroup and network namespace. It grows when creates come faster than the pools refill, and `pools.cgroup_depth` and `pools.netns_depth` set how many are kept ready.
- **`rootfs`** is fetching and mounting the image. It grows with cold images, a slow store, or a cache too small for the images in use (`images.cache_bytes`).
- **`start`** and **`handshake`** are starting the container and the drone answering. They grow with CPU load on the node.

A node that is slow because it is busy pulling new images will settle by itself. A node that is slow because it is full should take fewer cells: cordon it for a while, or lower `node.max_cells` or `node.mem_mib`.

## HiveCombDown

Prometheus cannot scrape a comb, for a minute now. This pages. The node's cells keep running, but nothing on the node can be created, run or stopped, and in a cluster the comb's lease runs out within `lease_ms`.

```
systemctl status hive-comb
journalctl -u hive-comb -n 100
```

- If the comb is not running, start it with `systemctl start hive-comb`. If systemd gave up after too many restarts, run `systemctl reset-failed hive-comb` first. If it exits at start, the last lines of the journal say why. A config error names the key.
- If it runs but does not answer, check `hivectl node status`. If that answers, the problem is between Prometheus and `node.metrics`, not the comb. If it does not, restart the comb. The cells live through it.
- If the host itself is down, the cells on it are lost. In a cluster the gates fail them as lost once the lease runs out. Bring the host back, and the comb fails what was left from the old epoch when it registers again.

## HiveCombRestarting

A comb started three times or more in the last hour. Each start takes the cells back, so the cells keep running, but each one is a gap in which nothing on the node answers.

```
systemctl show hive-comb -p NRestarts -p ExecMainStatus
journalctl -u hive-comb --since -1h | grep -v "^hive-comb: took back"
journalctl -k --since -1h | grep -i "oom\|hive-comb"
```

Look for a panic, a kernel OOM kill of the comb itself, or someone restarting it by hand. A comb that is killed by OOM again and again needs more room: raise `node.reserved_mem_mib`, so cells get less of the machine. A panic is a bug. Note the message and the release and open an issue, and if it started with an upgrade, roll back with `deploy/upgrade-comb.sh` and the old binary.

## HiveCombLostCells

A comb that started could not take back some of the cells its WAL had. Those cells failed with cause `recovery`, or `node_lost` when they were from an older epoch. The alert clears the next time the comb starts.

The comb logs one line for the start, `took back N cells, M ended while no comb ran, K lost`, and the reason for each cell before it:

```
journalctl -u hive-comb | grep "^hive-comb: \(took back\|no .* driver\|dropping\|cut \|stopped .* from before epoch\)"
```

- `no BACKEND driver for ID` means the new comb cannot run that backend, for example a build without it or a plugin socket that was not there. Fix the comb's config before the next restart, or the same happens again.
- `dropping an unreadable WAL record` or `cut N damaged bytes off the end of the WAL` means the WAL was damaged, usually by a disk that filled up or a host that lost power. Check `df` on `data_dir`.
- `stopped N cells from before epoch E` means the comb came back after its lease ran out, see [A comb lost its lease](#a-comb-lost-its-lease).

If it happened during an upgrade, `upgrade-comb.sh` has already put the old comb back and left the node cordoned.

## HiveMemoryStall

Cells on a node have spent more than 10% of the time stalled on memory for 10 minutes. The node is packed too tight for what its cells are doing.

The comb's pressure brake should be handling this. Over `density.psi_stop_admit` (20%) it takes no new cells, and it pauses running cells that have been idle `density.pressure_idle`. `hive_pressure_pauses_total` counts those pauses. If the stall stays high anyway, the memory is in use by cells that are busy.

- Cordon the node so it takes no more cells while it works through the ones it has.
- Find the cells using the most memory: `for d in /sys/fs/cgroup/hive.slice/*/*/; do echo "$(cat $d/memory.current) $d"; done | sort -n | tail`, with the path under `pools.cgroup_root`.
- Lower `node.mem_mib` on the node, which makes it commit less memory to cells, and restart the comb. The cells live through the restart.
- Check that the node has swap, or that `lifecycle.reclaim_after` lets frozen cells go to it.

## HiveNodeCordonedLong

A node has been cordoned for 12 hours, so it is taking no new cells. That is lost capacity unless someone meant it.

`hivectl node status` on the node shows it cordoned. Find out why before lifting it:

- A failed upgrade leaves the node cordoned on purpose. `upgrade-comb.sh` and `roll.sh` print why they rolled back. Check that the node runs the comb it should (`hivectl node status`), and that `hive_comb_start_cells{result="lost"}` is 0.
- A drain leaves the node cordoned when it is done. If the maintenance is over, the node can take cells again.
- A comb that starts cordoned logs `the node is cordoned, so it takes no new cells until uncordoned`. The cordon is kept across restarts.

When it is fixed, run `hivectl node uncordon`.

## HiveScoutNodesDown

Scout has not heard from one or more nodes for 5 minutes, so it no longer places cells on them. Gates fail calls for cells on them as `CELL_LOST`.

- Compare with `up{job="hive-comb"}`. A node whose comb is down is [HiveCombDown](#hivecombdown).
- If the comb is up, it cannot reach scout. The comb logs `reporting to scout at URL` at start, so check that URL is right in `scout.endpoint` and that the node reaches it: `curl -sS URL`.
- If many nodes went down together, the problem is more likely scout or the network around it than the nodes. Check `hive_scout_reports_total` is still going up.

## HiveControlPlaneDown

A gate or scout does not answer scrapes for 2 minutes. Cells keep running, but creates through this gate fail, or with scout down the gates go on with the last view of the nodes they had.

Neither keeps any state, so restarting is safe: `systemctl restart hive-gate` or `systemctl restart hive-scout`, then read `journalctl -u hive-gate -n 50` if it does not come back. A gate that exits at start with a config error names the key. A gate needs `gate.scout`, and at least one `[[key]]` or `gate.keeper`.

## HiveMetricsOverflow

A metric hit its cap of 1000 series, and new label values are being counted under the label value `other`. The metric undercounts by label from then on. The alert names the metric.

This means a label took more values than it should, usually because something outside hivebox fed it more names than expected, such as a long list of policy plugins. Look at the metric's series in Prometheus to find which label took too many values, fix the source, and restart the service, which starts its series over.

## Failures without an alert

### A comb lost its lease

In a cluster, a comb that does not renew its lease in time has lost its node, since the keeper may give the node's number to another comb. It stops all its cells and logs `lost the lease, stopped N cells`, and the gates fail those cells as lost with cause `node_lost`. When it starts again it registers anew, gets a new epoch, and stops whatever was left from the old one.

This comes from the comb being stopped longer than `lease_ms`, the node losing its network to the keeper, or the keeper losing its majority. Find which one in `journalctl -u hive-comb` and `journalctl -u hive-keeper` on the members. If upgrades take longer than the lease, raise `keeper.lease_ms`.

### The keeper lost its majority

The keeper has no metrics yet, so watch its journal. Without a majority, combs cannot register or renew their leases, and their cells stop once the leases run out. The gates cannot read keys from it either, so only the `[[key]]` keys in `gate.toml` keep working. Bring members back one at a time until a majority is up.

### The guard did not load

With `network.guard` on and a guard that could not load, the comb logs it and gives every cell loopback only. Cells that need the network fail their work but do not fail as cells, so no alert fires. Check that bpffs is mounted at `/sys/fs/bpf` and that the kernel is new enough, then restart the comb.

### The data disk is full

The WAL, the cell directories and the image cache are all under `data_dir` by default. A full disk fails creates and can damage the end of the WAL. Free space, then restart the comb. It cuts off a damaged tail by itself and logs how many bytes it cut. Keep `images.cache_bytes` well under the free space.

### The audit chain does not verify

`hivectl audit verify DIR` exits 1 and says where the chain breaks. A line was edited, dropped, added or moved there. Keep a copy of the files as they are, since this is evidence, and check them against the roots the keeper holds with `--keeper HOST:PORT`.
