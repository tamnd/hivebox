# Deploying hivebox

Systemd units for the four services, scripts that upgrade a node's comb without stopping its cells, and Prometheus rules and a Grafana dashboard for the SLOs.

## Units

| unit | binary | config |
|---|---|---|
| `systemd/hive-comb.service` | `/usr/local/bin/hive-comb` | `/etc/hivebox/comb.toml` |
| `systemd/hive-gate.service` | `/usr/local/bin/hive-gate` | `/etc/hivebox/gate.toml` |
| `systemd/hive-scout.service` | `/usr/local/bin/hive-scout` | flags in `SCOUT_ARGS` in `/etc/hivebox/scout.env` |
| `systemd/hive-keeper.service` | `/usr/local/bin/hive-keeper` | `/etc/hivebox/keeper.toml` |

Copy them to `/etc/systemd/system`, run `systemctl daemon-reload`, then `systemctl enable --now` each one the host runs.

The comb's unit has `KillMode=process`, and it has to keep it. A comb that stops leaves its cells running, and the next comb takes them back from its WAL. With the default kill mode systemd would kill every process in the unit on a stop or restart, the drones included, and every cell on the node would be lost. `Delegate=yes` lets the comb manage the cgroups under `cgroup_root`.

## Upgrading a comb

`upgrade-comb.sh NEW_BINARY` upgrades the comb on the host it runs on:

1. It checks the new binary runs and the comb answers on its socket.
2. It cordons the node, so no new cells come while it works.
3. It puts the new binary in place with a rename, keeping the old one as `hive-comb.prev`.
4. It restarts the unit and waits for a comb that started after the restart to answer.
5. It checks `hivectl node status`, which says how many cells the new comb took back when it started, how many had ended on their own meanwhile, and how many it lost.
6. If none were lost it lifts the cordon, unless the node was cordoned before it began.

If the new comb does not answer within `HIVE_UP_WAIT` seconds, or lost a cell, the old binary goes back, the comb is restarted again, and the node stays cordoned for someone to look at. The script exits 0 when the node runs the new comb, 1 when it rolled back, and 2 when the old comb did not come back either.

`--drain DURATION` drains the node first, for when the host is to be rebooted after. It is not needed for the comb alone, since the cells live through the restart.

`roll.sh NEW_BINARY HOST...` runs the upgrade over ssh on one host after another, waiting `ROLL_PAUSE` seconds (30 by default) between them, and stops at the first host that does not end on the new comb.

The check on cells taken back reads what the comb that just started reports, and combs before 0.0.43 do not report it. Upgrading from 0.0.42 is checked in full, but a rollback to 0.0.42 cannot tell whether it lost cells.

Both read `HIVE_COMB_BIN`, `HIVE_COMB_UNIT`, `HIVE_SOCKET`, `HIVECTL` and `HIVE_UP_WAIT` when the defaults do not fit.

## Metrics, alerts and the dashboard

Each service serves Prometheus metrics when given an address: `metrics` under `[node]` in comb.toml, `metrics` under `[gate]` in gate.toml, and `--metrics` for scout. Bind them to an address only Prometheus reaches. Scrape them under the jobs `hive-comb`, `hive-gate` and `hive-scout`, which the alerts name:

```yaml
rule_files:
  - /etc/prometheus/hivebox-rules.yml
scrape_configs:
  - job_name: hive-comb
    static_configs: [{targets: ["node1:9464", "node2:9464"]}]
  - job_name: hive-gate
    static_configs: [{targets: ["gate1:9467"]}]
  - job_name: hive-scout
    static_configs: [{targets: ["scout1:9469"]}]
```

`prometheus/hivebox-rules.yml` has the recording rules and alerts. The SLOs are the ones in spec/02: at most 0.1% of cells fail for an infrastructure cause, and warm creates finish within 400 ms at p99 for containers and 300 ms for microVMs. Infra failures are alerted on by burn rate over two windows each, for cells as the combs count them and for creates as the gates count them:

| alert | when | severity |
|---|---|---|
| `HiveCellInfraFailuresFast`, `HiveCreateInfraFailuresFast` | the budget burns 14.4 times too fast over 1h and 5m, or 6 times over 6h and 30m | page |
| `HiveCellInfraFailuresSlow`, `HiveCreateInfraFailuresSlow` | 3 times over 1d and 2h, or 1 time over 3d and 6h, for an hour | ticket |
| `HiveCreateSlow` | a node's create p99 is past the target over both 5m and 1h, for 15 minutes | warning |
| `HiveCombDown` | a comb does not answer scrapes for a minute | page |
| `HiveControlPlaneDown` | a gate or scout does not answer scrapes for 2 minutes | page |
| `HiveCombRestarting` | a comb started at least 3 times in an hour | warning |
| `HiveCombLostCells` | a comb could not take back some cells when it started | warning |
| `HiveMemoryStall` | cells on a node stall on memory more than 10% of the time for 10 minutes | warning |
| `HiveScoutNodesDown` | scout has not heard from a node for 5 minutes | warning |
| `HiveNodeCordonedLong` | a node has been cordoned for 12 hours | ticket |
| `HiveMetricsOverflow` | a metric hit its series cap and folds new labels into `other` | ticket |

Latencies are taken per node and never summed across nodes. A node only writes the histogram buckets that hold something, so the buckets of two nodes do not line up and a quantile of their sum would be wrong. Exec p99 is on the dashboard but not alerted on, since it includes the command's own run time. There is no alert for keeper leader churn yet, since keeper serves no metrics.

`promtool test rules prometheus/hivebox-rules.test.yml` runs the unit tests for the rules, one case each alert fires on and cases where it must not.

`grafana/hivebox.json` is a dashboard over the same metrics: the SLOs and burn rates, cells by state and how they end, creates by result and stage, exec, stop and snapshot latencies, memory pressure, what each comb found when it started, cordoned nodes, scout's view and the gate's calls. Import it, or put it in a Grafana dashboard provisioning directory. It asks for the Prometheus data source and the nodes to show.
