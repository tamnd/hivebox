# Deploying hivebox

Systemd units for the four services, and scripts that upgrade a node's comb without stopping its cells.

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
