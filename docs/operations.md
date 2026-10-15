# Running hivebox

This is how to set hivebox up and keep it running. The keys named here are all in [configuration.md](configuration.md), and what to do when something breaks is in [runbooks.md](runbooks.md).

## The services

| service | runs on | what it does | keeps state |
|---|---|---|---|
| `hive-comb` | every node | runs the cells on its node and serves the API for them | a WAL and a directory per cell under `data_dir` |
| `hive-drone` | inside every cell | runs commands and moves files for the comb | nothing |
| `hive-gate` | anywhere, as many as you like | the API for clients, checks keys and quotas and sends each call to the right comb | nothing |
| `hive-scout` | one host | hears from every comb and tells the gates which nodes have room | nothing, it rebuilds its view from the combs' reports |
| `hive-keeper` | three or five hosts | a Raft group that hands out node numbers, epochs and leases, and holds project keys and quotas | its database under `data` |

A single node needs only the comb. Clients reach it over its Unix socket with `hivectl` or the SDKs, and every call on the socket is trusted. A cluster adds keeper, scout and gates, and clients reach the gates with a project key.

## Requirements

The node needs Linux 6.12 or newer with only cgroup v2 mounted, and KVM on bare metal for microVMs. The comb runs as root. It makes cgroups under `cgroup_root`, network namespaces in `netns_dir`, and mounts layers under `layers_dir`. Container cells need a static `hive-drone`, built for the musl target and put at `/usr/lib/hivebox/hive-drone` or wherever `backends.container.drone` says.

The guard, which keeps each cell's network to what its profile allows, needs bpffs mounted at `/sys/fs/bpf`. If it cannot load, the comb says so and cells get loopback only. Lazy image layers need the `nbd` module. Snapshot layers need `mkfs.erofs` 1.7 or newer.

## One node

1. Put `hive-comb`, `hivectl` and `hive-nectar` in `/usr/local/bin`, and `hive-drone` in `/usr/lib/hivebox`.
2. Write `/etc/hivebox/comb.toml`. With no file at all the comb runs on its defaults, which suit a dedicated host. On a shared one, set at least `node.mem_mib`, `node.max_cells`, and a `network.cells` range that clashes with nothing on the host.
3. Copy `deploy/systemd/hive-comb.service` to `/etc/systemd/system`, then `systemctl daemon-reload` and `systemctl enable --now hive-comb`.
4. Check it with `hivectl node status`. The first line names the node and its version and counts its cells.

## Images

A cell's image is looked up by name, first among the images its project committed, then in `data_dir/images`. A directory there is a root filesystem used as it is. A file holds the id of an image in the blob store, which the comb fetches and mounts the first time a cell needs it.

To add an image, import it into the store and name it:

```
hive-nectar import-oci ./python-layout --store /var/lib/hivebox/store > /var/lib/hivebox/images/python
```

`import-oci` takes an OCI image layout directory and `import-tar` a tar of one, and both print the image's id. Point `images.store` at the same directory, or use `--s3 URL` in place of `--store` with a bucket every node shares, so an image imported once runs anywhere. Keep `backends.container.uid_base` the same on every node, since images are made for one base.

## Cleaning the store

The store keeps every blob it is given until it is collected. `hive-nectar gc` takes the images to keep, as ids, files of ids or directories of name files, and removes every blob none of them needs that was written or last used more than `--keep-hours` ago (24 by default):

```
hive-nectar gc /var/lib/hivebox/images,/var/lib/hivebox/projects --store /var/lib/hivebox/store
hive-nectar gc /var/lib/hivebox/images,/var/lib/hivebox/projects --store /var/lib/hivebox/store --sweep on
```

A store in a bucket is collected the same way, with `--s3 URL` in place of `--store DIR` and the same keys the combs use. In a bucket a blob gets a new time by being copied onto itself, which S3 does inside the store without sending the bytes again.

The first prints what it would remove, and the second removes it, along with what puts that died left behind: temporary files in a directory, and multipart uploads never finished in a bucket. Imports and snapshots made while it runs are kept. When nodes share a store, pass the image directories of all of them. A comb gives the image of each of its cells a new time when the cell starts and every half hour after that, so the image a cell runs from is kept while the cell lives, even a snapshot with no name. A snapshot no cell runs from is kept only while it is younger than `--keep-hours`, so set that longer than clients hold on to snapshot ids, or put the ids in a file and pass that too. A client done with a snapshot can delete it (`Snapshots.Delete`, or `delete_snapshot` in the Python SDK), which removes it from the store at once, and the next collection removes whatever of its layers no other image needs.

## A cluster

Start the services in this order, since each one looks for the ones before it.

1. **Keeper.** Pick three hosts, or five to survive two failures. Give each a `keeper.toml` with its own `id` and the same `[[member]]` list, and start `hive-keeper`. The list is only read when the group first forms. After that the members come from the group's own log, so editing the file does nothing.
2. **Scout.** Run one `hive-scout` with `SCOUT_ARGS` in `/etc/hivebox/scout.env`, for example `--listen 0.0.0.0:7410 --metrics 127.0.0.1:9469`. It keeps nothing on disk, so it can move to another host at any time. Point everything at the new address after a move.
3. **Combs.** On each node, set `node.listen` to an address on a network only the gates reach, `scout.endpoint` to the scout, and `keeper.members` to the keeper members. The keeper then gives the node its number and epoch, so `node.node` and `node.epoch` are not set by hand. The node keeps the same number across restarts as long as `keeper.name`, which is the host name by default, stays the same.
4. **Gates.** Set `gate.scout` and `gate.keeper` in `gate.toml`, and start as many gates as the load needs behind any TCP load balancer. With a keeper, the gates read project keys and quotas from it.

A comb holds a lease from the keeper, `lease_ms` long (10 s by default), and keeps renewing it. A comb that restarts within its lease keeps its cells. A comb whose lease ran out has lost its node, since the keeper may give its number to another comb. It stops its cells, and when it starts again it registers anew and fails the cells left from the old epoch. Keep the keeper close to the nodes, and keep a majority of its members up.

## Keys

Clients send a project key to the gate. Make one with:

```
hive-gate key PROJECT
```

It prints the key, which goes to the client as `HIVE_TOKEN`, and a `[[key]]` block with the key's blake3 hash, which goes in `gate.toml`. The gate keeps only the hash. With `gate.keeper` set, the gates also take the keys the keeper holds, and `[[key]]` is for keys that have to work while the keeper is down. To take a key out of `gate.toml`, remove its block and restart the gates one at a time.

## Upgrades

Upgrade in this order: keepers one at a time, then scout, then the combs, then the gates. Read the changelog first for anything that changes between them.

A comb is upgraded in place with `deploy/upgrade-comb.sh`, and a fleet with `deploy/roll.sh`. Both are described in [deploy/README.md](../deploy/README.md). The cells keep running while the comb restarts, and the new comb takes them back from the WAL. In a cluster the new comb has to register again within the lease, so if a comb takes longer than `lease_ms` to start, raise it before upgrading. To upgrade the kernel or reboot the host, drain the node first, see below.

A gate or scout keeps nothing, so a plain `systemctl restart` is fine. A gate that restarts fails the calls in flight on it, so restart gates one at a time behind the load balancer. Restart keeper members one at a time, waiting for each to come back before the next, so the group always has a majority.

## Taking a node out

```
hivectl node cordon                         # no new cells, the rest keep running
hivectl node drain --timeout 10m --snapshot # wait for cells to end, snapshot the idle ones
hivectl node uncordon                       # take cells again
```

Run these on the node, or point `--endpoint` at its `node.listen`. A drain cordons the node, stops idle cells at once, and waits up to the timeout for the busy ones to end. `--snapshot` snapshots each idle cell before it stops, so its work can go on from the snapshot on another node. `--stop-rest` stops the cells still running at the timeout. Without it they are left running, and `drain` exits 1. Cells a drain stops end with cause `drained`, which counts against the infra failure SLO, so drain in quiet hours and keep the timeout long.

The node stays cordoned after a drain, so it does not fill up again before the work on it is done. To remove a node for good, drain it with `--stop-rest`, stop the comb, and disable its unit. Scout marks it down after it stops reporting, and the keeper lets its lease run out.

## Backups

The keeper's database is the only state that has to be backed up. Each member has a full copy, so losing one member loses nothing. Copy `keeper.data` from one member while it is stopped, or from a filesystem snapshot. The WAL and cell directories under a comb's `data_dir` matter only to that node. If the node is lost, its cells are lost with it, and the keeper's lease makes sure no other comb takes its number while it might still be running.

## Metrics and logs

Each service serves Prometheus metrics on the address it is given, and [deploy/README.md](../deploy/README.md) has the scrape config, the alerts and the dashboard. Each alert links to its section of [runbooks.md](runbooks.md).

All the services write their logs to stderr, so they end up in the journal: `journalctl -u hive-comb`. The comb writes one line each time it starts, saying how many cells it took back. The audit log is under `data_dir/audit`. It has one line for every control call, exec, file write, policy change and snapshot, each carrying the hash of the line before, in one file per hour. `hivectl audit verify DIR` checks the chain, and with `--keeper` it also checks the hourly roots the comb sent to the keeper.
