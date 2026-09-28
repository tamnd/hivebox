# Networking: hive-guard

> The design has to handle thousands of short-lived network endpoints per node, create and destroy them at hundreds per second, apply default-deny policies that can change mid-life, and assume zero trust between cells. The main kernel bottleneck is RTNL lock contention. For that reason network objects are pooled and recycled, and policy lives in shared eBPF maps rather than in per-cell iptables.

## 1. Topology

```
                        ┌───────────── node ─────────────────────────────────────────┐
  fabric (L3, ECMP) ◀──▶│ uplink ── tc/XDP (guard-edge) ── host routing              │
                        │                     │                                      │
                        │   hive0 (L3 "bridge": per-cell /32 routes, proxy_arp/ndp)  │
                        │    ├─ veth-c1 ◀▶ netns(cell 1)  [container]                 │
                        │    ├─ tap-c2  ◀▶ Firecracker (cell 2) virtio-net            │
                        │    └─ ...                                                   │
                        │   link-local VIPs: 169.254.77.53 (DNS), .80 (mirrors),      │
                        │                    .81 (LLM gateway), .82 (gate data-plane) │
                        │   hive-guard-dns · mirror proxy · llm proxy (node-local)    │
                        └─────────────────────────────────────────────────────────────┘
```

The network is L3 only. There is no shared L2 between cells. Each cell has a /32 (IPv4) and /128 (IPv6) route on its host-side interface, and the host answers neighbor discovery. This removes ARP spoofing and L2 floods.

Each node owns a /20 IPv4 slice of `100.64.0.0/10` (CGNAT space, not internet-routable), which gives 4,094 cells, plus an IPv6 ULA /64 per node. Egress to the internet, when allowed, goes through node SNAT to the node IP, or through a central egress NAT pool for IP reputation isolation.

VMs use tap plus virtio-net (vhost-net). Firecracker has its own netns under jailer. The tap lives in the jail netns and is linked to the host through a pooled veth pair.

## 2. Datapath: aya eBPF programs

| Program | Hook | Function |
|---|---|---|
| `guard_cell_egress` | tc ingress on the host-side veth/tap (traffic *from* cell) | anti-spoof (src IP/MAC must match the cell), policy lookup, rate limit, redirect to VIP services |
| `guard_cell_ingress` | tc egress on the host-side veth/tap | allow only established flows + exposed ports from the gate |
| `guard_uplink` | XDP/tc on the uplink | drop fabric traffic to cell IPs not originating from gate/mirrors; DDoS guard |
| `guard_sock` | cgroup/connect4/6, sendmsg (container cells) | early deny + VIP rewrite before routing (cheap) |

Maps are pinned in `/sys/fs/bpf/hive/`, so they survive a comb restart:

```
cell_by_ifindex : HASH<u32 ifindex, CellNet{cell_idx, ip4, ip6, mac, profile_id, group_id, flags}>
profile_rules   : HASH<(profile_id, proto, port), Verdict>      + LPM_TRIE<(profile_id, prefix), Verdict>
dns_allow       : LRU_HASH<(profile_id|cell_idx, ip), expiry>    // filled by DNS proxy
edt_state       : HASH<cell_idx, {rate_bps, t_last}>             // EDT pacing (skb->tstamp) with fq qdisc
conn_limits     : PERCPU_HASH<cell_idx, counters>                 // new conns/s, total conns
events          : RINGBUF                                         // denies → audit/tamper signals
```

All cells share one program instance. Attaching a cell is a map insert plus a `tc` filter attach on a pooled interface, and the filter attach happens at pool-fill time. Policy updates are therefore O(1) map writes with no program reloads.

Egress bandwidth is limited with Earliest Departure Time pacing, in the style of the Cilium bandwidth manager. New connections per second use a token bucket (default 50/s), and there is a cap on concurrent connections (default 512).

## 3. Policy model

```
NetworkProfile {
  name: "mirrors",
  allow: [
    Vip(Dns), Vip(Mirrors), Vip(Llm)?,
    Domain("*.pypi.org"), Domain("files.pythonhosted.org"),
    Cidr("10.20.0.0/16", tcp:[443]),
  ],
  intra_group: false,
  egress_bps: 50_000_000, new_conn_per_s: 50,
}
```

Built-in profiles:

- `none`: DNS only returns NXDOMAIN. This is the RL rollout default.
- `mirrors`: package mirrors via VIP only.
- `llm`: `mirrors` plus the LLM gateway.
- `build`: `mirrors` plus github, the registries and a curated domain list.
- `open`: the internet except RFC1918, metadata, and cluster ranges. It needs project permission.

Profiles are stored in keeper and cached on comb as `profile_id`. Changing a profile mid-life (`Cells.UpdatePolicy`) rewrites one map entry and flushes that cell's conntrack and `dns_allow` entries.

## 4. DNS proxy (`hive-guard-dns`, hickory-dns)

- The cell's resolv.conf points to `169.254.77.53`. The proxy resolves only names that match the profile's domain patterns. Answers are recorded into `dns_allow` with a TTL (min 30 s, max 10 min) *before* the response is returned, so a connection to a resolved IP is permitted. Other names get NXDOMAIN.
- The proxy guards against DNS tunneling with a per-cell query rate limit, label length and entropy heuristics, and no TXT queries in `none` or `mirrors`.
- Direct-IP egress is only allowed through explicit CIDR rules.

## 5. Mirrors and services on VIPs

- A node-local reverse proxy (hyper/pingora-style Rust) on `169.254.77.80` serves pypi, npm, crates, go proxy, apt, conda, Maven and HF mirrors, keyed by Host/SNI. It forwards to cluster mirror caches (for example devpi, verdaccio, artifactory, or plain object-store-backed caches).
- The node proxy is also a cache layer (NVMe, content-addressed). It cuts the cost of cold-start pip installs across thousands of identical rollouts.
- For the LLM route, see 11 section 6.

## 6. Ingress (exposed ports)

`Cells.ExposePort(cell, port)` returns `https://{port}-{cell}.cells.unit1.hive.example`, in the same style as E2B. A request goes from the client to gate (TLS, auth token or cookie), then to comb (h2 stream), then to drone (vsock/UDS), which calls `connect(127.0.0.1:port)` inside the cell. The cell network needs no inbound routing, and exposed ports cannot be reached from the fabric directly. WebSocket and HTTP/2 are supported. Raw TCP goes over the h2 CONNECT tunnel.

## 7. Pool mechanics and RTNL avoidance

- Pre-create netns and veth pairs (or tap plus veth for the FC jail) in batches of 32, with one netlink socket per batch. Refill at ≤200/s so the pool never starves the create path.
- On cell destroy, the interface returns to the pool: flush addresses inside the netns (unless preserved), clear the conntrack zone, and update `cell_by_ifindex`. The netns is reused, not deleted, because `netns` deletion triggers expensive `cleanup_net` work under RTNL.
- Sysctls: raise `net.core.netdev_max_backlog` and `net.ipv4.neigh.default.gc_thresh*`, and set `net.ipv4.conf.all.rp_filter=1`.
- Measured target: ≤0.5 ms per cell network attach from the pool, compared with about 20-60 ms for a fresh CNI setup.

## 8. Crate stack

`aya` 0.14 (+ `aya-ebpf`, `aya-log`), `rtnetlink` 0.23, `netlink-packet-route`, `hickory-server`/`hickory-resolver`, `ipnet`, `tokio`, `hyper` 1.x for the proxies.
