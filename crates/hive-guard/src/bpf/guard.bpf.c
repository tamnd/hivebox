// The egress filter every cell's traffic goes through. It runs on tc ingress of the host side of
// the cell's veth, so it sees what the cell sends. One program serves every cell, and all the
// state is in the maps, which hive-guard pins in /sys/fs/bpf/hive and fills from user space.
//
// It is plain C with no libbpf headers, so the only thing a build needs is clang. The layouts
// here must match the repr(C) types in src/maps.rs.

typedef unsigned char __u8;
typedef unsigned short __u16;
typedef unsigned int __u32;
typedef unsigned long long __u64;

#define SEC(name) __attribute__((section(name), used))
#define __uint(name, val) int (*name)[val]
#define __type(name, val) typeof(val) *name
#define always_inline inline __attribute__((always_inline))

#define BPF_MAP_TYPE_HASH 1
#define BPF_MAP_TYPE_ARRAY 2
#define BPF_MAP_TYPE_PERCPU_ARRAY 6
#define BPF_MAP_TYPE_LRU_HASH 9
#define BPF_MAP_TYPE_RINGBUF 27
#define BPF_F_NO_PREALLOC 1

#define TC_ACT_OK 0
#define TC_ACT_SHOT 2

#define ETH_P_IP 0x0800
#define ETH_P_ARP 0x0806
#define ETH_P_IPV6 0x86DD
#define ARPHRD_ETHER 1

#define IPPROTO_ICMP 1
#define IPPROTO_TCP 6
#define IPPROTO_UDP 17

#define bswap16(x) __builtin_bswap16(x)

static void *(*bpf_map_lookup_elem)(void *map, const void *key) = (void *)1;
static __u64 (*bpf_ktime_get_boot_ns)(void) = (void *)125;
static long (*bpf_ringbuf_output)(void *ringbuf, void *data, __u64 size, __u64 flags) = (void *)130;

// Only the fields the program reads, in the kernel's order.
struct __sk_buff {
	__u32 len, pkt_type, mark, queue_mapping, protocol, vlan_present, vlan_tci, vlan_proto;
	__u32 priority, ingress_ifindex, ifindex, tc_index, cb[5], hash, tc_classid;
	__u32 data, data_end;
};

struct ethhdr {
	__u8 dst[6];
	__u8 src[6];
	__u16 proto;
} __attribute__((packed));

struct iphdr {
	__u8 ihl_version;
	__u8 tos;
	__u16 tot_len, id, frag_off;
	__u8 ttl, protocol;
	__u16 check;
	__u32 saddr, daddr;
};

struct arphdr {
	__u16 hrd, pro;
	__u8 hln, pln;
	__u16 op;
	__u8 sha[6];
	__u8 spa[4];
} __attribute__((packed));

// A cell on one interface. ip4 is in network order. A zero mac skips the source MAC check.
struct cell {
	__u32 idx;
	__u32 ip4;
	__u32 profile;
	__u32 flags;
	__u8 mac[6];
	__u8 pad[2];
};

// An allowed destination for a profile. Port 0 means any port, and proto 0 any protocol. ip and
// port are in network order.
struct rule_key {
	__u32 profile;
	__u32 ip;
	__u16 port;
	__u8 proto;
	__u8 pad;
};

// Which kinds of rule a profile has, so a packet only pays for the lookups that can match. The
// Rust side sets a bit before it adds a rule of that kind and clears it after the last one goes.
#define SHAPE_PORT 1
#define SHAPE_ANY_PORT 2
#define SHAPE_ANY_PROTO 4
#define PROFILES 65536

// An address the DNS proxy resolved for a cell, allowed until the boot time in the value.
struct dns_key {
	__u32 cell;
	__u32 ip;
};

// Why a packet passed or dropped. Each is a slot in the stats map and the reason in an event.
enum {
	PASS_RULE,
	PASS_DNS,
	PASS_ARP,
	DROP_NO_CELL,
	DROP_SPOOF,
	DROP_IPV6,
	DROP_PROTO,
	DROP_MALFORMED,
	DROP_POLICY,
	REASONS,
};

struct deny {
	__u32 cell;
	__u32 reason;
	__u32 ip;
	__u16 port;
	__u8 proto;
	__u8 pad;
};

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, __u32);
	__type(value, struct cell);
} cells SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_HASH);
	__uint(max_entries, 65536);
	__uint(map_flags, BPF_F_NO_PREALLOC);
	__type(key, struct rule_key);
	__type(value, __u32);
} rules SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_ARRAY);
	__uint(max_entries, PROFILES);
	__type(key, __u32);
	__type(value, __u32);
} profiles SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_LRU_HASH);
	__uint(max_entries, 262144);
	__type(key, struct dns_key);
	__type(value, __u64);
} dns_allow SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_PERCPU_ARRAY);
	__uint(max_entries, REASONS);
	__type(key, __u32);
	__type(value, __u64);
} stats SEC(".maps");

struct {
	__uint(type, BPF_MAP_TYPE_RINGBUF);
	__uint(max_entries, 1 << 18);
} events SEC(".maps");

static always_inline int count(__u32 reason)
{
	__u64 *n = bpf_map_lookup_elem(&stats, &reason);
	if (n)
		*n += 1;
	return reason < DROP_NO_CELL ? TC_ACT_OK : TC_ACT_SHOT;
}

static always_inline int deny(__u32 cell, __u32 reason, __u32 ip, __u16 port, __u8 proto)
{
	struct deny d = { .cell = cell, .reason = reason, .ip = ip, .port = port, .proto = proto };
	// A full ring loses the event, never the drop.
	bpf_ringbuf_output(&events, &d, sizeof(d), 0);
	return count(reason);
}

static always_inline int allowed(__u32 profile, __u32 ip, __u16 port, __u8 proto)
{
	__u32 *shapes = bpf_map_lookup_elem(&profiles, &profile);
	if (!shapes)
		return 0;
	__u32 has = *shapes;
	struct rule_key k = { .profile = profile, .ip = ip, .port = port, .proto = proto };
	__u32 *v;
	if ((has & SHAPE_PORT) && port) {
		v = bpf_map_lookup_elem(&rules, &k);
		if (v)
			return *v;
	}
	k.port = 0;
	if (has & SHAPE_ANY_PORT) {
		v = bpf_map_lookup_elem(&rules, &k);
		if (v)
			return *v;
	}
	k.proto = 0;
	if (has & SHAPE_ANY_PROTO) {
		v = bpf_map_lookup_elem(&rules, &k);
		if (v)
			return *v;
	}
	return 0;
}

static always_inline int same_mac(const __u8 *a, const __u8 *b)
{
	return a[0] == b[0] && a[1] == b[1] && a[2] == b[2] && a[3] == b[3] && a[4] == b[4] &&
	       a[5] == b[5];
}

SEC("classifier")
int guard_cell_egress(struct __sk_buff *skb)
{
	void *data = (void *)(long)skb->data;
	void *end = (void *)(long)skb->data_end;
	__u32 ifindex = skb->ifindex;

	struct cell *c = bpf_map_lookup_elem(&cells, &ifindex);
	if (!c)
		return count(DROP_NO_CELL);

	struct ethhdr *eth = data;
	if ((void *)(eth + 1) > end)
		return deny(c->idx, DROP_MALFORMED, 0, 0, 0);
	__u8 zero[6] = {};
	if (!same_mac(c->mac, zero) && !same_mac(eth->src, c->mac))
		return deny(c->idx, DROP_SPOOF, 0, 0, 0);

	if (eth->proto == bswap16(ETH_P_ARP)) {
		struct arphdr *arp = (void *)(eth + 1);
		if ((void *)(arp + 1) > end)
			return deny(c->idx, DROP_MALFORMED, 0, 0, 0);
		if (arp->hrd != bswap16(ARPHRD_ETHER) || arp->pro != bswap16(ETH_P_IP) ||
		    arp->hln != 6 || arp->pln != 4)
			return deny(c->idx, DROP_MALFORMED, 0, 0, 0);
		__u32 spa = arp->spa[0] | arp->spa[1] << 8 | arp->spa[2] << 16 | (__u32)arp->spa[3] << 24;
		if (spa != c->ip4 || !same_mac(arp->sha, eth->src))
			return deny(c->idx, DROP_SPOOF, spa, 0, 0);
		return count(PASS_ARP);
	}
	if (eth->proto == bswap16(ETH_P_IPV6))
		return deny(c->idx, DROP_IPV6, 0, 0, 0);
	if (eth->proto != bswap16(ETH_P_IP))
		return deny(c->idx, DROP_PROTO, 0, 0, 0);

	struct iphdr *ip = (void *)(eth + 1);
	if ((void *)(ip + 1) > end)
		return deny(c->idx, DROP_MALFORMED, 0, 0, 0);
	__u32 hlen = (ip->ihl_version & 0x0F) * 4;
	if ((ip->ihl_version >> 4) != 4 || hlen < sizeof(*ip))
		return deny(c->idx, DROP_MALFORMED, 0, 0, 0);
	if (ip->saddr != c->ip4)
		return deny(c->idx, DROP_SPOOF, ip->saddr, 0, 0);

	// Later fragments carry no ports, so only rules for any port match them.
	__u16 port = 0;
	__u8 proto = ip->protocol;
	int first = (ip->frag_off & bswap16(0x1FFF)) == 0;
	if (first && (proto == IPPROTO_TCP || proto == IPPROTO_UDP)) {
		__u16 *ports = (void *)ip + hlen;
		if ((void *)(ports + 2) > end)
			return deny(c->idx, DROP_MALFORMED, ip->daddr, 0, proto);
		port = ports[1];
	}

	if (allowed(c->profile, ip->daddr, port, proto))
		return count(PASS_RULE);
	struct dns_key k = { .cell = c->idx, .ip = ip->daddr };
	__u64 *until = bpf_map_lookup_elem(&dns_allow, &k);
	if (until && *until > bpf_ktime_get_boot_ns())
		return count(PASS_DNS);
	return deny(c->idx, DROP_POLICY, ip->daddr, port, proto);
}

char _license[] SEC("license") = "Dual MIT/GPL";
