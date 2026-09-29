//! Network policy for cells. An eBPF program on each cell's interface enforces the egress rules,
//! and the DNS proxy decides which names resolve at all.
//!
//! [`Guard`] loads the program, attaches it to interfaces and keeps the maps it reads. The maps
//! and the attachments are pinned in `/sys/fs/bpf/hive`, so a node that restarts keeps enforcing
//! the policy it had while the new process comes up.
//!
//! A cell is only allowed what its [`Profile`] allows, and anything else is dropped:
//!
//! - ARP with the cell's own address as the sender, so the host can reach it.
//! - IPv4 from the cell's own address, to a destination a [`Rule`] for its profile allows or one
//!   the DNS proxy resolved for it.
//!
//! IPv6 and every other protocol are dropped, and so is anything from an interface with no cell.
//! The design is in `spec/12_networking.md`.
//!
//! [`wire`] gives a network namespace its interface, a veth pair with the cell's address, routes
//! and neighbours set on both ends, through the small rtnetlink client in [`link`].
//!
//! [`dns`] is the proxy on [`DNS_VIP`]. It answers only the names a profile lists, and lets the
//! cell reach the addresses in each answer.

#![deny(unsafe_code)]

use std::fmt;
use std::net::Ipv4Addr;

pub mod dns;
#[cfg(target_os = "linux")]
mod guard;
#[cfg(target_os = "linux")]
pub mod link;
#[cfg(target_os = "linux")]
mod maps;
#[cfg(target_os = "linux")]
pub mod wire;

#[cfg(target_os = "linux")]
pub use guard::{DnsAllow, Guard};

/// Where the maps and links are pinned. Each layout of the maps gets its own directory, so a
/// newer program never reads a map made for an older one.
pub const PIN_DIR: &str = "/sys/fs/bpf/hive/guard-v1";

/// The DNS proxy's address, the same in every cell.
pub const DNS_VIP: Ipv4Addr = Ipv4Addr::new(169, 254, 77, 53);
/// The node's package mirror proxy.
pub const MIRRORS_VIP: Ipv4Addr = Ipv4Addr::new(169, 254, 77, 80);
/// The next hop every cell routes through, answered by the host side of its interface.
pub const GATEWAY: Ipv4Addr = Ipv4Addr::new(169, 254, 77, 1);

/// A set of rules a cell follows. The program only sees the number.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Profile(pub u32);

impl Profile {
    /// Nothing but the DNS proxy, which answers NXDOMAIN. The default for RL rollouts.
    pub const NONE: Self = Self(1);
    /// The DNS proxy and the package mirrors.
    pub const MIRRORS: Self = Self(2);

    /// A built-in profile by name.
    #[must_use]
    pub fn builtin(name: &str) -> Option<Self> {
        match name {
            "none" => Some(Self::NONE),
            "mirrors" => Some(Self::MIRRORS),
            _ => None,
        }
    }

    /// What a built-in profile allows. Other profiles have no rules of their own.
    #[must_use]
    pub fn builtin_rules(self) -> Vec<Rule> {
        let dns = [Proto::Udp, Proto::Tcp].map(|proto| Rule {
            profile: self,
            ip: DNS_VIP,
            proto,
            port: 53,
        });
        match self {
            Self::NONE => dns.to_vec(),
            Self::MIRRORS => {
                let mut rules = dns.to_vec();
                rules.extend([80, 443].map(|port| Rule {
                    profile: self,
                    ip: MIRRORS_VIP,
                    proto: Proto::Tcp,
                    port,
                }));
                rules
            }
            _ => Vec::new(),
        }
    }
}

/// The protocols a rule can name.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Proto {
    /// Any protocol, and then the port is ignored.
    Any,
    /// ICMP, which has no ports.
    Icmp,
    /// TCP.
    Tcp,
    /// UDP.
    Udp,
}

impl Proto {
    /// The IP protocol number, 0 for any.
    #[must_use]
    pub const fn number(self) -> u8 {
        match self {
            Self::Any => 0,
            Self::Icmp => 1,
            Self::Tcp => 6,
            Self::Udp => 17,
        }
    }
}

/// One destination a profile allows.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Rule {
    /// The profile it belongs to.
    pub profile: Profile,
    /// Where to.
    pub ip: Ipv4Addr,
    /// Over what.
    pub proto: Proto,
    /// To which port, 0 for any. Ignored unless `proto` is TCP or UDP.
    pub port: u16,
}

/// A cell as the program knows it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CellNet {
    /// A number no other cell on the node has had since the node started, so what the DNS proxy
    /// allowed an old cell never carries over to a new one on the same interface.
    pub idx: u32,
    /// The only source address the cell may use.
    pub ip: Ipv4Addr,
    /// The only source MAC it may use, or any when `None`.
    pub mac: Option<[u8; 6]>,
    /// What it may reach.
    pub profile: Profile,
}

/// Why the program passed or dropped a packet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Reason {
    /// A rule for the cell's profile allowed it.
    PassRule,
    /// The DNS proxy had resolved the destination for the cell.
    PassDns,
    /// ARP from the cell's own address.
    PassArp,
    /// The interface has no cell.
    NoCell,
    /// The source address or MAC was not the cell's.
    Spoof,
    /// IPv6, which cells do not get.
    Ipv6,
    /// Neither IPv4 nor ARP.
    Proto,
    /// Too short, or not a header the program understands.
    Malformed,
    /// Nothing allowed the destination.
    Policy,
}

impl Reason {
    /// Every reason, in the order the program counts them.
    pub const ALL: [Self; 9] = [
        Self::PassRule,
        Self::PassDns,
        Self::PassArp,
        Self::NoCell,
        Self::Spoof,
        Self::Ipv6,
        Self::Proto,
        Self::Malformed,
        Self::Policy,
    ];

    /// Whether the packet went through.
    #[must_use]
    pub const fn passed(self) -> bool {
        matches!(self, Self::PassRule | Self::PassDns | Self::PassArp)
    }

    fn from_index(i: u32) -> Option<Self> {
        Self::ALL.get(i as usize).copied()
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::PassRule => "rule",
            Self::PassDns => "dns",
            Self::PassArp => "arp",
            Self::NoCell => "no cell",
            Self::Spoof => "spoofed source",
            Self::Ipv6 => "ipv6",
            Self::Proto => "protocol",
            Self::Malformed => "malformed",
            Self::Policy => "policy",
        })
    }
}

/// Packets counted by reason, across every cell and CPU since the maps were made.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats(pub [u64; Reason::ALL.len()]);

impl Stats {
    /// Packets with this reason.
    #[must_use]
    pub fn get(&self, reason: Reason) -> u64 {
        self.0[reason as usize]
    }

    /// Packets passed.
    #[must_use]
    pub fn passed(&self) -> u64 {
        Reason::ALL.iter().filter(|r| r.passed()).map(|&r| self.get(r)).sum()
    }

    /// Packets dropped.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        Reason::ALL.iter().filter(|r| !r.passed()).map(|&r| self.get(r)).sum()
    }
}

/// A packet a cell sent that was dropped. The program reports these in a ring, and when the ring
/// is full it still drops but reports nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Deny {
    /// The cell's `idx`.
    pub cell: u32,
    /// Why.
    pub reason: Reason,
    /// The destination, or for a spoofed packet the source it claimed.
    pub ip: Ipv4Addr,
    /// The destination port, 0 when there was none.
    pub port: u16,
    /// The IP protocol number, 0 when it was not IP.
    pub proto: u8,
}

impl Deny {
    /// Reads one event as the program writes it.
    #[must_use]
    pub fn parse(bytes: &[u8]) -> Option<Self> {
        let &[a, b, c, d, e, f, g, h, i, j, k, l, m, n, o, _] = bytes.get(..16)? else {
            return None;
        };
        Some(Self {
            cell: u32::from_ne_bytes([a, b, c, d]),
            reason: Reason::from_index(u32::from_ne_bytes([e, f, g, h]))?,
            ip: Ipv4Addr::new(i, j, k, l),
            port: u16::from_be_bytes([m, n]),
            proto: o,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builtin_profiles_reach_only_the_node_services() {
        assert_eq!(Profile::builtin("none"), Some(Profile::NONE));
        assert_eq!(Profile::builtin("open"), None);
        let none = Profile::NONE.builtin_rules();
        assert!(none.iter().all(|r| r.ip == DNS_VIP && r.port == 53));
        let mirrors = Profile::MIRRORS.builtin_rules();
        assert_eq!(mirrors.len(), 4);
        assert!(mirrors.iter().all(|r| r.profile == Profile::MIRRORS));
        assert!(Profile(9).builtin_rules().is_empty());
    }

    #[test]
    fn a_deny_event_reads_back() {
        let mut b = [0u8; 16];
        b[0..4].copy_from_slice(&7u32.to_ne_bytes());
        b[4..8].copy_from_slice(&(Reason::Policy as u32).to_ne_bytes());
        b[8..12].copy_from_slice(&[192, 0, 2, 1]);
        b[12..14].copy_from_slice(&443u16.to_be_bytes());
        b[14] = 6;
        let d = Deny::parse(&b).unwrap();
        assert_eq!(
            d,
            Deny {
                cell: 7,
                reason: Reason::Policy,
                ip: Ipv4Addr::new(192, 0, 2, 1),
                port: 443,
                proto: 6
            }
        );
        b[4] = 200;
        assert_eq!(Deny::parse(&b), None);
        assert_eq!(Deny::parse(&b[..15]), None);
    }
}
