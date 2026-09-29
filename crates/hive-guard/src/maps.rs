//! The map keys and values, laid out as `src/bpf/guard.bpf.c` has them. Addresses and ports are
//! kept in network order, as they are in the packet.

#![allow(unsafe_code)]

use std::net::Ipv4Addr;

use crate::{CellNet, Rule};

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Cell {
    pub idx: u32,
    pub ip4: u32,
    pub profile: u32,
    pub flags: u32,
    pub mac: [u8; 6],
    pub pad: [u8; 2],
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub(crate) struct RuleKey {
    pub profile: u32,
    pub ip: u32,
    pub port: u16,
    pub proto: u8,
    pub pad: u8,
}

impl RuleKey {
    /// The bit in the profile's entry that says it has rules like this one.
    pub(crate) fn shape(&self) -> u32 {
        match (self.proto, self.port) {
            (0, _) => 4,
            (_, 0) => 2,
            _ => 1,
        }
    }
}

#[repr(C)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct DnsKey {
    pub cell: u32,
    pub ip: u32,
}

// SAFETY: plain integers with the padding spelled out, so every bit pattern is a valid value and
// no byte is uninitialized.
unsafe impl aya::Pod for Cell {}
// SAFETY: as above.
unsafe impl aya::Pod for RuleKey {}
// SAFETY: as above.
unsafe impl aya::Pod for DnsKey {}

pub(crate) fn net(ip: Ipv4Addr) -> u32 {
    u32::from_ne_bytes(ip.octets())
}

impl From<&CellNet> for Cell {
    fn from(c: &CellNet) -> Self {
        Self {
            idx: c.idx,
            ip4: net(c.ip),
            profile: c.profile.0,
            flags: 0,
            mac: c.mac.unwrap_or_default(),
            pad: [0; 2],
        }
    }
}

impl From<&Rule> for RuleKey {
    fn from(r: &Rule) -> Self {
        let port = match r.proto {
            crate::Proto::Tcp | crate::Proto::Udp => r.port.to_be(),
            _ => 0,
        };
        Self { profile: r.profile.0, ip: net(r.ip), port, proto: r.proto.number(), pad: 0 }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Profile, Proto};

    #[test]
    fn layouts_match_the_program() {
        assert_eq!(size_of::<Cell>(), 24);
        assert_eq!(size_of::<RuleKey>(), 12);
        assert_eq!(size_of::<DnsKey>(), 8);
        let k = RuleKey::from(&Rule {
            profile: Profile(3),
            ip: Ipv4Addr::new(10, 0, 0, 1),
            proto: Proto::Tcp,
            port: 443,
        });
        assert_eq!(k.ip.to_ne_bytes(), [10, 0, 0, 1]);
        assert_eq!(k.port.to_ne_bytes(), [1, 187]);
        let any = RuleKey::from(&Rule {
            profile: Profile(3),
            ip: Ipv4Addr::new(10, 0, 0, 1),
            proto: Proto::Icmp,
            port: 9,
        });
        assert_eq!((any.port, any.proto), (0, 1));
        assert_eq!((k.shape(), any.shape()), (1, 2));
        let all = RuleKey::from(&Rule {
            profile: Profile(3),
            ip: Ipv4Addr::new(10, 0, 0, 1),
            proto: Proto::Any,
            port: 9,
        });
        assert_eq!((all.port, all.proto, all.shape()), (0, 0, 4));
    }
}
