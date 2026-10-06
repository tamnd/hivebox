//! Giving a pooled network namespace its interface: a veth pair with the program on the host end,
//! one address for the cell, and routes and neighbours on both ends that never need ARP.

use std::fs::File;
use std::io;
use std::net::Ipv4Addr;
use std::path::Path;

use crate::link::{CELL_IFINDEX, Netlink};
use crate::{DNS_VIP, GATEWAY, LLM_VIP, MIRRORS_VIP};

/// The interface that holds the VIPs on the node.
pub const VIP_DEVICE: &str = "hive0";

/// The name the cell sees its interface under.
pub const CELL_DEVICE: &str = "eth0";

/// A cell's interface as the host sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Veth {
    /// The host end's name, `hv` and the namespace's number.
    pub host: String,
    /// The host end's index, which is the key the program looks the cell up by.
    pub ifindex: u32,
    /// The cell's only address.
    pub ip: Ipv4Addr,
    /// The cell end's MAC.
    pub mac: [u8; 6],
}

/// The host end's name for namespace `n`. Interface names are at most 15 bytes, which leaves 13
/// digits.
#[must_use]
pub fn host_name(n: u64) -> String {
    format!("hv{n}")
}

/// Locally administered MACs for the two ends, from the low 24 bits of `n`. Each pair is a link of
/// its own, so they only have to differ from each other.
#[must_use]
pub fn macs(n: u64) -> ([u8; 6], [u8; 6]) {
    let [_, _, _, _, _, a, b, c] = n.to_be_bytes();
    ([0x02, 0x68, 0x76, a, b, c], [0x02, 0x68, 0x63, a, b, c])
}

/// Brings up the dummy interface with the DNS, mirror and LLM gateway VIPs, or leaves it as it is.
///
/// # Errors
///
/// The kernel refused a change.
pub fn vips(nl: &mut Netlink) -> io::Result<()> {
    nl.dummy(VIP_DEVICE).apply()?;
    let ifindex = crate::guard::ifindex(VIP_DEVICE)?;
    nl.address(ifindex, DNS_VIP).address(ifindex, MIRRORS_VIP).address(ifindex, LLM_VIP).apply()
}

/// Wires the namespace at `ns`, number `n`, with `ip` as the cell's address. `nl` is a socket in
/// the host's namespace. Nothing is attached yet, and until the program is, the cell can reach
/// the host through its gateway like any directly connected neighbour.
///
/// # Errors
///
/// The namespace could not be joined or the kernel refused a change. What was made is deleted.
pub fn wire(nl: &mut Netlink, ns: &Path, n: u64, ip: Ipv4Addr) -> io::Result<Veth> {
    let file = File::open(ns)?;
    // The cell gets no IPv6. Set before its interface arrives, the default applies to it.
    let mut inside = Netlink::open_in(&file, || {
        std::fs::write("/proc/sys/net/ipv6/conf/default/disable_ipv6", "1")
    })?;
    let host = host_name(n);
    let (host_mac, cell_mac) = macs(n);
    nl.veth(&host, host_mac, CELL_DEVICE, cell_mac, &file).apply()?;
    let ifindex = crate::guard::ifindex(&host)?;
    let rest = (|| {
        let _ = std::fs::write(format!("/proc/sys/net/ipv6/conf/{host}/disable_ipv6"), "1");
        nl.route(ip, 32, ifindex, None).neighbour(ifindex, ip, cell_mac).apply()?;
        inside
            .up(CELL_IFINDEX)
            .address(CELL_IFINDEX, ip)
            .neighbour(CELL_IFINDEX, GATEWAY, host_mac)
            .route(Ipv4Addr::UNSPECIFIED, 0, CELL_IFINDEX, Some(GATEWAY))
            .apply()
    })();
    if let Err(e) = rest {
        let _ = nl.delete(ifindex).apply();
        return Err(e);
    }
    Ok(Veth { host, ifindex, ip, mac: cell_mac })
}

/// Deletes a cell's interface, both ends of it, and the host's route to it with it. One already
/// gone is fine.
///
/// # Errors
///
/// The kernel refused.
pub fn unwire(nl: &mut Netlink, veth: &Veth) -> io::Result<()> {
    match crate::guard::ifindex(&veth.host) {
        Ok(i) if i == veth.ifindex => nl.delete(i).apply(),
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_and_macs_fit() {
        assert_eq!(host_name(42), "hv42");
        assert!(host_name(9_999_999_999_999).len() <= 15);
        let (h, c) = macs(0x0102_0304);
        assert_eq!(h, [2, 0x68, 0x76, 2, 3, 4]);
        assert_eq!(c, [2, 0x68, 0x63, 2, 3, 4]);
        assert_ne!(h, c);
    }
}
