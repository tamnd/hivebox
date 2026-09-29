//! Just enough rtnetlink to wire a cell: a veth pair with one end in the cell's namespace, the
//! addresses, routes and neighbours on both ends, and the node's dummy interface for the VIPs.
//!
//! Every message asks for an acknowledgement, and a batch of them goes out before any answer is
//! read, so wiring a cell costs a few system calls and no round trips in between.

use std::io;
use std::net::Ipv4Addr;
use std::os::fd::{AsFd, AsRawFd, OwnedFd};

use rustix::net::{AddressFamily, RecvFlags, SendFlags, SocketFlags, SocketType};

const RTM_NEWLINK: u16 = 16;
const RTM_DELLINK: u16 = 17;
const RTM_NEWADDR: u16 = 20;
const RTM_NEWROUTE: u16 = 24;
const RTM_NEWNEIGH: u16 = 28;
const NLMSG_ERROR: u16 = 2;

const NLM_F_REQUEST: u16 = 0x1;
const NLM_F_ACK: u16 = 0x4;
const NLM_F_REPLACE: u16 = 0x100;
const NLM_F_EXCL: u16 = 0x200;
const NLM_F_CREATE: u16 = 0x400;

const IFF_UP: u32 = 1;
const IFLA_ADDRESS: u16 = 1;
const IFLA_IFNAME: u16 = 3;
const IFLA_LINKINFO: u16 = 18;
const IFLA_NET_NS_FD: u16 = 28;
const IFLA_INFO_KIND: u16 = 1;
const IFLA_INFO_DATA: u16 = 2;
const VETH_INFO_PEER: u16 = 1;
const NLA_F_NESTED: u16 = 0x8000;

const AF_INET: u8 = 2;
const IFA_ADDRESS: u16 = 1;
const IFA_LOCAL: u16 = 2;
const RT_TABLE_MAIN: u8 = 254;
const RTPROT_STATIC: u8 = 4;
const RT_SCOPE_UNIVERSE: u8 = 0;
const RT_SCOPE_LINK: u8 = 253;
const RTN_UNICAST: u8 = 1;
const RTNH_F_ONLINK: u32 = 4;
const RTA_DST: u16 = 1;
const RTA_OIF: u16 = 4;
const RTA_GATEWAY: u16 = 5;
const NUD_PERMANENT: u16 = 0x80;
const NDA_DST: u16 = 1;
const NDA_LLADDR: u16 = 2;

/// The index the cell's end of a veth pair gets in its namespace, where only loopback is before it.
pub const CELL_IFINDEX: u32 = 2;

/// One message being built: a header, a fixed part and attributes, some of them nested.
struct Msg {
    buf: Vec<u8>,
    open: Vec<usize>,
}

impl Msg {
    fn new(kind: u16, flags: u16) -> Self {
        let mut buf = Vec::with_capacity(128);
        buf.extend_from_slice(&0u32.to_ne_bytes()); // length, set in `done`
        buf.extend_from_slice(&kind.to_ne_bytes());
        buf.extend_from_slice(&(flags | NLM_F_REQUEST | NLM_F_ACK).to_ne_bytes());
        buf.extend_from_slice(&0u32.to_ne_bytes()); // sequence, set when sent
        buf.extend_from_slice(&0u32.to_ne_bytes()); // port id, the kernel's
        Self { buf, open: Vec::new() }
    }

    fn bytes(mut self, b: &[u8]) -> Self {
        self.buf.extend_from_slice(b);
        self
    }

    /// struct ifinfomsg for interface `index`, up if `up`.
    fn ifinfo(self, index: u32, up: bool) -> Self {
        let flags = if up { IFF_UP } else { 0 };
        self.bytes(&[0, 0, 0, 0])
            .bytes(&index.to_ne_bytes())
            .bytes(&flags.to_ne_bytes())
            .bytes(&flags.to_ne_bytes())
    }

    fn attr(mut self, kind: u16, value: &[u8]) -> Self {
        let len = u16::try_from(4 + value.len()).expect("attributes are small");
        self.buf.extend_from_slice(&len.to_ne_bytes());
        self.buf.extend_from_slice(&kind.to_ne_bytes());
        self.buf.extend_from_slice(value);
        self.pad()
    }

    fn pad(mut self) -> Self {
        while !self.buf.len().is_multiple_of(4) {
            self.buf.push(0);
        }
        self
    }

    fn name(self, kind: u16, name: &str) -> Self {
        let mut v = name.as_bytes().to_vec();
        v.push(0);
        self.attr(kind, &v)
    }

    fn nest(mut self, kind: u16) -> Self {
        self.open.push(self.buf.len());
        self.buf.extend_from_slice(&0u16.to_ne_bytes());
        self.buf.extend_from_slice(&(kind | NLA_F_NESTED).to_ne_bytes());
        self
    }

    fn end(mut self) -> Self {
        let at = self.open.pop().expect("a nest to end");
        let len = u16::try_from(self.buf.len() - at).expect("attributes are small");
        self.buf[at..at + 2].copy_from_slice(&len.to_ne_bytes());
        self
    }

    fn done(mut self, seq: u32) -> Vec<u8> {
        debug_assert!(self.open.is_empty());
        let len = u32::try_from(self.buf.len()).expect("messages are small");
        self.buf[0..4].copy_from_slice(&len.to_ne_bytes());
        self.buf[8..12].copy_from_slice(&seq.to_ne_bytes());
        self.buf
    }
}

/// A route socket in the namespace it was opened in, which it keeps whatever thread uses it.
#[derive(Debug)]
pub struct Netlink {
    sock: OwnedFd,
    seq: u32,
    batch: Vec<(u32, &'static str, Vec<u8>)>,
}

impl Netlink {
    /// A socket in the calling thread's network namespace.
    ///
    /// # Errors
    ///
    /// The kernel would not make one.
    pub fn open() -> io::Result<Self> {
        let sock = rustix::net::socket_with(
            AddressFamily::NETLINK,
            SocketType::RAW,
            SocketFlags::CLOEXEC,
            None,
        )?;
        Ok(Self { sock, seq: 0, batch: Vec::new() })
    }

    /// A socket in the network namespace `ns`, made on a short lived thread that joins it and
    /// runs `also` there first.
    ///
    /// # Errors
    ///
    /// The namespace could not be joined, or `also` failed.
    pub fn open_in(
        ns: &std::fs::File,
        also: impl FnOnce() -> io::Result<()> + Send,
    ) -> io::Result<Self> {
        std::thread::scope(|s| {
            s.spawn(|| {
                rustix::thread::move_into_link_name_space(
                    ns.as_fd(),
                    Some(rustix::thread::LinkNameSpaceType::Network),
                )?;
                also()?;
                Self::open()
            })
            .join()
            .map_err(|_| io::Error::other("the netlink thread panicked"))?
        })
    }

    fn push(&mut self, what: &'static str, msg: Msg) -> &mut Self {
        self.seq = self.seq.wrapping_add(1);
        self.batch.push((self.seq, what, msg.done(self.seq)));
        self
    }

    /// Queues a veth pair: `host` here, up, and `peer` inside the namespace `ns` with index
    /// [`CELL_IFINDEX`] there, down. The peer can't come up in the same message, since the kernel
    /// opens it before the host end exists.
    pub fn veth(
        &mut self,
        host: &str,
        host_mac: [u8; 6],
        peer: &str,
        peer_mac: [u8; 6],
        ns: &impl AsRawFd,
    ) -> &mut Self {
        let fd = ns.as_raw_fd().cast_unsigned();
        let msg = Msg::new(RTM_NEWLINK, NLM_F_CREATE | NLM_F_EXCL)
            .ifinfo(0, true)
            .name(IFLA_IFNAME, host)
            .attr(IFLA_ADDRESS, &host_mac)
            .nest(IFLA_LINKINFO)
            .name(IFLA_INFO_KIND, "veth")
            .nest(IFLA_INFO_DATA)
            .nest(VETH_INFO_PEER)
            .ifinfo(CELL_IFINDEX, false)
            .name(IFLA_IFNAME, peer)
            .attr(IFLA_ADDRESS, &peer_mac)
            .attr(IFLA_NET_NS_FD, &fd.to_ne_bytes())
            .end()
            .end()
            .end();
        self.push("making a veth pair", msg)
    }

    /// Queues a dummy interface, up. An existing one is left as it is.
    pub fn dummy(&mut self, name: &str) -> &mut Self {
        let msg = Msg::new(RTM_NEWLINK, NLM_F_CREATE)
            .ifinfo(0, true)
            .name(IFLA_IFNAME, name)
            .nest(IFLA_LINKINFO)
            .name(IFLA_INFO_KIND, "dummy")
            .end();
        self.push("making a dummy interface", msg)
    }

    /// Queues bringing an interface up.
    pub fn up(&mut self, ifindex: u32) -> &mut Self {
        self.push("bringing an interface up", Msg::new(RTM_NEWLINK, 0).ifinfo(ifindex, true))
    }

    /// Queues deleting an interface. For a veth, that deletes both ends.
    pub fn delete(&mut self, ifindex: u32) -> &mut Self {
        self.push("deleting an interface", Msg::new(RTM_DELLINK, 0).ifinfo(ifindex, false))
    }

    /// Queues `ip/32` on an interface, replacing it if it is there.
    pub fn address(&mut self, ifindex: u32, ip: Ipv4Addr) -> &mut Self {
        let msg = Msg::new(RTM_NEWADDR, NLM_F_CREATE | NLM_F_REPLACE)
            .bytes(&[AF_INET, 32, 0, RT_SCOPE_UNIVERSE])
            .bytes(&ifindex.to_ne_bytes())
            .attr(IFA_LOCAL, &ip.octets())
            .attr(IFA_ADDRESS, &ip.octets());
        self.push("adding an address", msg)
    }

    /// Queues a route to `dst/len` out of `ifindex`, through `via` if given, which is taken to be
    /// on the link whatever the interface's addresses say.
    pub fn route(
        &mut self,
        dst: Ipv4Addr,
        len: u8,
        ifindex: u32,
        via: Option<Ipv4Addr>,
    ) -> &mut Self {
        let scope = if via.is_some() { RT_SCOPE_UNIVERSE } else { RT_SCOPE_LINK };
        let flags = if via.is_some() { RTNH_F_ONLINK } else { 0 };
        let mut msg = Msg::new(RTM_NEWROUTE, NLM_F_CREATE | NLM_F_REPLACE)
            .bytes(&[AF_INET, len, 0, 0, RT_TABLE_MAIN, RTPROT_STATIC, scope, RTN_UNICAST])
            .bytes(&flags.to_ne_bytes());
        if len > 0 {
            msg = msg.attr(RTA_DST, &dst.octets());
        }
        msg = msg.attr(RTA_OIF, &ifindex.to_ne_bytes());
        if let Some(gw) = via {
            msg = msg.attr(RTA_GATEWAY, &gw.octets());
        }
        self.push("adding a route", msg)
    }

    /// Queues a permanent neighbour entry, so neither end ever has to ask with ARP.
    pub fn neighbour(&mut self, ifindex: u32, ip: Ipv4Addr, mac: [u8; 6]) -> &mut Self {
        let msg = Msg::new(RTM_NEWNEIGH, NLM_F_CREATE | NLM_F_REPLACE)
            .bytes(&[AF_INET, 0, 0, 0])
            .bytes(&ifindex.to_ne_bytes())
            .bytes(&NUD_PERMANENT.to_ne_bytes())
            .bytes(&[0, 0])
            .attr(NDA_DST, &ip.octets())
            .attr(NDA_LLADDR, &mac);
        self.push("adding a neighbour", msg)
    }

    /// Sends everything queued and waits for every answer. The kernel handles the messages in
    /// order and goes on after one fails, so the first failure is returned once all are in.
    ///
    /// # Errors
    ///
    /// A message failed, or the socket did.
    pub fn apply(&mut self) -> io::Result<()> {
        let batch = std::mem::take(&mut self.batch);
        if batch.is_empty() {
            return Ok(());
        }
        let all: Vec<u8> = batch.iter().flat_map(|(_, _, m)| m.iter().copied()).collect();
        let sent = rustix::net::send(&self.sock, &all, SendFlags::empty())?;
        if sent != all.len() {
            return Err(io::Error::other("netlink took part of a batch"));
        }
        let mut left = batch.len();
        let mut failed = None;
        let mut buf = vec![0u8; 16 * 1024];
        while left > 0 {
            let (n, _) = rustix::net::recv(&self.sock, &mut buf, RecvFlags::empty())?;
            let mut at = 0;
            while at + 16 <= n {
                let word = |i: usize| {
                    let b = &buf[at + i..];
                    u32::from_ne_bytes([b[0], b[1], b[2], b[3]])
                };
                let len = word(0) as usize;
                let kind = u16::from_ne_bytes([buf[at + 4], buf[at + 5]]);
                let seq = word(8);
                if kind == NLMSG_ERROR && at + 20 <= n {
                    left -= 1;
                    let errno = word(16) as i32;
                    if errno != 0 && failed.is_none() {
                        let what = batch.iter().find(|b| b.0 == seq).map_or("netlink", |b| b.1);
                        let e = io::Error::from_raw_os_error(-errno);
                        failed = Some(io::Error::new(e.kind(), format!("{what}: {e}")));
                    }
                }
                if len < 16 {
                    break;
                }
                at += len.next_multiple_of(4);
            }
        }
        failed.map_or(Ok(()), Err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nested_attributes_have_their_lengths_and_padding() {
        let m = Msg::new(RTM_NEWLINK, 0)
            .ifinfo(0, true)
            .nest(IFLA_LINKINFO)
            .name(IFLA_INFO_KIND, "veth")
            .end();
        let b = m.done(7);
        assert_eq!(u32::from_ne_bytes(b[0..4].try_into().unwrap()) as usize, b.len());
        assert_eq!(u32::from_ne_bytes(b[8..12].try_into().unwrap()), 7);
        // 16 header, 16 ifinfomsg, then the nest: 4 for itself and 12 for "veth\0" padded.
        assert_eq!(b.len(), 16 + 16 + 4 + 12);
        assert_eq!(u16::from_ne_bytes([b[32], b[33]]), 16);
        assert_eq!(u16::from_ne_bytes([b[34], b[35]]), IFLA_LINKINFO | NLA_F_NESTED);
        assert_eq!(&b[40..45], b"veth\0");
    }
}
