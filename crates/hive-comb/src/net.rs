//! Cell networking from `spec/08_node_agent.md`, section 3: every pooled namespace gets a veth
//! pair with `hive-guard` on the host end, an address from the node's range and a default route to
//! the gateway. What a cell may reach is one entry in the guard's maps, written when the cell is
//! made, so a create costs a map write and no netlink.
//!
//! The DNS proxy runs here too, on the guard's DNS address, and answers each cell by the profile
//! it was made with.

use hive_guard::dns::{self, Cells, Policy, Proxy, Settings};
use hive_guard::link::Netlink;
use hive_guard::wire::{self, Veth};
use hive_guard::{CellNet, DNS_VIP, DnsAllow, Guard, Profile, Rule};
use std::collections::HashMap;
use std::future::Future;
use std::io;
use std::net::Ipv4Addr;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};
use std::time::Duration;

use crate::config::Network;

/// The node's side of cell networking. Every change goes through one lock, since the kernel takes
/// its own RTNL lock for each of them anyway.
pub(crate) struct Net {
    state: Mutex<State>,
    book: Arc<Book>,
    profiles: HashMap<String, Profile>,
    proxy: Arc<Proxy>,
}

/// The first id a profile from the config gets. The ones below are for built-in profiles.
const FIRST_CUSTOM: u32 = 16;

struct State {
    guard: Guard,
    nl: Netlink,
    ips: Ips,
}

impl std::fmt::Debug for Net {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Net").finish_non_exhaustive()
    }
}

impl Net {
    /// Loads the guard, brings up the VIPs and writes the profiles, built in and from the config.
    /// The profiles from the config get ids in the order of their names, from 16 on, and reach
    /// the DNS proxy and what it resolves for them.
    pub(crate) fn open(cfg: &Network) -> io::Result<Self> {
        let ips = Ips::new(cfg.cells.0, cfg.cells.1)?;
        let mut guard = Guard::open(&cfg.pin_dir)?;
        let mut profiles = HashMap::new();
        let mut policies = HashMap::new();
        for p in [Profile::NONE, Profile::MIRRORS] {
            guard.set_profile(p, &p.builtin_rules())?;
        }
        profiles.insert("none".to_string(), Profile::NONE);
        profiles.insert("mirrors".to_string(), Profile::MIRRORS);
        for (id, (name, domains)) in (FIRST_CUSTOM..).zip(&cfg.profiles) {
            let p = Profile(id);
            let rules: Vec<Rule> = Profile::NONE
                .builtin_rules()
                .into_iter()
                .map(|r| Rule { profile: p, ..r })
                .collect();
            guard.set_profile(p, &rules)?;
            let policy = Policy::new(domains).map_err(|e| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("profile {name}: {e}"))
            })?;
            profiles.insert(name.clone(), p);
            policies.insert(p, policy);
        }
        let mut upstream = cfg.upstream.clone();
        if upstream.is_empty() {
            upstream = dns::upstreams(&std::fs::read_to_string("/etc/resolv.conf")?);
        }
        if upstream.is_empty() {
            eprintln!("hive-comb: no resolvers to ask, so every lookup a cell makes fails");
        }
        let book =
            Arc::new(Book { cells: RwLock::default(), allow: Mutex::new(guard.dns_allow()?) });
        let settings = Settings { upstream, policies, ..Settings::default() };
        let proxy = Arc::new(Proxy::new(settings, book.clone()));
        let mut nl = Netlink::open()?;
        wire::vips(&mut nl)?;
        Ok(Self { state: Mutex::new(State { guard, nl, ips }), book, profiles, proxy })
    }

    /// The DNS proxy on the guard's DNS address, to run until the comb stops. The comb before a
    /// restart can hold the port for a moment while its last answers go out, so binding is tried
    /// for a few seconds before the proxy gives up and says so.
    pub(crate) fn dns(&self) -> impl Future<Output = ()> + use<> {
        let proxy = self.proxy.clone();
        async move {
            let mut tries = 0;
            let sock = loop {
                match tokio::net::UdpSocket::bind((DNS_VIP, 53)).await {
                    Ok(sock) => break sock,
                    Err(e) if e.kind() == io::ErrorKind::AddrInUse && tries < 50 => {
                        tries += 1;
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Err(e) => {
                        eprintln!("hive-comb: the DNS proxy did not start: {e}");
                        return;
                    }
                }
            };
            proxy.serve(sock).await;
        }
    }

    /// The profile a spec's `network_profile` names.
    pub(crate) fn profile(&self, name: &str) -> Option<Profile> {
        self.profiles.get(name).copied()
    }

    fn state(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Gives the namespace at `ns`, number `n`, its interface, with the guard on it and no cell,
    /// so it passes nothing until [`Net::assign`].
    pub(crate) fn wire(&self, ns: &Path, n: u64) -> io::Result<Veth> {
        let mut s = self.state();
        let ip = s.ips.take().ok_or_else(|| {
            io::Error::new(io::ErrorKind::AddrNotAvailable, "every cell address is in use")
        })?;
        let wired = wire::wire(&mut s.nl, ns, n, ip).and_then(|v| {
            // An entry left by a comb that died could name this index, and must not carry over.
            s.guard.remove_cell(v.ifindex)?;
            match s.guard.attach(&v.host) {
                Ok(_) => Ok(v),
                Err(e) => {
                    let _ = wire::unwire(&mut s.nl, &v);
                    Err(e)
                }
            }
        });
        if wired.is_err() {
            s.ips.free(ip);
        }
        wired
    }

    /// Puts a cell on its interface: from now on it reaches what `profile` allows.
    pub(crate) fn assign(&self, veth: &Veth, idx: u32, profile: Profile) -> io::Result<()> {
        let cell = CellNet { idx, ip: veth.ip, mac: Some(veth.mac), profile };
        self.state().guard.set_cell(veth.ifindex, &cell)?;
        self.book.set(veth.ip, Some((idx, profile)));
        Ok(())
    }

    /// Takes the program and the cell off an interface and frees its address. The interface
    /// itself goes with its namespace.
    pub(crate) fn release(&self, veth: &Veth) {
        self.book.set(veth.ip, None);
        let mut s = self.state();
        let _ = s.guard.remove_cell(veth.ifindex);
        let _ = s.guard.detach(veth.ifindex);
        s.ips.free(veth.ip);
    }

    /// The interface of namespace `n` after a restart, with its address marked as used again.
    pub(crate) fn recover(&self, n: u64) -> Option<Veth> {
        let host = wire::host_name(n);
        let ifindex = std::fs::read_to_string(format!("/sys/class/net/{host}/ifindex")).ok()?;
        let ifindex = ifindex.trim().parse().ok()?;
        let mut s = self.state();
        let cell = s.guard.cell(ifindex).ok()??;
        s.ips.claim(cell.ip);
        self.book.set(cell.ip, Some((cell.idx, cell.profile)));
        Some(Veth { host, ifindex, ip: cell.ip, mac: cell.mac.unwrap_or(wire::macs(n).1) })
    }

    /// Takes the program off the interface of namespace `n`, if it has one, before the namespace
    /// goes.
    pub(crate) fn forget(&self, n: u64) {
        let host = wire::host_name(n);
        if let Ok(i) = std::fs::read_to_string(format!("/sys/class/net/{host}/ifindex"))
            && let Ok(ifindex) = i.trim().parse()
        {
            let mut s = self.state();
            if let Ok(Some(cell)) = s.guard.cell(ifindex) {
                self.book.set(cell.ip, None);
            }
            let _ = s.guard.remove_cell(ifindex);
            let _ = s.guard.detach(ifindex);
        }
    }

    /// Where cells send DNS queries.
    pub(crate) fn nameserver() -> Ipv4Addr {
        DNS_VIP
    }
}

/// Which cell has which address, for the DNS proxy, which reads it on every query and so has it
/// apart from the lock wiring holds.
struct Book {
    cells: RwLock<HashMap<Ipv4Addr, (u32, Profile)>>,
    allow: Mutex<DnsAllow>,
}

impl Book {
    fn set(&self, ip: Ipv4Addr, cell: Option<(u32, Profile)>) {
        let mut cells = self.cells.write().unwrap_or_else(PoisonError::into_inner);
        match cell {
            Some(c) => cells.insert(ip, c),
            None => cells.remove(&ip),
        };
    }
}

impl Cells for Book {
    fn cell(&self, ip: Ipv4Addr) -> Option<(u32, Profile)> {
        self.cells.read().unwrap_or_else(PoisonError::into_inner).get(&ip).copied()
    }

    fn allow(&self, idx: u32, ips: &[Ipv4Addr], ttl: Duration) -> io::Result<()> {
        let mut allow = self.allow.lock().unwrap_or_else(PoisonError::into_inner);
        ips.iter().try_for_each(|&ip| allow.allow(idx, ip, ttl))
    }
}

/// The node's cell addresses, handed out in turn so a freed one is the last to be used again.
#[derive(Debug)]
struct Ips {
    base: u32,
    used: Vec<bool>,
    next: usize,
    free: usize,
}

impl Ips {
    fn new(base: Ipv4Addr, prefix: u8) -> io::Result<Self> {
        if !(8..=30).contains(&prefix) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("cells = {base}/{prefix} needs a prefix from 8 to 30"),
            ));
        }
        let size = 1usize << (32 - prefix);
        let base = u32::from(base) & !(u32::MAX >> prefix);
        let mut used = vec![false; size];
        // Neither the network's own address nor its broadcast one goes to a cell.
        used[0] = true;
        used[size - 1] = true;
        Ok(Self { base, used, next: 1, free: size - 2 })
    }

    fn take(&mut self) -> Option<Ipv4Addr> {
        if self.free == 0 {
            return None;
        }
        let size = self.used.len();
        while self.used[self.next] {
            self.next = (self.next + 1) % size;
        }
        let i = self.next;
        self.used[i] = true;
        self.free -= 1;
        self.next = (i + 1) % size;
        Some(Ipv4Addr::from(self.base + i as u32))
    }

    fn slot(&self, ip: Ipv4Addr) -> Option<usize> {
        let i = u32::from(ip).checked_sub(self.base)? as usize;
        (i < self.used.len()).then_some(i)
    }

    fn claim(&mut self, ip: Ipv4Addr) {
        if let Some(i) = self.slot(ip)
            && !self.used[i]
        {
            self.used[i] = true;
            self.free -= 1;
        }
    }

    fn free(&mut self, ip: Ipv4Addr) {
        if let Some(i) = self.slot(ip)
            && i != 0
            && i != self.used.len() - 1
            && self.used[i]
        {
            self.used[i] = false;
            self.free += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn addresses_go_round_and_are_never_given_twice() {
        let mut ips = Ips::new(Ipv4Addr::new(100, 64, 0, 3), 29).unwrap();
        let got: Vec<Ipv4Addr> = std::iter::from_fn(|| ips.take()).collect();
        assert_eq!(got.len(), 6);
        assert_eq!(got[0], Ipv4Addr::new(100, 64, 0, 1));
        assert_eq!(got[5], Ipv4Addr::new(100, 64, 0, 6));
        ips.free(got[2]);
        ips.free(got[2]);
        ips.free(Ipv4Addr::new(100, 64, 0, 7));
        ips.free(Ipv4Addr::new(10, 0, 0, 1));
        assert_eq!(ips.take(), Some(got[2]));
        assert_eq!(ips.take(), None);
        ips.free(got[0]);
        ips.free(got[4]);
        // The search goes on from the last one given out, not from the lowest free one.
        assert_eq!(ips.take(), Some(got[4]));
        ips.claim(got[0]);
        assert_eq!(ips.take(), None);
        assert!(Ips::new(Ipv4Addr::new(10, 0, 0, 0), 31).is_err());
    }
}
