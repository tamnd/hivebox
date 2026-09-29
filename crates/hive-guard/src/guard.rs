//! Loading the program, attaching it and writing its maps.

use std::collections::HashSet;
use std::fmt::Display;
use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use aya::maps::{Array, HashMap, Map, MapData, PerCpuArray, RingBuf};
use aya::programs::links::FdLink;
use aya::programs::{SchedClassifier, TcAttachType};
use aya::{Ebpf, EbpfLoader};
use rustix::time::{ClockId, clock_gettime};

use crate::maps::{Cell, DnsKey, RuleKey, net};
use crate::{CellNet, Deny, Profile, Reason, Rule, Stats};

static OBJECT: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/guard.o"));
const PROGRAM: &str = "guard_cell_egress";
/// Profiles are indexes into an array this long.
const PROFILES: u32 = 65536;

fn failed(what: impl Display, e: impl Display) -> io::Error {
    io::Error::other(format!("{what}: {e}"))
}

/// The egress program and its maps. One per node: the maps it opens are the ones already pinned
/// when there are some, so a second `Guard` sees what the first one wrote.
pub struct Guard {
    ebpf: Ebpf,
    dir: PathBuf,
    cells: HashMap<MapData, u32, Cell>,
    rules: HashMap<MapData, RuleKey, u32>,
    profiles: Array<MapData, u32>,
    dns: HashMap<MapData, DnsKey, u64>,
    stats: PerCpuArray<MapData, u64>,
    events: RingBuf<MapData>,
}

impl std::fmt::Debug for Guard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Guard").field("dir", &self.dir).finish_non_exhaustive()
    }
}

impl Guard {
    /// Loads the program, with its maps pinned in `dir`, usually [`crate::PIN_DIR`]. Maps
    /// already pinned there are reused, as they are, so cells and rules survive a restart.
    /// Links whose interface is gone are swept up.
    ///
    /// # Errors
    ///
    /// The crate was built without clang, `dir` is not on a bpffs, or the kernel refused the
    /// program or a map.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        if OBJECT.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "hive-guard was built without clang, so it has no eBPF program to load",
            ));
        }
        let dir = dir.as_ref().to_path_buf();
        std::fs::create_dir_all(dir.join("links"))?;
        let paths: Vec<(&str, PathBuf)> =
            ["cells", "rules", "profiles", "dns_allow", "stats", "events"]
                .map(|m| (m, dir.join(m)))
                .into();
        let mut loader = EbpfLoader::new();
        for (name, path) in &paths {
            loader.map_pin_path(name, path.as_path());
        }
        let mut ebpf = loader.load(OBJECT).map_err(|e| failed("loading the guard program", e))?;
        let prog: &mut SchedClassifier = ebpf
            .program_mut(PROGRAM)
            .ok_or_else(|| failed(PROGRAM, "not in the object"))?
            .try_into()
            .map_err(|e| failed(PROGRAM, e))?;
        prog.load().map_err(|e| failed("the verifier", e))?;
        let mut take = |name: &str| ebpf.take_map(name).ok_or_else(|| failed(name, "no such map"));
        let cells = HashMap::try_from(take("cells")?).map_err(|e| failed("cells", e))?;
        let rules = HashMap::try_from(take("rules")?).map_err(|e| failed("rules", e))?;
        let profiles = Array::try_from(take("profiles")?).map_err(|e| failed("profiles", e))?;
        let dns = HashMap::try_from(take("dns_allow")?).map_err(|e| failed("dns_allow", e))?;
        let stats = PerCpuArray::try_from(take("stats")?).map_err(|e| failed("stats", e))?;
        let events = RingBuf::try_from(take("events")?).map_err(|e| failed("events", e))?;
        let guard = Self { ebpf, dir, cells, rules, profiles, dns, stats, events };
        guard.sweep()?;
        Ok(guard)
    }

    /// Where the maps and links are pinned.
    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    fn program(&mut self) -> &mut SchedClassifier {
        self.ebpf.program_mut(PROGRAM).unwrap().try_into().unwrap()
    }

    /// Filters what `iface` receives, which for the host side of a cell's veth is what the cell
    /// sends. Until the interface has a cell everything from it is dropped. Attaching again
    /// replaces the old link with one to this process's program, and the new one is in place
    /// before the old one goes, so nothing gets through in between. Returns the interface's
    /// index.
    ///
    /// # Errors
    ///
    /// There is no such interface, or the kernel is older than 6.6 and has no tcx links.
    pub fn attach(&mut self, iface: &str) -> io::Result<u32> {
        let ifindex = ifindex(iface)?;
        let old = self.pins(ifindex)?;
        let n = old.iter().map(|(n, _)| n + 1).max().unwrap_or(0);
        let prog = self.program();
        let id = prog.attach(iface, TcAttachType::Ingress).map_err(|e| failed(iface, e))?;
        let link = prog.take_link(id).map_err(|e| failed(iface, e))?;
        let link = FdLink::try_from(link)
            .map_err(|e| failed(format!("{iface} needs Linux 6.6 or newer for tcx"), e))?;
        link.pin(self.dir.join("links").join(format!("{ifindex}-{n}")))
            .map_err(|e| failed(iface, e))?;
        for (_, path) in old {
            remove(&path)?;
        }
        Ok(ifindex)
    }

    /// Takes the program off an interface, after which it filters nothing.
    ///
    /// # Errors
    ///
    /// A pinned link could not be removed.
    pub fn detach(&mut self, ifindex: u32) -> io::Result<()> {
        for (_, path) in self.pins(ifindex)? {
            remove(&path)?;
        }
        Ok(())
    }

    /// Removes the links of interfaces that no longer exist, and returns how many there were.
    ///
    /// # Errors
    ///
    /// The links directory or `/sys/class/net` could not be read.
    pub fn sweep(&self) -> io::Result<usize> {
        let live: HashSet<u32> = std::fs::read_dir("/sys/class/net")?
            .filter_map(|e| {
                std::fs::read_to_string(e.ok()?.path().join("ifindex")).ok()?.trim().parse().ok()
            })
            .collect();
        let mut gone = 0;
        for (ifindex, _, path) in self.all_pins()? {
            if !live.contains(&ifindex) {
                remove(&path)?;
                gone += 1;
            }
        }
        Ok(gone)
    }

    fn all_pins(&self) -> io::Result<Vec<(u32, u32, PathBuf)>> {
        let mut pins = Vec::new();
        for entry in std::fs::read_dir(self.dir.join("links"))? {
            let entry = entry?;
            let name = entry.file_name();
            let parsed = name
                .to_str()
                .and_then(|n| n.split_once('-'))
                .and_then(|(i, n)| Some((i.parse().ok()?, n.parse().ok()?)));
            if let Some((ifindex, n)) = parsed {
                pins.push((ifindex, n, entry.path()));
            }
        }
        Ok(pins)
    }

    fn pins(&self, ifindex: u32) -> io::Result<Vec<(u32, PathBuf)>> {
        Ok(self
            .all_pins()?
            .into_iter()
            .filter(|p| p.0 == ifindex)
            .map(|(_, n, p)| (n, p))
            .collect())
    }

    /// Gives the interface a cell, or a new one in place of the old.
    ///
    /// # Errors
    ///
    /// The map is full or the kernel refused the write.
    pub fn set_cell(&mut self, ifindex: u32, cell: &CellNet) -> io::Result<()> {
        self.cells.insert(ifindex, Cell::from(cell), 0).map_err(|e| failed("cells", e))
    }

    /// The cell on an interface, if it has one.
    ///
    /// # Errors
    ///
    /// The kernel refused the read.
    pub fn cell(&self, ifindex: u32) -> io::Result<Option<CellNet>> {
        match self.cells.get(&ifindex, 0) {
            Ok(c) => Ok(Some(CellNet {
                idx: c.idx,
                ip: Ipv4Addr::from(c.ip4.to_ne_bytes()),
                mac: (c.mac != [0; 6]).then_some(c.mac),
                profile: Profile(c.profile),
            })),
            Err(aya::maps::MapError::KeyNotFound) => Ok(None),
            Err(e) => Err(failed("cells", e)),
        }
    }

    /// Takes the cell off an interface, so everything from it is dropped again. What the DNS
    /// proxy allowed the cell stays in the map until it expires or is pushed out, which is
    /// harmless because no later cell has the same `idx`.
    ///
    /// # Errors
    ///
    /// The kernel refused the write.
    pub fn remove_cell(&mut self, ifindex: u32) -> io::Result<()> {
        match self.cells.remove(&ifindex) {
            Ok(()) | Err(aya::maps::MapError::KeyNotFound) => Ok(()),
            // aya reports a delete of a missing key as the syscall's ENOENT.
            Err(aya::maps::MapError::SyscallError(e))
                if e.io_error.kind() == io::ErrorKind::NotFound =>
            {
                Ok(())
            }
            Err(e) => Err(failed("cells", e)),
        }
    }

    /// Makes `rules` all that `profile` allows, besides what the DNS proxy resolves. Cells with
    /// the profile follow the new rules from their next packet, and while the change is being
    /// written a packet sees either the old rule or the new one for each destination.
    ///
    /// # Errors
    ///
    /// The profile is 65536 or more, a rule is for another profile, the map is full, or the
    /// kernel refused a write.
    pub fn set_profile(&mut self, profile: Profile, rules: &[Rule]) -> io::Result<()> {
        let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidInput, msg);
        if profile.0 >= PROFILES {
            return Err(invalid(format!("profile {} is not below {PROFILES}", profile.0)));
        }
        if let Some(r) = rules.iter().find(|r| r.profile != profile) {
            return Err(invalid(format!("{r:?} is not for {profile:?}")));
        }
        let new: HashSet<RuleKey> = rules.iter().map(RuleKey::from).collect();
        let old: Vec<RuleKey> = self
            .rules
            .keys()
            .filter_map(Result::ok)
            .filter(|k| k.profile == profile.0 && !new.contains(k))
            .collect();
        // The program skips kinds of rule the profile has no bit for, so a bit is set before a
        // rule of its kind goes in and cleared only once the last one is out.
        let shapes =
            |keys: &mut dyn Iterator<Item = &RuleKey>| keys.fold(0, |acc, k| acc | k.shape());
        let now = shapes(&mut new.iter());
        let before = self.profiles.get(&profile.0, 0).map_err(|e| failed("profiles", e))?;
        self.profiles.set(profile.0, before | now, 0).map_err(|e| failed("profiles", e))?;
        for k in &new {
            self.rules.insert(k, 1, 0).map_err(|e| failed("rules", e))?;
        }
        for k in &old {
            match self.rules.remove(k) {
                Ok(()) | Err(aya::maps::MapError::KeyNotFound) => {}
                Err(e) => return Err(failed("rules", e)),
            }
        }
        self.profiles.set(profile.0, now, 0).map_err(|e| failed("profiles", e))
    }

    /// Lets the cell with this `idx` reach `ip` for `ttl`, on any port. The DNS proxy calls it
    /// with each address it is about to answer with.
    ///
    /// # Errors
    ///
    /// The kernel refused the write.
    pub fn allow(&mut self, cell: u32, ip: Ipv4Addr, ttl: Duration) -> io::Result<()> {
        let until = boottime().saturating_add(u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX));
        self.dns.insert(DnsKey { cell, ip: net(ip) }, until, 0).map_err(|e| failed("dns_allow", e))
    }

    /// A handle of its own on the map [`Guard::allow`] writes, for the DNS proxy, so answers do
    /// not wait on whoever holds the `Guard` to wire an interface.
    ///
    /// # Errors
    ///
    /// The pinned map could not be opened.
    pub fn dns_allow(&self) -> io::Result<DnsAllow> {
        let data =
            MapData::from_pin(self.dir.join("dns_allow")).map_err(|e| failed("dns_allow", e))?;
        let map = HashMap::try_from(Map::from_map_data(data).map_err(|e| failed("dns_allow", e))?)
            .map_err(|e| failed("dns_allow", e))?;
        Ok(DnsAllow(map))
    }

    /// Packets counted so far, summed over the CPUs.
    ///
    /// # Errors
    ///
    /// The kernel refused a read.
    pub fn stats(&self) -> io::Result<Stats> {
        let mut stats = Stats::default();
        for r in Reason::ALL {
            let values = self.stats.get(&(r as u32), 0).map_err(|e| failed("stats", e))?;
            stats.0[r as usize] = values.iter().sum();
        }
        Ok(stats)
    }

    /// The drops reported since the last call, oldest first.
    pub fn denies(&mut self) -> Vec<Deny> {
        let mut out = Vec::new();
        while let Some(item) = self.events.next() {
            out.extend(Deny::parse(&item));
        }
        out
    }
}

/// The DNS proxy's handle on the guard's allow list, from [`Guard::dns_allow`].
pub struct DnsAllow(HashMap<MapData, DnsKey, u64>);

impl std::fmt::Debug for DnsAllow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DnsAllow").finish_non_exhaustive()
    }
}

impl DnsAllow {
    /// Does what [`Guard::allow`] does.
    ///
    /// # Errors
    ///
    /// The kernel refused the write.
    pub fn allow(&mut self, cell: u32, ip: Ipv4Addr, ttl: Duration) -> io::Result<()> {
        let until = boottime().saturating_add(u64::try_from(ttl.as_nanos()).unwrap_or(u64::MAX));
        self.0.insert(DnsKey { cell, ip: net(ip) }, until, 0).map_err(|e| failed("dns_allow", e))
    }
}

fn boottime() -> u64 {
    let t = clock_gettime(ClockId::Boottime);
    u64::try_from(t.tv_sec).unwrap_or(0) * 1_000_000_000 + u64::try_from(t.tv_nsec).unwrap_or(0)
}

pub(crate) fn ifindex(iface: &str) -> io::Result<u32> {
    if iface.is_empty() || iface.contains('/') || iface.starts_with('.') {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{iface:?} is not an interface name"),
        ));
    }
    let raw = std::fs::read_to_string(Path::new("/sys/class/net").join(iface).join("ifindex"))
        .map_err(|e| io::Error::new(e.kind(), format!("interface {iface}: {e}")))?;
    raw.trim().parse().map_err(|e| failed(iface, e))
}

fn remove(path: &Path) -> io::Result<()> {
    match std::fs::remove_file(path) {
        Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
        _ => Ok(()),
    }
}
