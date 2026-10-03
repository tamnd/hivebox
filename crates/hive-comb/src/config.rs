use hive_types::Backend;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;

/// How one comb runs. The defaults are the ones in `spec/08_node_agent.md`, section 11.
#[derive(Clone, Debug)]
pub struct Config {
    /// Where the WAL and every cell's own directory live.
    pub data_dir: PathBuf,
    /// The unit this node belongs to, which goes into every cell id.
    pub unit: u8,
    /// This node's number in its unit.
    pub node: u16,
    /// The epoch this node runs in. Fixed in standalone mode, and handed out by the keeper once
    /// there is one.
    pub epoch: u16,
    /// Memory the cells may commit before overcommit, in MiB. `None` means the machine's total
    /// less [`Config::reserved_mem_mib`].
    pub mem_mib: Option<u64>,
    /// Memory kept back for the host, the comb and the page cache, in MiB.
    pub reserved_mem_mib: u64,
    /// Most cells alive at once, whatever their size.
    pub max_cells: usize,
    /// Most creates in flight per backend. More wait their turn.
    pub create_limit: BTreeMap<Backend, usize>,
    /// Longest a create may wait for its turn, and then longest it may take from its turn to the
    /// guest agent's handshake. A create that waits too long fails with `CAPACITY_UNAVAILABLE`.
    pub create_deadline: Duration,
    /// How long a stop waits for the workload to end on its own before killing it.
    pub stop_grace: Duration,
    /// How long a stopped or failed cell stays visible before its record is dropped.
    pub keep_ended: Duration,
    /// How long a cell stays frozen with its memory in place before the node reclaims it, swapping
    /// the cell's memory out so a resume pages it back in.
    pub reclaim_after: Duration,
    /// How long a cell may stay paused before it is stopped.
    pub pause_ttl: Duration,
    /// Longest an exec or file call on a paused cell waits for the resume it causes.
    pub resume_timeout: Duration,
    /// The cgroup the comb puts its cells under, which it makes if it has to. `None` runs cells
    /// with no cgroup of their own, with no limits and no kill on stop, which only suits tests.
    pub cgroup_root: Option<PathBuf>,
    /// Empty cgroups kept ready per QoS class, so a create does not wait on `mkdir`.
    pub cgroup_depth: usize,
    /// Where the comb keeps its cells' network namespaces. `None` runs cells in the host's network,
    /// which only suits tests.
    pub netns_dir: Option<PathBuf>,
    /// Network namespaces kept ready, so a create does not wait on making one.
    pub netns_depth: usize,
    /// Where the local API listens.
    pub api_socket: PathBuf,
    /// Where gates reach the API over TCP, if anywhere. Whoever can connect is trusted the way
    /// whoever can open the socket is, so this belongs on the network only gates are on.
    pub listen: Option<SocketAddr>,
    /// Where `/metrics` is served, if anywhere.
    pub metrics: Option<SocketAddr>,
    /// The container backend.
    pub container: ContainerBackend,
    /// Where images come from.
    pub images: Images,
    /// How cells reach the network.
    pub network: Network,
    /// The scout this comb reports to. `None` is standalone, reporting to no one.
    pub scout: Option<ScoutLink>,
    /// The keeper group this comb registers with and holds a lease from. With one, the node
    /// index and the epoch come from the keeper and not from `node.node` and `node.epoch`.
    pub keeper: Option<KeeperLink>,
}

/// Where the comb sends its reports, and how it says it can be reached.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScoutLink {
    /// The scout's address, like `http://10.0.0.5:7410`.
    pub endpoint: String,
    /// Where the gate reaches this comb, which goes in every report. By default `node.listen`,
    /// or without one the API socket as `unix:PATH`, which only a gate on the same machine can
    /// use. A `node.listen` on every address needs this said.
    pub advertise: String,
}

/// The keeper group a comb registers with, and what it registers as.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct KeeperLink {
    /// The members of the group, as `host:port`. Any of them takes the calls.
    pub members: Vec<String>,
    /// The name the node keeps across restarts, the host name unless `keeper.name` says.
    pub name: String,
    /// Where gates reach this comb, worked out the way `scout.advertise` is.
    pub advertise: String,
}

/// How cells in their own network namespaces reach anything. Needs [`Config::netns_dir`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Network {
    /// Off gives each cell loopback and nothing else. On, the comb also runs cells with loopback
    /// only if `hive-guard` cannot load, and says so.
    pub guard: bool,
    /// The range this node's cells get their addresses from, as a base and a prefix length.
    pub cells: (Ipv4Addr, u8),
    /// Where the guard pins its maps and links, under a bpf filesystem.
    pub pin_dir: PathBuf,
    /// The resolvers the DNS proxy asks. Empty means the ones in the host's `/etc/resolv.conf`.
    pub upstream: Vec<SocketAddr>,
    /// Profiles of the node's own, by name, each with the names its cells may look up, like
    /// `pypi.org` or `*.pythonhosted.org`. A cell reaches the proxy and whatever it resolved for
    /// it, and nothing else.
    pub profiles: BTreeMap<String, Vec<String>>,
}

impl Default for Network {
    fn default() -> Self {
        Self {
            guard: true,
            cells: (Ipv4Addr::new(100, 64, 0, 0), 20),
            pin_dir: PathBuf::from("/sys/fs/bpf/hive/guard-v1"),
            upstream: Vec::new(),
            profiles: BTreeMap::new(),
        }
    }
}

/// Where the comb gets images made by `hive-nectar`. An image name is looked up in
/// `data_dir/images`: a directory there is an unpacked root filesystem as `hive-oci import` makes
/// one, and a file holds the id of an image in the store.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Images {
    /// The blob store images are in: a directory, or a bucket as in
    /// `http://10.0.0.5:9000/bucket/prefix` with keys from `AWS_ACCESS_KEY_ID` and
    /// `AWS_SECRET_ACCESS_KEY`. `None` leaves only unpacked images.
    pub store: Option<PathBuf>,
    /// The node's cache of blobs from the store, `data_dir/cache` unless set.
    pub cache_dir: PathBuf,
    /// Most bytes the cache keeps.
    pub cache_bytes: u64,
    /// Where layers are mounted, once each for every cell that uses them.
    pub layers_dir: PathBuf,
    /// Whether layers are mounted before their data is in, filled as they are read. It needs the
    /// `nbd` module.
    pub lazy: bool,
}

impl Default for Images {
    fn default() -> Self {
        Self {
            store: None,
            cache_dir: PathBuf::from("/var/lib/hivebox/cache"),
            cache_bytes: 64 << 30,
            layers_dir: PathBuf::from("/run/hivebox/layers"),
            lazy: false,
        }
    }
}

/// How the comb runs container cells.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ContainerBackend {
    /// Off leaves the backend out even where the node could run it.
    pub enabled: bool,
    /// The static drone binary put in every container.
    pub drone: PathBuf,
    /// Where each container cell gets its bundle, its state and its log.
    pub state_dir: PathBuf,
    /// Processes kept to make containers, which is how many creates go on at once.
    pub workers: usize,
    /// The first host id root in a cell maps to. Images have to be imported with the same base.
    pub uid_base: u32,
    /// How many ids each cell has.
    pub uid_count: u32,
}

impl Default for ContainerBackend {
    fn default() -> Self {
        Self {
            enabled: true,
            drone: PathBuf::from("/usr/lib/hivebox/hive-drone"),
            state_dir: PathBuf::from("/run/hivebox/oci"),
            workers: 8,
            uid_base: 1_000_000,
            uid_count: 65536,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("/var/lib/hivebox"),
            unit: 1,
            node: 1,
            epoch: 1,
            mem_mib: None,
            reserved_mem_mib: 4096,
            max_cells: 3200,
            create_limit: BTreeMap::from([
                (Backend::Fncall, 256),
                (Backend::Container, 128),
                (Backend::Microvm, 64),
                (Backend::Fullvm, 8),
            ]),
            create_deadline: Duration::from_secs(30),
            stop_grace: Duration::from_secs(10),
            keep_ended: Duration::from_secs(600),
            reclaim_after: Duration::from_secs(600),
            pause_ttl: Duration::from_secs(24 * 3600),
            resume_timeout: Duration::from_secs(5),
            cgroup_root: Some(PathBuf::from("/sys/fs/cgroup/hive.slice")),
            cgroup_depth: 256,
            netns_dir: Some(PathBuf::from("/run/hivebox/netns")),
            netns_depth: 400,
            api_socket: PathBuf::from("/run/hivebox/comb.sock"),
            listen: None,
            metrics: None,
            container: ContainerBackend::default(),
            images: Images::default(),
            network: Network::default(),
            scout: None,
            keeper: None,
        }
    }
}

impl Config {
    /// Reads a config file. Anything the file leaves out keeps its default, and a key the comb
    /// does not know is an error, so a typo never passes for a setting.
    ///
    /// ```toml
    /// [node]
    /// data_dir = "/var/lib/hivebox"
    /// unit = 1
    /// node = 7
    /// socket = "/run/hivebox/comb.sock"
    /// listen = "10.0.0.7:7400"
    /// metrics = "127.0.0.1:9464"
    /// reserved_mem_mib = 4096
    ///
    /// [pools]
    /// cgroup_root = "/sys/fs/cgroup/hive.slice"
    /// netns_depth = 400
    ///
    /// [lifecycle]
    /// create_deadline = "30s"
    /// keep_ended = "10m"
    /// reclaim_after = "10m"
    /// pause_ttl = "24h"
    /// resume_timeout = "5s"
    ///
    /// [backends.create_limit]
    /// container = 128
    ///
    /// [backends.container]
    /// drone = "/usr/lib/hivebox/hive-drone"
    /// workers = 8
    ///
    /// [network]
    /// cells = "100.64.16.0/20"
    /// upstream = ["1.1.1.1", "8.8.8.8:53"]
    ///
    /// [network.profiles.pypi]
    /// domains = ["pypi.org", "*.pythonhosted.org"]
    ///
    /// [images]
    /// store = "/srv/hivebox/store"
    /// cache_dir = "/var/lib/hivebox/cache"
    /// cache_bytes = 68719476736
    /// lazy = false
    ///
    /// [scout]
    /// endpoint = "http://10.0.0.5:7410"
    /// advertise = "http://10.0.0.7:7400"
    ///
    /// [keeper]
    /// members = ["10.0.0.1:7430", "10.0.0.2:7430", "10.0.0.3:7430"]
    /// name = "node-7"
    /// ```
    ///
    /// An empty `cgroup_root` or `netns_dir` turns that pool off.
    ///
    /// # Errors
    ///
    /// The file is not TOML, or holds a key or a value the comb does not take.
    pub fn from_toml(text: &str) -> Result<Self, String> {
        let file: File = toml::from_str(text).map_err(|e| e.to_string())?;
        let mut c = Self::default();
        let n = file.node;
        set(&mut c.data_dir, n.data_dir);
        set(&mut c.unit, n.unit);
        set(&mut c.node, n.node);
        set(&mut c.epoch, n.epoch);
        set(&mut c.api_socket, n.socket);
        if let Some(text) = n.listen {
            c.listen = Some(text.parse().map_err(|_| {
                format!("node.listen = {text:?} is not an address like 10.0.0.7:7400")
            })?);
        }
        if let Some(text) = n.metrics {
            c.metrics = Some(text.parse().map_err(|_| {
                format!("node.metrics = {text:?} is not an address like 127.0.0.1:9464")
            })?);
        }
        if n.mem_mib.is_some() {
            c.mem_mib = n.mem_mib;
        }
        set(&mut c.reserved_mem_mib, n.reserved_mem_mib);
        set(&mut c.max_cells, n.max_cells);
        let p = file.pools;
        if let Some(root) = p.cgroup_root {
            c.cgroup_root = (!root.as_os_str().is_empty()).then_some(root);
        }
        if let Some(dir) = p.netns_dir {
            c.netns_dir = (!dir.as_os_str().is_empty()).then_some(dir);
        }
        set(&mut c.cgroup_depth, p.cgroup_depth);
        set(&mut c.netns_depth, p.netns_depth);
        let l = file.lifecycle;
        for (field, value, name) in [
            (&mut c.create_deadline, l.create_deadline, "create_deadline"),
            (&mut c.stop_grace, l.stop_grace, "stop_grace"),
            (&mut c.keep_ended, l.keep_ended, "keep_ended"),
            (&mut c.reclaim_after, l.reclaim_after, "reclaim_after"),
            (&mut c.pause_ttl, l.pause_ttl, "pause_ttl"),
            (&mut c.resume_timeout, l.resume_timeout, "resume_timeout"),
        ] {
            if let Some(v) = value {
                *field = duration(&v).ok_or_else(|| {
                    format!("lifecycle.{name} = {v:?} is not a duration like 500ms, 30s or 10m")
                })?;
            }
        }
        for (name, limit) in file.backends.create_limit {
            let backend = Backend::ALL
                .into_iter()
                .find(|b| b.as_str() == name && *b != Backend::Auto)
                .ok_or_else(|| format!("backends.create_limit has no backend named {name:?}"))?;
            c.create_limit.insert(backend, limit);
        }
        let k = file.backends.container;
        let b = &mut c.container;
        set(&mut b.enabled, k.enabled);
        set(&mut b.drone, k.drone);
        set(&mut b.state_dir, k.state_dir);
        set(&mut b.workers, k.workers);
        set(&mut b.uid_base, k.uid_base);
        set(&mut b.uid_count, k.uid_count);
        if b.workers == 0 {
            return Err("backends.container.workers must not be 0".into());
        }
        if b.uid_count == 0 || b.uid_base.checked_add(b.uid_count).is_none() {
            return Err("backends.container ids run past the last uid".into());
        }
        let i = file.images;
        if let Some(store) = i.store {
            c.images.store = (!store.as_os_str().is_empty()).then_some(store);
        }
        c.images.cache_dir = i.cache_dir.unwrap_or_else(|| c.data_dir.join("cache"));
        set(&mut c.images.cache_bytes, i.cache_bytes);
        set(&mut c.images.layers_dir, i.layers_dir);
        set(&mut c.images.lazy, i.lazy);
        let w = file.network;
        set(&mut c.network.guard, w.guard);
        set(&mut c.network.pin_dir, w.pin_dir);
        if let Some(text) = w.cells {
            c.network.cells =
                cidr(&text).filter(|(_, len)| (8..=30).contains(len)).ok_or_else(|| {
                    format!("network.cells = {text:?} is not a range like 100.64.0.0/20")
                })?;
        }
        for text in w.upstream {
            let addr = text.parse().or_else(|_| text.parse().map(|ip| SocketAddr::new(ip, 53)));
            c.network.upstream.push(addr.map_err(|_| {
                format!("network.upstream has {text:?}, which is not an address like 1.1.1.1")
            })?);
        }
        for (name, p) in w.profiles {
            if hive_guard::Profile::builtin(&name).is_some() {
                return Err(format!("network.profiles.{name} is built in and cannot be changed"));
            }
            hive_guard::dns::Policy::new(&p.domains)
                .map_err(|e| format!("network.profiles.{name}.domains: {e}"))?;
            c.network.profiles.insert(name, p.domains);
        }
        let s = file.scout;
        let advertise = match (s.advertise.clone(), c.listen) {
            (Some(a), _) => Ok(a),
            (None, Some(l)) if l.ip().is_unspecified() => Err(format!(
                "node.listen = \"{l}\" is every address, so scout.advertise has to say which one gates use"
            )),
            (None, Some(l)) => Ok(format!("http://{l}")),
            (None, None) => Ok(format!("unix:{}", c.api_socket.display())),
        };
        if let Some(endpoint) = s.endpoint.filter(|e| !e.is_empty()) {
            if !endpoint.starts_with("http://") && !endpoint.starts_with("https://") {
                return Err(format!(
                    "scout.endpoint = {endpoint:?} is not a URL like http://10.0.0.5:7410"
                ));
            }
            c.scout = Some(ScoutLink { endpoint, advertise: advertise.clone()? });
        } else if s.advertise.is_some() {
            return Err("scout.advertise needs scout.endpoint".into());
        }
        let k = file.keeper;
        if !k.members.is_empty() {
            for m in &k.members {
                if m.contains("://") || !m.contains(':') {
                    return Err(format!("keeper.members has {m:?}, which is not host:port"));
                }
            }
            let name = match k.name {
                Some(n) => n,
                None => std::fs::read_to_string("/proc/sys/kernel/hostname")
                    .map(|h| h.trim().to_owned())
                    .map_err(|e| format!("keeper.name is not set and the host name: {e}"))?,
            };
            if !hive_types::is_name(&name) {
                return Err(format!("keeper.name = {name:?} is not a name like node-7"));
            }
            c.keeper = Some(KeeperLink { members: k.members, name, advertise: advertise? });
        } else if k.name.is_some() {
            return Err("keeper.name needs keeper.members".into());
        }
        if c.node == 0 {
            return Err("node.node must not be 0".into());
        }
        Ok(c)
    }
}

fn set<T>(field: &mut T, value: Option<T>) {
    if let Some(v) = value {
        *field = v;
    }
}

/// A duration such as `250ms`, `30s`, `10m` or `2h`.
fn duration(s: &str) -> Option<Duration> {
    let split = s.find(|c: char| !c.is_ascii_digit())?;
    let (n, unit) = s.split_at(split);
    let n: u64 = n.parse().ok()?;
    let secs = |k: u64| n.checked_mul(k).map(Duration::from_secs);
    match unit {
        "ms" => Some(Duration::from_millis(n)),
        "s" => secs(1),
        "m" => secs(60),
        "h" => secs(3600),
        _ => None,
    }
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct File {
    node: NodeFile,
    pools: PoolsFile,
    lifecycle: LifecycleFile,
    backends: BackendsFile,
    images: ImagesFile,
    network: NetworkFile,
    scout: ScoutFile,
    keeper: KeeperFile,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct KeeperFile {
    members: Vec<String>,
    name: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ScoutFile {
    endpoint: Option<String>,
    advertise: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NetworkFile {
    guard: Option<bool>,
    cells: Option<String>,
    pin_dir: Option<PathBuf>,
    upstream: Vec<String>,
    profiles: BTreeMap<String, ProfileFile>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProfileFile {
    domains: Vec<String>,
}

fn cidr(text: &str) -> Option<(Ipv4Addr, u8)> {
    let (ip, len) = text.split_once('/')?;
    let len: u8 = len.parse().ok()?;
    (len <= 32).then_some((ip.parse().ok()?, len))
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ImagesFile {
    store: Option<PathBuf>,
    cache_dir: Option<PathBuf>,
    cache_bytes: Option<u64>,
    layers_dir: Option<PathBuf>,
    lazy: Option<bool>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct NodeFile {
    data_dir: Option<PathBuf>,
    unit: Option<u8>,
    node: Option<u16>,
    epoch: Option<u16>,
    socket: Option<PathBuf>,
    listen: Option<String>,
    metrics: Option<String>,
    mem_mib: Option<u64>,
    reserved_mem_mib: Option<u64>,
    max_cells: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct PoolsFile {
    cgroup_root: Option<PathBuf>,
    cgroup_depth: Option<usize>,
    netns_dir: Option<PathBuf>,
    netns_depth: Option<usize>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LifecycleFile {
    create_deadline: Option<String>,
    stop_grace: Option<String>,
    keep_ended: Option<String>,
    reclaim_after: Option<String>,
    pause_ttl: Option<String>,
    resume_timeout: Option<String>,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct BackendsFile {
    create_limit: BTreeMap<String, usize>,
    container: ContainerFile,
}

#[derive(Default, Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ContainerFile {
    enabled: Option<bool>,
    drone: Option<PathBuf>,
    state_dir: Option<PathBuf>,
    workers: Option<usize>,
    uid_base: Option<u32>,
    uid_count: Option<u32>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_empty_file_is_the_defaults() {
        let c = Config::from_toml("").unwrap();
        assert_eq!(c.data_dir, Config::default().data_dir);
        assert_eq!(c.netns_depth, 400);
    }

    #[test]
    fn a_file_sets_what_it_names() {
        let c = Config::from_toml(
            r#"
            [node]
            data_dir = "/tmp/hb"
            node = 7
            socket = "/tmp/hb/comb.sock"
            metrics = "127.0.0.1:9464"
            mem_mib = 8192

            [pools]
            cgroup_root = ""
            netns_depth = 16

            [lifecycle]
            create_deadline = "5s"
            stop_grace = "250ms"
            keep_ended = "10m"
            reclaim_after = "30s"

            [backends.create_limit]
            container = 8

            [backends.container]
            drone = "/opt/hive-drone"
            workers = 2

            [images]
            store = "/srv/store"
            cache_bytes = 1048576

            [network]
            cells = "100.64.16.0/20"
            upstream = ["1.1.1.1", "[2606:4700::1111]:53"]

            [network.profiles.pypi]
            domains = ["pypi.org", "*.pythonhosted.org"]
            "#,
        )
        .unwrap();
        assert_eq!(c.data_dir, PathBuf::from("/tmp/hb"));
        assert_eq!(c.node, 7);
        assert_eq!(c.api_socket, PathBuf::from("/tmp/hb/comb.sock"));
        assert_eq!(c.mem_mib, Some(8192));
        assert_eq!(c.metrics, Some("127.0.0.1:9464".parse().unwrap()));
        assert_eq!(c.cgroup_root, None);
        assert_eq!(c.netns_dir, Config::default().netns_dir);
        assert_eq!(c.netns_depth, 16);
        assert_eq!(c.create_deadline, Duration::from_secs(5));
        assert_eq!(c.stop_grace, Duration::from_millis(250));
        assert_eq!(c.keep_ended, Duration::from_secs(600));
        assert_eq!(c.reclaim_after, Duration::from_secs(30));
        assert_eq!(c.pause_ttl, Duration::from_secs(24 * 3600));
        assert_eq!(c.create_limit[&Backend::Container], 8);
        assert_eq!(c.create_limit[&Backend::Microvm], 64);
        assert_eq!(c.container.drone, PathBuf::from("/opt/hive-drone"));
        assert_eq!(c.container.workers, 2);
        assert_eq!(c.container.uid_base, 1_000_000);
        assert_eq!(c.images.store, Some(PathBuf::from("/srv/store")));
        assert_eq!(c.images.cache_bytes, 1 << 20);
        assert_eq!(c.images.layers_dir, Images::default().layers_dir);
        assert_eq!(c.network.cells, (Ipv4Addr::new(100, 64, 16, 0), 20));
        assert!(c.network.guard);
        assert_eq!(
            c.network.upstream,
            ["1.1.1.1:53".parse().unwrap(), "[2606:4700::1111]:53".parse().unwrap()]
        );
        assert_eq!(c.network.profiles["pypi"], ["pypi.org", "*.pythonhosted.org"]);
        assert_eq!(c.scout, None);
    }

    #[test]
    fn a_scout_endpoint_turns_reports_on() {
        let c = Config::from_toml(
            "[node]\nsocket = \"/tmp/c.sock\"\n[scout]\nendpoint = \"http://10.0.0.5:7410\"",
        )
        .unwrap();
        let s = c.scout.unwrap();
        assert_eq!(s.endpoint, "http://10.0.0.5:7410");
        assert_eq!(s.advertise, "unix:/tmp/c.sock");
        let e = Config::from_toml("[scout]\nendpoint = \"10.0.0.5:7410\"").unwrap_err();
        assert!(e.contains("not a URL"), "{e}");
        let e = Config::from_toml("[scout]\nadvertise = \"http://a:1\"").unwrap_err();
        assert!(e.contains("needs scout.endpoint"), "{e}");
    }

    #[test]
    fn a_keeper_section_registers_the_node() {
        let keeper =
            "[keeper]\nmembers = [\"10.0.0.1:7430\", \"10.0.0.2:7430\"]\nname = \"node-7\"";
        let c =
            Config::from_toml(&format!("[node]\nlisten = \"10.0.0.7:7400\"\n{keeper}")).unwrap();
        let k = c.keeper.unwrap();
        assert_eq!(k.members, ["10.0.0.1:7430", "10.0.0.2:7430"]);
        assert_eq!(k.name, "node-7");
        assert_eq!(k.advertise, "http://10.0.0.7:7400");
        for (text, says) in [
            ("[keeper]\nmembers = [\"http://10.0.0.1:7430\"]", "not host:port"),
            ("[keeper]\nmembers = [\"a:1\"]\nname = \"two words\"", "not a name"),
            ("[keeper]\nname = \"node-7\"", "needs keeper.members"),
            ("[node]\nlisten = \"0.0.0.0:7400\"\n[keeper]\nmembers = [\"a:1\"]", "scout.advertise"),
        ] {
            let e = Config::from_toml(text).unwrap_err();
            assert!(e.contains(says), "{text}: {e}");
        }
    }

    #[test]
    fn a_listen_address_is_what_the_comb_advertises() {
        let scout = "[scout]\nendpoint = \"http://10.0.0.5:7410\"";
        let c = Config::from_toml(&format!("[node]\nlisten = \"10.0.0.7:7400\"\n{scout}")).unwrap();
        assert_eq!(c.listen, Some("10.0.0.7:7400".parse().unwrap()));
        assert_eq!(c.scout.unwrap().advertise, "http://10.0.0.7:7400");
        // A gate cannot dial every address.
        let e =
            Config::from_toml(&format!("[node]\nlisten = \"0.0.0.0:7400\"\n{scout}")).unwrap_err();
        assert!(e.contains("scout.advertise has to say"), "{e}");
        let c = Config::from_toml(&format!(
            "[node]\nlisten = \"0.0.0.0:7400\"\n{scout}\nadvertise = \"http://10.0.0.7:7400\""
        ))
        .unwrap();
        assert_eq!(c.scout.unwrap().advertise, "http://10.0.0.7:7400");
        let e = Config::from_toml("[node]\nlisten = \"7400\"").unwrap_err();
        assert!(e.contains("not an address"), "{e}");
    }

    #[test]
    fn mistakes_are_errors() {
        assert!(Config::from_toml("[node]\ndata_dri = \"/x\"").unwrap_err().contains("data_dri"));
        assert!(Config::from_toml("[lifecycle]\nstop_grace = \"10\"").is_err());
        assert!(Config::from_toml("[lifecycle]\nstop_grace = \"1d\"").is_err());
        assert!(Config::from_toml("[backends.create_limit]\nauto = 1").is_err());
        assert!(Config::from_toml("[node]\nnode = 0").is_err());
        assert!(Config::from_toml("[node]\nmetrics = \"localhost\"").is_err());
        assert!(Config::from_toml("[network]\ncells = \"100.64.0.0\"").is_err());
        assert!(Config::from_toml("[network]\ncells = \"100.64.0.0/31\"").is_err());
        assert!(Config::from_toml("[network]\nupstream = [\"dns.google\"]").is_err());
        assert!(Config::from_toml("[network.profiles.none]\ndomains = [\"a.com\"]").is_err());
        assert!(Config::from_toml("[network.profiles.x]\ndomains = [\"a.*.com\"]").is_err());
        assert!(Config::from_toml("[network.profiles.x]\ndomain = [\"a.com\"]").is_err());
        assert!(Config::from_toml("[backends.container]\nworkers = 0").is_err());
        assert!(Config::from_toml("[backends.container]\nuid_base = 4294967295").is_err());
    }
}
