//! Turning the driver types into `hivebox.plugin.v1` messages and back. Paths travel as strings,
//! so a path that is not UTF-8 is refused rather than changed on the way.

use hive_cell::{
    CellHandle, DriverCaps, ExitInfo, GuestChannel, Liveness, NodeFit, PauseMode, RootfsPlan, Slot,
    SnapshotCaps,
};
use hive_proto::convert;
use hive_proto::plugin::v1 as wire;
use hive_proto::v1;
use hive_types::{CellId, Error, Reason, Resources};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub(crate) type Result<T> = std::result::Result<T, Error>;

fn bad(what: impl Into<String>) -> Error {
    Error::new(Reason::InvalidArgument, what.into())
}

pub(crate) fn path(p: &Path) -> Result<String> {
    p.to_str().map(str::to_string).ok_or_else(|| bad(format!("{} is not UTF-8", p.display())))
}

fn opt_path(p: Option<&PathBuf>) -> Result<String> {
    p.map_or_else(|| Ok(String::new()), |p| path(p))
}

fn from_opt(s: String) -> Option<PathBuf> {
    (!s.is_empty()).then(|| PathBuf::from(s))
}

pub(crate) fn cell_id(s: &str) -> Result<CellId> {
    s.parse().map_err(|e| bad(format!("cell id {s:?}: {e}")))
}

pub(crate) fn caps_to(c: DriverCaps) -> wire::Caps {
    let snapshot = match c.snapshot {
        SnapshotCaps::None => wire::caps::Snapshot::None,
        SnapshotCaps::Disk => wire::caps::Snapshot::Disk,
        SnapshotCaps::DiskMem => wire::caps::Snapshot::DiskMem,
    };
    wire::Caps {
        pause: c.pause,
        snapshot: snapshot.into(),
        fork: c.fork,
        resize: c.resize,
        gpu: c.gpu,
        trim: c.trim,
    }
}

pub(crate) fn caps_from(c: &wire::Caps) -> DriverCaps {
    let snapshot = match c.snapshot() {
        wire::caps::Snapshot::None => SnapshotCaps::None,
        wire::caps::Snapshot::Disk => SnapshotCaps::Disk,
        wire::caps::Snapshot::DiskMem => SnapshotCaps::DiskMem,
    };
    DriverCaps {
        pause: c.pause,
        snapshot,
        fork: c.fork,
        resize: c.resize,
        gpu: c.gpu,
        trim: c.trim,
    }
}

pub(crate) fn fit_to(f: NodeFit) -> wire::NodeFit {
    wire::NodeFit { ready: f.ready, notes: f.notes }
}

pub(crate) fn fit_from(f: wire::NodeFit) -> NodeFit {
    NodeFit { ready: f.ready, notes: f.notes }
}

pub(crate) fn slot_to(s: &Slot) -> Result<wire::Slot> {
    Ok(wire::Slot {
        cgroup: path(&s.cgroup)?,
        netns: opt_path(s.netns.as_ref())?,
        nameserver: s.nameserver.map(|a| a.to_string()).unwrap_or_default(),
        dir: path(&s.dir)?,
        secret: s.secret.to_vec(),
    })
}

pub(crate) fn slot_from(s: wire::Slot) -> Result<Slot> {
    let nameserver = match s.nameserver.as_str() {
        "" => None,
        a => Some(a.parse().map_err(|_| bad(format!("nameserver {a:?} is not an IPv4 address")))?),
    };
    let secret = s
        .secret
        .as_slice()
        .try_into()
        .map_err(|_| bad(format!("the secret has {} bytes, not 32", s.secret.len())))?;
    Ok(Slot {
        cgroup: s.cgroup.into(),
        netns: from_opt(s.netns),
        nameserver,
        dir: s.dir.into(),
        secret,
    })
}

pub(crate) fn rootfs_to(r: &RootfsPlan) -> Result<wire::Rootfs> {
    Ok(wire::Rootfs {
        lowers: r.lowers.iter().map(|p| path(p)).collect::<Result<_>>()?,
        upper: path(&r.upper)?,
        seed: opt_path(r.seed.as_ref())?,
    })
}

pub(crate) fn rootfs_from(r: wire::Rootfs) -> RootfsPlan {
    RootfsPlan {
        lowers: r.lowers.into_iter().map(PathBuf::from).collect(),
        upper: r.upper.into(),
        seed: from_opt(r.seed),
    }
}

pub(crate) fn handle_to(h: &CellHandle) -> Result<wire::Handle> {
    let channel = match &h.channel {
        GuestChannel::Unix(p) => wire::handle::Channel::Unix(path(p)?),
        GuestChannel::Vsock { uds, port } => {
            wire::handle::Channel::Vsock(wire::Vsock { uds: path(uds)?, port: *port })
        }
    };
    Ok(wire::Handle {
        cell_id: h.id.to_string(),
        backend: convert::backend_to_v1(h.backend).into(),
        pid: h.pid,
        channel: Some(channel),
        cgroup: path(&h.cgroup)?,
        netns: opt_path(h.netns.as_ref())?,
        extra: h.extra.clone().into_iter().collect(),
    })
}

pub(crate) fn handle_from(h: wire::Handle) -> Result<CellHandle> {
    let backend = convert::backend_from_v1(h.backend());
    let channel = match h.channel {
        Some(wire::handle::Channel::Unix(p)) => GuestChannel::Unix(p.into()),
        Some(wire::handle::Channel::Vsock(v)) => {
            GuestChannel::Vsock { uds: v.uds.into(), port: v.port }
        }
        None => return Err(bad("the handle has no channel")),
    };
    Ok(CellHandle {
        id: cell_id(&h.cell_id)?,
        backend,
        pid: h.pid,
        channel,
        cgroup: h.cgroup.into(),
        netns: from_opt(h.netns),
        extra: h.extra.into_iter().collect(),
    })
}

pub(crate) fn mode_to(m: PauseMode) -> wire::PauseMode {
    match m {
        PauseMode::Freeze => wire::PauseMode::Freeze,
        PauseMode::Reclaim => wire::PauseMode::Reclaim,
        PauseMode::SnapshotKill => wire::PauseMode::SnapshotKill,
    }
}

pub(crate) fn mode_from(m: wire::PauseMode) -> Result<PauseMode> {
    match m {
        wire::PauseMode::Freeze => Ok(PauseMode::Freeze),
        wire::PauseMode::Reclaim => Ok(PauseMode::Reclaim),
        wire::PauseMode::SnapshotKill => Ok(PauseMode::SnapshotKill),
        wire::PauseMode::Unspecified => Err(bad("the pause has no mode")),
    }
}

pub(crate) fn resources_to(r: &Resources) -> v1::Resources {
    v1::Resources {
        vcpu_milli: r.vcpu_milli,
        mem_mib: r.mem_mib,
        disk_gib: r.disk_gib,
        pids: r.pids,
        open_files: r.open_files,
    }
}

pub(crate) fn resources_from(r: Option<v1::Resources>) -> Result<Resources> {
    let r = r.ok_or_else(|| bad("the resize has no resources"))?;
    Ok(Resources {
        vcpu_milli: r.vcpu_milli,
        mem_mib: r.mem_mib,
        disk_gib: r.disk_gib,
        pids: r.pids,
        open_files: r.open_files,
    })
}

pub(crate) fn grace_from(d: Option<prost_types::Duration>) -> Result<Duration> {
    Ok(convert::duration_from_v1(d, "grace")?.unwrap_or_default())
}

pub(crate) fn exit_to(e: ExitInfo) -> wire::Exit {
    wire::Exit { code: e.code, signal: e.signal, oom: e.oom }
}

pub(crate) fn exit_from(e: Option<wire::Exit>) -> ExitInfo {
    e.map(|e| ExitInfo { code: e.code, signal: e.signal, oom: e.oom }).unwrap_or_default()
}

pub(crate) fn liveness_to(l: Liveness) -> wire::Liveness {
    use wire::liveness::State;
    match l {
        Liveness::Alive => wire::Liveness { state: State::Alive.into(), exit: None },
        Liveness::Paused => wire::Liveness { state: State::Paused.into(), exit: None },
        Liveness::Gone(e) => wire::Liveness { state: State::Gone.into(), exit: Some(exit_to(e)) },
    }
}

pub(crate) fn liveness_from(l: wire::Liveness) -> Result<Liveness> {
    use wire::liveness::State;
    match l.state() {
        State::Alive => Ok(Liveness::Alive),
        State::Paused => Ok(Liveness::Paused),
        State::Gone => Ok(Liveness::Gone(exit_from(l.exit))),
        State::Unspecified => Err(Error::new(Reason::Internal, "the plugin gave no liveness")),
    }
}
