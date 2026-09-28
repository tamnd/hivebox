//! What the WAL stores for each cell: everything a restarted comb needs to find the cell again
//! and carry on. It is protobuf, so a newer comb reads an older one's WAL.

use hive_cell::{CellHandle, GuestChannel};
use hive_proto::convert;
use hive_proto::v1;
use hive_types::{Backend, Cause, CellId, CellSpec, CellState, Error};
use prost::Message;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A cell's record in the WAL. The key is the cell id.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Record {
    #[prost(message, optional, tag = "1")]
    pub(crate) spec: Option<v1::CellSpec>,
    #[prost(enumeration = "v1::CellState", tag = "2")]
    pub(crate) state: i32,
    #[prost(enumeration = "v1::Cause", tag = "3")]
    pub(crate) cause: i32,
    #[prost(string, tag = "4")]
    pub(crate) message: String,
    #[prost(message, optional, tag = "5")]
    pub(crate) handle: Option<Handle>,
    /// The guest agent's secret for the next handshake.
    #[prost(bytes = "vec", tag = "6")]
    pub(crate) secret: Vec<u8>,
    #[prost(uint64, tag = "7")]
    pub(crate) created_ms: u64,
    #[prost(uint64, tag = "8")]
    pub(crate) changed_ms: u64,
    #[prost(string, tag = "9")]
    pub(crate) project: String,
    #[prost(string, tag = "10")]
    pub(crate) idem_key: String,
}

/// A [`CellHandle`] as stored.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Handle {
    #[prost(uint32, optional, tag = "1")]
    pub(crate) pid: Option<u32>,
    #[prost(string, tag = "2")]
    pub(crate) unix: String,
    #[prost(string, tag = "3")]
    pub(crate) vsock: String,
    #[prost(uint32, tag = "4")]
    pub(crate) vsock_port: u32,
    #[prost(string, tag = "5")]
    pub(crate) cgroup: String,
    #[prost(btree_map = "string, string", tag = "6")]
    pub(crate) extra: BTreeMap<String, String>,
}

/// The WAL key that holds the next free sequence number. Real cell ids never have every bit set,
/// since the comb never hands out the last sequence number.
pub(crate) const SEQ_KEY: u128 = u128::MAX;

/// The sequence counter's record.
#[derive(Clone, PartialEq, Message)]
pub(crate) struct Seq {
    /// Every sequence number below this may have been used.
    #[prost(uint64, tag = "1")]
    pub(crate) next: u64,
}

impl Record {
    pub(crate) fn cell_state(&self) -> Option<CellState> {
        convert::state_from_v1(v1::CellState::try_from(self.state).ok()?)
    }

    pub(crate) fn set_cell_state(&mut self, s: CellState) {
        self.state = convert::state_to_v1(s) as i32;
        self.changed_ms = now_ms();
    }

    pub(crate) fn cell_cause(&self) -> Option<Cause> {
        convert::cause_from_v1(v1::Cause::try_from(self.cause).ok()?)
    }

    pub(crate) fn set_cell_cause(&mut self, c: Cause) {
        self.cause = convert::cause_to_v1(c) as i32;
    }

    pub(crate) fn spec(&self) -> Result<CellSpec, Error> {
        convert::spec_from_v1(self.spec.clone().unwrap_or_default())
    }

    pub(crate) fn secret(&self) -> Option<[u8; 32]> {
        self.secret.as_slice().try_into().ok()
    }

    pub(crate) fn handle(&self, id: CellId, backend: Backend) -> Option<CellHandle> {
        self.handle.as_ref().map(|h| h.to_cell(id, backend))
    }
}

impl Handle {
    pub(crate) fn from_cell(h: &CellHandle) -> Self {
        let (unix, vsock, vsock_port) = match &h.channel {
            GuestChannel::Unix(p) => (text(p), String::new(), 0),
            GuestChannel::Vsock { uds, port } => (String::new(), text(uds), *port),
        };
        Self {
            pid: h.pid,
            unix,
            vsock,
            vsock_port,
            cgroup: text(&h.cgroup),
            extra: h.extra.clone(),
        }
    }

    fn to_cell(&self, id: CellId, backend: Backend) -> CellHandle {
        let channel = if self.vsock.is_empty() {
            GuestChannel::Unix(PathBuf::from(&self.unix))
        } else {
            GuestChannel::Vsock { uds: PathBuf::from(&self.vsock), port: self.vsock_port }
        };
        CellHandle {
            id,
            backend,
            pid: self.pid,
            channel,
            cgroup: PathBuf::from(&self.cgroup),
            extra: self.extra.clone(),
        }
    }
}

fn text(p: &std::path::Path) -> String {
    // Paths the comb and the drivers make are always UTF-8.
    p.to_string_lossy().into_owned()
}

pub(crate) fn now_ms() -> u64 {
    ms(SystemTime::now())
}

pub(crate) fn ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

pub(crate) fn time(ms: u64) -> SystemTime {
    UNIX_EPOCH + Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use hive_types::Source;

    #[test]
    fn a_record_round_trips() {
        let id = CellId::new(1, 2, 3, 4, 5).unwrap();
        let spec = CellSpec::new(
            Source::Image("docker.io/library/python:3.12".into()),
            Backend::Container,
        );
        let handle = CellHandle {
            id,
            backend: Backend::Container,
            pid: Some(4242),
            channel: GuestChannel::Unix("/run/hive/cells/x/drone.sock".into()),
            cgroup: "/sys/fs/cgroup/hive.slice/cell-7".into(),
            extra: BTreeMap::from([("bundle".into(), "/var/lib/x".into())]),
        };
        let mut r = Record {
            spec: Some(convert::spec_to_v1(&spec)),
            handle: Some(Handle::from_cell(&handle)),
            secret: vec![9; 32],
            created_ms: 1,
            project: "p".into(),
            ..Record::default()
        };
        r.set_cell_state(CellState::Running);
        r.set_cell_cause(Cause::Oom);
        let back = Record::decode(r.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back, r);
        assert_eq!(back.cell_state(), Some(CellState::Running));
        assert_eq!(back.cell_cause(), Some(Cause::Oom));
        assert_eq!(back.spec().unwrap(), spec);
        assert_eq!(back.secret(), Some([9; 32]));
        assert_eq!(back.handle(id, Backend::Container), Some(handle));

        let vm = CellHandle {
            channel: GuestChannel::Vsock { uds: "/srv/jail/v.sock".into(), port: 52 },
            pid: None,
            ..back.handle(id, Backend::Microvm).unwrap()
        };
        let stored = Handle::from_cell(&vm);
        assert_eq!(stored.to_cell(id, Backend::Microvm), vm);
    }

    #[test]
    fn an_empty_record_has_no_state() {
        let r = Record::default();
        assert_eq!(r.cell_state(), None);
        assert_eq!(r.cell_cause(), None);
        assert_eq!(r.secret(), None);
    }
}
