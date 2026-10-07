//! Handing a VM's userfaultfd from the process that accepted it to the process that serves it, so
//! a server that dies can be replaced while its VMs wait.

use crate::sys;
use std::io::{self, Read, Write};
use std::os::fd::{AsRawFd, OwnedFd};
use std::os::unix::net::UnixStream;

/// The most a VMM's region list may take.
const MAX_TEXT: usize = 64 << 10;
/// The most ranges given back one hand over carries. Neighbours are merged, so a guest has to
/// give back this many separate ranges to reach it.
const MAX_REMOVED: usize = 1 << 14;

/// What a worker tells the process that handed it VMs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Report {
    /// The VM hung up, or could not be served, so it is done with.
    Done(u64),
    /// The VM's guest gave back `start..end`.
    Removed {
        /// The VM, by the number it was handed over with.
        id: u64,
        /// The first byte given back.
        start: u64,
        /// The byte after the last.
        end: u64,
    },
}

impl Report {
    /// Sends the report to `to`.
    ///
    /// # Errors
    ///
    /// The socket fails.
    pub fn send(self, to: &UnixStream) -> io::Result<()> {
        let mut data = Vec::with_capacity(24);
        match self {
            Self::Done(id) => data.extend_from_slice(&id.to_le_bytes()),
            Self::Removed { id, start, end } => {
                for v in [id, start, end] {
                    data.extend_from_slice(&v.to_le_bytes());
                }
            }
        }
        (&*to).write_all(&data)
    }

    /// Reads the next report from `from`, or `None` once the other end hung up.
    ///
    /// # Errors
    ///
    /// The socket fails, or a message is not one [`Report::send`] makes.
    pub fn receive(from: &UnixStream) -> io::Result<Option<Self>> {
        let mut b = [0u8; 24];
        let n = (&*from).read(&mut b)?;
        let word = |at: usize| u64::from_le_bytes(b[at..at + 8].try_into().unwrap_or_default());
        match n {
            0 => Ok(None),
            8 => Ok(Some(Self::Done(word(0)))),
            24 => Ok(Some(Self::Removed { id: word(0), start: word(8), end: word(16) })),
            _ => Err(io::Error::new(io::ErrorKind::InvalidData, "not a report")),
        }
    }
}

/// Sorts `ranges` and joins the ones that touch or overlap.
pub(crate) fn merge(mut ranges: Vec<(u64, u64)>) -> Vec<(u64, u64)> {
    ranges.sort_unstable();
    let mut merged: Vec<(u64, u64)> = Vec::with_capacity(ranges.len());
    for (s, e) in ranges {
        match merged.last_mut() {
            Some(last) if s <= last.1 => last.1 = last.1.max(e),
            _ => merged.push((s, e)),
        }
    }
    merged
}

/// What a VMM sends as soon as it connects: its regions as it wrote them, and the userfaultfd that
/// covers them. The stream it came on stays with it, since the VMM hanging up is how a session
/// ends. So does the memory the guest has given back so far, which has to keep reading as zeros
/// when another server takes the VM over.
#[derive(Debug)]
pub struct Hello {
    pub(crate) text: Vec<u8>,
    pub(crate) uffd: OwnedFd,
    pub(crate) stream: UnixStream,
    pub(crate) removed: Vec<(u64, u64)>,
}

impl Hello {
    /// Reads the first message of a VMM that just connected.
    ///
    /// # Errors
    ///
    /// The socket fails, or the VMM sends no descriptor.
    pub fn read(stream: UnixStream) -> io::Result<Self> {
        let mut buf = vec![0u8; MAX_TEXT];
        let (n, fd) = sys::recv_fd(&stream, &mut buf)?;
        let uffd = fd.ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "the VMM sent no userfaultfd")
        })?;
        buf.truncate(n);
        Ok(Self { text: buf, uffd, stream, removed: Vec::new() })
    }

    /// Notes that the guest gave back `start..end`, as a [`Report::Removed`] says.
    pub fn gave_back(&mut self, start: u64, end: u64) {
        self.removed.push((start, end));
        self.removed = merge(std::mem::take(&mut self.removed));
    }

    /// Sends this to the serving process at the other end of `to`, as VM `id`. The kernel gives
    /// the other end its own copies of the descriptors, and this side keeps its own, so the VM's
    /// memory stays covered if the server dies.
    ///
    /// # Errors
    ///
    /// The socket fails, as it does once the serving process is gone.
    pub fn send(&self, to: &UnixStream, id: u64) -> io::Result<()> {
        let mut data = id.to_le_bytes().to_vec();
        let count = u32::try_from(self.removed.len()).map_err(io::Error::other)?;
        data.extend_from_slice(&count.to_le_bytes());
        for (start, end) in &self.removed {
            data.extend_from_slice(&start.to_le_bytes());
            data.extend_from_slice(&end.to_le_bytes());
        }
        data.extend_from_slice(&self.text);
        sys::send_fds(to, &data, &[self.uffd.as_raw_fd(), self.stream.as_raw_fd()])
    }

    /// Takes the next VM sent with [`Hello::send`], or `None` once the other end hung up.
    ///
    /// # Errors
    ///
    /// The socket fails, or a message is not one [`Hello::send`] makes.
    pub fn receive(from: &UnixStream) -> io::Result<Option<(u64, Self)>> {
        let mut buf = vec![0u8; MAX_TEXT + MAX_REMOVED * 16 + 12];
        let (n, fds) = sys::recv_fds(from, &mut buf)?;
        if n == 0 && fds.is_empty() {
            return Ok(None);
        }
        let bad = || io::Error::new(io::ErrorKind::InvalidData, "not a VM handed over");
        let mut fds = fds.into_iter();
        let (Some(uffd), Some(stream), None) = (fds.next(), fds.next(), fds.next()) else {
            return Err(bad());
        };
        let (id, rest) = buf[..n].split_first_chunk::<8>().ok_or_else(bad)?;
        let (count, mut rest) = rest.split_first_chunk::<4>().ok_or_else(bad)?;
        let mut removed = Vec::new();
        for _ in 0..u32::from_le_bytes(*count) {
            let (start, more) = rest.split_first_chunk::<8>().ok_or_else(bad)?;
            let (end, more) = more.split_first_chunk::<8>().ok_or_else(bad)?;
            removed.push((u64::from_le_bytes(*start), u64::from_le_bytes(*end)));
            rest = more;
        }
        let hello = Self { text: rest.to_vec(), uffd, stream: UnixStream::from(stream), removed };
        Ok(Some((u64::from_le_bytes(*id), hello)))
    }
}

/// A connected pair of Unix sockets that keep the bounds of each message, for one process to
/// hand VMs to another with [`Hello::send`].
///
/// # Errors
///
/// The kernel refuses the sockets.
pub fn pair() -> io::Result<(UnixStream, UnixStream)> {
    sys::seqpacket_pair()
}
