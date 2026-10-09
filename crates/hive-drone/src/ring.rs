//! A bounded buffer for command output that keeps the start and the end.

use bytes::Bytes;

/// The most of the start of a stream a [`Ring`] keeps.
pub(crate) const HEAD: usize = 64 * 1024;

/// Keeps the first bytes written to it and the most recent ones, up to a limit, and drops the
/// middle. A build log that runs to a gigabyte still shows the command line at the top and the
/// error at the bottom, and memory stays bounded.
#[derive(Debug)]
pub struct Ring {
    head_cap: usize,
    tail_cap: usize,
    head: Vec<u8>,
    // Circular once full. `pos` is where the oldest byte is, and the next write goes.
    tail: Vec<u8>,
    pos: usize,
    total: u64,
}

impl Ring {
    /// A ring that keeps at most `limit` bytes.
    #[must_use]
    pub fn new(limit: usize) -> Self {
        let head_cap = HEAD.min(limit / 2);
        Self {
            head_cap,
            tail_cap: limit - head_cap,
            head: Vec::new(),
            tail: Vec::new(),
            pos: 0,
            total: 0,
        }
    }

    /// Adds `data` at the end.
    pub fn push(&mut self, mut data: &[u8]) {
        self.total += data.len() as u64;
        if self.head.len() < self.head_cap {
            let n = (self.head_cap - self.head.len()).min(data.len());
            self.head.extend_from_slice(&data[..n]);
            data = &data[n..];
        }
        if data.is_empty() || self.tail_cap == 0 {
            return;
        }
        if data.len() >= self.tail_cap {
            self.tail.clear();
            self.tail.extend_from_slice(&data[data.len() - self.tail_cap..]);
            self.pos = 0;
            return;
        }
        let room = self.tail_cap - self.tail.len();
        if room > 0 {
            let n = room.min(data.len());
            self.tail.extend_from_slice(&data[..n]);
            data = &data[n..];
        }
        // What is left is shorter than the tail, so this wraps at most once.
        while !data.is_empty() {
            let n = (self.tail_cap - self.pos).min(data.len());
            self.tail[self.pos..self.pos + n].copy_from_slice(&data[..n]);
            self.pos = (self.pos + n) % self.tail_cap;
            data = &data[n..];
        }
    }

    /// Bytes written so far, kept or not.
    #[must_use]
    pub fn total(&self) -> u64 {
        self.total
    }

    /// Whether anything was dropped.
    #[must_use]
    pub fn truncated(&self) -> bool {
        self.total > (self.head.len() + self.tail.len()) as u64
    }

    /// What was kept, in order.
    pub fn take(&mut self) -> Bytes {
        let mut out = std::mem::take(&mut self.head);
        out.reserve(self.tail.len());
        out.extend_from_slice(&self.tail[self.pos..]);
        out.extend_from_slice(&self.tail[..self.pos]);
        self.tail.clear();
        self.pos = 0;
        out.into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_output_is_kept_whole() {
        let mut r = Ring::new(1 << 20);
        r.push(b"hello ");
        r.push(b"world");
        assert!(!r.truncated());
        assert_eq!(r.total(), 11);
        assert_eq!(&r.take()[..], b"hello world");
    }

    #[test]
    fn long_output_keeps_the_start_and_the_end() {
        let mut r = Ring::new(10);
        for b in b"abcdefghijklmnopqrstuvwxyz" {
            r.push(&[*b]);
        }
        assert!(r.truncated());
        assert_eq!(r.total(), 26);
        assert_eq!(&r.take()[..], b"abcdevwxyz");
    }

    #[test]
    fn big_writes_wrap_the_same_as_small_ones() {
        let data: Vec<u8> = (0..400_000u32).map(|i| (i % 251) as u8).collect();
        let mut one = Ring::new(200_000);
        one.push(&data);
        let mut many = Ring::new(200_000);
        for chunk in data.chunks(777) {
            many.push(chunk);
        }
        let (a, b) = (one.take(), many.take());
        assert_eq!(a, b);
        assert_eq!(a.len(), 200_000);
        assert_eq!(&a[..HEAD], &data[..HEAD]);
        assert_eq!(&a[HEAD..], &data[data.len() - (200_000 - HEAD)..]);
    }

    #[test]
    fn a_zero_limit_keeps_nothing() {
        let mut r = Ring::new(0);
        r.push(b"abc");
        assert!(r.truncated());
        assert!(r.take().is_empty());
    }
}
