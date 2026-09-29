//! Blob names.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A blob's name: the BLAKE3 hash of its bytes, written as 64 lowercase hex digits.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BlobId([u8; 32]);

impl BlobId {
    /// The name of these bytes.
    #[must_use]
    pub fn of(bytes: &[u8]) -> Self {
        Self(*blake3::hash(bytes).as_bytes())
    }

    /// A name from a finished hash.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// The raw hash.
    #[must_use]
    pub const fn as_bytes(&self) -> &[u8; 32] {
        &self.0
    }
}

impl From<blake3::Hash> for BlobId {
    fn from(h: blake3::Hash) -> Self {
        Self(*h.as_bytes())
    }
}

impl fmt::Display for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for b in self.0 {
            write!(f, "{b:02x}")?;
        }
        Ok(())
    }
}

impl fmt::Debug for BlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "BlobId({self})")
    }
}

/// What went wrong reading a [`BlobId`] from text.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BadBlobId;

impl fmt::Display for BadBlobId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("a blob id is 64 lowercase hex digits")
    }
}

impl std::error::Error for BadBlobId {}

impl FromStr for BlobId {
    type Err = BadBlobId;

    fn from_str(s: &str) -> Result<Self, BadBlobId> {
        let s = s.as_bytes();
        if s.len() != 64 {
            return Err(BadBlobId);
        }
        let digit = |c: u8| match c {
            b'0'..=b'9' => Ok(c - b'0'),
            b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(BadBlobId),
        };
        let mut out = [0; 32];
        for (i, [hi, lo]) in s.as_chunks::<2>().0.iter().enumerate() {
            out[i] = digit(*hi)? << 4 | digit(*lo)?;
        }
        Ok(Self(out))
    }
}

impl Serialize for BlobId {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for BlobId {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_reads_back_what_it_writes() {
        let id = BlobId::of(b"hivebox");
        let text = id.to_string();
        assert_eq!(text.len(), 64);
        assert_eq!(text.parse::<BlobId>(), Ok(id));
        assert_eq!(
            serde_json::from_str::<BlobId>(&serde_json::to_string(&id).unwrap()).unwrap(),
            id
        );
        assert_eq!(text.to_uppercase().parse::<BlobId>(), Err(BadBlobId));
        assert_eq!(text[1..].parse::<BlobId>(), Err(BadBlobId));
    }
}
