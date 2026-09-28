//! Cell identifiers.
//!
//! A cell id is 128 bits laid out as `unit:8 | node:16 | epoch:16 | seq:40 | tag:48`, written as
//! the letter `c` followed by 26 characters of lowercase RFC 4648 base32 without padding. The gate
//! routes a request by reading `node` and `epoch` straight out of the id, which is what lets it
//! route without a lookup, and it rejects a stale `epoch` with `CELL_LOST`.
//!
//! The `tag` is a truncated HMAC over the other fields under a per unit key. Computing and checking
//! it needs the key, so that lives in `hive-auth`. This module only moves the bits.

use std::fmt;
use std::str::FromStr;

const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
const PREFIX: char = 'c';
/// 128 bits at five bits a character is 25.6, so the last character carries two bits of padding.
const CHARS: usize = 26;

const SEQ_BITS: u32 = 40;
const TAG_BITS: u32 = 48;
const SEQ_MAX: u64 = (1 << SEQ_BITS) - 1;
const TAG_MAX: u64 = (1 << TAG_BITS) - 1;

/// The identifier of one cell, unique for the life of a unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CellId(u128);

impl CellId {
    /// Packs the fields into an id. `seq` must fit in 40 bits and `tag` in 48, and anything wider
    /// is a bug in the caller rather than something to truncate quietly, so it returns `None`.
    #[must_use]
    pub fn new(unit: u8, node: u16, epoch: u16, seq: u64, tag: u64) -> Option<Self> {
        if seq > SEQ_MAX || tag > TAG_MAX {
            return None;
        }
        let bits = (u128::from(unit) << 120)
            | (u128::from(node) << 104)
            | (u128::from(epoch) << 88)
            | (u128::from(seq) << 48)
            | u128::from(tag);
        Some(Self(bits))
    }

    /// The raw 128 bits, for storage and for the wire.
    #[must_use]
    pub const fn to_bits(self) -> u128 {
        self.0
    }

    /// The inverse of [`CellId::to_bits`]. Every 128 bit value is a well formed id, and whether
    /// its tag is genuine is a separate question.
    #[must_use]
    pub const fn from_bits(bits: u128) -> Self {
        Self(bits)
    }

    /// The cluster unit the cell was created in.
    #[must_use]
    pub const fn unit(self) -> u8 {
        (self.0 >> 120) as u8
    }

    /// The registered index of the node agent that owns the cell.
    #[must_use]
    pub const fn node(self) -> u16 {
        (self.0 >> 104) as u16
    }

    /// The node agent's registration epoch when the cell was created.
    #[must_use]
    pub const fn epoch(self) -> u16 {
        (self.0 >> 88) as u16
    }

    /// The per node sequence number.
    #[must_use]
    pub const fn seq(self) -> u64 {
        (self.0 >> 48) as u64 & SEQ_MAX
    }

    /// The 48 bit tag that makes the id unguessable.
    #[must_use]
    pub const fn tag(self) -> u64 {
        self.0 as u64 & TAG_MAX
    }

    /// The bytes the tag is computed over: every field except the tag itself, big endian.
    #[must_use]
    pub fn tagged_bytes(self) -> [u8; 10] {
        let mut out = [0; 10];
        out.copy_from_slice(&(self.0 >> 48).to_be_bytes()[6..]);
        out
    }
}

impl fmt::Display for CellId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut out = [0u8; CHARS];
        // Shifted left by two so the 130 bits the characters hold line up with the 128 we have,
        // leaving the padding in the low bits of the last character.
        let wide = self.0;
        for (i, slot) in out.iter_mut().enumerate() {
            let shift = 128 - 5 * (i as i32 + 1);
            let chunk =
                if shift >= 0 { (wide >> shift) as usize } else { (wide << -shift) as usize };
            *slot = ALPHABET[chunk & 31];
        }
        write!(f, "{PREFIX}")?;
        // The alphabet is ASCII, so every byte written above is a whole character.
        f.write_str(std::str::from_utf8(&out).map_err(|_| fmt::Error)?)
    }
}

/// Why a string is not a cell id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParseCellIdError {
    /// It does not start with `c`.
    Prefix,
    /// It is not 27 characters long.
    Length,
    /// A character outside the lowercase base32 alphabet, at this byte offset.
    Character(usize),
    /// The two padding bits in the last character are not zero. Accepting them would give one id
    /// four spellings, and a gate that caches by the string would then see four different cells.
    Padding,
}

impl fmt::Display for ParseCellIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Prefix => write!(f, "a cell id starts with `{PREFIX}`"),
            Self::Length => write!(f, "a cell id is {} characters long", CHARS + 1),
            Self::Character(at) => write!(f, "a cell id has no character like the one at {at}"),
            Self::Padding => write!(f, "a cell id ends in a character that is not canonical"),
        }
    }
}

impl std::error::Error for ParseCellIdError {}

impl FromStr for CellId {
    type Err = ParseCellIdError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let rest = s.strip_prefix(PREFIX).ok_or(ParseCellIdError::Prefix)?;
        if rest.len() != CHARS {
            return Err(ParseCellIdError::Length);
        }
        let mut acc: u128 = 0;
        for (i, byte) in rest.bytes().enumerate() {
            let value = match byte {
                b'a'..=b'z' => byte - b'a',
                b'2'..=b'7' => byte - b'2' + 26,
                _ => return Err(ParseCellIdError::Character(i + 1)),
            };
            if i + 1 == CHARS {
                if value & 0b11 != 0 {
                    return Err(ParseCellIdError::Padding);
                }
                acc = (acc << 3) | u128::from(value >> 2);
            } else {
                acc = (acc << 5) | u128::from(value);
            }
        }
        Ok(Self(acc))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> CellId {
        CellId::new(3, 0x1234, 7, 0xab_cdef_0123, 0xdead_beef_cafe).unwrap()
    }

    #[test]
    fn fields_come_back_out() {
        let id = sample();
        assert_eq!(id.unit(), 3);
        assert_eq!(id.node(), 0x1234);
        assert_eq!(id.epoch(), 7);
        assert_eq!(id.seq(), 0xab_cdef_0123);
        assert_eq!(id.tag(), 0xdead_beef_cafe);
    }

    #[test]
    fn oversized_fields_are_refused() {
        assert!(CellId::new(0, 0, 0, SEQ_MAX + 1, 0).is_none());
        assert!(CellId::new(0, 0, 0, 0, TAG_MAX + 1).is_none());
        assert!(CellId::new(u8::MAX, u16::MAX, u16::MAX, SEQ_MAX, TAG_MAX).is_some());
    }

    #[test]
    fn text_round_trips() {
        for bits in [0, 1, u128::MAX, sample().to_bits(), 1 << 127, 0x5555 << 60] {
            let id = CellId::from_bits(bits);
            let text = id.to_string();
            assert_eq!(text.len(), 27, "{text}");
            assert_eq!(text.parse::<CellId>(), Ok(id), "{text}");
        }
    }

    #[test]
    fn zero_and_max_have_the_expected_spelling() {
        assert_eq!(CellId::from_bits(0).to_string(), format!("c{}", "a".repeat(26)));
        assert_eq!(CellId::from_bits(u128::MAX).to_string(), format!("c{}4", "7".repeat(25)));
    }

    #[test]
    fn order_of_text_matches_order_of_bits() {
        let a = CellId::from_bits(41);
        let b = CellId::from_bits(42);
        assert!(a < b);
        assert!(a.to_string() < b.to_string());
    }

    #[test]
    fn bad_text_is_refused_with_a_reason() {
        let good = sample().to_string();
        assert_eq!("x".parse::<CellId>(), Err(ParseCellIdError::Prefix));
        assert_eq!(good[..20].parse::<CellId>(), Err(ParseCellIdError::Length));
        let upper = good.to_uppercase().replacen('C', "c", 1);
        assert_eq!(upper.parse::<CellId>(), Err(ParseCellIdError::Character(1)));
        let mut padded = good.clone();
        padded.pop();
        padded.push('b');
        assert_eq!(padded.parse::<CellId>(), Err(ParseCellIdError::Padding));
    }

    #[test]
    fn tagged_bytes_cover_everything_but_the_tag() {
        let a = CellId::new(1, 2, 3, 4, 5).unwrap();
        let b = CellId::new(1, 2, 3, 4, 6).unwrap();
        assert_eq!(a.tagged_bytes(), b.tagged_bytes());
        assert_eq!(a.tagged_bytes(), [1, 0, 2, 0, 3, 0, 0, 0, 0, 4]);
    }
}
