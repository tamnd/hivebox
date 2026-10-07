//! Images as hivebox keeps them: a stack of EROFS layers and the config to run them with.

use serde::{Deserialize, Serialize};

use crate::BlobId;

/// One layer: the two blobs built from one tar.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct LayerRef {
    /// The layer's name, the BLAKE3 hash of its metadata blob's name followed by its data
    /// blob's name.
    pub digest: BlobId,
    /// Superblock, inodes, directories and chunk indexes.
    pub meta: BlobId,
    /// File contents, in whole chunks.
    pub data: BlobId,
    /// The metadata blob's size in bytes.
    pub meta_size: u64,
    /// The data blob's size in bytes.
    pub data_size: u64,
    /// The chunk size it was built with.
    pub chunk_size: u32,
    /// The [`crate::Leaves`] of the data blob, which lazy filling checks each chunk against.
    /// Layers imported before there were leaves have none and are fetched whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_leaves: Option<BlobId>,
    /// The chunks of the data blob a run of the image read, in the order it first read them, as
    /// a blob of little endian `u32` chunk numbers. A lazy mount fills these first.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_trace: Option<BlobId>,
    /// A copy of the data blob with the traced chunks first, in the order the trace read them,
    /// and the rest after them in order, so a lazy mount fetches what a run reads first in a few
    /// long reads. [`crate::relayout`] makes it. The data blob stays as it was, and is what a
    /// whole fetch gets and what the cache keeps.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_relaid: Option<BlobId>,
    /// Which chunk of the data blob each chunk of `data_relaid` is, as a blob of little endian
    /// `u32` chunk numbers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_order: Option<BlobId>,
    /// The [`crate::cas`] recipe the data blob is kept as, when it is kept as chunks. The store
    /// then has the chunks and not the data blob itself.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data_chunks: Option<BlobId>,
    /// The same for `data_relaid`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relaid_chunks: Option<BlobId>,
    /// The OCI digest of the uncompressed tar it came from, when it came from one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub diff_id: Option<String>,
}

impl LayerRef {
    /// The name a layer made of these two blobs has.
    #[must_use]
    pub fn digest_of(meta: BlobId, data: BlobId) -> BlobId {
        let mut h = blake3::Hasher::new();
        h.update(meta.as_bytes());
        h.update(data.as_bytes());
        h.finalize().into()
    }
}

/// How to run an image, taken from its OCI config.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImageConfig {
    /// `KEY=value` pairs.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<String>,
    /// The command a plain run starts with.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub entrypoint: Vec<String>,
    /// Arguments after the entrypoint, or the command when there is none.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cmd: Vec<String>,
    /// Where commands start. Empty means `/`.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub working_dir: String,
    /// Who they run as. Empty means root.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub user: String,
}

/// An image: its layers from the bottom up, and its config. The manifest is itself stored as a
/// blob, and its name is the image's name.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Manifest {
    /// The layers, bottom first. The overlay puts the last one on top.
    pub layers: Vec<LayerRef>,
    /// How to run it.
    #[serde(default)]
    pub config: ImageConfig,
    /// Where it came from, such as `docker.io/library/python:3.12-slim`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub source: Option<String>,
    /// For an image committed from a cell, what it was committed from and how.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<Provenance>,
}

/// Where a committed image came from.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Provenance {
    /// The image the new layer went on top of.
    pub parent: BlobId,
    /// What was committed, such as a cell's id.
    pub from: String,
    /// When, in seconds since the Unix epoch.
    pub at: u64,
    /// What scrubbing did, or nothing when the commit was not scrubbed.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scrubbed: Option<Scrubbed>,
}

/// What scrubbing did to a committed layer.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Scrubbed {
    /// Files left out: histories, credential files, and env files that held a secret.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub removed: Vec<String>,
    /// Files written with credentials taken out, such as a `.git/config` with a token in a url.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub rewritten: Vec<String>,
    /// Secrets found in paths the caller allowed.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub allowed: Vec<Finding>,
    /// Files too big to search for secrets.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unsearched: Vec<String>,
}

/// A secret found in a file: where, and what kind. The secret itself is never kept.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Finding {
    /// The file, relative to the root.
    pub path: String,
    /// The line it starts on, from 1.
    pub line: u64,
    /// The rule it matched, such as `aws access key`.
    pub rule: String,
}

impl Manifest {
    /// The bytes it is stored as. Field order is fixed, so the same manifest always has the same
    /// name.
    ///
    /// # Panics
    ///
    /// Never. Every field serializes.
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("a manifest always serializes")
    }

    /// Reads one back.
    ///
    /// # Errors
    ///
    /// The bytes are not a manifest.
    pub fn from_bytes(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }

    /// The manifest's name.
    #[must_use]
    pub fn id(&self) -> BlobId {
        BlobId::of(&self.to_bytes())
    }

    /// Bytes across every blob of every layer.
    #[must_use]
    pub fn size(&self) -> u64 {
        self.layers.iter().map(|l| l.meta_size + l.data_size).sum()
    }
}
