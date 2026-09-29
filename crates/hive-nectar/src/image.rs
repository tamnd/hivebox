//! Images as hivebox keeps them: a stack of EROFS layers and the config to run them with.

use serde::{Deserialize, Serialize};

use crate::BlobId;

/// One layer: the two blobs `mkfs.erofs` made from one tar.
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
