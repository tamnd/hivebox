# Storage and image pipeline (`hive-nectar`)

> Crates: `hive-nectar` (library: formats, chunk store, caches), `hive-imaged` (build/convert service), `hive-blockd` (ublk targets, runs inside `hive-comb` or as a sidecar).
> Goal: start ≥5,000 sandboxes/s across ~160 nodes (≈31/s/node) from >10K base images and >100K workspaces, where each sandbox reads only 4-13% of its image (DSec Table 3), with an image-path budget of ≤15 ms warm and ≤100 ms cold (excluding runtime boot).

## 1. Design principles (inherited from DSec, extended)

1. Compose, don't rebuild. An image is `base` + `workspace` + `toolkit*` layers, stacked at mount time. Upgrading a toolkit is O(1), not O(#workspaces).
2. Immutable layers are EROFS. It is compact, has random-access compression, needs no write bookkeeping, lives in the kernel, and supports DAX when uncompressed.
3. Metadata is local, data is remote, writes are local. Path lookups must never cross the network. 5K starts/s × 1000s of lookups would crush any FUSE or remote metadata path, and 3FS FUSE tops out at ~400K 4K-ops/s per client.
4. Read on demand, in bulk. Fetch aligned extents of ≥256 KiB, and use recorded access traces to prefetch in one batched submission.
5. Deterministic builds allow fixed-size chunk dedup. AWS Lambda found that with deterministic flattened images, 80% of uploads have zero unique chunks and the median unique fraction is 2.5%. CDC is used only for streams and diffs.
6. Backends are pluggable. 3FS is the reference L2 store, but hivebox must also run on S3/MinIO/NVMe-only clusters for users without 3FS.

## 2. Layer model

```text
Image ref  := hive://<project>/<name>@<digest>
Manifest   := { base: LayerRef, workspace: Option<LayerRef>, toolkits: Vec<LayerRef>,
                env: EnvSpec, entry: Option<Cmd>, prefetch: Option<TraceRef>,
                labels, created_by, provenance(SLSA) }
LayerRef   := { digest: Blake3, kind: Base|Workspace|Toolkit|Snapshot,
                meta_blob: BlobId, data_blob: BlobId, size, chunk_size, compression,
                dax_ok: bool, fs_verity_root: Option<Hash> }
```

- Stack order, from bottom to top: `base`, `workspace`, `toolkit₁..ₙ`, `snapshot*`, then the writable upper.
- Total lowers are limited to 16, because overlayfs mount-option length issues show up near ~100 layers. Offline layer collapsing merges consecutive layers below a threshold (default 3 GiB) into one (meta, data) pair and preserves whiteouts (DSec).
- Snapshot layers come from `pack_diff` or checkpoint (see 07) and are first-class layers, so an agent-built environment is just a new manifest.

## 3. On-disk formats

### 3.1 EROFS layer blobs

- Layers are built by the writer in `hive-nectar`, which reads the tar as it streams in and writes the same format as `mkfs.erofs --tar=f --blobdev` (erofs-utils 1.7 or newer): chunk based files in the data blob, and inodes, dirents, xattrs and chunk indexes in the metadata blob. `mkfs.erofs` can still be used instead with `[images] mkfs` or `--mkfs`. We keep a Rust reader (`erofs-rs`) for indexing, verification and tracing.
- Multi-device blob split: a metadata blob (inodes, dirents, xattrs, chunk indexes) plus one or more data blobs. Metadata blobs are small (typically under 1-2% of the layer) and are pushed to every node that may run the image.
- Data uses a chunk-based layout with chunk size 64 KiB to 1 MiB. The default is 256 KiB, which matches DSec's OverlayBD fetch unit.
- Compression: workspaces and toolkits use `lz4hc` (fast decompress) or `zstd` level 3. Bases intended for pmem+DAX stay uncompressed, because DAX requires uncompressed inodes.
- Determinism: sorted dirents, fixed mtime (SOURCE_DATE_EPOCH), stable inode numbering and uid/gid normalization. Identical inputs give identical bytes, which enables chunk-level dedup.
- Content fingerprints are stored as xattrs. Kernels with EROFS `inode_share` (reported merged for 7.0; verify on the target kernel) can then share page cache for identical files across images, for example the same libpython across 11K images.
- Integrity: an fs-verity digest per blob, and a signed manifest (ed25519 / sigstore). In `strict` mode a node refuses unsigned layers.

### 3.2 Block images (microVM writable disks, full VMs)

- We use an OverlayBD-compatible layered block format served via ublk (`libublk` crate). DSec already ported OverlayBD to Rust (`kvcache-ai/AgentENV/storage/overlaybd`), so we will evaluate adopting or forking that code rather than reimplementing it.
- The writable upper is a log-structured sparse file on local NVMe. Sealing it yields an immutable snapshot layer, which gives incremental disk snapshots without repacking to EROFS.
- Full VM images: qcow2 is accepted at import and converted to OverlayBD layers.

### 3.3 Chunk store (`hive-nectar::cas`)

- Chunk id = BLAKE3(content). Chunks are cut where the content says (content defined, FastCDC style with a gear hash) rather than at fixed offsets, because `mkfs.erofs` pads each file to a 4 KiB block, so the same file lands at a different offset in every layer that holds it and fixed windows miss most of it.
- What is in now: `hive-nectar import-oci` and `import-tar` take `--dedup chunks` to keep each data blob as chunks plus a recipe, the list of chunks with their lengths. Chunks average 64 KiB (`--cas-avg`, a power of two from 16 KiB to 1 MiB) and are a quarter to four times that. Only chunks the store lacks are written. Layers point at the recipe with `data_chunks`, and relayout copies are kept as chunks too with `relaid_chunks`, cut the same way as the original, so most of a copy is already in the store. Mounts, `fetch` and `copy` read through the recipe, and the cache still holds the whole blob, so the data path past the store is unchanged. The index of chunks is the store itself for now, and there is no GC yet.
- Measured on server3 (8 cores, load 76 to 106 from other jobs) with four python slim images from Docker Hub (3.12.4, 3.12, 3.11, 3.13) and six django checkouts (4.2, 4.2.1, 4.2.2, 5.0, 5.0.1, 5.1), 682.7 MiB in distinct data blobs: kept whole the store is 693 MiB in 67 blobs, and as 64 KiB chunks it is 495 MiB in 6212 blobs. 256 KiB chunks keep 542 MiB, and fixed 256 KiB windows would keep 589.7 MiB. Each django point release adds 3.9 to 8.8 MiB of its 55 MiB, and python 3.11 and 3.13 add 29.8 and 33.2 MiB after 3.12, against 47.8 and 40.4 MiB whole. Imports take 2.6 to 13 s with chunks against 1.6 to 2.4 s whole. A whole fetch of python 3.12 into an empty cache reads 30 MiB/s from chunks and 45 MiB/s from whole blobs, and both caches end up with the same bytes. A lazy run of `python3 -c 'import json, sqlite3, asyncio'` took 2.79 and 3.40 s from chunks and 3.50 and 3.27 s whole.
- The chunk-to-location index lives in the metadata store: FoundationDB when co-deployed with 3FS, otherwise `redb` or TiKV, behind the trait `ChunkIndex`.
- GC is refcounted with a two-phase mark (manifests reachable from live projects, plus a retention window) followed by a sweep.
- Convergent encryption is optional (Lambda-style: key = KDF(content hash, salt)) for multi-tenant deployments. A salt per trust domain trades dedup against blast radius.

## 4. Storage backends (trait `BlobStore`)

```rust
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    fn caps(&self) -> BlobCaps;                       // rdma, max_io, ideal_io, supports_mmap
    async fn read_vectored(&self, blob: BlobId, reqs: &mut [ReadReq]) -> Result<()>; // batched, aligned
    async fn put(&self, blob: BlobId, src: impl AsyncRead + Send) -> Result<PutReceipt>;
    async fn stat(&self, blob: BlobId) -> Result<BlobStat>;
    async fn delete(&self, blob: BlobId) -> Result<()>;
}
```

| Backend | Crate | Notes |
|---|---|---|
| `threefs` (reference) | FFI to `libhf3fs_api_shared` (`usrbio` crate) | USRBIO: shared-mem Iov registered for RDMA + io_uring-like Ior; one Ior per thread; reads must not cross 3FS block boundaries. Published: 6.6 TiB/s aggregate on 180 storage nodes. FUSE mount only as fallback. |
| `s3` | `object_store` / `opendal` | ranged GETs, 8-16 parallel; used for cloud bursting and small deployments |
| `posix` | tokio-uring/io-uring | NFS/Lustre/local, for dev and single-node |
| `registry` | `oci-client` | import only (never on the hot path) |

## 5. Node data path

```
          ┌────────── hive-comb (node agent) ──────────┐
 create → │ ImageResolver → LayerMounter → RootfsBuilder│
          └──────┬───────────────┬──────────────────────┘
                 │               │
     L0 page cache (host)   L1 NVMe chunk cache (sparse files + bitmap, persisted)
                 │               │ miss
                 └──── Filler ◀──┘──▶ L2 BlobStore (3FS RDMA / S3)
```

### 5.1 Lazy-fill mechanisms (ordered by preference, chosen at node boot by kernel probe)

1. EROFS file-backed mount over a sparse L1 file, with a fanotify pre-content (`FAN_PRE_ACCESS`) filler. The kernel fills holes by asking our daemon. There is no FUSE and no loop device. This requires pre-content hooks (≥6.14); confirm page-fault coverage on the fleet kernel.
2. A ublk device exporting the blob (`hive-blockd`), with EROFS mounted on the block device. This works on ≥6.0 and also serves microVMs natively. `libublk` reports ~3M 4K IOPS with 8 queues.
3. Eager whole-layer fetch into L1. This is the fallback. It is correct but slow: DSec measured 1.71× slower bursts and 2.3× more disk writes.

> Not used: EROFS over fscache "ondemand" (reported deprecated or removed in 2026 kernels; verify), and FUSE lazy formats (eStargz/SOCI), because of FUSE overhead on the hot path.

### 5.2 Filler

- The filler rounds requests up to `ideal_io` (256 KiB to 1 MiB), coalesces adjacent misses, dedups in-flight fetches (single-flight per chunk), and bounds per-node concurrent L2 bytes. That bound is a token bucket shared with admission (see 08).
- Priority classes: `demand` (a process is blocked) > `prefetch-hot` (recorded trace for a sandbox being created now) > `prefetch-warm` (predicted by the scheduler) > `background` (metadata push, pinning).
- Chunks are verified on arrival (BLAKE3 per chunk) before they are exposed to the kernel.

### 5.3 L1 cache

- Each blob has a sparse file and a persisted bitmap. This is crash-safe: the bitmap is flushed after the data `fdatasync`, and on restart unknown chunks are re-verified lazily.
- Eviction is scan-resistant (S3-FIFO or LRU-2) with pins. Bases and toolkits (high fanout) are pinned by the reference count of running sandboxes plus popularity. Workspaces (fanout ≈1) are evicted first.
- Capacity target: 2-3 TB NVMe per node. Alarm when working-set churn exceeds 30%/hour.
- Metadata blobs are always local. `hive-imaged` pushes them on registration to the nodes in the image's affinity set, or to all nodes if the image is popular.

### 5.4 Prefetch traces

- On the first successful run of an image (or during `hive-imaged verify`), record the ordered chunk-access list from the filler demand log.
- `hive-imaged relayout` rewrites the data blob so trace chunks are contiguous in first-access order. Prefetch then becomes 1-3 large sequential 3FS reads. DADI reports that trace-replay prefetch removes ~95% of the cold/warm gap.
- What is in now: `hive-nectar relayout IMAGE` (the command will move to `hive-imaged`) stores, for each traced layer, a copy of the data blob with the traced chunks first in trace order and every other chunk after them in order, plus the order itself as a list of the original chunk each copy chunk holds. The short last chunk is padded to a whole chunk in the copy so it can move with the trace. The original data blob stays, so whole mode and older nodes are unchanged. A lazy mount fetches long runs from the copy, checks each chunk against the original leaves and writes it at its own offset, so the cache still ends up holding the original blob. Setting a new trace drops the copy, and running relayout again skips layers that already have one.
- The trace is stored as `TraceRef` in the manifest. The node issues it as one batched `read_vectored` at admit time, before the runtime boots.

## 6. Rootfs assembly per backend

| Backend | Read-only layers | Writable layer | Quota |
|---|---|---|---|
| Process/Wasm (FnCall) | shared pre-mounted base, bind RO | tmpfs (size-capped) | tmpfs size |
| Container | EROFS mounts → overlayfs `lowerdir=toolkits:ws:base` | overlay upper on local XFS | XFS project quota per sandbox (`prjquota`) |
| MicroVM | EROFS base/toolkits via virtio-pmem+DAX (trusted domain) or virtio-blk (untrusted); guest overlayfs | ext4 on ublk CoW device (OverlayBD upper) or reflinked sparse template (RunD-style) | device size |
| Full VM | OverlayBD disk via ublk → virtio-blk | OverlayBD upper | device size |

Notes:

- pmem sharing is a side channel. Firecracker docs advise against sharing the same pmem backing file across mutually untrusted VMs. Our policy is to share only within a trust domain (the same project subtree marked `share_pagecache=true`) and otherwise use per-VM virtio-blk.
- The guest pays for `struct page` on pmem (1/64 of size). Cap the pmem layer size per VM, or collapse toolkits into the base for pmem.
- `hive-comb` does mount work with `fsopen/fsconfig/fsmount/move_mount` (the new mount API, via `rustix`) inside the sandbox's mount namespace.

## 7. Build/convert pipeline (`hive-imaged`)

```
 OCI image / Dockerfile / SWE task spec (repo@commit + install script) / pack_diff snapshot
   → sandboxed builder (a hivebox microVM itself; no host docker)
   → layer split (base | workspace | toolkit) → deterministic EROFS (meta+data) → fs-verity
   → chunk + BLAKE3 → dedup vs CAS → upload new chunks to BlobStore
   → verify run (entry + tests) with tracing → relayout → sign manifest → register
   → push metadata blobs to affinity nodes
```

- Throughput target: convert 10K SWE workspaces/hour on a 16-node builder pool.
- SWE-style datasets (SWE-bench, SWE-Gym, R2E-Gym, SWE-smith, SWE-rebench) use one small set of language bases (py3.x, node, go, jvm, rust, c++), one workspace layer per `repo@commit` (checkout plus installed deps), and toolkit layers (harness, test runner shims). This replaces thousands of monolithic Docker images.
- Leakage hygiene for workspace layers: strip `.git` future refs and objects beyond the base commit, remove build-time residue, and record provenance (10).

## 8. Workspace I/O APIs (exposed through the sandbox API)

- `files.read/write/list/stat/remove` for small files, and `files.upload/download` (streamed, chunked).
- `diff(format=git|tar|layer)`: `git` runs `git diff` against the workspace base commit inside the sandbox. `layer` seals the upper into a snapshot layer.
- Artifacts: large outputs (logs, coverage, trajectories) are chunked into the CAS with BLAKE3 and FastCDC (`fastcdc` crate).

## 9. Capacity math (defaults, to validate in 13)

- 31 starts/s/node × 3 GiB image × 4-13% read gives 3.7-12 GiB/s per node if nothing is cached. Base and toolkit pins plus page-cache sharing should cut remote reads by ≥5×, which gives ≤2.5 GiB/s/node and ≤400 GiB/s cluster peak on the L2. That is within reach of a 3FS deployment of tens of servers.
- Metadata is 100% local, so there are zero remote metadata ops on the hot path.

## 10. Start-path budget (image portion)

| Step | Warm | Cold |
|---|---|---|
| Resolve manifest (cached) | 0.2 ms | 2 ms |
| Metadata blobs local | 0 | ≤5 ms (single 3FS read if not pushed) |
| Mount EROFS + overlay / attach ublk/pmem | ≤5 ms | ≤5 ms |
| Writable layer (dir + quota / reflink) | ≤3 ms | ≤3 ms |
| Trace prefetch | ~0 | ≤50 ms (≈200 MiB batched) |
| **Total** | **≤10 ms** | **≤70 ms** + overlapped demand misses (1-3 ms each) |

## 11. Kernel and risk matrix

| Feature | Min kernel | Fallback |
|---|---|---|
| EROFS file-backed mount | 6.12 | loop device |
| fanotify pre-content | 6.14 (verify) | ublk path |
| ublk | 6.0 (6.16+ for per-IO daemons) | eager fetch |
| EROFS inode_share | 7.0 (verify) | none (just more page cache) |
| virtio-pmem in Firecracker | FC ≥1.14 (verify) | virtio-blk |

Fleet guidance: pin one kernel (7.x LTS). The CI matrix covers the fallback paths.

## 12. Open questions

- Should we adopt DSec's Rust OverlayBD port directly, or write our own ublk CoW target? We lean toward adopting it and upstreaming changes.
- How mature is the pure-Rust EROFS writer for deterministic builds?
- Should we add a Dragonfly (Rust dfdaemon) P2P tier for S3-only deployments? It would help for bases and would be pointless for fanout-1 workspaces.
