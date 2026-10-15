//! [`S3Store`], a [`BlobStore`] in an S3 bucket or anything that speaks its API, such as MinIO,
//! Ceph RGW or R2.
//!
//! Requests are signed with AWS Signature Version 4 here rather than through an SDK, since a
//! blob store needs a handful of calls and the SDKs bring a large tree with them. Reads are ranged
//! GETs, with neighbouring requests of a batch merged into one, and up to [`PARALLEL`] in flight.
//! Blobs up to [`SINGLE_PUT`] go up in one PUT and bigger ones as a multipart upload.
//!
//! A blob put or reused again gets a new time as it does in a directory, for `hive-nectar gc`:
//! the store copies it onto itself, which leaves the bytes as they are and sets `LastModified`.
//!
//! Only plain `http://` endpoints work for now, since the tree has no TLS stack yet. That covers a
//! store inside the cluster network or a local proxy in front of a remote one.

use std::fmt::Write as _;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD as BASE64;
use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode, Uri, http};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use md5::Md5;
use sha2::{Digest, Sha256};

use crate::BlobId;
use crate::store::{
    BlobCaps, BlobStat, BlobStore, Listed, PutReceipt, REFRESH, ReadReq, blocking, hash_file,
};

/// Ranged GETs or part uploads in flight for one call.
pub const PARALLEL: usize = 16;

/// The biggest blob that goes up in one PUT. Bigger ones are uploaded in parts.
pub const SINGLE_PUT: u64 = 64 << 20;

/// The size of each part of a multipart upload, unless the blob needs more than S3's 10,000 parts.
const PART: u64 = 16 << 20;

/// Parts uploaded at once. Each holds a part in memory.
const PARTS_IN_FLIGHT: usize = 4;

/// The biggest object S3 copies in one request. Bigger ones are copied in parts.
const MAX_COPY: u64 = 5 << 30;

/// The size of each part of a copy in parts.
const COPY_PART: u64 = 1 << 30;

/// The most objects one DeleteObjects request may name.
const DELETE_MAX: usize = 1000;

/// DeleteObjects requests in flight at once. A store may remove the objects one request names one
/// after another, so a sweep splits what it removes into at least this many requests.
const DELETES_IN_FLIGHT: usize = 16;

/// The longest one request may take, body included, before it is tried again.
const TIMEOUT: Duration = Duration::from_secs(60);

/// How many times a request is tried when the store fails or does not answer.
const ATTEMPTS: u32 = 3;

/// The hash S3 expects for an empty body.
const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

/// Where a store is and who to sign as.
#[derive(Clone)]
pub struct S3Config {
    /// The endpoint, such as `http://10.0.0.5:9000`. Buckets are addressed by path.
    pub endpoint: String,
    /// The bucket the blobs go in, which has to be there already.
    pub bucket: String,
    /// Put in front of every blob's name, such as `blobs/`.
    pub prefix: String,
    /// The region signed for, which stores outside AWS mostly ignore.
    pub region: String,
    /// The access key id to sign as.
    pub access_key: String,
    /// The secret that goes with it.
    pub secret_key: String,
    /// For temporary credentials.
    pub session_token: Option<String>,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .field("region", &self.region)
            .field("access_key", &self.access_key)
            .finish_non_exhaustive()
    }
}

impl S3Config {
    /// A config from a URL such as `http://10.0.0.5:9000/bucket/some/prefix`, with the
    /// credentials and region from `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
    /// `AWS_SESSION_TOKEN` and `AWS_REGION`, which is `us-east-1` if unset.
    ///
    /// # Errors
    ///
    /// The URL has no bucket or the keys are not set.
    pub fn from_url_and_env(url: &str) -> io::Result<Self> {
        let bad = |why: &str| io::Error::new(io::ErrorKind::InvalidInput, format!("{url}: {why}"));
        let uri: Uri = url.parse().map_err(|_| bad("not a URL"))?;
        let (Some(scheme), Some(authority)) = (uri.scheme_str(), uri.authority()) else {
            return Err(bad("needs a scheme and a host"));
        };
        let path = uri.path().trim_matches('/');
        let (bucket, prefix) = path.split_once('/').unwrap_or((path, ""));
        if bucket.is_empty() {
            return Err(bad("needs a bucket, as in http://host:9000/bucket"));
        }
        let env = |name: &str| {
            std::env::var(name).map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is not set"))
            })
        };
        Ok(Self {
            endpoint: format!("{scheme}://{authority}"),
            bucket: bucket.to_owned(),
            prefix: if prefix.is_empty() { String::new() } else { format!("{prefix}/") },
            region: std::env::var("AWS_REGION").unwrap_or_else(|_| "us-east-1".into()),
            access_key: env("AWS_ACCESS_KEY_ID")?,
            secret_key: env("AWS_SECRET_ACCESS_KEY")?,
            session_token: std::env::var("AWS_SESSION_TOKEN").ok(),
        })
    }
}

/// A blob store in an S3 bucket.
#[derive(Debug)]
pub struct S3Store {
    cfg: S3Config,
    /// The endpoint's `host:port`, which is signed as the `host` header.
    host: String,
    client: Client<HttpConnector, Full<Bytes>>,
    /// The biggest blob copied onto itself in one request, which is [`MAX_COPY`] but in tests.
    copy_max: u64,
}

impl S3Store {
    /// A store for `cfg`. It makes no request until it is used.
    ///
    /// # Errors
    ///
    /// The endpoint is not a plain `http://` URL.
    pub fn new(cfg: S3Config) -> io::Result<Self> {
        let uri: Uri = cfg.endpoint.parse().map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, format!("{}: not a URL", cfg.endpoint))
        })?;
        if uri.scheme_str() != Some("http") {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                format!(
                    "{}: only http endpoints work for now, so put a local proxy in front of an \
                     https one",
                    cfg.endpoint
                ),
            ));
        }
        let host = uri.authority().map(ToString::to_string).unwrap_or_default();
        let mut http = HttpConnector::new();
        http.set_nodelay(true);
        http.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_max_idle_per_host(PARALLEL * 2)
            .build(http);
        Ok(Self { cfg, host, client, copy_max: MAX_COPY })
    }

    /// The path of `blob` in the bucket, percent encoded.
    fn path(&self, blob: BlobId) -> String {
        self.key_path(&format!("{}{blob}", self.cfg.prefix))
    }

    /// The path of the object `key` in the bucket, percent encoded.
    fn key_path(&self, key: &str) -> String {
        format!("/{}/{}", encode(&self.cfg.bucket, false), encode(key, true))
    }

    /// The bucket itself, for listing it.
    fn bucket_path(&self) -> String {
        format!("/{}", encode(&self.cfg.bucket, false))
    }

    /// The size of `blob`, and when it was last written if the store says.
    async fn head(&self, blob: BlobId) -> io::Result<(u64, Option<SystemTime>)> {
        let call = Call::new(Method::HEAD, self.path(blob));
        let resp = self.send(&call).await?;
        match resp.status() {
            StatusCode::NOT_FOUND => {
                Err(io::Error::new(io::ErrorKind::NotFound, format!("{blob} is not in the store")))
            }
            s if s.is_success() => {
                let headers = resp.headers();
                let size = headers
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok()?.parse().ok())
                    .ok_or_else(|| io::Error::other("the store gave no Content-Length"))?;
                let modified = headers
                    .get(http::header::LAST_MODIFIED)
                    .and_then(|v| http_date(v.to_str().ok()?));
                Ok((size, modified))
            }
            s => Err(failure(&call, s, b"")),
        }
    }

    /// Gives `blob` the time now by copying it onto itself, in parts if it is too big for one
    /// copy. The object is whole all the while, since S3 swaps the copy in at once.
    async fn touch(&self, blob: BlobId, size: u64) -> io::Result<()> {
        let path = self.path(blob);
        if size > self.copy_max {
            let part = COPY_PART.min(self.copy_max);
            let upload = self.start_upload(&path).await?;
            let copied = futures::stream::iter(0..size.div_ceil(part))
                .map(|i| {
                    let (path, upload) = (path.clone(), upload.clone());
                    async move {
                        let (start, end) = (i * part, (i * part + part).min(size) - 1);
                        let mut call = Call::new(Method::PUT, path.clone());
                        call.query = vec![
                            ("partNumber".into(), (i + 1).to_string()),
                            ("uploadId".into(), upload),
                        ];
                        call.headers = vec![
                            ("x-amz-copy-source", path),
                            ("x-amz-copy-source-range", format!("bytes={start}-{end}")),
                        ];
                        let (_, body) = self.ok(&call).await?;
                        let etag = between(&body, "<ETag>", "</ETag>")
                            .ok_or_else(|| failure(&call, StatusCode::OK, &body))?;
                        Ok::<_, io::Error>((i + 1, etag))
                    }
                })
                .buffered(PARTS_IN_FLIGHT)
                .try_collect::<Vec<_>>()
                .await;
            return self.finish_upload(path, upload, copied).await;
        }
        let mut call = Call::new(Method::PUT, path.clone());
        call.headers =
            vec![("x-amz-copy-source", path), ("x-amz-metadata-directive", "REPLACE".into())];
        // As with a finished upload, an error can come in the body of a 200.
        let (_, body) = self.ok(&call).await?;
        if between(&body, "<Error>", "</Error>").is_some() {
            return Err(failure(&call, StatusCode::OK, &body));
        }
        Ok(())
    }

    /// Every blob under the prefix, with its size and time. Objects whose names are not blobs
    /// are left out.
    pub(crate) async fn list_blobs(&self) -> io::Result<Vec<Listed>> {
        let mut all = Vec::new();
        let mut next: Option<String> = None;
        loop {
            let mut call = Call::new(Method::GET, self.bucket_path());
            call.query =
                vec![("list-type".into(), "2".into()), ("prefix".into(), self.cfg.prefix.clone())];
            if let Some(token) = next.take() {
                call.query.push(("continuation-token".into(), token));
            }
            let (_, body) = self.ok(&call).await?;
            let body = listing(&call, &body)?;
            for entry in elements(body, "Contents") {
                let bad = || io::Error::other(format!("the store listed {entry:?}"));
                let key = unescape(field(entry, "Key").ok_or_else(bad)?);
                let Some(blob) = key.strip_prefix(&self.cfg.prefix).and_then(|n| n.parse().ok())
                else {
                    continue;
                };
                let size = field(entry, "Size").and_then(|n| n.parse().ok()).ok_or_else(bad)?;
                let modified = field(entry, "LastModified").and_then(iso_date).ok_or_else(bad)?;
                all.push(Listed { blob, size, modified });
            }
            if field(body, "IsTruncated") != Some("true") {
                return Ok(all);
            }
            let token = field(body, "NextContinuationToken")
                .ok_or_else(|| io::Error::other("the store cut a listing short with no token"))?;
            next = Some(unescape(token));
        }
    }

    /// The multipart uploads of blobs under the prefix begun before `cutoff` and never finished
    /// or aborted, as their keys and upload ids. A put that died leaves one, holding its parts.
    pub(crate) async fn uploads(&self, cutoff: SystemTime) -> io::Result<Vec<(String, String)>> {
        let mut old = Vec::new();
        let mut next: Option<(String, String)> = None;
        loop {
            let mut call = Call::new(Method::GET, self.bucket_path());
            call.query =
                vec![("uploads".into(), String::new()), ("prefix".into(), self.cfg.prefix.clone())];
            if let Some((key, id)) = next.take() {
                call.query.push(("key-marker".into(), key));
                call.query.push(("upload-id-marker".into(), id));
            }
            let (_, body) = self.ok(&call).await?;
            let body = listing(&call, &body)?;
            for upload in elements(body, "Upload") {
                let bad = || io::Error::other(format!("the store listed {upload:?}"));
                let key = unescape(field(upload, "Key").ok_or_else(bad)?);
                let id = unescape(field(upload, "UploadId").ok_or_else(bad)?);
                let begun = field(upload, "Initiated").and_then(iso_date).ok_or_else(bad)?;
                // One of something else, such as a store under a longer prefix, is not ours.
                let blob =
                    key.strip_prefix(&self.cfg.prefix).and_then(|n| n.parse::<BlobId>().ok());
                if begun < cutoff && blob.is_some() {
                    old.push((key, id));
                }
            }
            if field(body, "IsTruncated") != Some("true") {
                return Ok(old);
            }
            let marker = |name| field(body, name).map(unescape);
            let (Some(key), Some(id)) = (marker("NextKeyMarker"), marker("NextUploadIdMarker"))
            else {
                return Err(io::Error::other("the store cut a listing short with no marker"));
            };
            next = Some((key, id));
        }
    }

    /// Aborts the upload `id` to `key`, which frees its parts. One that is gone already is fine.
    pub(crate) async fn abort(&self, key: &str, id: &str) -> io::Result<()> {
        let mut call = Call::new(Method::DELETE, self.key_path(key));
        call.query = vec![("uploadId".into(), id.to_owned())];
        let resp = self.send(&call).await?;
        let status = resp.status();
        if status.is_success() || status == StatusCode::NOT_FOUND {
            return Ok(());
        }
        let body = resp.into_body().collect().await.map(|b| b.to_bytes()).unwrap_or_default();
        Err(failure(&call, status, &body))
    }

    /// Removes `blobs` in [`DELETES_IN_FLIGHT`] requests or more, at most [`DELETE_MAX`] to a
    /// request. One that is gone already is fine.
    pub(crate) async fn delete_many(&self, blobs: &[BlobId]) -> io::Result<()> {
        let per = blobs.len().div_ceil(DELETES_IN_FLIGHT).clamp(1, DELETE_MAX);
        let mut batches = blobs.chunks(per);
        let mut running = futures::stream::FuturesUnordered::new();
        loop {
            while running.len() < DELETES_IN_FLIGHT {
                let Some(batch) = batches.next() else { break };
                running.push(self.delete_batch(batch));
            }
            match running.next().await {
                Some(done) => done?,
                None => return Ok(()),
            }
        }
    }

    /// Removes `blobs` with one DeleteObjects request.
    async fn delete_batch(&self, blobs: &[BlobId]) -> io::Result<()> {
        let mut xml = String::from("<Delete><Quiet>true</Quiet>");
        for b in blobs {
            let key = escape(&format!("{}{b}", self.cfg.prefix));
            let _ = write!(xml, "<Object><Key>{key}</Key></Object>");
        }
        xml.push_str("</Delete>");
        let mut call = Call::new(Method::POST, self.bucket_path());
        call.query = vec![("delete".into(), String::new())];
        call.headers = vec![("content-md5", BASE64.encode(Md5::digest(&xml)))];
        call.body = Payload::Signed(Bytes::from(xml));
        let (_, body) = self.ok(&call).await?;
        // Quiet, the store names only the objects it failed to remove.
        let body = std::str::from_utf8(&body).map_err(io::Error::other)?;
        for e in elements(body, "Error") {
            if field(e, "Code") != Some("NoSuchKey") {
                let key = field(e, "Key").map(unescape).unwrap_or_default();
                let code = field(e, "Code").unwrap_or_default();
                return Err(io::Error::other(format!(
                    "{} {}: {key} {code}",
                    call.method, call.path
                )));
            }
        }
        Ok(())
    }

    /// Begins a multipart upload to `path` and returns its id.
    async fn start_upload(&self, path: &str) -> io::Result<String> {
        let mut start = Call::new(Method::POST, path.to_owned());
        start.query = vec![("uploads".into(), String::new())];
        let (_, body) = self.ok(&start).await?;
        between(&body, "<UploadId>", "</UploadId>")
            .ok_or_else(|| io::Error::other("the store started an upload with no UploadId"))
    }

    /// Finishes the upload `upload` to `path` from its numbered parts and their ETags, or aborts
    /// it if a part failed.
    async fn finish_upload(
        &self,
        path: String,
        upload: String,
        parts: io::Result<Vec<(u64, String)>>,
    ) -> io::Result<()> {
        let parts = match parts {
            Ok(parts) => parts,
            Err(e) => {
                let mut abort = Call::new(Method::DELETE, path);
                abort.query = vec![("uploadId".into(), upload)];
                let _ = self.send(&abort).await;
                return Err(e);
            }
        };
        let mut xml = String::from("<CompleteMultipartUpload>");
        for (n, etag) in parts {
            let _ = write!(xml, "<Part><PartNumber>{n}</PartNumber><ETag>{etag}</ETag></Part>");
        }
        xml.push_str("</CompleteMultipartUpload>");
        let mut done = Call::new(Method::POST, path);
        done.query = vec![("uploadId".into(), upload)];
        done.body = Payload::Signed(Bytes::from(xml));
        // The store answers 200 before it is done, so an error can come in the body.
        let (_, body) = self.ok(&done).await?;
        if between(&body, "<Error>", "</Error>").is_some() {
            return Err(failure(&done, StatusCode::OK, &body));
        }
        Ok(())
    }

    /// Sends one signed request, trying again when the store fails or does not answer, and
    /// returns the status, the headers and the response body as it comes.
    async fn send(&self, call: &Call) -> io::Result<Response> {
        let mut wait = Duration::from_millis(50);
        for _ in 1..ATTEMPTS {
            let req = self.signed(call, SystemTime::now())?;
            match tokio::time::timeout(TIMEOUT, self.client.request(req)).await {
                Ok(Ok(resp)) if !resp.status().is_server_error() => return Ok(resp),
                // A 5xx, a dropped connection or no answer: worth another go.
                _ => {}
            }
            tokio::time::sleep(wait).await;
            wait *= 4;
        }
        let req = self.signed(call, SystemTime::now())?;
        match tokio::time::timeout(TIMEOUT, self.client.request(req)).await {
            Ok(r) => r.map_err(io::Error::other),
            Err(_) => Err(io::Error::new(io::ErrorKind::TimedOut, "the store did not answer")),
        }
    }

    /// `call` as a request signed at `now`.
    fn signed(&self, call: &Call, now: SystemTime) -> io::Result<Request<Full<Bytes>>> {
        let date = AmzDate::of(now);
        let payload = match &call.body {
            Payload::Empty => EMPTY_SHA256.to_owned(),
            Payload::Signed(b) => hex(&Sha256::digest(b)),
            Payload::Unsigned(_) => "UNSIGNED-PAYLOAD".to_owned(),
        };
        let mut headers = vec![
            ("host", self.host.clone()),
            ("x-amz-content-sha256", payload.clone()),
            ("x-amz-date", date.full.clone()),
        ];
        if let Some(r) = &call.range {
            headers.push(("range", r.clone()));
        }
        if let Some(t) = &self.cfg.session_token {
            headers.push(("x-amz-security-token", t.clone()));
        }
        headers.extend(call.headers.iter().cloned());
        headers.sort_unstable_by_key(|(k, _)| *k);
        let auth = authorization(&Signing {
            method: call.method.as_str(),
            path: &call.path,
            query: &call.query,
            headers: &headers,
            payload: &payload,
            date: &date,
            region: &self.cfg.region,
            access_key: &self.cfg.access_key,
            secret_key: &self.cfg.secret_key,
        });
        let query = canonical_query(&call.query);
        let uri = if query.is_empty() {
            format!("{}{}", self.cfg.endpoint, call.path)
        } else {
            format!("{}{}?{query}", self.cfg.endpoint, call.path)
        };
        let mut req = Request::builder().method(call.method.clone()).uri(uri);
        for (k, v) in &headers {
            req = req.header(*k, v);
        }
        let body = match &call.body {
            Payload::Empty => Bytes::new(),
            Payload::Signed(b) | Payload::Unsigned(b) => b.clone(),
        };
        req.header(http::header::AUTHORIZATION, auth)
            .header(http::header::CONTENT_LENGTH, body.len())
            .body(Full::new(body))
            .map_err(io::Error::other)
    }

    /// A request that has to succeed, with its whole response body.
    async fn ok(&self, call: &Call) -> io::Result<(http::HeaderMap, Bytes)> {
        let resp = self.send(call).await?;
        let (parts, body) = resp.into_parts();
        let body = body.collect().await.map_err(io::Error::other)?.to_bytes();
        if !parts.status.is_success() {
            return Err(failure(call, parts.status, &body));
        }
        Ok((parts.headers, body))
    }

    /// Reads `[start, end)` of `blob` into the requests in `group`, which cover it end to end.
    async fn read_span(
        &self,
        blob: BlobId,
        mut group: Vec<ReadReq>,
        start: u64,
        end: u64,
    ) -> io::Result<Vec<ReadReq>> {
        let mut call = Call::new(Method::GET, self.path(blob));
        call.range = Some(format!("bytes={start}-{}", end - 1));
        let resp = self.send(&call).await?;
        let status = resp.status();
        if status == StatusCode::RANGE_NOT_SATISFIABLE {
            return Err(past_end(blob, end));
        }
        if status != StatusCode::PARTIAL_CONTENT && status != StatusCode::OK {
            let body = resp.into_body().collect().await.map(|b| b.to_bytes()).unwrap_or_default();
            return Err(failure(&call, status, &body));
        }
        // A store that ignores the range sends the whole blob with 200, so skip to the start.
        let mut skip = if status == StatusCode::OK { start } else { 0 };
        let (mut req, mut at) = (0, 0);
        let mut body = resp.into_body();
        while let Some(frame) = tokio::time::timeout(TIMEOUT, body.frame())
            .await
            .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "the store stopped sending"))?
        {
            let frame = frame.map_err(io::Error::other)?;
            let Ok(mut data) = frame.into_data() else { continue };
            if skip > 0 {
                let n = usize::try_from(skip).unwrap_or(usize::MAX).min(data.len());
                data = data.slice(n..);
                skip -= n as u64;
            }
            while !data.is_empty() && req < group.len() {
                let buf = &mut group[req].buf;
                let n = (buf.len() - at).min(data.len());
                buf[at..at + n].copy_from_slice(&data[..n]);
                data = data.slice(n..);
                at += n;
                if at == buf.len() {
                    req += 1;
                    at = 0;
                }
            }
            if req == group.len() {
                break;
            }
        }
        // Zero length requests at the tail are full already.
        while req < group.len() && group[req].buf.len() == at {
            req += 1;
            at = 0;
        }
        if req < group.len() {
            return Err(past_end(blob, end));
        }
        Ok(group)
    }

    async fn put_parts(&self, blob: BlobId, src: &Path, size: u64) -> io::Result<()> {
        let path = self.path(blob);
        let upload = self.start_upload(&path).await?;
        let part = PART.max(size.div_ceil(10_000));
        let file = std::sync::Arc::new(File::open(src)?);
        let uploaded = futures::stream::iter(0..size.div_ceil(part))
            .map(|i| {
                let (file, path, upload) = (file.clone(), path.clone(), upload.clone());
                async move {
                    let (offset, len) = (i * part, part.min(size - i * part));
                    let bytes = blocking(move || {
                        let mut buf = vec![0; usize::try_from(len).map_err(io::Error::other)?];
                        file.read_exact_at(&mut buf, offset)?;
                        Ok(Bytes::from(buf))
                    })
                    .await?;
                    let mut call = Call::new(Method::PUT, path);
                    call.query = vec![
                        ("partNumber".into(), (i + 1).to_string()),
                        ("uploadId".into(), upload),
                    ];
                    call.body = Payload::Unsigned(bytes);
                    let (headers, _) = self.ok(&call).await?;
                    let etag = headers
                        .get(http::header::ETAG)
                        .and_then(|v| v.to_str().ok())
                        .ok_or_else(|| io::Error::other("the store took a part with no ETag"))?;
                    Ok::<_, io::Error>((i + 1, etag.to_owned()))
                }
            })
            .buffered(PARTS_IN_FLIGHT)
            .try_collect::<Vec<_>>()
            .await;
        self.finish_upload(path, upload, uploaded).await
    }
}

impl BlobStore for S3Store {
    fn caps(&self) -> BlobCaps {
        BlobCaps { max_io: 16 << 20, ideal_io: 4 << 20, supports_mmap: false }
    }

    fn read_vectored(
        &self,
        blob: BlobId,
        reqs: Vec<ReadReq>,
    ) -> BoxFuture<'_, io::Result<Vec<ReadReq>>> {
        Box::pin(async move {
            let max = self.caps().max_io as u64;
            // Requests that follow on from each other become one GET, up to max_io.
            let mut groups: Vec<(u64, u64, Vec<ReadReq>)> = Vec::new();
            for r in reqs {
                let end = r.offset + r.buf.len() as u64;
                match groups.last_mut() {
                    Some((s, e, g)) if *e == r.offset && end - *s <= max => {
                        *e = end;
                        g.push(r);
                    }
                    _ => groups.push((r.offset, end, vec![r])),
                }
            }
            let filled: Vec<Vec<ReadReq>> = futures::stream::iter(groups)
                .map(|(start, end, g)| async move {
                    if start == end { Ok(g) } else { self.read_span(blob, g, start, end).await }
                })
                .buffered(PARALLEL)
                .try_collect()
                .await?;
            Ok(filled.into_iter().flatten().collect())
        })
    }

    fn put<'a>(&'a self, blob: BlobId, src: &'a Path) -> BoxFuture<'a, io::Result<PutReceipt>> {
        Box::pin(async move {
            let owned = src.to_path_buf();
            let got = blocking(move || hash_file(&owned)).await?;
            if got != blob {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("{} hashes to {got}, not {blob}", src.display()),
                ));
            }
            match self.reuse(blob).await {
                Ok(s) => return Ok(PutReceipt { size: s.size, existed: true }),
                Err(e) if e.kind() == io::ErrorKind::NotFound => {}
                Err(e) => return Err(e),
            }
            let size = std::fs::metadata(src)?.len();
            if size > SINGLE_PUT {
                self.put_parts(blob, src, size).await?;
            } else {
                let owned = src.to_path_buf();
                let bytes = blocking(move || std::fs::read(owned)).await?;
                let mut call = Call::new(Method::PUT, self.path(blob));
                call.body = Payload::Unsigned(Bytes::from(bytes));
                self.ok(&call).await?;
            }
            Ok(PutReceipt { size, existed: false })
        })
    }

    fn stat(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>> {
        Box::pin(async move { Ok(BlobStat { size: self.head(blob).await?.0 }) })
    }

    fn reuse(&self, blob: BlobId) -> BoxFuture<'_, io::Result<BlobStat>> {
        Box::pin(async move {
            let (size, modified) = self.head(blob).await?;
            if modified.is_none_or(|t| t + REFRESH < SystemTime::now()) {
                // As in a directory, a store that will not take the copy, such as one the keys
                // may only read, keeps its times.
                let _ = self.touch(blob, size).await;
            }
            Ok(BlobStat { size })
        })
    }

    fn delete(&self, blob: BlobId) -> BoxFuture<'_, io::Result<()>> {
        Box::pin(async move {
            let call = Call::new(Method::DELETE, self.path(blob));
            let resp = self.send(&call).await?;
            let status = resp.status();
            if status.is_success() || status == StatusCode::NOT_FOUND {
                return Ok(());
            }
            let body = resp.into_body().collect().await.map(|b| b.to_bytes()).unwrap_or_default();
            Err(failure(&call, status, &body))
        })
    }
}

type Response = hyper::Response<hyper::body::Incoming>;

/// One request before it is signed.
struct Call {
    method: Method,
    path: String,
    query: Vec<(String, String)>,
    range: Option<String>,
    /// More headers to send and sign, with lowercase names.
    headers: Vec<(&'static str, String)>,
    body: Payload,
}

impl Call {
    fn new(method: Method, path: String) -> Self {
        Self {
            method,
            path,
            query: Vec::new(),
            range: None,
            headers: Vec::new(),
            body: Payload::Empty,
        }
    }
}

enum Payload {
    Empty,
    /// Hashed into the signature, for small bodies.
    Signed(Bytes),
    /// Left out of the signature, so a big body is not hashed twice.
    Unsigned(Bytes),
}

fn failure(call: &Call, status: StatusCode, body: &[u8]) -> io::Error {
    let kind = match status {
        StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => io::ErrorKind::PermissionDenied,
        StatusCode::NOT_FOUND => io::ErrorKind::NotFound,
        _ => io::ErrorKind::Other,
    };
    let code = between(body, "<Code>", "</Code>").unwrap_or_default();
    io::Error::new(kind, format!("{} {}: {status} {code}", call.method, call.path))
}

fn past_end(blob: BlobId, end: u64) -> io::Error {
    io::Error::new(io::ErrorKind::UnexpectedEof, format!("{blob} ends before byte {end}"))
}

/// The text between `open` and `close` in `body`, for the few XML fields the store sends back.
fn between(body: &[u8], open: &str, close: &str) -> Option<String> {
    let s = std::str::from_utf8(body).ok()?;
    let start = s.find(open)? + open.len();
    let len = s[start..].find(close)?;
    Some(s[start..start + len].to_owned())
}

/// A listing's body as text, or an error if it is not one.
fn listing<'a>(call: &Call, body: &'a [u8]) -> io::Result<&'a str> {
    match std::str::from_utf8(body) {
        Ok(s) if !s.contains("<Error>") => Ok(s),
        _ => Err(failure(call, StatusCode::OK, body)),
    }
}

/// The text of each `<name>` element in `xml`, in order.
fn elements<'a>(xml: &'a str, name: &str) -> impl Iterator<Item = &'a str> {
    let (open, close) = (format!("<{name}>"), format!("</{name}>"));
    let mut rest = xml;
    std::iter::from_fn(move || {
        let start = rest.find(&open)? + open.len();
        let len = rest[start..].find(&close)?;
        let text = &rest[start..start + len];
        rest = &rest[start + len + close.len()..];
        Some(text)
    })
}

/// The text of the first `<name>` element in `xml`.
fn field<'a>(xml: &'a str, name: &str) -> Option<&'a str> {
    elements(xml, name).next()
}

/// `text` with the characters XML takes as markup escaped.
fn escape(text: &str) -> String {
    text.replace('&', "&amp;").replace('<', "&lt;").replace('>', "&gt;")
}

/// `text` with the five XML escapes undone, and the numeric ones MinIO uses for quotes.
fn unescape(text: &str) -> String {
    if !text.contains('&') {
        return text.to_owned();
    }
    let mut out = String::with_capacity(text.len());
    let mut rest = text;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let Some(end) = rest.find(';') else { break };
        let decoded = match &rest[1..end] {
            "amp" => Some('&'),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            n => n
                .strip_prefix("#x")
                .map_or_else(
                    || n.strip_prefix('#')?.parse().ok(),
                    |h| u32::from_str_radix(h, 16).ok(),
                )
                .and_then(char::from_u32),
        };
        match decoded {
            Some(c) => {
                out.push(c);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// A time as a listing gives it, such as `2026-10-15T14:03:09.000Z`. Some stores give it with
/// an offset from UTC in place of the `Z`, such as `+02:00`.
fn iso_date(s: &str) -> Option<SystemTime> {
    let (date, time) = s.split_once('T')?;
    let (time, offset) = match time.find(['Z', '+', '-']) {
        Some(i) if &time[i..] == "Z" => (&time[..i], 0),
        Some(i) => {
            let (h, m) = time[i + 1..].split_once(':')?;
            let secs = i64::from(h.parse::<u32>().ok()? * 3600 + m.parse::<u32>().ok()? * 60);
            (&time[..i], if time[i..].starts_with('-') { -secs } else { secs })
        }
        None => (time, 0),
    };
    let mut d = date.splitn(3, '-').map(str::parse::<u32>);
    let (y, m, day) = (d.next()?.ok()?, d.next()?.ok()?, d.next()?.ok()?);
    let time = time.split('.').next()?;
    let mut t = time.splitn(3, ':').map(str::parse::<u32>);
    let local = at(i64::from(y), m, day, (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?))?;
    let shift = Duration::from_secs(offset.unsigned_abs());
    if offset > 0 { local.checked_sub(shift) } else { local.checked_add(shift) }
}

/// A time as an HTTP header gives it, such as `Thu, 15 Oct 2026 14:03:09 GMT`.
fn http_date(s: &str) -> Option<SystemTime> {
    const MONTHS: [&str; 12] =
        ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
    let mut words = s.split_whitespace().skip(1);
    let day = words.next()?.parse().ok()?;
    let month = words.next()?;
    let month = MONTHS.iter().position(|m| *m == month)?;
    let year = words.next()?.parse().ok()?;
    let mut t = words.next()?.splitn(3, ':').map(str::parse::<u32>);
    let month = u32::try_from(month).ok()? + 1;
    at(year, month, day, (t.next()?.ok()?, t.next()?.ok()?, t.next()?.ok()?))
}

/// The time at a date and an hour, minute and second, in UTC.
fn at(year: i64, month: u32, day: u32, (h, m, s): (u32, u32, u32)) -> Option<SystemTime> {
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) || h > 23 || m > 59 || s > 60 {
        return None;
    }
    let secs = days_from_civil(year, month, day) * 86_400 + i64::from(h * 3600 + m * 60 + s);
    Some(UNIX_EPOCH + Duration::from_secs(u64::try_from(secs).ok()?))
}

/// The days since 1970-01-01 of a calendar date, the other way from [`civil_from_days`].
fn days_from_civil(year: i64, month: u32, day: u32) -> i64 {
    let y = if month <= 2 { year - 1 } else { year };
    let (era, yoe) = (y.div_euclid(400), y.rem_euclid(400));
    let mp = i64::from(if month > 2 { month - 3 } else { month + 9 });
    let doy = (153 * mp + 2) / 5 + i64::from(day) - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

/// A request time in the two forms the signature uses.
struct AmzDate {
    /// `20130524T000000Z`
    full: String,
    /// `20130524`
    day: String,
}

impl AmzDate {
    fn of(t: SystemTime) -> Self {
        let secs = t.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_secs());
        let (y, m, d) = civil_from_days(i64::try_from(secs / 86_400).unwrap_or(0));
        let s = secs % 86_400;
        let day = format!("{y:04}{m:02}{d:02}");
        let full = format!("{day}T{:02}{:02}{:02}Z", s / 3600, s / 60 % 60, s % 60);
        Self { full, day }
    }
}

/// The calendar date of a day count since 1970-01-01, from Howard Hinnant's date algorithms.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = u32::try_from(doy - (153 * mp + 2) / 5 + 1).expect("a day of the month");
    let m = u32::try_from(if mp < 10 { mp + 3 } else { mp - 9 }).expect("a month");
    (yoe + era * 400 + i64::from(m <= 2), m, d)
}

/// What goes into one signature.
struct Signing<'a> {
    method: &'a str,
    /// Already percent encoded.
    path: &'a str,
    query: &'a [(String, String)],
    /// Lowercase names, sorted.
    headers: &'a [(&'a str, String)],
    payload: &'a str,
    date: &'a AmzDate,
    region: &'a str,
    access_key: &'a str,
    secret_key: &'a str,
}

/// The `Authorization` header for a request, as AWS Signature Version 4 has it.
fn authorization(s: &Signing<'_>) -> String {
    let mut canonical = format!("{}\n{}\n{}\n", s.method, s.path, canonical_query(s.query));
    for (k, v) in s.headers {
        let _ = writeln!(canonical, "{k}:{}", v.trim());
    }
    let signed: Vec<&str> = s.headers.iter().map(|(k, _)| *k).collect();
    let signed = signed.join(";");
    let _ = write!(canonical, "\n{signed}\n{}", s.payload);
    let scope = format!("{}/{}/s3/aws4_request", s.date.day, s.region);
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        s.date.full,
        hex(&Sha256::digest(canonical.as_bytes()))
    );
    let mut key = hmac(format!("AWS4{}", s.secret_key).as_bytes(), s.date.day.as_bytes());
    for part in [s.region, "s3", "aws4_request"] {
        key = hmac(&key, part.as_bytes());
    }
    format!(
        "AWS4-HMAC-SHA256 Credential={}/{scope}, SignedHeaders={signed}, Signature={}",
        s.access_key,
        hex(&hmac(&key, to_sign.as_bytes()))
    )
}

fn canonical_query(query: &[(String, String)]) -> String {
    let mut pairs: Vec<(String, String)> =
        query.iter().map(|(k, v)| (encode(k, false), encode(v, false))).collect();
    pairs.sort_unstable();
    let pairs: Vec<String> = pairs.into_iter().map(|(k, v)| format!("{k}={v}")).collect();
    pairs.join("&")
}

/// Percent encodes everything but the unreserved characters, and `/` too if `slash` is set.
fn encode(s: &str, slash: bool) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || b"-._~".contains(&b) || (slash && b == b'/') {
            out.push(char::from(b));
        } else {
            let _ = write!(out, "%{b:02X}");
        }
    }
    out
}

/// HMAC-SHA256, which is short enough to write here rather than take another crate for.
fn hmac(key: &[u8], msg: &[u8]) -> [u8; 32] {
    let mut block = [0u8; 64];
    if key.len() > 64 {
        block[..32].copy_from_slice(&Sha256::digest(key));
    } else {
        block[..key.len()].copy_from_slice(key);
    }
    let pad = |x: u8| block.map(|b| b ^ x);
    let inner = Sha256::new().chain_update(pad(0x36)).chain_update(msg).finalize();
    Sha256::new().chain_update(pad(0x5c)).chain_update(inner).finalize().into()
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_signature_matches_the_aws_example() {
        // "Example: GET Object" from the Signature Version 4 docs for S3.
        let date = AmzDate::of(UNIX_EPOCH + Duration::from_secs(1_369_353_600));
        assert_eq!(date.full, "20130524T000000Z");
        let headers = [
            ("host", "examplebucket.s3.amazonaws.com".to_owned()),
            ("range", "bytes=0-9".to_owned()),
            ("x-amz-content-sha256", EMPTY_SHA256.to_owned()),
            ("x-amz-date", date.full.clone()),
        ];
        let auth = authorization(&Signing {
            method: "GET",
            path: "/test.txt",
            query: &[],
            headers: &headers,
            payload: EMPTY_SHA256,
            date: &date,
            region: "us-east-1",
            access_key: "AKIAIOSFODNN7EXAMPLE",
            secret_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        });
        assert_eq!(
            auth,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    #[test]
    fn hmac_matches_rfc_4231() {
        // Test case 2.
        assert_eq!(
            hex(&hmac(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
        // Test case 6, with a key longer than a block.
        assert_eq!(
            hex(&hmac(&[0xaa; 131], b"Test Using Larger Than Block-Size Key - Hash Key First")),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn days_become_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
        assert_eq!(civil_from_days(20_728), (2026, 10, 2));
    }

    #[test]
    fn dates_go_both_ways() {
        for days in [0, 59, 60, 11_016, 20_728, 20_741, 47_482] {
            let (y, m, d) = civil_from_days(days);
            assert_eq!(days_from_civil(y, m, d), days);
        }
        let t = UNIX_EPOCH + Duration::from_secs(1_791_986_589);
        assert_eq!(iso_date("2026-10-14T14:03:09.000Z"), Some(t));
        assert_eq!(iso_date("2026-10-14T14:03:09Z"), Some(t));
        assert_eq!(iso_date("2026-10-14T16:03:09+02:00"), Some(t));
        assert_eq!(iso_date("2026-10-14T09:03:09.5-05:00"), Some(t));
        assert_eq!(http_date("Wed, 14 Oct 2026 14:03:09 GMT"), Some(t));
        assert_eq!(AmzDate::of(t).full, "20261014T140309Z");
        assert_eq!(iso_date("2026-13-14T14:03:09Z"), None);
        assert_eq!(http_date("Wed, 14 Okt 2026 14:03:09 GMT"), None);
    }

    /// A store under a prefix of its own in the bucket `HB_S3_URL` names, if it names one.
    fn bucket(name: &str) -> Option<S3Store> {
        let url = std::env::var("HB_S3_URL").ok()?;
        let url = format!("{}/{name}-{}", url.trim_end_matches('/'), std::process::id());
        Some(S3Store::new(S3Config::from_url_and_env(&url).unwrap()).unwrap())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn a_blob_copied_onto_itself_gets_a_new_time() {
        let Some(mut store) = bucket("touch") else { return };
        store.copy_max = 5 << 20;
        let dir = crate::tests::scratch();
        // One copied in one request, and one in parts with a short last part.
        for len in [3 << 20, (12 << 20) + 17] {
            let bytes: Vec<u8> = (0..len).map(|i| (i * 7 % 251) as u8).collect();
            let blob = BlobId::of(&bytes);
            let path = dir.join(blob.to_string());
            std::fs::write(&path, &bytes).unwrap();
            store.put(blob, &path).await.unwrap();
            let (_, before) = store.head(blob).await.unwrap();
            // The store gives times to the second.
            tokio::time::sleep(Duration::from_millis(1100)).await;
            store.touch(blob, len as u64).await.unwrap();
            let (size, after) = store.head(blob).await.unwrap();
            assert_eq!(size, len as u64);
            assert!(after.unwrap() > before.unwrap(), "{len}: {before:?} then {after:?}");
            let listed = store.list_blobs().await.unwrap();
            assert!(listed.iter().any(|b| b.blob == blob && b.modified > before.unwrap()));
            let read = vec![ReadReq { offset: 0, buf: vec![0; len] }];
            assert!(store.read_vectored(blob, read).await.unwrap()[0].buf == bytes);
            store.delete(blob).await.unwrap();
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn uploads_never_finished_are_found_and_aborted() {
        let Some(store) = bucket("uploads") else { return };
        let blob = BlobId::of(b"never finished");
        let key = format!("{}{blob}", store.cfg.prefix);
        let id = store.start_upload(&store.path(blob)).await.unwrap();
        let other = format!("{}not-a-blob", store.cfg.prefix);
        let other_id = store.start_upload(&store.key_path(&other)).await.unwrap();
        let soon = SystemTime::now() + Duration::from_secs(60);
        assert_eq!(store.uploads(soon).await.unwrap(), [(key.clone(), id.clone())]);
        let ago = SystemTime::now() - Duration::from_secs(60);
        assert!(store.uploads(ago).await.unwrap().is_empty());
        store.abort(&key, &id).await.unwrap();
        store.abort(&key, &id).await.unwrap();
        assert!(store.uploads(soon).await.unwrap().is_empty());
        store.abort(&other, &other_id).await.unwrap();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn many_blobs_go_in_few_requests() {
        let Some(store) = bucket("deletes") else { return };
        let dir = crate::tests::scratch();
        // More than one request takes, and one blob that stays.
        let blobs: Vec<BlobId> = futures::stream::iter(0..=DELETE_MAX + 2)
            .map(|i| {
                let (store, path) = (&store, dir.join(i.to_string()));
                async move {
                    std::fs::write(&path, i.to_string()).unwrap();
                    let blob = BlobId::of(i.to_string().as_bytes());
                    store.put(blob, &path).await.unwrap();
                    blob
                }
            })
            .buffer_unordered(16)
            .collect()
            .await;
        let (kept, gone) = blobs.split_last().unwrap();
        let mut doomed = gone.to_vec();
        doomed.push(BlobId::of(b"never stored"));
        store.delete_many(&doomed).await.unwrap();
        let listed: Vec<BlobId> =
            store.list_blobs().await.unwrap().iter().map(|b| b.blob).collect();
        assert_eq!(listed, [*kept]);
        store.delete(*kept).await.unwrap();
    }

    #[test]
    fn listings_are_read() {
        let xml = "<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>p/a</Key>\
                   <Size>3</Size></Contents><Contents><Key>p/b&amp;c</Key><Size>4</Size>\
                   </Contents><NextContinuationToken>x&#43;y</NextContinuationToken>\
                   </ListBucketResult>";
        let keys: Vec<String> =
            elements(xml, "Contents").map(|c| unescape(field(c, "Key").unwrap())).collect();
        assert_eq!(keys, ["p/a", "p/b&c"]);
        assert_eq!(field(xml, "IsTruncated"), Some("true"));
        assert_eq!(unescape(field(xml, "NextContinuationToken").unwrap()), "x+y");
        assert_eq!(unescape("&#34;e&#x22; &amp;&bogus; &"), "\"e\" &&bogus; &");
        // An upload's id is not taken for the upload.
        let up = "<Upload><Key>k</Key><UploadId>u</UploadId></Upload>";
        assert_eq!(elements(up, "Upload").count(), 1);
        assert_eq!(field(up, "UploadId"), Some("u"));
    }

    #[test]
    fn query_values_are_encoded_and_sorted() {
        let q = [
            ("uploadId".to_owned(), "a+b/c=".to_owned()),
            ("partNumber".to_owned(), "2".to_owned()),
        ];
        assert_eq!(canonical_query(&q), "partNumber=2&uploadId=a%2Bb%2Fc%3D");
        assert_eq!(canonical_query(&[("uploads".to_owned(), String::new())]), "uploads=");
    }

    #[test]
    fn urls_give_bucket_and_prefix() {
        let cfg = S3Config {
            endpoint: "http://127.0.0.1:9000".into(),
            bucket: "b".into(),
            prefix: "blobs/".into(),
            region: "us-east-1".into(),
            access_key: "k".into(),
            secret_key: "s".into(),
            session_token: None,
        };
        let store = S3Store::new(cfg.clone()).unwrap();
        let blob = BlobId::from(blake3::hash(b"x"));
        assert_eq!(store.path(blob), format!("/b/blobs/{blob}"));
        assert_eq!(store.host, "127.0.0.1:9000");
        let https = S3Config { endpoint: "https://s3.amazonaws.com".into(), ..cfg };
        assert_eq!(S3Store::new(https).unwrap_err().kind(), io::ErrorKind::Unsupported);
    }
}
