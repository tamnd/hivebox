//! [`S3Store`], a [`BlobStore`] in an S3 bucket or anything that speaks its API, such as MinIO,
//! Ceph RGW or R2.
//!
//! Requests are signed with AWS Signature Version 4 here rather than through an SDK, since a
//! blob store needs five calls and the SDKs bring a large tree with them. Reads are ranged GETs,
//! with neighbouring requests of a batch merged into one, and up to [`PARALLEL`] in flight. Blobs
//! up to [`SINGLE_PUT`] go up in one PUT and bigger ones as a multipart upload.
//!
//! Only plain `http://` endpoints work for now, since the tree has no TLS stack yet. That covers a
//! store inside the cluster network or a local proxy in front of a remote one.

use std::fmt::Write as _;
use std::fs::File;
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use futures::future::BoxFuture;
use futures::{StreamExt, TryStreamExt};
use http_body_util::{BodyExt, Full};
use hyper::{Method, Request, StatusCode, Uri, http};
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use sha2::{Digest, Sha256};

use crate::BlobId;
use crate::store::{BlobCaps, BlobStat, BlobStore, PutReceipt, ReadReq, blocking, hash_file};

/// Ranged GETs or part uploads in flight for one call.
pub const PARALLEL: usize = 16;

/// The biggest blob that goes up in one PUT. Bigger ones are uploaded in parts.
pub const SINGLE_PUT: u64 = 64 << 20;

/// The size of each part of a multipart upload, unless the blob needs more than S3's 10,000 parts.
const PART: u64 = 16 << 20;

/// Parts uploaded at once. Each holds a part in memory.
const PARTS_IN_FLIGHT: usize = 4;

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
        Ok(Self { cfg, host, client })
    }

    /// The path of `blob` in the bucket, percent encoded.
    fn path(&self, blob: BlobId) -> String {
        let key = format!("{}{blob}", self.cfg.prefix);
        format!("/{}/{}", encode(&self.cfg.bucket, false), encode(&key, true))
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
        let mut start = Call::new(Method::POST, path.clone());
        start.query = vec![("uploads".into(), String::new())];
        let (_, body) = self.ok(&start).await?;
        let upload = between(&body, "<UploadId>", "</UploadId>")
            .ok_or_else(|| io::Error::other("the store started an upload with no UploadId"))?;
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
        let parts = match uploaded {
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
            match self.stat(blob).await {
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
        Box::pin(async move {
            let call = Call::new(Method::HEAD, self.path(blob));
            let resp = self.send(&call).await?;
            match resp.status() {
                StatusCode::NOT_FOUND => Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    format!("{blob} is not in the store"),
                )),
                s if s.is_success() => resp
                    .headers()
                    .get(http::header::CONTENT_LENGTH)
                    .and_then(|v| v.to_str().ok()?.parse().ok())
                    .map(|size| BlobStat { size })
                    .ok_or_else(|| io::Error::other("the store gave no Content-Length")),
                s => Err(failure(&call, s, b"")),
            }
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
    body: Payload,
}

impl Call {
    fn new(method: Method, path: String) -> Self {
        Self { method, path, query: Vec::new(), range: None, body: Payload::Empty }
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
