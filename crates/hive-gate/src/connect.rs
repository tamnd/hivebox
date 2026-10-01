//! Connect, the HTTP protocol for the same API, so `curl` and a browser can call the gate.
//!
//! A Connect call is turned into a gRPC call and goes through the gate the same way, so keys,
//! placement and routing are the same for both. Unary calls are `application/json` or
//! `application/proto` with the message as the body. Streaming calls are
//! `application/connect+json` or `application/connect+proto` with each message in a 5 byte
//! envelope, the same as gRPC's, and a last envelope carrying the status where gRPC has trailers.
//! JSON is read and written with the descriptors from `hive-proto`, in the proto3 JSON mapping.
//!
//! Not taken yet: GET for unary calls, and compressed requests.

use std::pin::Pin;
use std::sync::LazyLock;
use std::task::{Context, Poll, ready};

use base64::Engine;
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http_body::Frame;
use http_body_util::{BodyExt, Full, LengthLimitError, Limited};
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, MethodDescriptor};
use serde_json::{Value, json};
use tonic::body::Body;
use tonic::codegen::{Service, http};
use tonic::{Code, Status};

use crate::{Gate, MAX_REQUEST};

/// The biggest answer a unary call is let collect before it is turned into one body.
const MAX_ANSWER: usize = 64 << 20;

/// The flag on the envelope that ends a Connect stream.
const END_STREAM: u8 = 2;

static POOL: LazyLock<Option<DescriptorPool>> =
    LazyLock::new(|| DescriptorPool::decode(hive_proto::FILE_DESCRIPTOR_SET).ok());

/// How a Connect call is encoded, from its content type.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Codec {
    json: bool,
    stream: bool,
}

impl Codec {
    /// The codec for a request, or `None` when it is not a Connect call.
    pub(crate) fn of(headers: &http::HeaderMap) -> Option<Self> {
        let ct = headers.get(http::header::CONTENT_TYPE)?.to_str().ok()?;
        let ct = ct.split(';').next()?.trim();
        let (json, stream) = match ct {
            "application/json" => (true, false),
            "application/proto" => (false, false),
            "application/connect+json" => (true, true),
            "application/connect+proto" => (false, true),
            _ => return None,
        };
        Some(Self { json, stream })
    }

    fn content_type(self) -> &'static str {
        match (self.json, self.stream) {
            (true, false) => "application/json",
            (false, false) => "application/proto",
            (true, true) => "application/connect+json",
            (false, true) => "application/connect+proto",
        }
    }

    /// What the caller gets for `s`: an HTTP status and a JSON error for a unary call, and for a
    /// stream a last envelope with the error in it.
    fn error(self, s: &Status) -> http::Response<Body> {
        if self.stream {
            return answer(http::StatusCode::OK, self.content_type(), end_of(Some(s)));
        }
        let body = serde_json::to_vec(&error_json(s)).unwrap_or_default();
        answer(http_status(s.code()), "application/json", body.into())
    }
}

/// Answers the Connect call `req`, which [`Codec::of`] said is one.
pub(crate) async fn call(
    gate: Gate,
    codec: Codec,
    req: http::Request<Body>,
) -> http::Response<Body> {
    match serve(gate, codec, req).await {
        Ok(r) => r,
        Err(s) => codec.error(&s),
    }
}

async fn serve(
    mut gate: Gate,
    codec: Codec,
    req: http::Request<Body>,
) -> Result<http::Response<Body>, Status> {
    if req.method() != http::Method::POST {
        return Err(Status::unimplemented("the gate takes Connect calls as POST only"));
    }
    let method = method(req.uri().path())?;
    let streams = method.is_client_streaming() || method.is_server_streaming();
    if streams != codec.stream {
        let want = if streams { "a streaming" } else { "a unary" };
        return Err(Status::invalid_argument(format!(
            "{} is {want} method, and {} does not fit it",
            method.full_name(),
            codec.content_type()
        )));
    }
    let encoding = if codec.stream { "connect-content-encoding" } else { "content-encoding" };
    if req.headers().get(encoding).is_some_and(|v| v != "identity") {
        return Err(Status::unimplemented("the gate does not take compressed requests yet"));
    }
    let (head, body) = req.into_parts();
    if !codec.stream {
        let (bytes, _) = collect(body, MAX_REQUEST).await?;
        let msg = if codec.json { json_to_proto(&method.input(), &bytes)? } else { bytes };
        let resp = send(&mut gate, head, Body::new(Full::new(envelope(0, &msg)))).await?;
        let (parts, body) = resp.into_parts();
        let early = Status::from_header_map(&parts.headers);
        let (data, trailers) = collect(body, MAX_ANSWER).await?;
        let status = early.or_else(|| trailers.as_ref().and_then(Status::from_header_map));
        let status = status.unwrap_or_else(|| Status::internal("the answer came without a status"));
        if status.code() != Code::Ok {
            return Err(status);
        }
        let mut data = BytesMut::from(&data[..]);
        let msg = next(&mut data)?.ok_or_else(|| Status::internal("the answer has no message"))?;
        let out = if codec.json { proto_to_json(&method.output(), &msg)? } else { msg };
        return Ok(answer(http::StatusCode::OK, codec.content_type(), out));
    }
    let body = if codec.json {
        Body::new(Envelopes::new(body, Some(method.input()), Side::Request))
    } else {
        body
    };
    let resp = send(&mut gate, head, body).await?;
    let (parts, body) = resp.into_parts();
    let out = match Status::from_header_map(&parts.headers) {
        // No messages, only a status, the way gRPC fails a call before it starts.
        Some(s) => Body::new(Full::new(end_of(Some(&s)))),
        None => Body::new(Envelopes::new(body, codec.json.then(|| method.output()), Side::Answer)),
    };
    Ok(http::Response::builder()
        .header(http::header::CONTENT_TYPE, codec.content_type())
        .body(out)
        .unwrap_or_default())
}

/// The method at `path`, as `/hivebox.v1.Cells/Create`.
fn method(path: &str) -> Result<MethodDescriptor, Status> {
    let none = || Status::unimplemented(format!("no method at {path}"));
    let (service, name) =
        path.strip_prefix('/').and_then(|p| p.split_once('/')).ok_or_else(none)?;
    let pool = POOL.as_ref().ok_or_else(|| Status::internal("the gate cannot read its protos"))?;
    let service = pool.get_service_by_name(service).ok_or_else(none)?;
    service.methods().find(|m| m.name() == name).ok_or_else(none)
}

/// Sends the call on as gRPC through the gate. The Connect headers go, the rest stay.
async fn send(
    gate: &mut Gate,
    head: http::request::Parts,
    body: Body,
) -> Result<http::Response<Body>, Status> {
    let mut req = http::Request::builder()
        .method(http::Method::POST)
        .uri(head.uri)
        .version(http::Version::HTTP_2)
        .body(body)
        .map_err(|e| Status::internal(e.to_string()))?;
    let headers = req.headers_mut();
    for (name, value) in &head.headers {
        let n = name.as_str();
        let drop = n.starts_with("connect-")
            || matches!(
                n,
                "content-type"
                    | "content-length"
                    | "content-encoding"
                    | "accept-encoding"
                    | "transfer-encoding"
                    | "connection"
                    | "keep-alive"
                    | "host"
                    | "te"
                    | "upgrade"
            );
        if !drop {
            headers.append(name, value.clone());
        }
    }
    headers.insert(http::header::CONTENT_TYPE, http::HeaderValue::from_static("application/grpc"));
    headers.insert(http::header::TE, http::HeaderValue::from_static("trailers"));
    let timeout = head.headers.get("connect-timeout-ms").and_then(|v| v.to_str().ok());
    if let Some(ms) = timeout.and_then(|v| v.parse::<u64>().ok()) {
        // gRPC allows at most 8 digits.
        let v = format!("{}m", ms.min(99_999_999));
        if let Ok(v) = http::HeaderValue::from_str(&v) {
            headers.insert("grpc-timeout", v);
        }
    }
    match gate.call(req).await {
        Ok(r) => Ok(r),
        Err(never) => match never {},
    }
}

/// All of `body` and its trailers, if it is no bigger than `limit`.
async fn collect(body: Body, limit: usize) -> Result<(Bytes, Option<http::HeaderMap>), Status> {
    let all = Limited::new(body, limit).collect().await.map_err(|e| {
        if e.is::<LengthLimitError>() {
            return Status::resource_exhausted(format!("a message over {limit} bytes"));
        }
        match e.downcast::<Status>() {
            Ok(s) => *s,
            Err(e) => Status::cancelled(e.to_string()),
        }
    })?;
    let trailers = all.trailers().cloned();
    Ok((all.to_bytes(), trailers))
}

fn answer(
    status: http::StatusCode,
    content_type: &'static str,
    body: Bytes,
) -> http::Response<Body> {
    http::Response::builder()
        .status(status)
        .header(http::header::CONTENT_TYPE, content_type)
        .body(Body::new(Full::new(body)))
        .unwrap_or_default()
}

fn envelope(flags: u8, msg: &[u8]) -> Bytes {
    let mut out = BytesMut::with_capacity(5 + msg.len());
    out.put_u8(flags);
    out.put_u32(u32::try_from(msg.len()).unwrap_or(u32::MAX));
    out.put_slice(msg);
    out.freeze()
}

/// The next whole message in `buf`, taken out of it, and its flags.
fn next_flagged(buf: &mut BytesMut) -> Option<(u8, Bytes)> {
    let head = buf.get(..5)?;
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    if buf.len() < 5 + len {
        return None;
    }
    let flags = buf.get_u8();
    buf.advance(4);
    Some((flags, buf.split_to(len).freeze()))
}

/// The next whole message in `buf`, refusing compressed ones.
fn next(buf: &mut BytesMut) -> Result<Option<Bytes>, Status> {
    match next_flagged(buf) {
        Some((flags, _)) if flags & 1 != 0 => {
            Err(Status::unimplemented("the gate does not take compressed messages yet"))
        }
        other => Ok(other.map(|(_, m)| m)),
    }
}

/// The envelope that ends a stream: empty when the call went well, else the error.
fn end_of(status: Option<&Status>) -> Bytes {
    let end = match status {
        Some(s) if s.code() == Code::Ok => json!({}),
        Some(s) => json!({ "error": error_json(s) }),
        None => {
            json!({ "error": error_json(&Status::internal("the answer ended without a status")) })
        }
    };
    envelope(END_STREAM, &serde_json::to_vec(&end).unwrap_or_default())
}

fn json_to_proto(desc: &MessageDescriptor, json: &[u8]) -> Result<Bytes, Status> {
    let bad = |e: serde_json::Error| {
        Status::invalid_argument(format!("the body is not a {} in JSON: {e}", desc.full_name()))
    };
    let mut de = serde_json::Deserializer::from_slice(json);
    let msg = DynamicMessage::deserialize(desc.clone(), &mut de).map_err(bad)?;
    de.end().map_err(bad)?;
    Ok(msg.encode_to_vec().into())
}

fn proto_to_json(desc: &MessageDescriptor, msg: &[u8]) -> Result<Bytes, Status> {
    let msg = DynamicMessage::decode(desc.clone(), msg).map_err(|e| {
        Status::internal(format!("an answer that is not a {}: {e}", desc.full_name()))
    })?;
    serde_json::to_vec(&msg).map(Bytes::from).map_err(|e| Status::internal(e.to_string()))
}

/// Which way an [`Envelopes`] body goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// Connect messages from the caller, to gRPC.
    Request,
    /// gRPC messages and trailers from a comb, to Connect with the status last.
    Answer,
}

/// A stream of messages turned from one protocol to the other. Without a descriptor the messages
/// pass as they are, and only the answer's trailers become the last envelope.
struct Envelopes {
    inner: Body,
    json: Option<MessageDescriptor>,
    side: Side,
    buf: BytesMut,
    done: bool,
}

impl Envelopes {
    fn new(inner: Body, json: Option<MessageDescriptor>, side: Side) -> Self {
        Self { inner, json, side, buf: BytesMut::new(), done: false }
    }

    /// The next whole message in the buffer, turned.
    fn turn(&mut self) -> Result<Option<Bytes>, Status> {
        let Some(desc) = &self.json else { return Ok(None) };
        loop {
            let Some((flags, msg)) = next_flagged(&mut self.buf) else { return Ok(None) };
            if flags & 1 != 0 {
                return Err(Status::unimplemented(
                    "the gate does not take compressed messages yet",
                ));
            }
            if flags & END_STREAM != 0 {
                continue;
            }
            let out = match self.side {
                Side::Request => json_to_proto(desc, &msg)?,
                Side::Answer => proto_to_json(desc, &msg)?,
            };
            return Ok(Some(envelope(0, &out)));
        }
    }

    /// Ends the stream with `status`: the last envelope on an answer, an error on a request.
    fn end(&mut self, status: Option<Status>) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        self.done = true;
        match (self.side, status) {
            (Side::Answer, s) => Poll::Ready(Some(Ok(Frame::data(end_of(s.as_ref()))))),
            (Side::Request, Some(s)) if s.code() != Code::Ok => Poll::Ready(Some(Err(s))),
            (Side::Request, _) => Poll::Ready(None),
        }
    }
}

impl http_body::Body for Envelopes {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        let this = &mut *self;
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            match this.turn() {
                Ok(Some(out)) => return Poll::Ready(Some(Ok(Frame::data(out)))),
                Ok(None) => {}
                Err(s) => return this.end(Some(s)),
            }
            match ready!(Pin::new(&mut this.inner).poll_frame(cx)) {
                Some(Ok(frame)) => match frame.into_data() {
                    Ok(d) if this.json.is_some() => this.buf.extend_from_slice(&d),
                    Ok(d) => return Poll::Ready(Some(Ok(Frame::data(d)))),
                    Err(frame) => {
                        let status =
                            frame.into_trailers().ok().and_then(|t| Status::from_header_map(&t));
                        return this.end(status);
                    }
                },
                Some(Err(s)) => return this.end(Some(s)),
                None => return this.end(None),
            }
        }
    }
}

/// The JSON for an error, as Connect has it. The details keep their protobuf bytes, and the
/// hivebox `ErrorInfo` is spelled out in `debug` for people reading it.
fn error_json(s: &Status) -> Value {
    let mut e = json!({ "code": code_name(s.code()) });
    if !s.message().is_empty() {
        e["message"] = s.message().into();
    }
    let details = RpcStatus::decode(s.details()).map(|r| r.details).unwrap_or_default();
    if !details.is_empty() {
        let b64 = base64::engine::general_purpose::STANDARD_NO_PAD;
        let list: Vec<Value> = details
            .iter()
            .map(|any| {
                let name = any.type_url.rsplit('/').next().unwrap_or_default();
                let mut d = json!({ "type": name, "value": b64.encode(&any.value) });
                if name == "google.rpc.ErrorInfo"
                    && let Ok(i) = ErrorInfo::decode(&any.value[..])
                {
                    d["debug"] =
                        json!({ "reason": i.reason, "domain": i.domain, "metadata": i.metadata });
                }
                d
            })
            .collect();
        e["details"] = list.into();
    }
    e
}

/// `google.rpc.Status`, which gRPC carries in `grpc-status-details-bin`.
#[derive(Clone, PartialEq, Message)]
struct RpcStatus {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<Any>,
}

/// `google.protobuf.Any`.
#[derive(Clone, PartialEq, Message)]
struct Any {
    #[prost(string, tag = "1")]
    type_url: String,
    #[prost(bytes = "vec", tag = "2")]
    value: Vec<u8>,
}

/// `google.rpc.ErrorInfo`.
#[derive(Clone, PartialEq, Message)]
struct ErrorInfo {
    #[prost(string, tag = "1")]
    reason: String,
    #[prost(string, tag = "2")]
    domain: String,
    #[prost(map = "string, string", tag = "3")]
    metadata: std::collections::HashMap<String, String>,
}

fn code_name(code: Code) -> &'static str {
    match code {
        Code::Ok => "ok",
        Code::Cancelled => "canceled",
        Code::Unknown => "unknown",
        Code::InvalidArgument => "invalid_argument",
        Code::DeadlineExceeded => "deadline_exceeded",
        Code::NotFound => "not_found",
        Code::AlreadyExists => "already_exists",
        Code::PermissionDenied => "permission_denied",
        Code::ResourceExhausted => "resource_exhausted",
        Code::FailedPrecondition => "failed_precondition",
        Code::Aborted => "aborted",
        Code::OutOfRange => "out_of_range",
        Code::Unimplemented => "unimplemented",
        Code::Internal => "internal",
        Code::Unavailable => "unavailable",
        Code::DataLoss => "data_loss",
        Code::Unauthenticated => "unauthenticated",
    }
}

/// The HTTP status Connect gives each code on a unary call.
fn http_status(code: Code) -> http::StatusCode {
    let n = match code {
        Code::Ok => 200,
        Code::Cancelled => 499,
        Code::InvalidArgument | Code::FailedPrecondition | Code::OutOfRange => 400,
        Code::DeadlineExceeded => 504,
        Code::NotFound => 404,
        Code::AlreadyExists | Code::Aborted => 409,
        Code::PermissionDenied => 403,
        Code::ResourceExhausted => 429,
        Code::Unimplemented => 501,
        Code::Unavailable => 503,
        Code::Unauthenticated => 401,
        Code::Unknown | Code::Internal | Code::DataLoss => 500,
    };
    http::StatusCode::from_u16(n).unwrap_or(http::StatusCode::INTERNAL_SERVER_ERROR)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_method_is_found_and_json_goes_both_ways() {
        let m = method("/hivebox.v1.Cells/Get").unwrap();
        assert!(!m.is_server_streaming());
        assert!(method("/hivebox.v1.Cells/Create").unwrap().is_server_streaming());
        assert!(method("/hivebox.v1.Exec/Run").is_ok());
        assert!(method("/hivebox.v1.Files/Write").unwrap().is_client_streaming());
        assert_eq!(method("/hivebox.v1.Cells/Nope").unwrap_err().code(), Code::Unimplemented);
        assert_eq!(method("nope").unwrap_err().code(), Code::Unimplemented);
        let msg = json_to_proto(&m.input(), br#"{"id": "abc"}"#).unwrap();
        let req = hive_proto::v1::GetCellRequest::decode(msg).unwrap();
        assert_eq!(req.id, "abc");
        let bad = json_to_proto(&m.input(), br#"{"nope": 1}"#).unwrap_err();
        assert_eq!(bad.code(), Code::InvalidArgument);
        assert!(json_to_proto(&m.input(), br#"{"id": "a"} x"#).is_err(), "trailing bytes");
        let cell = hive_proto::v1::Cell { id: "c1".into(), node: "3".into(), ..Default::default() };
        let json = proto_to_json(&m.output(), &cell.encode_to_vec()).unwrap();
        let v: Value = serde_json::from_slice(&json).unwrap();
        assert_eq!(v["id"], "c1");
        assert_eq!(v["node"], "3");
    }

    #[test]
    fn a_hivebox_error_keeps_its_reason() {
        let e = hive_types::Error::new(hive_types::Reason::CapacityUnavailable, "full");
        let s = hive_proto::convert::error_to_status(&e);
        let v = error_json(&s);
        assert_eq!(v["code"], code_name(s.code()));
        assert_eq!(v["message"], "full");
        assert_eq!(v["details"][0]["type"], "google.rpc.ErrorInfo");
        assert_eq!(v["details"][0]["debug"]["reason"], "CAPACITY_UNAVAILABLE");
        assert_eq!(http_status(Code::NotFound), http::StatusCode::NOT_FOUND);
    }

    #[test]
    fn envelopes_are_taken_whole() {
        let mut buf = BytesMut::new();
        buf.extend_from_slice(&envelope(0, b"abc"));
        buf.extend_from_slice(&envelope(0, b"de")[..4]);
        assert_eq!(next(&mut buf).unwrap().unwrap(), Bytes::from_static(b"abc"));
        assert!(next(&mut buf).unwrap().is_none());
        buf.extend_from_slice(&[2, b'd', b'e']);
        assert_eq!(next(&mut buf).unwrap().unwrap(), Bytes::from_static(b"de"));
        buf.extend_from_slice(&envelope(1, b"z"));
        assert_eq!(next(&mut buf).unwrap_err().code(), Code::Unimplemented);
        let end: Value =
            serde_json::from_slice(&end_of(Some(&Status::new(Code::Ok, "")))[5..]).unwrap();
        assert_eq!(end, json!({}));
    }
}
