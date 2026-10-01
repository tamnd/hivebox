//! Calls about one cell, forwarded to its comb without decoding them.
//!
//! Every Exec and Files request names its cell in field 1, either as the id or inside a message
//! whose field 1 is the id, like `ProcessInput.start.cell_id`. The gate reads just enough of the
//! first gRPC message to find it and passes the call on as bytes, so a large file write or a long
//! exec stream costs the gate a copy and nothing more.

use std::pin::Pin;
use std::task::{Context, Poll};

use bytes::{Buf, Bytes, BytesMut};
use hive_proto::convert;
use hive_types::{CellId, Error, Reason};
use http_body::{Body as _, Frame, SizeHint};
use tonic::Status;
use tonic::body::Body;
use tonic::codegen::http;
use tower::ServiceExt;

use crate::nodes::Nodes;

/// The most of the first message read to find the cell id. Every request puts it first, so this
/// is far more than it ever takes.
const PEEK: usize = 4096;

/// Sends `req` to the comb that owns the cell it names and returns the comb's answer.
pub async fn forward(nodes: &Nodes, req: http::Request<Body>) -> http::Response<Body> {
    match route(nodes, req).await {
        Ok(r) => r,
        Err(s) => s.into_http(),
    }
}

async fn route(nodes: &Nodes, req: http::Request<Body>) -> Result<http::Response<Body>, Status> {
    let (head, mut body) = req.into_parts();
    let first = peek(&mut body).await?;
    let id = cell_of(&first)?;
    let channel = nodes.owner(id).map_err(|e| convert::error_to_status(&e))?;
    let body = Body::new(Prepend { first: Some(first), rest: body });
    let resp = channel.oneshot(http::Request::from_parts(head, body)).await.map_err(|e| {
        let e = Error::new(Reason::DroneUnreachable, format!("node {}: {e}", id.node()));
        convert::error_to_status(&e)
    })?;
    Ok(resp)
}

/// Reads the start of the body: the 5 byte gRPC frame header and up to [`PEEK`] bytes of the
/// message, or less when the message is shorter. It never waits for a second message, which a
/// streaming client may only send after the first answer.
async fn peek(body: &mut Body) -> Result<Bytes, Status> {
    let mut buf = BytesMut::new();
    loop {
        let want = match buf.get(..5) {
            None => 5,
            Some(h) => 5 + PEEK.min(u32::from_be_bytes([h[1], h[2], h[3], h[4]]) as usize),
        };
        if buf.len() >= want {
            return Ok(buf.freeze());
        }
        let frame = std::future::poll_fn(|cx| Pin::new(&mut *body).poll_frame(cx)).await;
        match frame {
            Some(Ok(f)) => match f.into_data() {
                Ok(d) => buf.extend_from_slice(&d),
                Err(_) => return Err(Status::invalid_argument("a request with trailers")),
            },
            Some(Err(e)) => return Err(Status::cancelled(e.to_string())),
            None if buf.len() >= 5 => return Ok(buf.freeze()),
            None => return Err(Status::invalid_argument("the request has no message")),
        }
    }
}

/// The cell the first message names.
fn cell_of(first: &[u8]) -> Result<CellId, Status> {
    let bad = || Status::invalid_argument("the request names no cell");
    if first.first() == Some(&1) {
        return Err(Status::unimplemented("the gate does not take compressed requests yet"));
    }
    let len = u32::from_be_bytes(first.get(1..5).ok_or_else(bad)?.try_into().map_err(|_| bad())?);
    let msg = &first[5..first.len().min(5 + len as usize)];
    let field = field_one(msg).ok_or_else(bad)?;
    if let Some(id) = parse(field) {
        return Ok(id);
    }
    field_one(field).and_then(parse).ok_or_else(bad)
}

fn parse(bytes: &[u8]) -> Option<CellId> {
    std::str::from_utf8(bytes).ok()?.parse().ok()
}

/// The bytes of field 1 when it is length delimited, the way strings and messages are.
fn field_one(mut msg: &[u8]) -> Option<&[u8]> {
    while !msg.is_empty() {
        let key = varint(&mut msg)?;
        let (field, wire) = (key >> 3, key & 7);
        match wire {
            0 => {
                varint(&mut msg)?;
            }
            1 | 5 => {
                let n = if wire == 1 { 8 } else { 4 };
                msg = msg.get(n..)?;
            }
            2 => {
                let n = usize::try_from(varint(&mut msg)?).ok()?;
                let value = msg.get(..n)?;
                if field == 1 {
                    return Some(value);
                }
                msg = &msg[n..];
            }
            _ => return None,
        }
    }
    None
}

fn varint(buf: &mut &[u8]) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&b, rest) = buf.split_first()?;
        *buf = rest;
        v |= u64::from(b & 0x7f) << shift;
        if b < 0x80 {
            return Some(v);
        }
    }
    None
}

/// The bytes already read, then the rest of the body.
struct Prepend {
    first: Option<Bytes>,
    rest: Body,
}

impl http_body::Body for Prepend {
    type Data = Bytes;
    type Error = Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Status>>> {
        if let Some(first) = self.first.take() {
            return Poll::Ready(Some(Ok(Frame::data(first))));
        }
        Pin::new(&mut self.rest).poll_frame(cx)
    }

    fn is_end_stream(&self) -> bool {
        self.first.is_none() && self.rest.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        let mut hint = self.rest.size_hint();
        let n = self.first.as_ref().map_or(0, |b| b.remaining() as u64);
        // Upper first: a lower bound past the upper one panics, and an exact size has them equal.
        if let Some(upper) = hint.upper() {
            hint.set_upper(upper + n);
        }
        hint.set_lower(hint.lower() + n);
        hint
    }
}

#[cfg(test)]
mod tests {
    use hive_proto::v1;
    use prost::Message;

    use super::*;

    fn framed(msg: &impl Message) -> Vec<u8> {
        let body = msg.encode_to_vec();
        let mut out = vec![0];
        out.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
        out.extend_from_slice(&body);
        out
    }

    #[test]
    fn the_cell_is_found_in_every_kind_of_request() {
        let id = CellId::new(1, 42, 3, 99, 7).unwrap();
        let s = id.to_string();
        let run = v1::RunRequest {
            argv: vec!["x".repeat(10_000)],
            env: [("A".into(), "B".into())].into(),
            cell_id: s.clone(),
            ..Default::default()
        };
        let start = v1::ProcessInput {
            input: Some(v1::process_input::Input::Start(v1::ProcessStart {
                cell_id: s.clone(),
                cwd: "/w".into(),
                ..Default::default()
            })),
        };
        let write = v1::WriteFileChunk {
            part: Some(v1::write_file_chunk::Part::Header(v1::WriteFileHeader {
                cell_id: s.clone(),
                path: "/a".into(),
                mode: 0o600,
                ..Default::default()
            })),
        };
        let session = v1::SessionRunRequest {
            session: Some(v1::SessionRef { cell_id: s.clone(), id: "7".into() }),
            command: "ls".into(),
            ..Default::default()
        };
        let signal = v1::SignalRequest { cell_id: s, pid: 9, signal: 15 };
        for frame in
            [framed(&run), framed(&start), framed(&write), framed(&session), framed(&signal)]
        {
            let cut = &frame[..frame.len().min(5 + PEEK)];
            assert_eq!(cell_of(cut).unwrap(), id);
        }
    }

    #[test]
    fn a_request_without_a_cell_is_refused() {
        let empty = framed(&v1::RunRequest::default());
        assert_eq!(cell_of(&empty).unwrap_err().code(), tonic::Code::InvalidArgument);
        let other = framed(&v1::RunRequest { cell_id: "nope".into(), ..Default::default() });
        assert_eq!(cell_of(&other).unwrap_err().code(), tonic::Code::InvalidArgument);
        let mut zipped = framed(&v1::SignalRequest::default());
        zipped[0] = 1;
        assert_eq!(cell_of(&zipped).unwrap_err().code(), tonic::Code::Unimplemented);
        assert!(cell_of(&[0, 0]).is_err());
        assert!(field_one(&[0x0a, 0xff]).is_none(), "a length past the end");
    }

    #[test]
    fn the_size_counts_the_bytes_put_back() {
        let rest = Body::new(http_body_util::Full::new(Bytes::from_static(b"world")));
        let body = Prepend { first: Some(Bytes::from_static(b"hello ")), rest };
        assert_eq!(body.size_hint().exact(), Some(11));
    }
}
