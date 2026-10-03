//! The parts of envd the E2B SDK uses to run commands and move files, served by the comb that
//! owns the cell.
//!
//! Processes and the filesystem are Connect services, which the SDK calls in JSON. Files go up
//! and down as plain HTTP on `/files`. A call names its user in `Authorization: Basic`, or for
//! files in the `username` parameter, and a relative path is taken from that user's home.

use std::collections::HashMap;
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use bytes::{Bytes, BytesMut};
use futures::{Stream, StreamExt, stream};
use hive_proto::v1;
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::files_client::FilesClient;
use http_body::Frame;
use http_body_util::{BodyExt, StreamBody};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tonic::body::Body;
use tonic::codegen::http::{self, Method, StatusCode};
use tonic::transport::Channel;
use tonic::{Code, Status};

use super::{Caller, SANDBOX_HEADER, empty, error, from_status, json_answer, query};
use crate::{MAX_REQUEST, connect};

/// The biggest message a comb sends back, which is a directory listing.
const MAX_ANSWER: usize = 64 << 20;

pub(super) async fn serve(c: &Caller, req: http::Request<Body>) -> http::Response<Body> {
    let Some(id) = req.headers().get(SANDBOX_HEADER).and_then(|v| v.to_str().ok()) else {
        return error(StatusCode::BAD_REQUEST, "the E2b-Sandbox-Id header is not text");
    };
    let id = id.to_owned();
    let path = req.uri().path().to_owned();
    let method = req.method().clone();
    match (&method, path.as_str()) {
        (&Method::GET, "/health") => health(c, &id).await,
        (&Method::GET, "/files") => read(c, &id, req).await.unwrap_or_else(|s| from_status(&s)),
        (&Method::POST, "/files") => write(c, &id, req).await.unwrap_or_else(|s| from_status(&s)),
        (&Method::POST, "/process.Process/Start") => start(c, &id, req).await,
        (&Method::POST, p)
            if p.starts_with("/process.Process/") || p.starts_with("/filesystem.Filesystem/") =>
        {
            match unary(c, &id, req).await {
                Ok(v) => connect::answer(StatusCode::OK, "application/json", v.to_string().into()),
                Err(s) => rpc_error(&s),
            }
        }
        _ => error(StatusCode::NOT_FOUND, &format!("envd has no {method} {path} here")),
    }
}

async fn health(c: &Caller, id: &str) -> http::Response<Body> {
    use hive_proto::v1::cells_server::Cells;
    let msg = v1::GetCellRequest { id: id.into() };
    match c.gate.api.get(c.req(msg)).await {
        Ok(r) if r.get_ref().state() == v1::CellState::Running => empty(StatusCode::NO_CONTENT),
        Ok(_) => error(StatusCode::BAD_GATEWAY, "the sandbox is not running"),
        Err(s) => from_status(&s),
    }
}

/// A unary Connect error: the HTTP status for the code, and the code and message in JSON.
fn rpc_error(s: &Status) -> http::Response<Body> {
    let mut e = json!({ "code": connect::code_name(s.code()) });
    if !s.message().is_empty() {
        e["message"] = s.message().into();
    }
    connect::answer(connect::http_status(s.code()), "application/json", e.to_string().into())
}

/// The user a call runs as, from `Authorization: Basic`, with no password.
fn user_of(headers: &http::HeaderMap) -> String {
    headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Basic "))
        .and_then(|b| STANDARD.decode(b.trim()).ok())
        .and_then(|raw| String::from_utf8(raw).ok())
        .map(|s| s.split(':').next().unwrap_or_default().to_owned())
        .unwrap_or_default()
}

/// `path` as an absolute path, a relative one taken from the home of `user`.
fn absolute(path: &str, user: &str) -> String {
    if path.starts_with('/') {
        return path.to_owned();
    }
    let home = if user.is_empty() || user == "root" {
        "/root".to_owned()
    } else {
        format!("/home/{user}")
    };
    let rest = path.strip_prefix('~').unwrap_or(path).trim_start_matches('/');
    let rest = rest.strip_prefix("./").unwrap_or(rest);
    if rest.is_empty() { home } else { format!("{home}/{rest}") }
}

fn exec(channel: Channel) -> ExecClient<Channel> {
    ExecClient::new(channel).max_decoding_message_size(MAX_ANSWER)
}

fn files(channel: Channel) -> FilesClient<Channel> {
    FilesClient::new(channel).max_decoding_message_size(MAX_ANSWER)
}

#[derive(Deserialize)]
struct StartRequest {
    process: ProcessConfig,
    #[serde(default)]
    stdin: Option<bool>,
    #[serde(default)]
    pty: Option<Value>,
}

#[derive(Deserialize)]
struct ProcessConfig {
    cmd: String,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    envs: HashMap<String, String>,
    #[serde(default)]
    cwd: Option<String>,
}

/// `Process/Start`, a server stream: the pid, then output as it comes, then how it ended.
async fn start(c: &Caller, id: &str, req: http::Request<Body>) -> http::Response<Body> {
    let (parts, body) = req.into_parts();
    let user = user_of(&parts.headers);
    let header =
        |name: &str| parts.headers.get(name).and_then(|v| v.to_str().ok()?.parse::<u64>().ok());
    let deadline = header("connect-timeout-ms").filter(|&ms| ms > 0).map(Duration::from_millis);
    let keepalive = header("keepalive-ping-interval").filter(|&s| s > 0).map(Duration::from_secs);
    let json = parts
        .headers
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|t| t.starts_with("application/connect+json"));
    if !json {
        return stream_answer(stream::iter([end_of(Some(&Status::unimplemented(
            "only application/connect+json is served",
        )))]));
    }
    match open(c, id, &user, deadline, body).await {
        Ok((cell, stdin, input, outputs)) => {
            let (tx, rx) = mpsc::channel::<Bytes>(64);
            tokio::spawn(pump(c.e2b.clone(), cell, stdin, input, outputs, keepalive, tx));
            stream_answer(stream::unfold(
                rx,
                |mut rx| async move { rx.recv().await.map(|b| (b, rx)) },
            ))
        }
        Err(s) => stream_answer(stream::iter([end_of(Some(&s))])),
    }
}

type Opened =
    (hive_types::CellId, bool, mpsc::Sender<v1::ProcessInput>, tonic::Streaming<v1::ProcessOutput>);

/// Starts the process the request asks for on the cell's comb.
async fn open(
    c: &Caller,
    id: &str,
    user: &str,
    deadline: Option<Duration>,
    body: Body,
) -> Result<Opened, Status> {
    let (bytes, _) = connect::collect(body, MAX_REQUEST).await?;
    let mut buf = BytesMut::from(&bytes[..]);
    let (flags, msg) = connect::next_flagged(&mut buf)
        .ok_or_else(|| Status::invalid_argument("the request has no message"))?;
    if flags & 1 != 0 {
        return Err(Status::unimplemented("compressed messages are not served"));
    }
    let start: StartRequest = serde_json::from_slice(&msg)
        .map_err(|e| Status::invalid_argument(format!("the body is not a StartRequest: {e}")))?;
    if start.pty.is_some() {
        return Err(Status::unimplemented("terminals are not served"));
    }
    let (cell, channel) = c.comb(id, "exec")?;
    let p = start.process;
    let mut argv = vec![p.cmd];
    argv.extend(p.args);
    let begin = v1::ProcessStart {
        cell_id: cell.to_string(),
        argv,
        cwd: p.cwd.map(|d| absolute(&d, user)).unwrap_or_default(),
        env: p.envs,
        user: user.to_owned(),
        timeout: deadline.map(hive_proto::convert::duration_to_v1),
        ..Default::default()
    };
    let (tx, rx) = mpsc::channel(16);
    let input = |i| v1::ProcessInput { input: Some(i) };
    let _ = tx.send(input(v1::process_input::Input::Start(begin))).await;
    let stdin = start.stdin.unwrap_or(false);
    if !stdin {
        let _ = tx.send(input(v1::process_input::Input::Eof(true))).await;
    }
    let inputs = stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|m| (m, rx)) });
    let outputs = exec(channel).start(c.req(inputs)).await?.into_inner();
    Ok((cell, stdin, tx, outputs))
}

/// Turns the comb's output into envd events until the process ends or the caller goes away.
async fn pump(
    e2b: std::sync::Arc<super::E2b>,
    cell: hive_types::CellId,
    stdin: bool,
    input: mpsc::Sender<v1::ProcessInput>,
    mut outputs: tonic::Streaming<v1::ProcessOutput>,
    keepalive: Option<Duration>,
    tx: mpsc::Sender<Bytes>,
) {
    use v1::process_output::Output;
    let mut ticks =
        keepalive.map(|every| tokio::time::interval_at(tokio::time::Instant::now() + every, every));
    let mut pid = None;
    let end = loop {
        let event = tokio::select! {
            m = outputs.message() => match m {
                Ok(Some(v1::ProcessOutput { output: Some(out) })) => match out {
                    Output::Pid(p) => {
                        pid = Some(p);
                        if stdin {
                            e2b.stdin().insert((cell, p), input.clone());
                        }
                        json!({ "start": { "pid": p } })
                    }
                    Output::Stdout(b) => json!({ "data": { "stdout": STANDARD.encode(b) } }),
                    Output::Stderr(b) => json!({ "data": { "stderr": STANDARD.encode(b) } }),
                    Output::Exit(r) => json!({ "end": ended(&r) }),
                },
                Ok(Some(_)) => continue,
                Ok(None) => break Some(Status::ok("")),
                Err(s) => break Some(s),
            },
            () = tick(ticks.as_mut()) => json!({ "keepalive": {} }),
            () = tx.closed() => break None,
        };
        let msg = json!({ "event": event }).to_string();
        if tx.send(connect::envelope(0, msg.as_bytes())).await.is_err() {
            break None;
        }
    };
    if let Some(p) = pid {
        e2b.stdin().remove(&(cell, p));
    }
    if let Some(s) = end {
        let _ = tx.send(end_of(Some(&s))).await;
    }
}

async fn tick(ticks: Option<&mut tokio::time::Interval>) {
    match ticks {
        Some(t) => {
            t.tick().await;
        }
        None => std::future::pending().await,
    }
}

/// The end event, in the words envd uses, which are Go's.
fn ended(r: &v1::RunResult) -> Value {
    let status = match r.signal {
        0 => format!("exit status {}", r.exit_code),
        9 => "signal: killed".to_owned(),
        15 => "signal: terminated".to_owned(),
        2 => "signal: interrupt".to_owned(),
        n => format!("signal: {n}"),
    };
    let mut end = json!({ "exitCode": r.exit_code, "exited": r.signal == 0, "status": status });
    if r.timed_out {
        end["error"] = "the command ran out of time".into();
    }
    end
}

/// The envelope that ends a Connect stream, empty when it went well.
fn end_of(s: Option<&Status>) -> Bytes {
    let end = match s {
        Some(s) if s.code() != Code::Ok => {
            json!({ "error": { "code": connect::code_name(s.code()), "message": s.message() } })
        }
        _ => json!({}),
    };
    connect::envelope(connect::END_STREAM, end.to_string().as_bytes())
}

fn stream_answer(frames: impl Stream<Item = Bytes> + Send + 'static) -> http::Response<Body> {
    let body = StreamBody::new(frames.map(|b| Ok::<_, std::convert::Infallible>(Frame::data(b))));
    http::Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/connect+json")
        .body(Body::new(body))
        .unwrap_or_default()
}

/// The unary calls of the two Connect services.
async fn unary(c: &Caller, id: &str, req: http::Request<Body>) -> Result<Value, Status> {
    let json = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|t| t.starts_with("application/json"));
    if !json {
        return Err(Status::unimplemented("only application/json is served"));
    }
    let user = user_of(req.headers());
    let method = req.uri().path().to_owned();
    let (bytes, _) = connect::collect(req.into_body(), MAX_REQUEST).await?;
    let msg: Value = if bytes.is_empty() {
        json!({})
    } else {
        serde_json::from_slice(&bytes)
            .map_err(|e| Status::invalid_argument(format!("the body is not JSON: {e}")))?
    };
    let text = |k: &str| msg.get(k).and_then(Value::as_str).unwrap_or_default().to_owned();
    match method.as_str() {
        "/process.Process/SendSignal" => {
            let pid = pid_of(&msg)?;
            let signal = match msg.get("signal") {
                Some(Value::String(s)) if s == "SIGNAL_SIGKILL" => 9,
                Some(Value::String(s)) if s == "SIGNAL_SIGTERM" => 15,
                Some(Value::Number(n)) if n.as_i64().is_some_and(|n| n == 9 || n == 15) => {
                    n.as_i64().unwrap_or(15) as i32
                }
                other => {
                    return Err(Status::invalid_argument(format!(
                        "signal {other:?} is not SIGKILL or SIGTERM"
                    )));
                }
            };
            let (cell, channel) = c.comb(id, "exec")?;
            let msg = v1::SignalRequest { cell_id: cell.to_string(), pid, signal };
            exec(channel).signal(c.req(msg)).await?;
            Ok(json!({}))
        }
        "/process.Process/SendInput" => {
            let pid = pid_of(&msg)?;
            let data = msg
                .pointer("/input/stdin")
                .and_then(Value::as_str)
                .ok_or_else(|| Status::unimplemented("only stdin input is served"))?;
            let data = STANDARD
                .decode(data)
                .map_err(|e| Status::invalid_argument(format!("stdin is not base64: {e}")))?;
            send(c, id, pid, v1::process_input::Input::Stdin(data.into()), false).await?;
            Ok(json!({}))
        }
        "/process.Process/CloseStdin" => {
            send(c, id, pid_of(&msg)?, v1::process_input::Input::Eof(true), true).await?;
            Ok(json!({}))
        }
        "/filesystem.Filesystem/Stat" => {
            let (cell, channel) = c.comb(id, "files")?;
            let info = stat(c, cell, channel, &absolute(&text("path"), &user)).await?;
            Ok(json!({ "entry": entry(&info) }))
        }
        "/filesystem.Filesystem/ListDir" => {
            let (cell, channel) = c.comb(id, "files")?;
            let depth = msg.get("depth").and_then(Value::as_u64).unwrap_or(1).max(1);
            let m = v1::ListDirRequest {
                cell_id: cell.to_string(),
                path: absolute(&text("path"), &user),
                depth: u32::try_from(depth).unwrap_or(u32::MAX),
            };
            let list = files(channel).list(c.req(m)).await?.into_inner();
            Ok(json!({ "entries": list.entries.iter().map(entry).collect::<Vec<_>>() }))
        }
        "/filesystem.Filesystem/Remove" => {
            let (cell, channel) = c.comb(id, "files")?;
            let m = v1::PathRequest {
                cell_id: cell.to_string(),
                path: absolute(&text("path"), &user),
                recursive: true,
            };
            files(channel).remove(c.req(m)).await?;
            Ok(json!({}))
        }
        "/filesystem.Filesystem/MakeDir" => {
            let (cell, channel) = c.comb(id, "files")?;
            let path = absolute(&text("path"), &user);
            match stat(c, cell, channel.clone(), &path).await {
                Ok(_) => return Err(Status::already_exists(format!("{path} is there already"))),
                Err(s) if s.code() == Code::NotFound => {}
                Err(s) => return Err(s),
            }
            run(
                c,
                cell,
                channel.clone(),
                &user,
                vec!["mkdir".into(), "-p".into(), "--".into(), path.clone()],
            )
            .await?;
            Ok(json!({ "entry": entry(&stat(c, cell, channel, &path).await?) }))
        }
        "/filesystem.Filesystem/Move" => {
            let (cell, channel) = c.comb(id, "files")?;
            let (from, to) =
                (absolute(&text("source"), &user), absolute(&text("destination"), &user));
            run(c, cell, channel.clone(), &user, vec!["mv".into(), "--".into(), from, to.clone()])
                .await?;
            Ok(json!({ "entry": entry(&stat(c, cell, channel, &to).await?) }))
        }
        _ => Err(Status::unimplemented(format!("{method} is not served"))),
    }
}

fn pid_of(msg: &Value) -> Result<u32, Status> {
    msg.pointer("/process/pid")
        .and_then(Value::as_u64)
        .and_then(|p| u32::try_from(p).ok())
        .ok_or_else(|| Status::unimplemented("only processes picked by pid are served"))
}

/// Sends `input` to a process this gate started with its input open.
async fn send(
    c: &Caller,
    id: &str,
    pid: u32,
    input: v1::process_input::Input,
    last: bool,
) -> Result<(), Status> {
    let (cell, _) = c.comb(id, "exec")?;
    let tx = {
        let mut open = c.e2b.stdin();
        if last { open.remove(&(cell, pid)) } else { open.get(&(cell, pid)).cloned() }
    };
    let tx = tx.ok_or_else(|| {
        Status::not_found(format!("process {pid} has no open input on this gate"))
    })?;
    tx.send(v1::ProcessInput { input: Some(input) })
        .await
        .map_err(|_| Status::not_found(format!("process {pid} has ended")))
}

async fn stat(
    c: &Caller,
    cell: hive_types::CellId,
    channel: Channel,
    path: &str,
) -> Result<v1::FileInfo, Status> {
    let m = v1::PathRequest { cell_id: cell.to_string(), path: path.into(), recursive: false };
    Ok(files(channel).stat(c.req(m)).await?.into_inner())
}

/// Runs `argv` in the cell as `user` and fails with its error output when it fails.
async fn run(
    c: &Caller,
    cell: hive_types::CellId,
    channel: Channel,
    user: &str,
    argv: Vec<String>,
) -> Result<(), Status> {
    let m =
        v1::RunRequest { cell_id: cell.to_string(), argv, user: user.into(), ..Default::default() };
    let r = exec(channel).run(c.req(m)).await?.into_inner();
    if r.exit_code == 0 {
        return Ok(());
    }
    let why = String::from_utf8_lossy(&r.stderr).trim().to_owned();
    let code = if why.contains("No such file") { Code::NotFound } else { Code::Internal };
    Err(Status::new(code, why))
}

/// A file as envd describes it.
fn entry(info: &v1::FileInfo) -> Value {
    let (kind, letter) = match info.r#type() {
        v1::FileType::Dir => ("FILE_TYPE_DIRECTORY", 'd'),
        v1::FileType::Symlink => ("FILE_TYPE_SYMLINK", 'l'),
        _ => ("FILE_TYPE_FILE", '-'),
    };
    let mut permissions = String::from(letter);
    for shift in [6, 3, 0] {
        let bits = (info.mode >> shift) & 7;
        permissions.push(if bits & 4 != 0 { 'r' } else { '-' });
        permissions.push(if bits & 2 != 0 { 'w' } else { '-' });
        permissions.push(if bits & 1 != 0 { 'x' } else { '-' });
    }
    let name = info.path.rsplit('/').next().unwrap_or_default();
    let mut e = json!({
        "name": name,
        "type": kind,
        "path": info.path,
        "size": info.size,
        "mode": info.mode & 0o7777,
        "permissions": permissions,
        "owner": "",
        "group": "",
    });
    if let Some(t) = &info.modified_at {
        e["modifiedTime"] = t.to_string().into();
    }
    if !info.symlink_target.is_empty() {
        e["symlinkTarget"] = info.symlink_target.clone().into();
    }
    e
}

/// `GET /files`: the file's bytes, streamed as the comb reads them.
async fn read(
    c: &Caller,
    id: &str,
    req: http::Request<Body>,
) -> Result<http::Response<Body>, Status> {
    let q = query(req.uri());
    let user = q.get("username").cloned().unwrap_or_default();
    let path = absolute(q.get("path").map_or("", String::as_str), &user);
    let (cell, channel) = c.comb(id, "files")?;
    let m = v1::ReadFileRequest { cell_id: cell.to_string(), path, ..Default::default() };
    let mut chunks = files(channel).read(c.req(m)).await?.into_inner();
    // A missing file fails on the first chunk, which has to be known before the answer starts.
    let first = chunks.message().await?;
    let rest = stream::unfold(chunks, |mut s| async move {
        match s.message().await {
            Ok(Some(chunk)) => Some((Ok(Frame::data(chunk.data)), s)),
            Ok(None) => None,
            Err(e) => Some((Err(e), s)),
        }
    });
    let head = stream::iter(first.map(|chunk| Ok(Frame::data(chunk.data))));
    Ok(http::Response::builder()
        .status(StatusCode::OK)
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .body(Body::new(StreamBody::new(head.chain(rest))))
        .unwrap_or_default())
}

/// `POST /files`: one file as the whole body, or any number as a multipart form whose parts are
/// named by their paths.
async fn write(
    c: &Caller,
    id: &str,
    req: http::Request<Body>,
) -> Result<http::Response<Body>, Status> {
    if req.headers().contains_key(http::header::CONTENT_ENCODING) {
        return Err(Status::unimplemented("compressed uploads are not served"));
    }
    let q = query(req.uri());
    let user = q.get("username").cloned().unwrap_or_default();
    let named = q.get("path").map(|p| absolute(p, &user));
    let kind = req
        .headers()
        .get(http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or_default()
        .to_owned();
    let (cell, channel) = c.comb(id, "files")?;
    let mut written = Vec::new();
    if let Some(boundary) = boundary(&kind) {
        let (bytes, _) = connect::collect(req.into_body(), MAX_REQUEST).await?;
        let parts = multipart(&bytes, &boundary).map_err(Status::invalid_argument)?;
        let one = parts.len() == 1;
        for (file, data) in parts {
            let path = match (&named, file) {
                (Some(p), _) if one => p.clone(),
                (_, Some(f)) => absolute(&f, &user),
                (Some(p), None) => p.clone(),
                (None, None) => {
                    return Err(Status::invalid_argument(
                        "a part has no file name and there is no path",
                    ));
                }
            };
            put(c, cell, channel.clone(), &path, &user, stream::iter([data])).await?;
            written.push(path);
        }
    } else {
        let path = named.ok_or_else(|| Status::invalid_argument("the path parameter is needed"))?;
        // An upload that breaks off must not leave half a file, so the stream to the comb never
        // ends cleanly then, and the call is dropped instead, which the comb takes as a failure.
        let (broke, why) = tokio::sync::oneshot::channel::<String>();
        let data =
            stream::unfold((req.into_body(), Some(broke)), |(mut body, mut broke)| async move {
                loop {
                    match body.frame().await {
                        Some(Ok(frame)) => {
                            if let Ok(data) = frame.into_data() {
                                return Some((data, (body, broke)));
                            }
                        }
                        Some(Err(e)) => {
                            if let Some(tx) = broke.take() {
                                let _ = tx.send(e.to_string());
                            }
                            std::future::pending::<()>().await;
                        }
                        None => return None,
                    }
                }
            });
        tokio::select! {
            r = put(c, cell, channel, &path, &user, data) => { r?; }
            Ok(why) = why => return Err(Status::invalid_argument(format!("the upload broke off: {why}"))),
        }
        written.push(path);
    }
    let infos: Vec<Value> = written
        .iter()
        .map(|p| json!({ "name": p.rsplit('/').next().unwrap_or_default(), "type": "file", "path": p }))
        .collect();
    Ok(json_answer(StatusCode::OK, &Value::Array(infos)))
}

async fn put(
    c: &Caller,
    cell: hive_types::CellId,
    channel: Channel,
    path: &str,
    user: &str,
    data: impl Stream<Item = Bytes> + Send + 'static,
) -> Result<v1::FileInfo, Status> {
    use v1::write_file_chunk::Part;
    let header = v1::WriteFileHeader {
        cell_id: cell.to_string(),
        path: path.into(),
        make_parents: true,
        user: user.into(),
        ..Default::default()
    };
    let head = stream::iter([v1::WriteFileChunk { part: Some(Part::Header(header)) }]);
    let body = data.map(|b| v1::WriteFileChunk { part: Some(Part::Data(b)) });
    Ok(files(channel).write(c.req(head.chain(body))).await?.into_inner())
}

/// The boundary of a multipart form, from its content type.
fn boundary(content_type: &str) -> Option<String> {
    let (kind, params) = content_type.split_once(';')?;
    if !kind.trim().eq_ignore_ascii_case("multipart/form-data") {
        return None;
    }
    params.split(';').find_map(|p| {
        let (k, v) = p.trim().split_once('=')?;
        k.eq_ignore_ascii_case("boundary").then(|| v.trim_matches('"').to_owned())
    })
}

/// The parts of a multipart form: each one's file name, if it has one, and its bytes.
fn multipart(body: &Bytes, boundary: &str) -> Result<Vec<(Option<String>, Bytes)>, String> {
    let delim = format!("--{boundary}");
    let mut at = find(body, delim.as_bytes(), 0).ok_or("the form has no boundary")? + delim.len();
    let mut parts = Vec::new();
    loop {
        if body.get(at..at + 2) == Some(b"--") {
            return Ok(parts);
        }
        at += if body.get(at..at + 2) == Some(b"\r\n") { 2 } else { 0 };
        let head_end = find(body, b"\r\n\r\n", at).ok_or("a part has no end to its headers")?;
        let head = String::from_utf8_lossy(&body[at..head_end]);
        let file = head.lines().find_map(|l| {
            let (name, value) = l.split_once(':')?;
            if !name.trim().eq_ignore_ascii_case("content-disposition") {
                return None;
            }
            value.split(';').find_map(|p| {
                let (k, v) = p.trim().split_once('=')?;
                (k == "filename").then(|| super::unescape(v.trim_matches('"')))
            })
        });
        let start = head_end + 4;
        let end_delim = format!("\r\n{delim}");
        let end =
            find(body, end_delim.as_bytes(), start).ok_or("a part has no boundary after it")?;
        parts.push((file, body.slice(start..end)));
        at = end + end_delim.len();
    }
}

fn find(hay: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    hay.get(from..)?.windows(needle.len()).position(|w| w == needle).map(|i| i + from)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn users_and_paths() {
        let mut h = http::HeaderMap::new();
        assert_eq!(user_of(&h), "");
        h.insert("authorization", format!("Basic {}", STANDARD.encode("agent:")).parse().unwrap());
        assert_eq!(user_of(&h), "agent");
        assert_eq!(absolute("/etc/x", "agent"), "/etc/x");
        assert_eq!(absolute("x/y", "agent"), "/home/agent/x/y");
        assert_eq!(absolute("~/x", "root"), "/root/x");
        assert_eq!(absolute("./x", ""), "/root/x");
        assert_eq!(absolute("", "agent"), "/home/agent");
    }

    #[test]
    fn forms_are_split_into_files() {
        let body = Bytes::from_static(
            b"--b0\r\nContent-Disposition: form-data; name=\"file\"; filename=\"/tmp/a.txt\"\r\n\
              Content-Type: application/octet-stream\r\n\r\nhello\r\n--b0\r\n\
              Content-Disposition: form-data; name=\"file\"; filename=\"b%20c\"\r\n\r\n\
              two\r\nlines\r\n--b0--\r\n",
        );
        assert_eq!(boundary("multipart/form-data; boundary=b0").as_deref(), Some("b0"));
        assert_eq!(boundary("multipart/form-data; boundary=\"b0\"").as_deref(), Some("b0"));
        assert_eq!(boundary("application/octet-stream"), None);
        let parts = multipart(&body, "b0").unwrap();
        assert_eq!(parts.len(), 2);
        assert_eq!(parts[0], (Some("/tmp/a.txt".into()), Bytes::from_static(b"hello")));
        assert_eq!(parts[1], (Some("b c".into()), Bytes::from_static(b"two\r\nlines")));
        assert!(multipart(&Bytes::from_static(b"nothing"), "b0").is_err());
    }

    #[test]
    fn ends_read_like_envd() {
        let r = v1::RunResult { exit_code: 3, ..Default::default() };
        assert_eq!(ended(&r), json!({ "exitCode": 3, "exited": true, "status": "exit status 3" }));
        let r = v1::RunResult { exit_code: -1, signal: 9, ..Default::default() };
        assert_eq!(ended(&r)["status"], "signal: killed");
        let info = v1::FileInfo {
            path: "/a/b".into(),
            r#type: v1::FileType::Dir.into(),
            mode: 0o755,
            ..Default::default()
        };
        let e = entry(&info);
        assert_eq!(e["name"], "b");
        assert_eq!(e["permissions"], "drwxr-xr-x");
        assert_eq!(e["type"], "FILE_TYPE_DIRECTORY");
    }
}
