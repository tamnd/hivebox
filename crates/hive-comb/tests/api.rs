//! The local API end to end: a real gRPC client on the comb's Unix socket, over the fake driver
//! from `common`.

#![cfg(target_os = "linux")]

mod common;
#[path = "../../hive-cell-wasm/tests/support/graders.rs"]
mod graders;

use common::*;
use hive_comb::api;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::files_client::FilesClient;
use hive_proto::v1::snapshots_client::SnapshotsClient;
use hive_proto::v1::verify_client::VerifyClient;
use tokio_util::sync::CancellationToken;
use tonic::transport::{Channel, Endpoint};

/// A comb serving its API on a socket in its data directory.
struct Served {
    comb: Comb,
    stop: CancellationToken,
    channel: Channel,
    socket: PathBuf,
}

impl Served {
    async fn new(s: &Scratch, fake: &Arc<Fake>) -> Self {
        let comb = open(config(&s.0), fake).await;
        let socket = s.0.join("comb.sock");
        let listener = api::bind(&socket).unwrap();
        let stop = CancellationToken::new();
        tokio::spawn(api::serve(comb.clone(), listener, None, stop.clone()));
        let channel = connect(&socket).await;
        Self { comb, stop, channel, socket }
    }

    fn cells(&self) -> CellsClient<Channel> {
        CellsClient::new(self.channel.clone())
    }

    fn exec(&self) -> ExecClient<Channel> {
        ExecClient::new(self.channel.clone())
    }

    fn files(&self) -> FilesClient<Channel> {
        FilesClient::new(self.channel.clone())
    }

    fn verify(&self) -> VerifyClient<Channel> {
        VerifyClient::new(self.channel.clone())
    }
}

async fn connect(socket: &Path) -> Channel {
    let socket = socket.to_path_buf();
    // The URI is required and never used, since the connector ignores it.
    Endpoint::from_static("http://comb")
        .connect_with_connector(tower::service_fn(move |_| {
            let socket = socket.clone();
            async move {
                let s = tokio::net::UnixStream::connect(socket).await?;
                Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
            }
        }))
        .await
        .unwrap()
}

/// A request in `project`.
fn req<T>(project: &str, msg: T) -> tonic::Request<T> {
    let mut r = tonic::Request::new(msg);
    r.metadata_mut().insert(api::PROJECT_HEADER, project.parse().unwrap());
    r
}

fn v1_spec(image: &str, labels: &[(&str, &str)]) -> v1::CellSpec {
    v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: image.into() })),
        backend: v1::Backend::Container.into(),
        resources: Some(v1::Resources { mem_mib: 256, ..Default::default() }),
        labels: labels.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect(),
        ..Default::default()
    }
}

/// Creates `count` cells and returns them in index order, failing the test on any error.
async fn create(
    cells: &mut CellsClient<Channel>,
    project: &str,
    count: u32,
    spec: v1::CellSpec,
) -> Vec<v1::Cell> {
    let r = v1::CreateRequest { spec: Some(spec), count, ..Default::default() };
    let mut events = cells.create(req(project, r)).await.unwrap().into_inner();
    let mut out = vec![None; count as usize];
    while let Some(e) = events.message().await.unwrap() {
        match e.result.unwrap() {
            v1::create_event::Result::Cell(c) => out[e.index as usize] = Some(c),
            v1::create_event::Result::Error(e) => panic!("a create failed: {e:?}"),
        }
    }
    out.into_iter().map(Option::unwrap).collect()
}

fn reason(s: &tonic::Status) -> Reason {
    hive_proto::convert::error_from_status(s).reason
}

fn by_id(id: &str) -> v1::CellSelector {
    v1::CellSelector { by: Some(v1::cell_selector::By::Id(id.into())) }
}

fn by_labels(labels: &[(&str, &str)]) -> v1::CellSelector {
    let r#match = labels.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect();
    v1::CellSelector { by: Some(v1::cell_selector::By::Labels(v1::LabelSelector { r#match })) }
}

#[tokio::test(flavor = "multi_thread")]
async fn cells_are_made_listed_watched_and_stopped() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mut cells = api.cells();

    let made = create(&mut cells, "p", 3, v1_spec("python", &[("run", "a")])).await;
    assert!(made.iter().all(|c| c.state() == v1::CellState::Running && c.project == "p"));
    assert_eq!(made[0].labels["run"], "a");
    let other = create(&mut cells, "p", 1, v1_spec("python", &[("run", "b")])).await;

    let got = cells.get(req("p", v1::GetCellRequest { id: made[1].id.clone() })).await.unwrap();
    assert_eq!(got.get_ref().id, made[1].id);
    assert_eq!(got.get_ref().spec.as_ref().unwrap().labels["run"], "a");

    // Two per page, in id order, until the token runs out.
    let mut seen = Vec::new();
    let mut token = String::new();
    loop {
        let r = v1::ListCellsRequest { page_size: 2, page_token: token, ..Default::default() };
        let page = cells.list(req("p", r)).await.unwrap().into_inner();
        seen.extend(page.cells.into_iter().map(|c| c.id));
        token = page.next_page_token;
        if token.is_empty() {
            break;
        }
    }
    assert_eq!(seen.len(), 4);
    let r = v1::ListCellsRequest {
        selector: Some(v1::LabelSelector { r#match: [("run".into(), "b".into())].into() }),
        ..Default::default()
    };
    let page = cells.list(req("p", r)).await.unwrap().into_inner();
    assert_eq!(page.cells.len(), 1);
    assert_eq!(page.cells[0].id, other[0].id);

    // A watch by id starts with where the cell is and ends once it has ended.
    let w = v1::WatchCellsRequest { selector: Some(by_id(&made[0].id)) };
    let mut watch = cells.watch(req("p", w)).await.unwrap().into_inner();
    let first = watch.message().await.unwrap().unwrap();
    assert_eq!(first.from(), v1::CellState::Unspecified);
    assert_eq!(first.cell.unwrap().state(), v1::CellState::Running);

    let paused = cells.pause(req("p", by_labels(&[("run", "a")]))).await.unwrap().into_inner();
    assert_eq!((paused.matched, paused.succeeded), (3, 3));
    for (from, to) in [
        (v1::CellState::Running, v1::CellState::Pausing),
        (v1::CellState::Pausing, v1::CellState::Paused),
    ] {
        let e = watch.message().await.unwrap().unwrap();
        assert_eq!((e.from(), e.cell.unwrap().state()), (from, to));
    }
    // Only paused cells are picked for a resume by label, so the running one is not counted.
    let resumed = cells.resume(req("p", by_labels(&[]))).await.unwrap().into_inner();
    assert_eq!((resumed.matched, resumed.succeeded), (3, 3));

    let stop = v1::StopRequest { selector: Some(by_labels(&[("run", "a")])), snapshot: false };
    let stopped = cells.stop(req("p", stop)).await.unwrap().into_inner();
    assert_eq!((stopped.matched, stopped.succeeded), (3, 3));
    let mut last = v1::CellState::Unspecified;
    while let Some(e) = watch.message().await.unwrap() {
        last = e.cell.unwrap().state();
    }
    assert_eq!(last, v1::CellState::Stopped);
    // Stopping by label again finds nothing left to stop.
    let stop = v1::StopRequest { selector: Some(by_labels(&[("run", "a")])), snapshot: false };
    assert_eq!(cells.stop(req("p", stop)).await.unwrap().into_inner().matched, 0);
    assert_eq!(fake.live(), 1);

    api.stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_project_sees_only_its_own_cells() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mut cells = api.cells();
    let mine = create(&mut cells, "p", 1, v1_spec("python", &[])).await.remove(0);

    let e = cells.get(req("q", v1::GetCellRequest { id: mine.id.clone() })).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotFound);
    let listed = cells.list(req("q", v1::ListCellsRequest::default())).await.unwrap();
    assert!(listed.into_inner().cells.is_empty());
    let stop = v1::StopRequest { selector: Some(by_id(&mine.id)), snapshot: false };
    assert_eq!(reason(&cells.stop(req("q", stop)).await.unwrap_err()), Reason::CellNotFound);
    let run =
        v1::RunRequest { cell_id: mine.id.clone(), shell: "true".into(), ..Default::default() };
    assert_eq!(reason(&api.exec().run(req("q", run)).await.unwrap_err()), Reason::CellNotFound);
    // Without the header a call is in the local project.
    let listed = cells.list(v1::ListCellsRequest::default()).await.unwrap().into_inner();
    assert!(listed.cells.is_empty());
    assert_eq!(fake.live(), 1);

    // Bad input is refused before anything is made.
    let e = cells.get(req("bad project", v1::GetCellRequest { id: mine.id })).await.unwrap_err();
    assert_eq!(reason(&e), Reason::InvalidArgument);
    let r =
        v1::CreateRequest { count: 5000, spec: Some(v1_spec("python", &[])), ..Default::default() };
    assert_eq!(reason(&cells.create(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    let r = v1::CreateRequest::default();
    assert_eq!(reason(&cells.create(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    let e = cells.get(req("p", v1::GetCellRequest { id: "nope".into() })).await.unwrap_err();
    assert_eq!(reason(&e), Reason::InvalidArgument);
    // A failed create comes back as an event, not as a failed call.
    let r = v1::CreateRequest { spec: Some(v1_spec("missing", &[])), ..Default::default() };
    let mut events = cells.create(req("p", r)).await.unwrap().into_inner();
    let e = events.message().await.unwrap().unwrap();
    let v1::create_event::Result::Error(e) = e.result.unwrap() else { panic!("made a cell") };
    assert_eq!(e.reason, "IMAGE_UNAVAILABLE");
    assert_eq!(fake.live(), 1);
    api.stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn commands_run_stream_and_keep_sessions() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let cell = create(&mut api.cells(), "p", 1, v1_spec("python", &[])).await.remove(0);
    let mut exec = api.exec();

    let run = v1::RunRequest {
        cell_id: cell.id.clone(),
        shell: "cat; echo err >&2; exit 3".into(),
        stdin: vec![7u8; 100_000].into(),
        ..Default::default()
    };
    let r = exec.run(req("p", run)).await.unwrap().into_inner();
    assert_eq!(r.exit_code, 3);
    assert_eq!(r.stdout.len(), 100_000);
    assert_eq!(&r.stderr[..], b"err\n");
    let run = v1::RunRequest {
        cell_id: cell.id.clone(),
        argv: vec!["sleep".into(), "5".into()],
        timeout: Some(prost_types::Duration { seconds: 0, nanos: 100_000_000 }),
        ..Default::default()
    };
    assert!(exec.run(req("p", run)).await.unwrap().into_inner().timed_out);

    // A streamed cat echoes what it is sent, and a signal through Exec.Signal ends it.
    let (tx, rx) = tokio::sync::mpsc::channel(8);
    let start =
        v1::ProcessStart { cell_id: cell.id.clone(), shell: "cat".into(), ..Default::default() };
    let input = |i| v1::ProcessInput { input: Some(i) };
    tx.send(input(v1::process_input::Input::Start(start))).await.unwrap();
    tx.send(input(v1::process_input::Input::Stdin("hello\n".into()))).await.unwrap();
    let stream = tokio_stream_of(rx);
    let mut out = exec.start(req("p", stream)).await.unwrap().into_inner();
    let mut pid = 0;
    let mut stdout = Vec::new();
    while let Some(o) = out.message().await.unwrap() {
        match o.output.unwrap() {
            v1::process_output::Output::Pid(p) => pid = p,
            v1::process_output::Output::Stdout(b) => {
                stdout.extend_from_slice(&b);
                if stdout == b"hello\n" {
                    let sig = v1::SignalRequest { cell_id: cell.id.clone(), pid, signal: 15 };
                    exec.signal(req("p", sig)).await.unwrap();
                }
            }
            v1::process_output::Output::Stderr(_) => {}
            v1::process_output::Output::Exit(r) => {
                assert_eq!(r.signal, 15);
                break;
            }
        }
    }
    assert_eq!(stdout, b"hello\n");
    drop(tx);
    // Once it is gone, its pid takes no more signals.
    let sig = v1::SignalRequest { cell_id: cell.id.clone(), pid, signal: 15 };
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(reason(&exec.signal(req("p", sig)).await.unwrap_err()), Reason::InvalidArgument);

    let session = exec
        .session_create(req(
            "p",
            v1::SessionCreateRequest { cell_id: cell.id.clone(), ..Default::default() },
        ))
        .await
        .unwrap()
        .into_inner();
    let sref = v1::SessionRef { cell_id: cell.id.clone(), id: session.id.clone() };
    for (command, want) in [("cd /tmp; X=42", ""), ("echo $PWD $X", "/tmp 42\n")] {
        let run = v1::SessionRunRequest {
            session: Some(sref.clone()),
            command: command.into(),
            ..Default::default()
        };
        let r = exec.session_run(req("p", run)).await.unwrap().into_inner();
        assert_eq!(String::from_utf8_lossy(&r.output), want);
    }
    exec.session_close(req("p", sref)).await.unwrap();
    api.stop.cancel();
}

/// Writes `data` to `path` in `cell` in chunks of `chunk` bytes.
async fn write(
    files: &mut FilesClient<Channel>,
    project: &str,
    cell: &str,
    path: &str,
    data: &[u8],
    chunk: usize,
) -> Result<v1::FileInfo, tonic::Status> {
    use v1::write_file_chunk::Part;
    let header = v1::WriteFileHeader {
        cell_id: cell.into(),
        path: path.into(),
        make_parents: true,
        ..Default::default()
    };
    let mut msgs = vec![v1::WriteFileChunk { part: Some(Part::Header(header)) }];
    msgs.extend(
        data.chunks(chunk).map(|c| v1::WriteFileChunk {
            part: Some(Part::Data(bytes::Bytes::copy_from_slice(c))),
        }),
    );
    files.write(req(project, futures::stream::iter(msgs))).await.map(tonic::Response::into_inner)
}

async fn read(
    files: &mut FilesClient<Channel>,
    project: &str,
    cell: &str,
    path: &str,
    offset: u64,
    length: u64,
) -> Result<Vec<u8>, tonic::Status> {
    let r = v1::ReadFileRequest { cell_id: cell.into(), path: path.into(), offset, length };
    let mut chunks = files.read(req(project, r)).await?.into_inner();
    let mut out = Vec::new();
    while let Some(c) = chunks.message().await? {
        out.extend_from_slice(&c.data);
    }
    Ok(out)
}

#[tokio::test(flavor = "multi_thread")]
async fn files_are_written_read_listed_watched_and_removed() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let cell = create(&mut api.cells(), "p", 1, v1_spec("python", &[])).await.remove(0);
    let mut files = api.files();
    // The fake's cells see the host's files, so the test works in a directory of its own.
    let dir = s.0.join("files");
    let at = |p: &str| dir.join(p).to_str().unwrap().to_string();

    let path = at("sub/small.txt");
    let info = write(&mut files, "p", &cell.id, &path, b"hello", 5).await.unwrap();
    assert_eq!((info.size, info.r#type(), info.mode & 0o777), (5, v1::FileType::File, 0o644));
    assert_eq!(read(&mut files, "p", &cell.id, &path, 0, 0).await.unwrap(), b"hello");
    assert_eq!(read(&mut files, "p", &cell.id, &path, 1, 3).await.unwrap(), b"ell");

    // Big enough to be streamed to the drone, in chunks that do not line up with anything.
    let big: Vec<u8> = (0..5_000_000u32).map(|i| (i % 251) as u8).collect();
    let path = at("big.bin");
    let info = write(&mut files, "p", &cell.id, &path, &big, 100_003).await.unwrap();
    assert_eq!(info.size, big.len() as u64);
    assert_eq!(read(&mut files, "p", &cell.id, &path, 0, 0).await.unwrap(), big);
    let got = read(&mut files, "p", &cell.id, &path, 4_999_990, 100).await.unwrap();
    assert_eq!(got, &big[4_999_990..]);
    // An empty file is still written.
    let info = write(&mut files, "p", &cell.id, &at("empty"), b"", 1).await.unwrap();
    assert_eq!(info.size, 0);

    let stat = |path: &str| v1::PathRequest {
        cell_id: cell.id.clone(),
        path: path.into(),
        recursive: false,
    };
    let info = files.stat(req("p", stat(&at("sub")))).await.unwrap().into_inner();
    assert_eq!(info.r#type(), v1::FileType::Dir);
    std::os::unix::fs::symlink("big.bin", dir.join("link")).unwrap();
    let info = files.stat(req("p", stat(&at("link")))).await.unwrap().into_inner();
    assert_eq!((info.r#type(), info.symlink_target.as_str()), (v1::FileType::Symlink, "big.bin"));
    let e = files.stat(req("p", stat(&at("nothing")))).await.unwrap_err();
    let e = hive_proto::convert::error_from_status(&e);
    assert_eq!((e.reason, e.errno.as_deref()), (Reason::FileError, Some("ENOENT")));

    let list = |depth| v1::ListDirRequest { cell_id: cell.id.clone(), path: at(""), depth };
    let names = |r: v1::ListDirResponse| -> Vec<String> {
        r.entries.into_iter().map(|e| e.path.rsplit('/').next().unwrap().to_string()).collect()
    };
    let r = files.list(req("p", list(1))).await.unwrap().into_inner();
    assert!(!r.truncated);
    assert_eq!(names(r), ["big.bin", "empty", "link", "sub"]);
    let r = files.list(req("p", list(2))).await.unwrap().into_inner();
    assert_eq!(names(r), ["big.bin", "empty", "link", "sub", "small.txt"]);

    // A watch sees a file made after it started. The drone writes a file next to it first and
    // moves it into place, so the file's own name comes after that one's.
    let w = v1::WatchDirRequest { cell_id: cell.id.clone(), path: at(""), recursive: true };
    let mut events = files.watch(req("p", w)).await.unwrap().into_inner();
    write(&mut files, "p", &cell.id, &at("sub/new"), b"x", 1).await.unwrap();
    let seen = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(e) = events.message().await.unwrap() {
            if e.path == at("sub/new") {
                return e.kind();
            }
        }
        panic!("the watch ended");
    });
    assert_eq!(seen.await.unwrap(), v1::FsEventKind::Create);
    drop(events);

    // A tar is unpacked where the caller says.
    let mut tar = tar::Builder::new(Vec::new());
    let mut h = tar::Header::new_gnu();
    h.set_size(3);
    h.set_mode(0o600);
    h.set_cksum();
    tar.append_data(&mut h, "a/b.txt", &b"abc"[..]).unwrap();
    let apply = v1::ApplyRequest {
        cell_id: cell.id.clone(),
        path: at("unpacked"),
        content: Some(v1::apply_request::Content::Tar(tar.into_inner().unwrap().into())),
    };
    files.apply(req("p", apply)).await.unwrap();
    assert_eq!(std::fs::read(dir.join("unpacked/a/b.txt")).unwrap(), b"abc");

    // Another project's caller cannot reach the cell's files.
    let e = read(&mut files, "q", &cell.id, &at("big.bin"), 0, 0).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotFound);
    let e = write(&mut files, "q", &cell.id, &at("x"), b"x", 1).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotFound);
    assert!(!dir.join("x").exists());

    // A directory goes only when asked to go with what is in it.
    let mut rm = stat(&at("sub"));
    assert!(files.remove(req("p", rm.clone())).await.is_err());
    rm.recursive = true;
    files.remove(req("p", rm)).await.unwrap();
    assert!(!dir.join("sub").exists());
    api.stop.cancel();
}

/// Runs git in `dir` and fails the test if it fails.
fn git(dir: &Path, args: &[&str]) {
    let ok = std::process::Command::new("git")
        .args(["-c", "user.name=t", "-c", "user.email=t@t", "-c", "safe.directory=*"])
        .args(args)
        .current_dir(dir)
        .status()
        .unwrap()
        .success();
    assert!(ok, "git {args:?}");
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_checks_a_subjects_changes_in_a_cell_of_its_own() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let repo = s.0.join("repo");
    std::fs::create_dir_all(&repo).unwrap();
    std::fs::write(repo.join("lib.py"), "x = 1\n").unwrap();
    std::fs::write(repo.join("test_lib.py"), "assert True\n").unwrap();
    git(&repo, &["init", "-q"]);
    git(&repo, &["add", "-A"]);
    git(&repo, &["commit", "-qm", "base"]);
    let subject = create(&mut api.cells(), "p", 1, v1_spec("python", &[])).await.remove(0);
    let base = v1::VerifyRequest {
        subject_cell_id: subject.id.clone(),
        verifier: Some(v1_spec("python", &[])),
        workdir: repo.to_str().unwrap().into(),
        // The hidden files and what the commands leave behind are in the subject's checkout too,
        // since the fake's cells share one filesystem.
        protected_paths: vec!["test_*.py".into(), "hidden/**".into(), "flip".into()],
        ..Default::default()
    };
    let sh = |script: &str| vec!["sh".to_owned(), "-c".to_owned(), script.to_owned()];
    let mut verify = api.verify();

    // Bad requests make nothing.
    let e = verify.run(req("p", base.clone())).await.unwrap_err();
    assert_eq!(reason(&e), Reason::InvalidArgument, "no command");
    let r = v1::VerifyRequest { argv: sh("true"), repeats: 17, ..base.clone() };
    assert_eq!(reason(&verify.run(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    let r = v1::VerifyRequest { argv: sh("true"), workdir: String::new(), ..base.clone() };
    assert_eq!(reason(&verify.run(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    let r = v1::VerifyRequest { argv: sh("true"), ..base.clone() };
    assert_eq!(reason(&verify.run(req("q", r)).await.unwrap_err()), Reason::CellNotFound);
    assert_eq!(fake.live(), 1);

    // A change to a protected file is left out and reported, and hidden files land in the
    // workdir before the command runs.
    std::fs::write(repo.join("test_lib.py"), "assert False\n").unwrap();
    let r = v1::VerifyRequest {
        argv: sh("cat hidden/check.txt && echo '=== 3 passed in 0.01s ==='"),
        files: [("hidden/check.txt".to_owned(), "ok\n".into())].into(),
        repeats: 2,
        ..base.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.error.is_none(), "{:?}", got.error);
    assert!(got.passed && !got.flaky);
    assert_eq!(got.runs_passed, 2);
    assert_eq!(got.tampered, ["test_lib.py"]);
    assert!(got.output.starts_with(b"ok\n"));
    assert_eq!(got.scores["diff_bytes"], 0.0);
    assert_eq!(got.scores["tests_passed"], 3.0);
    for step in ["diff_ms", "create_ms", "apply_ms", "run_ms"] {
        assert!(got.scores.contains_key(step), "{step}");
    }

    // Runs that disagree are flaky, and a verdict that is not all passes fails.
    let r = v1::VerifyRequest {
        argv: sh("test -e flip && exit 3; touch flip"),
        repeats: 2,
        ..base.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(!got.passed && got.flaky);
    assert_eq!((got.runs_passed, got.exit_code), (1, 3));

    // The fake's cells share one filesystem, so the verifier sees the subject's change already
    // made and the diff does not apply. That is the cell's doing, not hivebox's.
    std::fs::write(repo.join("lib.py"), "x = 2\n").unwrap();
    let r = v1::VerifyRequest { argv: sh("true"), ..base.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    let e = got.error.unwrap();
    assert!(!got.passed);
    assert_eq!(e.reason, "FILE_ERROR");
    assert!(!e.is_infra_error);
    assert!(got.scores["diff_bytes"] > 0.0);

    // So is a workdir that is not a git checkout.
    let r = v1::VerifyRequest {
        argv: sh("true"),
        workdir: s.0.to_str().unwrap().into(),
        ..base.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert_eq!(got.error.unwrap().reason, "FILE_ERROR");

    // Without a subject the image is verified as it is.
    let r = v1::VerifyRequest {
        subject_cell_id: String::new(),
        workdir: String::new(),
        argv: sh("echo plain"),
        ..base.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.passed);
    assert_eq!(&got.output[..], b"plain\n");

    // With a report, a run passes on what the report says. One left from before is removed
    // first, so exiting with 0 and writing none fails, whatever the output says.
    let report = s.0.join("report.xml");
    let xml = |cases: &str| {
        format!("printf '%s' \"<testsuite>{cases}</testsuite>\" > {}", report.display())
    };
    let good = xml("<testcase classname='t' name='a'/><testcase classname='t' name='b'/>");
    std::fs::write(&report, "<testsuite><testcase classname='t' name='a'/></testsuite>").unwrap();
    let plain = v1::VerifyRequest {
        subject_cell_id: String::new(),
        workdir: String::new(),
        report: report.to_str().unwrap().into(),
        must_pass: vec!["t.py::a".into()],
        ..base.clone()
    };
    let r = v1::VerifyRequest { argv: sh("echo '=== 3 passed in 0.01s ==='"), ..plain.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(!got.passed && got.error.is_none());
    assert_eq!((got.runs_passed, got.exit_code), (0, 0));
    assert_eq!(got.not_passed, ["t.py::a"]);
    assert_eq!(got.scores["report"], 0.0);
    assert!(!got.scores.contains_key("tests_passed"), "the printed counts are not taken");
    let r = v1::VerifyRequest {
        argv: sh(&format!("{good}; echo '=== 9 passed in 0.01s ==='")),
        repeats: 2,
        ..plain.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.passed && got.not_passed.is_empty());
    assert_eq!(got.runs_passed, 2);
    assert_eq!((got.scores["report"], got.scores["tests_passed"]), (1.0, 2.0));
    // A test that had to pass and is not there fails the run, and so does one that failed.
    let r = v1::VerifyRequest {
        argv: sh(&good),
        must_pass: vec!["t.py::a".into(), "t.py::c".into()],
        ..plain.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(!got.passed);
    assert_eq!(got.not_passed, ["t.py::c"]);
    let r = v1::VerifyRequest {
        argv: sh(&xml(
            "<testcase classname='t' name='a'/><testcase classname='t' name='b'><failure/></testcase>",
        )),
        ..plain.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(!got.passed && got.not_passed.is_empty());
    assert_eq!(got.scores["tests_failed"], 1.0);
    let r = v1::VerifyRequest { argv: sh("true"), report: String::new(), ..plain.clone() };
    assert_eq!(reason(&verify.run(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    let r = v1::VerifyRequest { argv: sh("true"), report: "r.xml".into(), ..plain.clone() };
    assert_eq!(reason(&verify.run(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    // Every verifier cell is gone, and the subject is still there.
    assert_eq!(fake.live(), 1);
    api.stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn verify_hands_the_runs_to_a_grader_for_a_reward() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let dir = s.0.join("graders");
    std::fs::create_dir_all(&dir).unwrap();
    for name in ["fraction", "exact", "echo", "fail", "spin"] {
        std::fs::write(dir.join(format!("{name}.wasm")), graders::component(name)).unwrap();
    }
    let out = s.0.join("out.txt");
    let base = v1::VerifyRequest {
        verifier: Some(v1_spec("python", &[])),
        argv: vec!["sh".into(), "-c".into(), "test -e flip && echo 42; touch flip".into()],
        workdir: s.0.to_str().unwrap().into(),
        repeats: 3,
        ..Default::default()
    };
    let mut verify = api.verify();

    // A grader is checked before anything is made, and its fields need one.
    for r in [
        v1::VerifyRequest { grader: "none".into(), ..base.clone() },
        v1::VerifyRequest { grader: "../exact".into(), ..base.clone() },
        v1::VerifyRequest { task: "42\n".into(), ..base.clone() },
        v1::VerifyRequest { grader_files: vec!["out.txt".into()], ..base.clone() },
        v1::VerifyRequest {
            grader: "exact".into(),
            grader_files: vec!["out.txt".into()],
            workdir: String::new(),
            ..base.clone()
        },
    ] {
        assert_eq!(reason(&verify.run(req("p", r)).await.unwrap_err()), Reason::InvalidArgument);
    }
    assert_eq!(fake.live(), 0);

    // Every run goes to the grader, not only the last.
    let r = v1::VerifyRequest { grader: "exact".into(), task: "42\n".into(), ..base.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.error.is_none(), "{:?}", got.error);
    assert_eq!((got.reward, got.grade_detail.as_str()), (Some(1.0), "exact"));
    assert!(got.grade_error.is_empty());
    assert!(got.scores.contains_key("grade_ms"));
    std::fs::remove_file(s.0.join("flip")).unwrap();
    let r = v1::VerifyRequest { grader: "fraction".into(), ..base.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.passed);
    assert_eq!(got.reward, Some(1.0));
    let r = v1::VerifyRequest {
        grader: "fraction".into(),
        argv: vec!["sh".into(), "-c".into(), "test -e flip && exit 1; touch flip".into()],
        ..base.clone()
    };
    std::fs::remove_file(s.0.join("flip")).unwrap();
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.flaky && !got.passed);
    assert_eq!(got.reward, Some(1.0 / 3.0));

    // The files asked for are read after the last run, and one that is not there is none.
    std::fs::write(&out, "made").unwrap();
    let r = v1::VerifyRequest {
        grader: "echo".into(),
        task: "the task".into(),
        grader_files: vec!["out.txt".into(), out.to_str().unwrap().into(), "gone".into()],
        repeats: 2,
        ..base.clone()
    };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert_eq!(got.reward, Some(20000.0 + 2000.0 + 300.0 + 1.0));
    assert_eq!(got.grade_detail, "the task");

    // A grader that cannot grade leaves the verdict as it was and gives no reward.
    let r = v1::VerifyRequest { grader: "fail".into(), repeats: 1, ..base.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert!(got.error.is_none() && got.passed);
    assert_eq!((got.reward, got.grade_error.as_str()), (None, "no answer"));
    let r = v1::VerifyRequest { grader: "spin".into(), repeats: 1, ..base.clone() };
    let got = verify.run(req("p", r)).await.unwrap().into_inner();
    assert_eq!(got.reward, None);
    assert_eq!(got.grade_error, "the grader ran past its 2s");
    assert_eq!(fake.live(), 0);
    api.stop.cancel();
}

/// The events in the audit log in `dir`, oldest first.
#[tokio::test(flavor = "multi_thread")]
async fn a_fork_makes_cells_like_the_parent_once_per_key() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mut cells = api.cells();
    let mut snaps = SnapshotsClient::new(api.channel.clone());
    let parent = create(&mut cells, "p", 1, v1_spec("python", &[("run", "a")])).await.remove(0);
    // The fake driver writes nothing, so the parent is given something to copy.
    let upper = s.0.join("cells").join(&parent.id).join("upper");
    std::fs::create_dir_all(upper.join("srv")).unwrap();
    std::fs::write(upper.join("srv/note"), "one").unwrap();
    let fork = |project: &str, count: u32, key: &str| {
        let labels = [("branch".to_string(), "b".to_string())].into();
        let r = v1::ForkRequest {
            cell_id: parent.id.clone(),
            count,
            labels,
            idempotency_key: key.into(),
        };
        req(project, r)
    };

    let e = snaps.fork(fork("p", 17, "")).await.unwrap_err();
    assert_eq!(reason(&e), Reason::InvalidArgument, "{e}");
    let e = snaps.fork(fork("q", 1, "")).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotFound, "{e}");

    let forked = |r| {
        let mut snaps = snaps.clone();
        async move {
            let mut events = snaps.fork(r).await.unwrap().into_inner();
            let mut out = vec![None; 3];
            while let Some(e) = events.message().await.unwrap() {
                match e.result.unwrap() {
                    v1::create_event::Result::Cell(c) => out[e.index as usize] = Some(c),
                    v1::create_event::Result::Error(e) => panic!("a child failed: {e:?}"),
                }
            }
            out.into_iter().map(Option::unwrap).collect::<Vec<_>>()
        }
    };
    let children = forked(fork("p", 3, "k")).await;
    for c in &children {
        assert_ne!(c.id, parent.id);
        assert_eq!(c.state(), v1::CellState::Running);
        let labels = &c.spec.as_ref().unwrap().labels;
        assert_eq!((labels["run"].as_str(), labels["branch"].as_str()), ("a", "b"));
    }
    // A retry gets the same children, and the parent is not frozen and copied again.
    let again = forked(fork("p", 3, "k")).await;
    let ids = |v: &[v1::Cell]| v.iter().map(|c| c.id.clone()).collect::<Vec<_>>();
    assert_eq!(ids(&again), ids(&children));
    let text = api.comb.metrics().registry().render();
    assert!(text.contains("hive_snapshot_seconds_count{stage=\"fork_copy\"} 1\n"), "{text}");
    let forks = s.0.join("forks");
    let until = Instant::now() + Duration::from_secs(5);
    while std::fs::read_dir(&forks).unwrap().next().is_some() {
        assert!(Instant::now() < until, "the fork's copy was left");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let stop = v1::StopRequest { selector: Some(by_id(&parent.id)), ..Default::default() };
    cells.stop(req("p", stop)).await.unwrap();
    let e = snaps.fork(fork("p", 1, "")).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotRunning, "{e}");
    api.stop.cancel();
}

fn audited(dir: &Path) -> Vec<hive_telemetry::AuditEvent> {
    let mut hours: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|x| x == "log"))
        .collect();
    hours.sort();
    let mut out = Vec::new();
    for h in hours {
        for line in std::fs::read_to_string(h).unwrap().lines() {
            let mut v: serde_json::Value = serde_json::from_str(line).unwrap();
            out.push(serde_json::from_value(v["event"].take()).unwrap());
        }
    }
    out
}

#[tokio::test(flavor = "multi_thread")]
async fn every_call_goes_into_the_audit_log_with_who_made_it() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let cell = create(&mut api.cells(), "p", 1, v1_spec("python", &[])).await.remove(0);
    let id = cell.id.as_str();

    // A gate says who the caller is, and the trace id comes from the W3C header.
    let trace = "4bf92f3577b34da6a3ce929d0e0e4736";
    let run = v1::RunRequest { cell_id: id.into(), shell: "echo hi".into(), ..Default::default() };
    let mut r = req("p", run);
    r.metadata_mut().insert(api::PRINCIPAL_HEADER, "key:0123456789abcdef".parse().unwrap());
    r.metadata_mut()
        .insert("traceparent", format!("00-{trace}-00f067aa0ba902b7-01").parse().unwrap());
    api.exec().run(r).await.unwrap();
    let path = s.0.join("files/secret.txt");
    write(&mut api.files(), "p", id, path.to_str().unwrap(), b"hunter2", 3).await.unwrap();
    let e = api.cells().get(req("q", v1::GetCellRequest { id: id.into() })).await.unwrap_err();
    assert_eq!(reason(&e), Reason::CellNotFound);
    // An empty principal is refused, not logged as no one.
    let mut r = req("p", v1::GetCellRequest { id: id.into() });
    r.metadata_mut().insert(api::PRINCIPAL_HEADER, "".parse().unwrap());
    assert_eq!(reason(&api.cells().get(r).await.unwrap_err()), Reason::InvalidArgument);
    let stop = v1::StopRequest { selector: Some(by_id(id)), snapshot: false };
    api.cells().stop(req("p", stop)).await.unwrap();

    let log = api.comb.audit().unwrap();
    tokio::task::block_in_place(|| log.flush()).unwrap();
    let dir = s.0.join("audit");
    let got = audited(&dir);
    let seen: Vec<_> = got
        .iter()
        .map(|e| (e.principal.as_str(), e.project.as_str(), e.op.as_str(), e.result.as_str()))
        .collect();
    assert_eq!(
        seen,
        [
            ("local", "p", "cell.create", "ok"),
            ("key:0123456789abcdef", "p", "exec.run", "exit=0 out=3 err=0"),
            ("local", "p", "file.write", "ok bytes=7"),
            ("local", "q", "cell.get", "NotFound"),
            ("local", "p", "cell.stop", "ok"),
        ]
    );
    assert!(got.iter().all(|e| e.cell == id && e.args.len() == 64));
    assert_eq!(got[1].trace, trace);
    assert!(got.iter().filter(|e| e.op != "exec.run").all(|e| e.trace.is_empty()));
    // The file's contents are only in the log as part of a hash.
    let text: String = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| std::fs::read_to_string(e.unwrap().path()).unwrap())
        .collect();
    assert!(!text.contains("hunter2"));
    let v = hive_telemetry::audit::verify(&dir).unwrap().unwrap();
    assert_eq!(v.events, 5);
    api.stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_quarantine_says_what_it_did_to_each_cell() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mut cells = api.cells();
    let mut spec = v1_spec("python", &[("run", "a")]);
    spec.hard_ttl = Some(prost_types::Duration { seconds: 3600, nanos: 0 });
    let made = create(&mut cells, "p", 2, spec).await;

    // Another project's cells are not found, so nothing happens to them.
    let other = v1::QuarantineRequest { selector: Some(by_id(&made[0].id)), reason: String::new() };
    assert_eq!(reason(&cells.quarantine(req("q", other)).await.unwrap_err()), Reason::CellNotFound);

    let r = v1::QuarantineRequest {
        selector: Some(by_labels(&[("run", "a")])),
        reason: "it sent a key out".into(),
    };
    let q = cells.quarantine(req("p", r)).await.unwrap().into_inner();
    let result = q.result.unwrap();
    assert_eq!((result.matched, result.succeeded, result.failures.len()), (2, 2, 0));
    let mut ids: Vec<&str> = q.cells.iter().map(|c| c.cell_id.as_str()).collect();
    ids.sort_unstable();
    let mut want: Vec<&str> = made.iter().map(|c| c.id.as_str()).collect();
    want.sort_unstable();
    assert_eq!(ids, want);
    // The fake's image is not from a store, so there is no snapshot, and says why.
    for c in &q.cells {
        assert_eq!(c.network, "unmanaged");
        assert!(c.snapshot_id.is_empty());
        let e = c.snapshot_error.as_ref().unwrap();
        assert_eq!(e.reason, "POLICY_DENIED", "{e:?}");
    }
    let got = cells.get(req("p", v1::GetCellRequest { id: made[0].id.clone() })).await.unwrap();
    let got = got.into_inner();
    assert_eq!((got.state(), got.quarantined), (v1::CellState::Paused, true));
    assert_eq!(got.expires_at, None, "no timer stops a quarantined cell");

    let log = api.comb.audit().unwrap();
    tokio::task::block_in_place(|| log.flush()).unwrap();
    let got: Vec<_> =
        audited(&s.0.join("audit")).into_iter().filter(|e| e.op == "cell.quarantine").collect();
    assert_eq!(got.len(), 3, "{got:?}");
    assert_eq!((got[0].project.as_str(), got[0].result.as_str()), ("q", "NotFound"));
    for e in &got[1..] {
        assert!(e.result.starts_with("ok network=unmanaged cut_ms="), "{e:?}");
        assert!(e.result.ends_with(" snapshot PermissionDenied"), "{e:?}");
    }
    api.stop.cancel();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_socket_is_the_owners_only_and_replaced_on_restart() {
    use std::os::unix::fs::PermissionsExt;
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mode = std::fs::metadata(&api.socket).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o600);
    // A socket left behind is replaced, and the new one answers.
    api.stop.cancel();
    let listener = api::bind(&api.socket).unwrap();
    let stop = CancellationToken::new();
    tokio::spawn(api::serve(api.comb.clone(), listener, None, stop.clone()));
    let mut cells = CellsClient::new(connect(&api.socket).await);
    cells.list(v1::ListCellsRequest::default()).await.unwrap();
    // A path no service answers is refused cleanly.
    let mut health = tonic::client::Grpc::new(connect(&api.socket).await);
    health.ready().await.unwrap();
    let path = tonic::codegen::http::uri::PathAndQuery::from_static("/grpc.health.v1.Health/Check");
    let codec = tonic_prost::ProstCodec::<v1::Empty, v1::Empty>::default();
    let e = health.unary(tonic::Request::new(v1::Empty {}), path, codec).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unimplemented);
    stop.cancel();
    let _ = std::fs::remove_file(&api.socket);
}

#[tokio::test(flavor = "multi_thread")]
async fn gates_reach_the_same_api_over_tcp() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let socket = s.0.join("comb.sock");
    let listener = api::bind(&socket).unwrap();
    let tcp = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", tcp.local_addr().unwrap());
    let stop = CancellationToken::new();
    let server = tokio::spawn(api::serve(comb.clone(), listener, Some(tcp), stop.clone()));
    let mut remote =
        CellsClient::new(Endpoint::from_shared(addr).unwrap().connect().await.unwrap());
    let create = v1::CreateRequest { spec: Some(v1_spec("python", &[])), ..Default::default() };
    let mut events = remote.create(req("swe", create)).await.unwrap().into_inner();
    let event = events.message().await.unwrap().unwrap();
    let Some(v1::create_event::Result::Cell(cell)) = event.result else { panic!("{event:?}") };
    // The same cell, over the socket.
    let mut local = CellsClient::new(connect(&socket).await);
    let got = local.get(req("swe", v1::GetCellRequest { id: cell.id.clone() })).await.unwrap();
    assert_eq!(got.into_inner().id, cell.id);
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server).await.unwrap().unwrap().unwrap();
    comb.shutdown().await;
}

/// Numbers for the API's own cost, next to the same calls made on the comb directly. Run it with
/// `cargo test --release -p hive-comb --test api -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn api_cost() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let api = Served::new(&s, &fake).await;
    let mut cells = api.cells();
    let mut exec = api.exec();
    let cell = create(&mut cells, "p", 1, v1_spec("python", &[])).await.remove(0);
    let id: CellId = cell.id.parse().unwrap();
    let n = 2000;
    // Small cells, so the node's memory ceiling is not what the waves below run into.
    let small = || {
        let mut spec = v1_spec("python", &[]);
        spec.resources = Some(v1::Resources { mem_mib: 64, ..Default::default() });
        spec
    };

    let mut get = Vec::with_capacity(n);
    for _ in 0..n {
        let t = Instant::now();
        cells.get(req("p", v1::GetCellRequest { id: cell.id.clone() })).await.unwrap();
        get.push(t.elapsed());
    }
    let mut direct_run = Vec::with_capacity(n);
    let mut api_run = Vec::with_capacity(n);
    let drone = api.comb.drone(id).await.unwrap();
    for _ in 0..n {
        let t = Instant::now();
        let r = RunRequest {
            command: Some(Command { argv: vec!["true".into()], ..Command::default() }),
            ..RunRequest::default()
        };
        drone.run(&r).await.unwrap();
        direct_run.push(t.elapsed());
        let t = Instant::now();
        let r = v1::RunRequest {
            cell_id: cell.id.clone(),
            argv: vec!["true".into()],
            ..Default::default()
        };
        exec.run(req("p", r)).await.unwrap();
        api_run.push(t.elapsed());
    }
    println!("get: {}", summary(&mut get));
    println!("run true, on the comb: {}", summary(&mut direct_run));
    println!("run true, through the API: {}", summary(&mut api_run));

    // Creates and stops through the API from many callers at once, on one connection each.
    for concurrency in [1usize, 16, 64] {
        let total = 1024.max(concurrency * 8);
        let started = Instant::now();
        let workers: Vec<_> = (0..concurrency)
            .map(|_| {
                let socket = api.socket.clone();
                tokio::spawn(async move {
                    let mut cells = CellsClient::new(connect(&socket).await);
                    let mut creates = Vec::new();
                    for _ in 0..total / concurrency {
                        let t = Instant::now();
                        let c = create(&mut cells, "p", 1, small()).await.remove(0);
                        creates.push(t.elapsed());
                        let stop =
                            v1::StopRequest { selector: Some(by_id(&c.id)), snapshot: false };
                        cells.stop(req("p", stop)).await.unwrap();
                    }
                    creates
                })
            })
            .collect();
        let mut creates = Vec::new();
        for w in workers {
            creates.extend(w.await.unwrap());
        }
        let secs = started.elapsed().as_secs_f64();
        println!(
            "c={concurrency}: {} cells made and stopped in {secs:.2}s, {:.0}/s, create {}",
            creates.len(),
            creates.len() as f64 / secs,
            summary(&mut creates)
        );
    }

    // One call making many cells at once.
    for count in [16u32, 64] {
        let t = Instant::now();
        let made = create(&mut cells, "batch", count, small()).await;
        let took = t.elapsed();
        let stop = v1::StopRequest { selector: Some(by_labels(&[])), snapshot: false };
        let t2 = Instant::now();
        let r = cells.stop(req("batch", stop)).await.unwrap().into_inner();
        println!(
            "count={count}: made in {took:?}, {:.0}/s, {} stopped in {:?}",
            f64::from(count) / took.as_secs_f64(),
            r.succeeded,
            t2.elapsed()
        );
        assert_eq!(made.len(), count as usize);
    }
    api.stop.cancel();
}

fn summary(samples: &mut [Duration]) -> String {
    samples.sort();
    let at = |q: f64| samples[((samples.len() - 1) as f64 * q) as usize];
    format!("p50 {:?}, p99 {:?}, max {:?}", at(0.5), at(0.99), at(1.0))
}

/// A request stream fed from a channel.
fn tokio_stream_of<T: Send + 'static>(
    mut rx: tokio::sync::mpsc::Receiver<T>,
) -> impl futures::Stream<Item = T> + Send + 'static {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx))
}
