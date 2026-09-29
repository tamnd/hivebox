//! The local API end to end: a real gRPC client on the comb's Unix socket, over the fake driver
//! from `common`.

#![cfg(target_os = "linux")]

mod common;

use common::*;
use hive_comb::api;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::exec_client::ExecClient;
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
        tokio::spawn(api::serve(comb.clone(), listener, stop.clone()));
        let channel = connect(&socket).await;
        Self { comb, stop, channel, socket }
    }

    fn cells(&self) -> CellsClient<Channel> {
        CellsClient::new(self.channel.clone())
    }

    fn exec(&self) -> ExecClient<Channel> {
        ExecClient::new(self.channel.clone())
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
    tokio::spawn(api::serve(api.comb.clone(), listener, stop.clone()));
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
fn tokio_stream_of(
    mut rx: tokio::sync::mpsc::Receiver<v1::ProcessInput>,
) -> impl futures::Stream<Item = v1::ProcessInput> + Send + 'static {
    futures::stream::poll_fn(move |cx| rx.poll_recv(cx))
}
