//! The gate end to end: a scout in the same process, two made up combs on TCP that answer from
//! memory, and a real client calling the gate.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::StreamExt;
use futures::stream::BoxStream;
use hive_gate::{Gate, Nodes};
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::cells_server::{Cells, CellsServer};
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::exec_server::{Exec, ExecServer};
use hive_proto::v1::files_server::{Files, FilesServer};
use hive_proto::v1::verify_client::VerifyClient;
use hive_proto::v1::verify_server::{Verify, VerifyServer};
use hive_scout::NodeReport;
use hive_types::{Backend, CellId, Reason};
use hive_waggle::{BackendSet, LayerBloom};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tonic::transport::Channel;
use tonic::{Request, Response, Status, Streaming};

const KEY: &str = "hb_test_key";

/// A comb that keeps its cells in a list and makes up exec output.
#[derive(Clone, Default)]
struct FakeComb {
    node: u16,
    /// Cells each create call may still make before it says there is no room.
    room: Arc<Mutex<u32>>,
    cells: Arc<Mutex<BTreeMap<CellId, v1::Cell>>>,
    /// The cell each single cell key made, checked before the room as a comb does.
    keys: Arc<Mutex<HashMap<String, v1::Cell>>>,
    /// The single cell keys turned away for room, turned away again unless the gate says not to.
    turned: Arc<Mutex<HashSet<String>>>,
    seq: Arc<Mutex<u64>>,
    /// Files by path, with the user that wrote each.
    files: Arc<Mutex<BTreeMap<String, (bytes::Bytes, String)>>>,
    /// The signals sent to processes, by pid.
    signals: Arc<Mutex<Vec<(u32, i32)>>>,
}

fn project<T>(r: &Request<T>) -> String {
    r.metadata().get("x-hive-project").unwrap().to_str().unwrap().to_owned()
}

fn not_found() -> Status {
    Status::not_found("CELL_NOT_FOUND: no such cell")
}

impl FakeComb {
    fn find(&self, project: &str, id: &str) -> Result<v1::Cell, Status> {
        let id: CellId = id.parse().map_err(|_| Status::invalid_argument("id"))?;
        let cells = self.cells.lock().unwrap();
        cells.get(&id).filter(|c| c.project == project).cloned().ok_or_else(not_found)
    }

    fn picked(&self, project: &str, sel: Option<v1::CellSelector>) -> Vec<CellId> {
        let cells = self.cells.lock().unwrap();
        cells
            .values()
            .filter(|c| c.project == project)
            .filter(|c| match sel.as_ref().and_then(|s| s.by.as_ref()) {
                Some(v1::cell_selector::By::Id(id)) => &c.id == id,
                Some(v1::cell_selector::By::Labels(l)) => {
                    let labels = &c.spec.as_ref().unwrap().labels;
                    l.r#match.iter().all(|(k, v)| labels.get(k) == Some(v))
                }
                None => false,
            })
            .map(|c| c.id.parse().unwrap())
            .collect()
    }

    fn set_state(
        &self,
        project: &str,
        sel: Option<v1::CellSelector>,
        state: v1::CellState,
    ) -> v1::BulkResult {
        let ids = self.picked(project, sel);
        let mut cells = self.cells.lock().unwrap();
        for id in &ids {
            cells.get_mut(id).unwrap().state = state.into();
        }
        v1::BulkResult { matched: ids.len() as u32, succeeded: ids.len() as u32, failures: vec![] }
    }
}

#[tonic::async_trait]
impl Cells for FakeComb {
    type CreateStream = BoxStream<'static, Result<v1::CreateEvent, Status>>;
    type WatchStream = BoxStream<'static, Result<v1::CellEvent, Status>>;

    async fn create(
        &self,
        req: Request<v1::CreateRequest>,
    ) -> Result<Response<Self::CreateStream>, Status> {
        let project = project(&req);
        let anyway = req.metadata().contains_key(hive_gate::ANYWAY_HEADER);
        let req = req.into_inner();
        let mut events = Vec::new();
        let key = (req.count <= 1 && !req.idempotency_key.is_empty())
            .then(|| format!("{project}\0{}", req.idempotency_key));
        if let Some(cell) = key.as_ref().and_then(|k| self.keys.lock().unwrap().get(k).cloned()) {
            let ev =
                v1::CreateEvent { index: 0, result: Some(v1::create_event::Result::Cell(cell)) };
            return Ok(Response::new(Box::pin(futures::stream::iter([Ok(ev)]))));
        }
        let turned = key.as_ref().is_some_and(|k| self.turned.lock().unwrap().contains(k));
        for index in 0..req.count.max(1) {
            let mut room = self.room.lock().unwrap();
            if *room == 0
                && let Some(k) = &key
            {
                self.turned.lock().unwrap().insert(k.clone());
            }
            let result = if *room == 0 || (turned && !anyway) {
                v1::create_event::Result::Error(v1::Error {
                    reason: "CAPACITY_UNAVAILABLE".into(),
                    message: "full".into(),
                    retryable: true,
                    ..Default::default()
                })
            } else {
                *room -= 1;
                let mut seq = self.seq.lock().unwrap();
                *seq += 1;
                let id = CellId::new(1, self.node, 1, *seq, 5).unwrap();
                let cell = v1::Cell {
                    id: id.to_string(),
                    project: project.clone(),
                    node: self.node.to_string(),
                    spec: req.spec.clone(),
                    state: v1::CellState::Running.into(),
                    ..Default::default()
                };
                self.cells.lock().unwrap().insert(id, cell.clone());
                if let Some(k) = &key {
                    self.keys.lock().unwrap().insert(k.clone(), cell.clone());
                }
                v1::create_event::Result::Cell(cell)
            };
            events.push(Ok(v1::CreateEvent { index, result: Some(result) }));
        }
        Ok(Response::new(Box::pin(futures::stream::iter(events))))
    }

    async fn get(&self, req: Request<v1::GetCellRequest>) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req);
        Ok(Response::new(self.find(&project, &req.get_ref().id)?))
    }

    async fn list(
        &self,
        req: Request<v1::ListCellsRequest>,
    ) -> Result<Response<v1::ListCellsResponse>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        let after: Option<CellId> =
            (!req.page_token.is_empty()).then(|| req.page_token.parse().unwrap());
        let cells = self.cells.lock().unwrap();
        let mut picked = cells
            .iter()
            .filter(|(id, c)| c.project == project && after.is_none_or(|a| **id > a))
            .map(|(_, c)| c.clone());
        let page: Vec<v1::Cell> = picked.by_ref().take(req.page_size as usize).collect();
        let next_page_token = match (picked.next(), page.last()) {
            (Some(_), Some(last)) => last.id.clone(),
            _ => String::new(),
        };
        Ok(Response::new(v1::ListCellsResponse { cells: page, next_page_token }))
    }

    async fn watch(
        &self,
        _req: Request<v1::WatchCellsRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }

    async fn pause(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req);
        Ok(Response::new(self.set_state(&project, Some(req.into_inner()), v1::CellState::Paused)))
    }

    async fn resume(
        &self,
        req: Request<v1::CellSelector>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req);
        Ok(Response::new(self.set_state(&project, Some(req.into_inner()), v1::CellState::Running)))
    }

    async fn stop(
        &self,
        req: Request<v1::StopRequest>,
    ) -> Result<Response<v1::BulkResult>, Status> {
        let project = project(&req);
        Ok(Response::new(self.set_state(
            &project,
            req.into_inner().selector,
            v1::CellState::Stopped,
        )))
    }

    async fn extend_ttl(
        &self,
        req: Request<v1::ExtendTtlRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req);
        Ok(Response::new(self.find(&project, &req.get_ref().id)?))
    }

    async fn update_policy(
        &self,
        req: Request<v1::UpdatePolicyRequest>,
    ) -> Result<Response<v1::Cell>, Status> {
        let project = project(&req);
        Ok(Response::new(self.find(&project, &req.get_ref().id)?))
    }

    async fn expose_port(
        &self,
        _req: Request<v1::ExposePortRequest>,
    ) -> Result<Response<v1::PortEndpoint>, Status> {
        Err(Status::unimplemented("expose"))
    }
}

type Out<T> = BoxStream<'static, Result<T, Status>>;

#[tonic::async_trait]
impl Exec for FakeComb {
    type StartStream = Out<v1::ProcessOutput>;
    type SessionInteractStream = Out<v1::SessionOutput>;

    async fn run(&self, req: Request<v1::RunRequest>) -> Result<Response<v1::RunResult>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        let cell = self.find(&project, &req.cell_id)?;
        let stdout = format!("node {} cell {} stdin {}", self.node, cell.id, req.stdin.len());
        Ok(Response::new(v1::RunResult {
            exit_code: 0,
            stdout: stdout.into(),
            ..Default::default()
        }))
    }

    /// Says the command and who runs it, then echoes stdin until it closes, and exits with 3.
    async fn start(
        &self,
        req: Request<Streaming<v1::ProcessInput>>,
    ) -> Result<Response<Self::StartStream>, Status> {
        use v1::process_input::Input;
        use v1::process_output::Output;
        let project = project(&req);
        let mut inputs = req.into_inner();
        let Some(Input::Start(start)) = inputs.message().await?.and_then(|m| m.input) else {
            return Err(Status::invalid_argument("start first"));
        };
        self.find(&project, &start.cell_id)?;
        let said = format!("{} as {:?} in {:?}", start.argv.join(" "), start.user, start.cwd);
        let first = [Output::Pid(7), Output::Stdout(said.into())];
        let echo = futures::stream::unfold(Some(inputs), |inputs| async move {
            let mut inputs = inputs?;
            loop {
                match inputs.message().await {
                    Ok(Some(v1::ProcessInput { input: Some(Input::Stdin(b)) })) => {
                        return Some((Output::Stdout(b), Some(inputs)));
                    }
                    Ok(Some(v1::ProcessInput { input: Some(Input::Eof(_)) }) | None) | Err(_) => {
                        let exit = v1::RunResult { exit_code: 3, ..Default::default() };
                        return Some((Output::Exit(exit), None));
                    }
                    Ok(Some(_)) => {}
                }
            }
        });
        let out = futures::stream::iter(first)
            .chain(echo)
            .map(|o| Ok(v1::ProcessOutput { output: Some(o) }));
        Ok(Response::new(Box::pin(out)))
    }

    async fn signal(&self, req: Request<v1::SignalRequest>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        self.find(&project, &req.cell_id)?;
        self.signals.lock().unwrap().push((req.pid, req.signal));
        Ok(Response::new(v1::Empty {}))
    }

    async fn session_create(
        &self,
        _req: Request<v1::SessionCreateRequest>,
    ) -> Result<Response<v1::Session>, Status> {
        Err(Status::unimplemented("session"))
    }

    async fn session_run(
        &self,
        _req: Request<v1::SessionRunRequest>,
    ) -> Result<Response<v1::SessionRunResult>, Status> {
        Err(Status::unimplemented("session"))
    }

    async fn session_interact(
        &self,
        _req: Request<Streaming<v1::SessionInput>>,
    ) -> Result<Response<Self::SessionInteractStream>, Status> {
        Err(Status::unimplemented("session"))
    }

    async fn session_close(
        &self,
        _req: Request<v1::SessionRef>,
    ) -> Result<Response<v1::Empty>, Status> {
        Err(Status::unimplemented("session"))
    }
}

#[tonic::async_trait]
impl Files for FakeComb {
    type ReadStream = Out<v1::Chunk>;
    type WatchStream = Out<v1::FsEvent>;

    async fn read(
        &self,
        req: Request<v1::ReadFileRequest>,
    ) -> Result<Response<Self::ReadStream>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        self.find(&project, &req.cell_id)?;
        let files = self.files.lock().unwrap();
        let (data, _) =
            files.get(&req.path).cloned().ok_or_else(|| Status::not_found("no file"))?;
        // Two chunks, to see they are put back together.
        let half = data.len() / 2;
        let chunks = [data.slice(..half), data.slice(half..)];
        let out = chunks.into_iter().map(|data| Ok(v1::Chunk { data }));
        Ok(Response::new(Box::pin(futures::stream::iter(out))))
    }

    async fn write(
        &self,
        req: Request<Streaming<v1::WriteFileChunk>>,
    ) -> Result<Response<v1::FileInfo>, Status> {
        use v1::write_file_chunk::Part;
        let project = project(&req);
        let mut chunks = req.into_inner();
        let Some(Part::Header(h)) = chunks.message().await?.and_then(|c| c.part) else {
            return Err(Status::invalid_argument("header first"));
        };
        self.find(&project, &h.cell_id)?;
        let mut data = Vec::new();
        while let Some(c) = chunks.message().await? {
            if let Some(Part::Data(b)) = c.part {
                data.extend_from_slice(&b);
            }
        }
        let size = data.len() as u64;
        self.files.lock().unwrap().insert(h.path.clone(), (data.into(), h.user));
        Ok(Response::new(v1::FileInfo { path: h.path, size, ..Default::default() }))
    }

    async fn stat(&self, req: Request<v1::PathRequest>) -> Result<Response<v1::FileInfo>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        self.find(&project, &req.cell_id)?;
        let files = self.files.lock().unwrap();
        let (data, _) = files.get(&req.path).ok_or_else(|| Status::not_found("no file"))?;
        let info = v1::FileInfo {
            path: req.path,
            r#type: v1::FileType::File.into(),
            size: data.len() as u64,
            mode: 0o644,
            ..Default::default()
        };
        Ok(Response::new(info))
    }

    async fn list(
        &self,
        _req: Request<v1::ListDirRequest>,
    ) -> Result<Response<v1::ListDirResponse>, Status> {
        Err(Status::unimplemented("list"))
    }

    async fn remove(&self, req: Request<v1::PathRequest>) -> Result<Response<v1::Empty>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        self.find(&project, &req.cell_id)?;
        self.files.lock().unwrap().remove(&req.path).ok_or_else(|| Status::not_found("no file"))?;
        Ok(Response::new(v1::Empty {}))
    }

    async fn watch(
        &self,
        _req: Request<v1::WatchDirRequest>,
    ) -> Result<Response<Self::WatchStream>, Status> {
        Err(Status::unimplemented("watch"))
    }

    async fn diff(
        &self,
        _req: Request<v1::DiffRequest>,
    ) -> Result<Response<v1::DiffResult>, Status> {
        Err(Status::unimplemented("diff"))
    }

    async fn apply(&self, _req: Request<v1::ApplyRequest>) -> Result<Response<v1::Empty>, Status> {
        Err(Status::unimplemented("apply"))
    }
}

/// Says which node ran the verify, in the scores, and takes a cell's room for the verifier.
#[tonic::async_trait]
impl Verify for FakeComb {
    async fn run(
        &self,
        req: Request<v1::VerifyRequest>,
    ) -> Result<Response<v1::VerifyResult>, Status> {
        let project = project(&req);
        let req = req.into_inner();
        if !req.subject_cell_id.is_empty() {
            self.find(&project, &req.subject_cell_id)?;
        }
        let mut room = self.room.lock().unwrap();
        if *room == 0 {
            let error = v1::Error {
                reason: "CAPACITY_UNAVAILABLE".into(),
                message: "full".into(),
                retryable: true,
                ..Default::default()
            };
            return Ok(Response::new(v1::VerifyResult {
                error: Some(error),
                ..Default::default()
            }));
        }
        *room -= 1;
        let scores = [("node".to_string(), f64::from(self.node))].into();
        Ok(Response::new(v1::VerifyResult {
            passed: true,
            runs_passed: 1,
            scores,
            ..Default::default()
        }))
    }
}

fn report(node: u16, addr: SocketAddr, seq: u64) -> NodeReport {
    NodeReport {
        node,
        epoch: 1,
        seq,
        addr: Arc::from(format!("http://{addr}")),
        healthy: true,
        backends: BackendSet::of(&[Backend::Container]),
        cpu_milli: 64_000,
        cpu_committed_milli: 0,
        mem_admit_mib: 1 << 20,
        mem_committed_mib: 0,
        cells: 0,
        max_cells: 100_000,
        pool_depth: 64,
        create_rate: 0.0,
        burst_cap: 1000,
        layers: Some(LayerBloom::default()),
        top_projects: vec![],
    }
}

async fn listen() -> (TcpListener, SocketAddr) {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    (l, a)
}

fn incoming(l: TcpListener) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::unfold(l, |l| async move { Some((l.accept().await.map(|(s, _)| s), l)) })
}

struct Cluster {
    combs: Vec<FakeComb>,
    scout: hive_scout::Service,
    comb_addrs: Vec<SocketAddr>,
    addr: SocketAddr,
    channel: Channel,
    stop: CancellationToken,
}

impl Cluster {
    /// Two combs, nodes 1 and 2, with room for `room` cells each.
    async fn new(room: u32) -> Self {
        Self::with(room, None).await
    }

    /// The same, with the gate serving E2B as `e2b` says.
    async fn with(room: u32, e2b: Option<hive_gate::config::E2b>) -> Self {
        let stop = CancellationToken::new();
        let scout = hive_scout::Service::new();
        tokio::spawn(scout.clone().run(stop.clone()));
        let (sl, saddr) = listen().await;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(scout.server())
                .serve_with_incoming_shutdown(incoming(sl), stop.clone().cancelled_owned()),
        );
        let (mut combs, mut comb_addrs) = (Vec::new(), Vec::new());
        for node in 1..=2u16 {
            let comb = FakeComb { node, room: Arc::new(Mutex::new(room)), ..Default::default() };
            let (l, addr) = listen().await;
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(CellsServer::new(comb.clone()))
                    .add_service(ExecServer::new(comb.clone()))
                    .add_service(FilesServer::new(comb.clone()))
                    .add_service(VerifyServer::new(comb.clone()))
                    .serve_with_incoming_shutdown(incoming(l), stop.clone().cancelled_owned()),
            );
            // Reports keep coming, as from a real comb, so the node never goes stale.
            let (scout, stop) = (scout.clone(), stop.clone());
            tokio::spawn(async move {
                for seq in 1.. {
                    scout.apply(report(node, addr, seq));
                    tokio::select! {
                        () = stop.cancelled() => return,
                        () = tokio::time::sleep(Duration::from_millis(300)) => {}
                    }
                }
            });
            combs.push(comb);
            comb_addrs.push(addr);
        }
        let nodes = Nodes::new(hive_scout::follow(format!("http://{saddr}"), stop.clone()));
        for _ in 0..100 {
            if nodes.all().len() == 2 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert_eq!(nodes.all(), vec![1, 2], "the gate sees both nodes");
        let keys = HashMap::from([(*blake3::hash(KEY.as_bytes()).as_bytes(), Arc::from("swe"))]);
        let mut gate = Gate::new(keys, nodes, None, &hive_telemetry::Registry::new());
        if let Some(e2b) = e2b {
            gate = gate.with_e2b(e2b);
        }
        let (gl, gaddr) = listen().await;
        tokio::spawn(hive_gate::serve(gate, gl, stop.clone()));
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{gaddr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self { combs, scout, comb_addrs, addr: gaddr, channel, stop }
    }

    fn cells(&self) -> CellsClient<Channel> {
        CellsClient::new(self.channel.clone())
    }

    fn exec(&self) -> ExecClient<Channel> {
        ExecClient::new(self.channel.clone())
    }

    fn verify(&self) -> VerifyClient<Channel> {
        VerifyClient::new(self.channel.clone())
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop.cancel();
    }
}

fn authed<T>(msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {KEY}").parse().unwrap());
    // Whatever the caller says its project is, the key decides.
    r.metadata_mut().insert("x-hive-project", "someone-else".parse().unwrap());
    r
}

fn spec(label: &str) -> v1::CellSpec {
    v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: "python:3.12".into() })),
        backend: v1::Backend::Container.into(),
        resources: Some(v1::Resources { mem_mib: 256, ..Default::default() }),
        labels: [("run".to_string(), label.to_string())].into(),
        ..Default::default()
    }
}

async fn create(c: &Cluster, count: u32, label: &str) -> Vec<v1::CreateEvent> {
    let req = v1::CreateRequest { spec: Some(spec(label)), count, ..Default::default() };
    let mut s = c.cells().create(authed(req)).await.unwrap().into_inner();
    let mut out = Vec::new();
    while let Some(ev) = s.message().await.unwrap() {
        out.push(ev);
    }
    out.sort_by_key(|e| e.index);
    out
}

fn cell(e: &v1::CreateEvent) -> Option<&v1::Cell> {
    match &e.result {
        Some(v1::create_event::Result::Cell(c)) => Some(c),
        _ => None,
    }
}

fn error(e: &v1::CreateEvent) -> Option<&v1::Error> {
    match &e.result {
        Some(v1::create_event::Result::Error(e)) => Some(e),
        _ => None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_call_without_a_good_key_is_turned_away() {
    let c = Cluster::new(100).await;
    let e = c.cells().list(Request::new(v1::ListCellsRequest::default())).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::Unauthenticated);
    let mut r = Request::new(v1::ListCellsRequest::default());
    r.metadata_mut().insert("authorization", "Bearer hb_wrong".parse().unwrap());
    assert_eq!(c.cells().list(r).await.unwrap_err().code(), tonic::Code::Unauthenticated);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_is_spread_and_every_call_finds_its_cell() {
    let c = Cluster::new(1000).await;
    let events = create(&c, 40, "a").await;
    assert_eq!(events.len(), 40);
    assert_eq!(events.iter().map(|e| e.index).collect::<Vec<_>>(), (0..40).collect::<Vec<_>>());
    let cells: Vec<&v1::Cell> = events.iter().map(|e| cell(e).expect("a cell")).collect();
    assert!(cells.iter().all(|c| c.project == "swe"), "the key's project, not the header's");
    let on = |n: &str| cells.iter().filter(|c| c.node == n).count();
    assert!(on("1") > 0 && on("2") > 0, "both nodes got some: {} and {}", on("1"), on("2"));

    // Get and exec go to the node in the id.
    for cell in &cells {
        let got = c.cells().get(authed(v1::GetCellRequest { id: cell.id.clone() })).await.unwrap();
        assert_eq!(got.get_ref().id, cell.id);
        let run = v1::RunRequest {
            cell_id: cell.id.clone(),
            stdin: vec![7; 100_000].into(),
            ..Default::default()
        };
        let out = c.exec().run(authed(run)).await.unwrap().into_inner();
        let want = format!("node {} cell {} stdin 100000", cell.node, cell.id);
        assert_eq!(String::from_utf8_lossy(&out.stdout), want);
    }
    let gone = CellId::new(1, 9, 1, 1, 1).unwrap().to_string();
    let e = c.cells().get(authed(v1::GetCellRequest { id: gone.clone() })).await.unwrap_err();
    assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
    let run = v1::RunRequest { cell_id: gone, ..Default::default() };
    assert_eq!(c.exec().run(authed(run)).await.unwrap_err().code(), tonic::Code::NotFound);

    // Pages of 7 walk both nodes and see every cell once.
    let mut seen = Vec::new();
    let mut token = String::new();
    loop {
        let req = v1::ListCellsRequest { page_size: 7, page_token: token, ..Default::default() };
        let page = c.cells().list(authed(req)).await.unwrap().into_inner();
        assert!(page.cells.len() <= 7);
        seen.extend(page.cells.into_iter().map(|c| c.id));
        if page.next_page_token.is_empty() {
            break;
        }
        token = page.next_page_token;
    }
    let mut want: Vec<String> = cells.iter().map(|c| c.id.clone()).collect();
    want.sort_by_key(|id| id.parse::<CellId>().unwrap());
    assert_eq!(seen, want);

    // By label, a stop reaches both nodes.
    create(&c, 5, "b").await;
    let sel = v1::CellSelector {
        by: Some(v1::cell_selector::By::Labels(v1::LabelSelector {
            r#match: [("run".to_string(), "a".to_string())].into(),
        })),
    };
    let r = c
        .cells()
        .stop(authed(v1::StopRequest { selector: Some(sel), snapshot: false }))
        .await
        .unwrap();
    assert_eq!((r.get_ref().matched, r.get_ref().succeeded), (40, 40));
    assert!(r.get_ref().failures.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cells_a_full_node_turns_away_go_to_the_other() {
    let c = Cluster::new(0).await;
    *c.combs[1].room.lock().unwrap() = 1000;
    let events = create(&c, 30, "a").await;
    assert_eq!(events.len(), 30);
    assert!(events.iter().all(|e| cell(e).is_some_and(|c| c.node == "2")), "{events:?}");

    // With no room anywhere, every cell says so once.
    *c.combs[1].room.lock().unwrap() = 0;
    let events = create(&c, 10, "b").await;
    assert_eq!(events.len(), 10);
    for e in &events {
        let Some(v1::create_event::Result::Error(err)) = &e.result else { panic!("{e:?}") };
        assert_eq!(err.reason, "CAPACITY_UNAVAILABLE");
    }
}

async fn verify(c: &Cluster, subject: &str) -> Result<v1::VerifyResult, Status> {
    let req = v1::VerifyRequest {
        subject_cell_id: subject.into(),
        verifier: Some(spec("v")),
        argv: vec!["pytest".into()],
        workdir: "/testbed".into(),
        ..Default::default()
    };
    c.verify().run(authed(req)).await.map(Response::into_inner)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_verify_goes_to_its_subjects_node_or_where_there_is_room() {
    let c = Cluster::new(1000).await;
    let events = create(&c, 8, "s").await;
    let mut seen = HashSet::new();
    for e in &events {
        let subject = cell(e).unwrap();
        let r = verify(&c, &subject.id).await.unwrap();
        assert!(r.passed);
        assert_eq!(r.scores["node"].to_string(), subject.node, "{}", subject.id);
        seen.insert(subject.node.clone());
    }
    assert_eq!(seen.len(), 2, "the subjects were on both nodes");

    // A subject the comb does not know, or that is not a cell id at all, is the caller's error.
    let gone = CellId::new(1, 2, 1, 999, 5).unwrap().to_string();
    assert_eq!(verify(&c, &gone).await.unwrap_err().code(), tonic::Code::NotFound);
    assert_eq!(verify(&c, "nope").await.unwrap_err().code(), tonic::Code::InvalidArgument);

    // With no subject, a node with no room for the verifier cell is passed over.
    *c.combs[0].room.lock().unwrap() = 0;
    for _ in 0..4 {
        assert_eq!(verify(&c, "").await.unwrap().scores["node"], 2.0);
    }
    *c.combs[1].room.lock().unwrap() = 0;
    let e = verify(&c, "").await.unwrap_err();
    assert_eq!(hive_proto::convert::error_from_status(&e).reason, Reason::CapacityUnavailable);

    // A call without a good key gets nowhere.
    let r = Request::new(v1::VerifyRequest::default());
    assert_eq!(c.verify().run(r).await.unwrap_err().code(), tonic::Code::Unauthenticated);
}

async fn create_keyed(c: &Cluster, key: &str) -> v1::CreateEvent {
    let req = v1::CreateRequest {
        spec: Some(spec("k")),
        count: 1,
        idempotency_key: key.into(),
        ..Default::default()
    };
    let mut s = c.cells().create(authed(req)).await.unwrap().into_inner();
    let ev = s.message().await.unwrap().expect("one event");
    assert!(s.message().await.unwrap().is_none());
    ev
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_keyed_create_sent_again_finds_its_cell() {
    let c = Cluster::new(1000).await;
    let mut first = Vec::new();
    for i in 0..40 {
        let ev = create_keyed(&c, &format!("task-{i}")).await;
        first.push(cell(&ev).expect("a cell").clone());
    }
    let on = |n: &str| first.iter().filter(|c| c.node == n).count();
    assert!(on("1") > 0 && on("2") > 0, "keys spread: {} and {}", on("1"), on("2"));
    for (i, was) in first.iter().enumerate() {
        let ev = create_keyed(&c, &format!("task-{i}")).await;
        assert_eq!(cell(&ev).expect("a cell").id, was.id, "task-{i}");
    }
    let made: usize = c.combs.iter().map(|k| k.cells.lock().unwrap().len()).sum();
    assert_eq!(made, 40);

    // A full home turns new keys away and they go to the other node, while the keys it has
    // still find their cells there.
    *c.combs[0].room.lock().unwrap() = 0;
    for i in 40..60 {
        let ev = create_keyed(&c, &format!("task-{i}")).await;
        assert_eq!(cell(&ev).expect("a cell").node, "2", "task-{i}");
    }
    for (i, was) in first.iter().enumerate() {
        let ev = create_keyed(&c, &format!("task-{i}")).await;
        assert_eq!(cell(&ev).expect("a cell").id, was.id, "task-{i}");
    }

    // With room again, the home turns away the keys it turned away before, so they still
    // find their cells on the other node rather than making second ones.
    *c.combs[0].room.lock().unwrap() = 1000;
    let homed = c.combs[0].turned.lock().unwrap().len();
    assert!(homed > 0, "some of the new keys are homed on node 1");
    for i in 40..60 {
        let ev = create_keyed(&c, &format!("task-{i}")).await;
        assert_eq!(cell(&ev).expect("a cell").node, "2", "task-{i}");
    }
    let made: usize = c.combs.iter().map(|k| k.cells.lock().unwrap().len()).sum();
    assert_eq!(made, 60);

    // When both nodes are full they turn every new key away. Once node 1 has room it still
    // turns them away at first, as it did before, and the gate asks again so it makes them.
    *c.combs[0].room.lock().unwrap() = 0;
    *c.combs[1].room.lock().unwrap() = 0;
    for i in 0..10 {
        let ev = create_keyed(&c, &format!("late-{i}")).await;
        let e = error(&ev).expect("no room anywhere");
        assert_eq!(e.reason, Reason::CapacityUnavailable.as_str(), "late-{i}");
    }
    *c.combs[0].room.lock().unwrap() = 1000;
    for i in 0..10 {
        let ev = create_keyed(&c, &format!("late-{i}")).await;
        let made = cell(&ev).expect("a cell").clone();
        assert_eq!(made.node, "1", "late-{i}");
        let ev = create_keyed(&c, &format!("late-{i}")).await;
        assert_eq!(cell(&ev).expect("a cell").id, made.id, "late-{i}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cells_from_an_older_epoch_of_their_node_are_lost() {
    let c = Cluster::new(1000).await;
    let events = create(&c, 20, "a").await;
    let cells: Vec<&v1::Cell> = events.iter().map(|e| cell(e).expect("a cell")).collect();
    let old = cells.iter().find(|c| c.node == "1").expect("a cell on node 1");
    let kept = cells.iter().find(|c| c.node == "2").expect("a cell on node 2");

    // Node 1 registers again. Its epoch 1 reports that keep coming are now stale to scout.
    c.scout.apply(NodeReport { epoch: 2, ..report(1, c.comb_addrs[0], 1) });
    let get = |id: &str| {
        let mut cells = c.cells();
        let req = authed(v1::GetCellRequest { id: id.to_owned() });
        async move { cells.get(req).await }
    };
    let mut e = None;
    for _ in 0..100 {
        match get(&old.id).await {
            Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await,
            Err(s) => {
                e = Some(s);
                break;
            }
        }
    }
    let e = e.expect("the gate sees the new epoch");
    assert_eq!(e.code(), tonic::Code::NotFound, "{e:?}");
    assert_eq!(hive_proto::convert::error_from_status(&e).reason, Reason::CellLost);
    let run = v1::RunRequest { cell_id: old.id.clone(), ..Default::default() };
    let e = c.exec().run(authed(run)).await.unwrap_err();
    assert_eq!(hive_proto::convert::error_from_status(&e).reason, Reason::CellLost);

    // The other node is as it was, and node 1 still takes cells from its new epoch.
    assert_eq!(get(&kept.id).await.unwrap().get_ref().id, kept.id);
    let new = CellId::new(1, 1, 2, 1, 5).unwrap().to_string();
    let e = get(&new).await.unwrap_err();
    assert!(e.message().contains("no such cell"), "the comb answered: {e:?}");
}

/// A Connect call over HTTP/1.1: the status, the content type and the body that came back.
async fn connect(
    c: &Cluster,
    path: &str,
    content_type: &str,
    key: Option<&str>,
    body: Vec<u8>,
) -> (u16, String, bytes::Bytes) {
    use http_body_util::BodyExt;
    let stream = tokio::net::TcpStream::connect(c.addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut send, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    let mut req = tonic::codegen::http::Request::post(path)
        .header("host", "gate")
        .header("content-type", content_type);
    if let Some(key) = key {
        req = req.header("authorization", format!("Bearer {key}"));
    }
    let req = req.body(http_body_util::Full::new(bytes::Bytes::from(body))).unwrap();
    let resp = send.send_request(req).await.unwrap();
    let status = resp.status().as_u16();
    let ct = resp.headers()["content-type"].to_str().unwrap().to_string();
    (status, ct, resp.into_body().collect().await.unwrap().to_bytes())
}

fn envelope(flags: u8, msg: &[u8]) -> Vec<u8> {
    let mut out = vec![flags];
    out.extend_from_slice(&u32::try_from(msg.len()).unwrap().to_be_bytes());
    out.extend_from_slice(msg);
    out
}

fn json(bytes: &[u8]) -> serde_json::Value {
    serde_json::from_slice(bytes).unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connect_callers_get_the_same_api_over_http_1() {
    use prost::Message;
    let c = Cluster::new(100).await;
    let get = "/hivebox.v1.Cells/Get";

    let (status, ct, body) = connect(&c, get, "application/json", None, b"{}".to_vec()).await;
    assert_eq!((status, ct.as_str()), (401, "application/json"));
    assert_eq!(json(&body)["code"], "unauthenticated");

    // A server stream: one envelope in, an envelope a cell and the status last.
    let create = serde_json::json!({
        "spec": {
            "image": { "ref": "python:3.12" },
            "backend": "BACKEND_CONTAINER",
            "resources": { "memMib": 256 },
            "labels": { "run": "connect" },
        },
        "count": 4,
    });
    let body = envelope(0, &serde_json::to_vec(&create).unwrap());
    let (status, ct, out) =
        connect(&c, "/hivebox.v1.Cells/Create", "application/connect+json", Some(KEY), body).await;
    assert_eq!((status, ct.as_str()), (200, "application/connect+json"));
    let (mut ids, mut rest, mut end) = (Vec::new(), &out[..], None);
    while !rest.is_empty() {
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        let msg = json(&rest[5..5 + len]);
        if rest[0] == 2 {
            end = Some(msg);
        } else {
            assert_eq!(msg["cell"]["project"], "swe", "{msg}");
            ids.push(msg["cell"]["id"].as_str().unwrap().to_string());
        }
        rest = &rest[5 + len..];
    }
    assert_eq!(ids.len(), 4);
    assert_eq!(end, Some(serde_json::json!({})));

    for id in &ids {
        let req = serde_json::to_vec(&serde_json::json!({ "id": id })).unwrap();
        let (status, _, body) = connect(&c, get, "application/json", Some(KEY), req).await;
        assert_eq!(status, 200);
        assert_eq!(json(&body)["id"], id.as_str());
    }

    // Protobuf in and out, through to the comb that owns the cell.
    let run = v1::RunRequest { cell_id: ids[0].clone(), stdin: "hi".into(), ..Default::default() };
    let (status, ct, body) =
        connect(&c, "/hivebox.v1.Exec/Run", "application/proto", Some(KEY), run.encode_to_vec())
            .await;
    assert_eq!((status, ct.as_str()), (200, "application/proto"));
    let result = v1::RunResult::decode(body).unwrap();
    assert!(String::from_utf8_lossy(&result.stdout).ends_with("stdin 2"), "{result:?}");

    let lost = CellId::new(1, 9, 1, 1, 5).unwrap().to_string();
    let req = serde_json::to_vec(&serde_json::json!({ "id": lost })).unwrap();
    let (status, _, body) = connect(&c, get, "application/json", Some(KEY), req).await;
    assert_eq!(status, 404);
    assert_eq!(json(&body)["code"], "not_found");

    let (status, _, body) =
        connect(&c, get, "application/json", Some(KEY), b"{\"nope\": 1}".to_vec()).await;
    assert_eq!((status, json(&body)["code"].as_str()), (400, Some("invalid_argument")));
    let (status, _, _) =
        connect(&c, "/hivebox.v1.Cells/Create", "application/json", Some(KEY), b"{}".to_vec())
            .await;
    assert_eq!(status, 400, "a unary content type on a streaming method");
}

/// One HTTP/1.1 call: the status, the headers and the body that came back.
async fn http(
    c: &Cluster,
    method: &str,
    path: &str,
    headers: &[(&str, &str)],
    body: impl Into<bytes::Bytes>,
) -> (u16, tonic::codegen::http::HeaderMap, bytes::Bytes) {
    use http_body_util::BodyExt;
    let stream = tokio::net::TcpStream::connect(c.addr).await.unwrap();
    let io = hyper_util::rt::TokioIo::new(stream);
    let (mut send, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
    tokio::spawn(conn);
    let mut req =
        tonic::codegen::http::Request::builder().method(method).uri(path).header("host", "gate");
    for (k, v) in headers {
        req = req.header(*k, *v);
    }
    let req = req.body(http_body_util::Full::new(body.into())).unwrap();
    let resp = send.send_request(req).await.unwrap();
    let (parts, body) = resp.into_parts();
    (parts.status.as_u16(), parts.headers, body.collect().await.unwrap().to_bytes())
}

/// The messages of a Connect stream, and whether each one ends it.
fn envelopes(mut rest: &[u8]) -> Vec<(bool, serde_json::Value)> {
    let mut out = Vec::new();
    while !rest.is_empty() {
        let len = u32::from_be_bytes(rest[1..5].try_into().unwrap()) as usize;
        out.push((rest[0] & 2 != 0, json(&rest[5..5 + len])));
        rest = &rest[5 + len..];
    }
    out
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_e2b_sdk_makes_sandboxes_moves_files_and_runs_commands() {
    use base64::Engine;
    let b64 = base64::engine::general_purpose::STANDARD;
    let e2b = hive_gate::config::E2b {
        image_key: Some("swe/image".into()),
        templates: [("base".to_string(), "python:3.12".to_string())].into(),
        ..Default::default()
    };
    let c = Cluster::with(100, Some(e2b)).await;
    let key = [("x-api-key", KEY), ("content-type", "application/json")];

    let (status, _, body) = http(&c, "POST", "/v2/sandboxes", &[], "{}").await;
    assert_eq!(status, 401, "{body:?}");
    let (status, _, body) =
        http(&c, "POST", "/v2/sandboxes", &key, r#"{"templateID":"other"}"#).await;
    assert_eq!(status, 400);
    assert!(json(&body)["message"].as_str().unwrap().contains("no image"), "{body:?}");

    let new =
        r#"{"templateID":"base","timeout":300,"metadata":{"swe/image":"swe:42","task":"t1"}}"#;
    let (status, _, body) = http(&c, "POST", "/v2/sandboxes", &key, new).await;
    assert_eq!(status, 201, "{body:?}");
    let made = json(&body);
    let id = made["sandboxID"].as_str().unwrap().to_string();
    assert_eq!(made["envdAccessToken"], KEY);
    assert_eq!(made["envdVersion"], "0.5.7");
    let cell = c.combs.iter().find_map(|comb| comb.find("swe", &id).ok()).unwrap();
    let spec = cell.spec.unwrap();
    assert!(
        matches!(spec.source, Some(v1::cell_spec::Source::Image(ref i)) if i.r#ref == "swe:42")
    );
    assert_eq!(spec.labels["task"], "t1");

    let (status, _, body) = http(&c, "GET", &format!("/sandboxes/{id}"), &key, "").await;
    assert_eq!(status, 200);
    assert_eq!(json(&body)["metadata"], serde_json::json!({ "swe/image": "swe:42", "task": "t1" }));
    let (status, headers, body) =
        http(&c, "GET", "/v2/sandboxes?metadata=task%3Dt1", &key, "").await;
    assert_eq!(status, 200);
    assert_eq!(json(&body).as_array().unwrap().len(), 1, "{headers:?}");

    // envd: the sandbox in a header, the token create gave back, and the user in Basic auth.
    let agent = format!("Basic {}", b64.encode("agent:"));
    let envd = |extra: &'static str| {
        let mut h = vec![("e2b-sandbox-id", id.clone()), ("x-access-token", KEY.to_string())];
        h.push(("authorization", agent.clone()));
        if !extra.is_empty() {
            h.push(("content-type", extra.to_string()));
        }
        h
    };
    let refs =
        |h: &[(&'static str, String)]| h.iter().map(|(k, v)| (*k, v.clone())).collect::<Vec<_>>();
    let call =
        |h: Vec<(&'static str, String)>, method: &'static str, path: String, body: bytes::Bytes| {
            let c = &c;
            async move {
                let h: Vec<(&str, &str)> = h.iter().map(|(k, v)| (*k, v.as_str())).collect();
                http(c, method, &path, &h, body).await
            }
        };
    let (status, _, _) = call(envd(""), "GET", "/health".into(), bytes::Bytes::new()).await;
    assert_eq!(status, 204);

    let form = "--xx\r\nContent-Disposition: form-data; name=\"file\"; filename=\"notes/a.txt\"\r\n\r\n\
                hello sandbox\r\n--xx--\r\n";
    let (status, _, body) = call(
        envd("multipart/form-data; boundary=xx"),
        "POST",
        "/files?path=notes%2Fa.txt&username=agent".into(),
        form.into(),
    )
    .await;
    assert_eq!(status, 200, "{body:?}");
    assert_eq!(json(&body)[0]["path"], "/home/agent/notes/a.txt");
    let raw = bytes::Bytes::from(vec![7u8; 100_000]);
    let (status, _, _) = call(
        envd("application/octet-stream"),
        "POST",
        "/files?path=/tmp/b.bin&username=root".into(),
        raw.clone(),
    )
    .await;
    assert_eq!(status, 200);
    {
        let files = c
            .combs
            .iter()
            .flat_map(|comb| comb.files.lock().unwrap().clone())
            .collect::<BTreeMap<_, _>>();
        assert_eq!(
            files["/home/agent/notes/a.txt"],
            (bytes::Bytes::from("hello sandbox"), "agent".to_string())
        );
        assert_eq!(files["/tmp/b.bin"].0.len(), 100_000);
        assert_eq!(files["/tmp/b.bin"].1, "root");
    }
    let (status, _, body) = call(
        envd(""),
        "GET",
        "/files?path=%2Ftmp%2Fb.bin&username=root".into(),
        bytes::Bytes::new(),
    )
    .await;
    assert_eq!((status, body), (200, raw));
    let (status, _, body) =
        call(envd(""), "GET", "/files?path=/nope&username=root".into(), bytes::Bytes::new()).await;
    assert_eq!(status, 404, "{body:?}");

    let stat = br#"{"path":"notes/a.txt"}"#.as_slice();
    let (status, _, body) =
        call(envd("application/json"), "POST", "/filesystem.Filesystem/Stat".into(), stat.into())
            .await;
    assert_eq!(status, 200, "{body:?}");
    assert_eq!(json(&body)["entry"]["size"], 13);
    assert_eq!(json(&body)["entry"]["permissions"], "-rw-r--r--");

    // A command with its input closed: the pid, what it said, and how it ended.
    let start = serde_json::json!({
        "process": { "cmd": "/bin/bash", "args": ["-l", "-c", "echo hi"], "cwd": "work" },
    });
    let mut h = envd("application/connect+json");
    h.push(("connect-timeout-ms", "60000".into()));
    let (status, headers, body) = call(
        h,
        "POST",
        "/process.Process/Start".into(),
        envelope(0, start.to_string().as_bytes()).into(),
    )
    .await;
    assert_eq!(status, 200);
    assert_eq!(headers["content-type"], "application/connect+json");
    let events = envelopes(&body);
    assert_eq!(events[0], (false, serde_json::json!({ "event": { "start": { "pid": 7 } } })));
    let said = b64.decode(events[1].1["event"]["data"]["stdout"].as_str().unwrap()).unwrap();
    assert_eq!(
        String::from_utf8(said).unwrap(),
        r#"/bin/bash -l -c echo hi as "agent" in "/home/agent/work""#
    );
    assert_eq!(
        events[2].1["event"]["end"],
        serde_json::json!({ "exitCode": 3, "exited": true, "status": "exit status 3" })
    );
    assert_eq!(events.last().unwrap(), &(true, serde_json::json!({})));

    // With its input open, the stream stays up until input sent through other calls closes it.
    let start = serde_json::json!({ "process": { "cmd": "cat" }, "stdin": true });
    let h = refs(&envd("application/connect+json"));
    let running = tokio::spawn({
        let addr = c.addr;
        let body = envelope(0, start.to_string().as_bytes());
        async move {
            let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
            let io = hyper_util::rt::TokioIo::new(stream);
            let (mut send, conn) = hyper::client::conn::http1::handshake(io).await.unwrap();
            tokio::spawn(conn);
            let mut req = tonic::codegen::http::Request::post("/process.Process/Start")
                .header("host", "gate");
            for (k, v) in &h {
                req = req.header(*k, v.as_str());
            }
            let resp = send
                .send_request(
                    req.body(http_body_util::Full::new(bytes::Bytes::from(body))).unwrap(),
                )
                .await
                .unwrap();
            http_body_util::BodyExt::collect(resp.into_body()).await.unwrap().to_bytes()
        }
    });
    let unary = |method: &'static str, msg: serde_json::Value| {
        call(
            envd("application/json"),
            "POST",
            format!("/process.Process/{method}"),
            msg.to_string().into(),
        )
    };
    let input =
        serde_json::json!({ "process": { "pid": 7 }, "input": { "stdin": b64.encode("typed") } });
    let mut sent = 0;
    for _ in 0..100 {
        let (status, _, _) = unary("SendInput", input.clone()).await;
        if status == 200 {
            sent += 1;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(sent, 1, "the process took input once it had started");
    let (status, _, _) = unary(
        "SendSignal",
        serde_json::json!({ "process": { "pid": 7 }, "signal": "SIGNAL_SIGKILL" }),
    )
    .await;
    assert_eq!(status, 200);
    let (status, _, _) = unary("CloseStdin", serde_json::json!({ "process": { "pid": 7 } })).await;
    assert_eq!(status, 200);
    let events = envelopes(&running.await.unwrap());
    let typed = events
        .iter()
        .find_map(|(_, e)| e["event"]["data"]["stdout"].as_str().map(|s| b64.decode(s).unwrap()));
    assert_eq!(typed.as_deref(), Some(b"cat as \"agent\" in \"\"".as_slice()));
    assert!(
        events.iter().any(|(_, e)| e["event"]["data"]["stdout"] == b64.encode("typed")),
        "{events:?}"
    );
    assert_eq!(events[events.len() - 2].1["event"]["end"]["exitCode"], 3);
    let signals: Vec<_> =
        c.combs.iter().flat_map(|comb| comb.signals.lock().unwrap().clone()).collect();
    assert_eq!(signals, vec![(7, 9)]);
    let (status, _, body) =
        unary("CloseStdin", serde_json::json!({ "process": { "pid": 7 } })).await;
    assert_eq!((status, json(&body)["code"].as_str()), (404, Some("not_found")));

    let (status, _, _) = http(&c, "POST", &format!("/sandboxes/{id}/pause"), &key, "").await;
    assert_eq!(status, 204);
    let (status, _, _) = http(&c, "POST", &format!("/sandboxes/{id}/pause"), &key, "").await;
    assert_eq!(status, 409);
    let (status, _, body) =
        http(&c, "POST", &format!("/sandboxes/{id}/connect"), &key, r#"{"timeout":60}"#).await;
    assert_eq!(status, 201, "{body:?}");
    let (status, _, _) = http(&c, "DELETE", &format!("/sandboxes/{id}"), &key, "").await;
    assert_eq!(status, 204);
    let (status, _, _) = http(&c, "GET", &format!("/sandboxes/{id}"), &key, "").await;
    assert_eq!(status, 404);
    let (status, _, _) = http(&c, "DELETE", "/sandboxes/not-a-cell", &key, "").await;
    assert_eq!(status, 404);
}
