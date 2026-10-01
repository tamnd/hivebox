//! The gate end to end: a scout in the same process, two made up combs on TCP that answer from
//! memory, and a real client calling the gate.

use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::stream::BoxStream;
use hive_gate::{Gate, Nodes};
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::cells_server::{Cells, CellsServer};
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::exec_server::{Exec, ExecServer};
use hive_scout::NodeReport;
use hive_types::{Backend, CellId};
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
    seq: Arc<Mutex<u64>>,
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
        let req = req.into_inner();
        let mut events = Vec::new();
        for index in 0..req.count.max(1) {
            let mut room = self.room.lock().unwrap();
            let result = if *room == 0 {
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

    async fn start(
        &self,
        _req: Request<Streaming<v1::ProcessInput>>,
    ) -> Result<Response<Self::StartStream>, Status> {
        Err(Status::unimplemented("start"))
    }

    async fn signal(
        &self,
        _req: Request<v1::SignalRequest>,
    ) -> Result<Response<v1::Empty>, Status> {
        Err(Status::unimplemented("signal"))
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
    addr: SocketAddr,
    channel: Channel,
    stop: CancellationToken,
}

impl Cluster {
    /// Two combs, nodes 1 and 2, with room for `room` cells each.
    async fn new(room: u32) -> Self {
        let stop = CancellationToken::new();
        let scout = hive_scout::Service::new();
        tokio::spawn(scout.clone().run(stop.clone()));
        let (sl, saddr) = listen().await;
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(scout.server())
                .serve_with_incoming_shutdown(incoming(sl), stop.clone().cancelled_owned()),
        );
        let mut combs = Vec::new();
        for node in 1..=2u16 {
            let comb = FakeComb { node, room: Arc::new(Mutex::new(room)), ..Default::default() };
            let (l, addr) = listen().await;
            tokio::spawn(
                tonic::transport::Server::builder()
                    .add_service(CellsServer::new(comb.clone()))
                    .add_service(ExecServer::new(comb.clone()))
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
        let gate = Gate::new(keys, nodes, &hive_telemetry::Registry::new());
        let (gl, gaddr) = listen().await;
        tokio::spawn(hive_gate::serve(gate, gl, stop.clone()));
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{gaddr}"))
            .unwrap()
            .connect()
            .await
            .unwrap();
        Self { combs, addr: gaddr, channel, stop }
    }

    fn cells(&self) -> CellsClient<Channel> {
        CellsClient::new(self.channel.clone())
    }

    fn exec(&self) -> ExecClient<Channel> {
        ExecClient::new(self.channel.clone())
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
