//! A driver served as a plugin acts as it does in process: every call and every field gets
//! through, errors keep their reason, and a plugin that restarts is found again.

use futures::future::BoxFuture;
use hive_cell::{
    CellDriver, CellHandle, DriverCaps, ExitInfo, GuestChannel, Liveness, NodeFit, PauseMode,
    RootfsPlan, Slot, SnapshotCaps,
};
use hive_cell_plugin::{API_VERSION, PluginDriver, bind, serve};
use hive_proto::plugin::v1 as wire;
use hive_proto::plugin::v1::driver_server::{Driver, DriverServer};
use hive_types::{Backend, CellId, CellSpec, Error, Reason, Resources, Source};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::oneshot;
use tonic::{Request, Response, Status};

/// A microVM backend that keeps what it was asked.
#[derive(Default)]
struct Fake {
    seen: Mutex<Vec<String>>,
    prepared: Mutex<Option<(CellId, CellSpec, RootfsPlan, Slot)>>,
}

impl Fake {
    fn saw(&self, what: String) {
        self.seen.lock().unwrap().push(what);
    }
}

const CAPS: DriverCaps = DriverCaps {
    pause: true,
    snapshot: SnapshotCaps::Disk,
    fork: false,
    resize: true,
    gpu: false,
    trim: true,
};

impl CellDriver for Fake {
    fn backend(&self) -> Backend {
        Backend::Microvm
    }

    fn caps(&self) -> DriverCaps {
        CAPS
    }

    fn probe(&self) -> BoxFuture<'_, hive_cell::Result<NodeFit>> {
        Box::pin(async { Ok(NodeFit { ready: true, notes: vec!["kvm".into(), "6.8".into()] }) })
    }

    fn prepare<'a>(
        &'a self,
        id: CellId,
        spec: &'a CellSpec,
        rootfs: &'a RootfsPlan,
        slot: &'a Slot,
    ) -> BoxFuture<'a, hive_cell::Result<CellHandle>> {
        Box::pin(async move {
            if spec.labels.contains_key("fail") {
                return Err(Error::new(Reason::ImageUnavailable, "no kernel for it"));
            }
            *self.prepared.lock().unwrap() = Some((id, spec.clone(), rootfs.clone(), slot.clone()));
            Ok(CellHandle {
                id,
                backend: Backend::Microvm,
                pid: None,
                channel: GuestChannel::Vsock { uds: slot.dir.join("v.sock"), port: 52 },
                cgroup: slot.cgroup.clone(),
                netns: slot.netns.clone(),
                extra: BTreeMap::from([(
                    "api".into(),
                    slot.dir.join("api.sock").display().to_string(),
                )]),
            })
        })
    }

    fn start<'a>(&'a self, h: &'a mut CellHandle) -> BoxFuture<'a, hive_cell::Result<()>> {
        Box::pin(async move {
            h.pid = Some(4242);
            Ok(())
        })
    }

    fn pause<'a>(
        &'a self,
        _: &'a CellHandle,
        mode: PauseMode,
    ) -> BoxFuture<'a, hive_cell::Result<()>> {
        Box::pin(async move {
            self.saw(format!("pause {mode:?}"));
            Ok(())
        })
    }

    fn resume<'a>(&'a self, _: &'a CellHandle) -> BoxFuture<'a, hive_cell::Result<()>> {
        Box::pin(async move {
            self.saw("resume".into());
            Ok(())
        })
    }

    fn trim<'a>(&'a self, _: &'a CellHandle) -> BoxFuture<'a, hive_cell::Result<u64>> {
        Box::pin(async { Ok(5 << 20) })
    }

    fn resize<'a>(
        &'a self,
        _: &'a CellHandle,
        r: &'a Resources,
    ) -> BoxFuture<'a, hive_cell::Result<()>> {
        Box::pin(async move {
            self.saw(format!("resize {} {}", r.vcpu_milli, r.mem_mib));
            Ok(())
        })
    }

    fn stop<'a>(
        &'a self,
        h: &'a CellHandle,
        grace: Duration,
    ) -> BoxFuture<'a, hive_cell::Result<ExitInfo>> {
        Box::pin(async move {
            self.saw(format!("stop {} {grace:?}", h.extra["api"]));
            Ok(ExitInfo { code: Some(3), signal: None, oom: true })
        })
    }

    fn check<'a>(&'a self, h: &'a CellHandle) -> BoxFuture<'a, hive_cell::Result<Liveness>> {
        Box::pin(async move {
            match h.pid {
                Some(4242) => Ok(Liveness::Alive),
                Some(1) => Ok(Liveness::Paused),
                Some(_) => Ok(Liveness::Gone(ExitInfo { code: None, signal: Some(9), oom: false })),
                None => Err(Error::new(Reason::CellNotFound, "never started")),
            }
        })
    }
}

/// A backend with nothing but the calls every driver has.
struct Bare;

impl CellDriver for Bare {
    fn backend(&self) -> Backend {
        Backend::Fullvm
    }

    fn caps(&self) -> DriverCaps {
        DriverCaps::default()
    }

    fn probe(&self) -> BoxFuture<'_, hive_cell::Result<NodeFit>> {
        Box::pin(async { Ok(NodeFit { ready: false, notes: vec!["no /dev/kvm".into()] }) })
    }

    fn prepare<'a>(
        &'a self,
        _: CellId,
        _: &'a CellSpec,
        _: &'a RootfsPlan,
        _: &'a Slot,
    ) -> BoxFuture<'a, hive_cell::Result<CellHandle>> {
        Box::pin(async { Err(Error::new(Reason::Internal, "the VMM would not start")) })
    }

    fn start<'a>(&'a self, _: &'a mut CellHandle) -> BoxFuture<'a, hive_cell::Result<()>> {
        Box::pin(async { Ok(()) })
    }

    fn stop<'a>(
        &'a self,
        _: &'a CellHandle,
        _: Duration,
    ) -> BoxFuture<'a, hive_cell::Result<ExitInfo>> {
        Box::pin(async { Ok(ExitInfo::default()) })
    }

    fn check<'a>(&'a self, _: &'a CellHandle) -> BoxFuture<'a, hive_cell::Result<Liveness>> {
        Box::pin(async { Ok(Liveness::Alive) })
    }
}

fn socket(name: &str) -> PathBuf {
    std::env::temp_dir().join(format!("hcp-{}-{name}.sock", std::process::id()))
}

/// Serves `driver` at `path` until the sender is dropped or sent to.
fn plugin(
    driver: Arc<dyn CellDriver>,
    path: &Path,
) -> (oneshot::Sender<()>, tokio::task::JoinHandle<()>) {
    let (tx, rx) = oneshot::channel::<()>();
    let listener = bind(path).unwrap();
    let task = tokio::spawn(async move {
        serve(driver, "fake 1.0", listener, async {
            let _ = rx.await;
        })
        .await
        .unwrap();
    });
    (tx, task)
}

fn id() -> CellId {
    CellId::new(1, 7, 2, 99, 12345).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn every_call_and_field_gets_through() {
    let path = socket("fields");
    let fake = Arc::new(Fake::default());
    let (_stop, _task) = plugin(fake.clone(), &path);
    let d = PluginDriver::connect(&path).await.unwrap();
    assert_eq!((d.backend(), d.caps(), d.name()), (Backend::Microvm, CAPS, "fake 1.0"));
    assert_eq!(d.probe().await.unwrap().notes, ["kvm", "6.8"]);

    let mut spec = CellSpec::new(Source::Image("python".into()), Backend::Microvm);
    spec.labels.insert("step".into(), "412".into());
    spec.env.insert("A".into(), "b c".into());
    spec.hard_ttl = Some(Duration::from_millis(90_500));
    spec.resources.mem_mib = 768;
    let rootfs = RootfsPlan {
        lowers: vec!["/l/a".into(), "/l/b".into()],
        upper: "/u/x".into(),
        seed: Some("/forks/1".into()),
    };
    let slot = Slot {
        cgroup: "/sys/fs/cgroup/hive/c1".into(),
        netns: Some("/run/hivebox/netns/cell-3".into()),
        nameserver: Some("100.64.0.1".parse().unwrap()),
        dir: "/run/hivebox/cells/c1".into(),
        secret: [7; 32],
    };
    let mut h = d.prepare(id(), &spec, &rootfs, &slot).await.unwrap();
    assert_eq!(*fake.prepared.lock().unwrap(), Some((id(), spec.clone(), rootfs, slot)));
    assert_eq!(h.pid, None);
    assert_eq!(
        h.channel,
        GuestChannel::Vsock { uds: "/run/hivebox/cells/c1/v.sock".into(), port: 52 }
    );
    d.start(&mut h).await.unwrap();
    assert_eq!(h.pid, Some(4242));
    assert_eq!(d.check(&h).await.unwrap(), Liveness::Alive);

    d.pause(&h, PauseMode::Reclaim).await.unwrap();
    d.resume(&h).await.unwrap();
    assert_eq!(d.trim(&h).await.unwrap(), 5 << 20);
    let more = Resources { vcpu_milli: 1500, mem_mib: 2048, ..spec.resources };
    d.resize(&h, &more).await.unwrap();
    let exit = d.stop(&h, Duration::from_millis(2500)).await.unwrap();
    assert_eq!(exit, ExitInfo { code: Some(3), signal: None, oom: true });
    assert_eq!(
        *fake.seen.lock().unwrap(),
        ["pause Reclaim", "resume", "resize 1500 2048", "stop /run/hivebox/cells/c1/api.sock 2.5s",]
    );

    h.pid = Some(1);
    assert_eq!(d.check(&h).await.unwrap(), Liveness::Paused);
    h.pid = Some(2);
    let gone = ExitInfo { code: None, signal: Some(9), oom: false };
    assert_eq!(d.check(&h).await.unwrap(), Liveness::Gone(gone));
}

#[tokio::test(flavor = "multi_thread")]
async fn errors_keep_their_reason() {
    let path = socket("errors");
    let (_stop, _task) = plugin(Arc::new(Fake::default()), &path);
    let d = PluginDriver::connect(&path).await.unwrap();
    let mut spec = CellSpec::new(Source::Image("python".into()), Backend::Microvm);
    spec.labels.insert("fail".into(), String::new());
    let slot = Slot { cgroup: "/c".into(), dir: "/d".into(), ..Slot::default() };
    let e = d.prepare(id(), &spec, &RootfsPlan::default(), &slot).await.unwrap_err();
    assert_eq!((e.reason, e.message.as_str()), (Reason::ImageUnavailable, "no kernel for it"));
    let h = CellHandle {
        id: id(),
        backend: Backend::Microvm,
        pid: None,
        channel: GuestChannel::Unix("/d/drone.sock".into()),
        cgroup: "/c".into(),
        netns: None,
        extra: BTreeMap::new(),
    };
    assert_eq!(d.check(&h).await.unwrap_err().reason, Reason::CellNotFound);

    // What a driver does not have is refused as it is in process.
    let path = socket("bare");
    let (_stop, _task) = plugin(Arc::new(Bare), &path);
    let d = PluginDriver::connect(&path).await.unwrap();
    assert_eq!((d.backend(), d.caps()), (Backend::Fullvm, DriverCaps::default()));
    assert!(!d.probe().await.unwrap().ready);
    for e in [
        d.pause(&h, PauseMode::Freeze).await.unwrap_err(),
        d.resume(&h).await.unwrap_err(),
        d.resize(&h, &Resources::default()).await.unwrap_err(),
    ] {
        assert_eq!(e.reason, Reason::PolicyDenied, "{e}");
    }
    assert_eq!(d.trim(&h).await.unwrap_err().reason, Reason::PolicyDenied);
    let e = d.prepare(id(), &spec, &RootfsPlan::default(), &slot).await.unwrap_err();
    assert_eq!(e.reason, Reason::Internal);
    assert!(e.message.ends_with("the VMM would not start"), "{e}");
}

/// Answers Describe as a plugin of another version would, and nothing else.
struct Future2;

#[tonic::async_trait]
impl Driver for Future2 {
    async fn describe(
        &self,
        _: Request<wire::DescribeRequest>,
    ) -> Result<Response<wire::Description>, Status> {
        Ok(Response::new(wire::Description {
            api_version: API_VERSION + 1,
            backend: hive_proto::v1::Backend::Microvm.into(),
            caps: None,
            name: "from the future".into(),
        }))
    }
    async fn probe(
        &self,
        _: Request<wire::ProbeRequest>,
    ) -> Result<Response<wire::NodeFit>, Status> {
        Err(Status::unimplemented("probe"))
    }
    async fn prepare(
        &self,
        _: Request<wire::PrepareRequest>,
    ) -> Result<Response<wire::Handle>, Status> {
        Err(Status::unimplemented("prepare"))
    }
    async fn start(&self, _: Request<wire::Handle>) -> Result<Response<wire::Handle>, Status> {
        Err(Status::unimplemented("start"))
    }
    async fn pause(
        &self,
        _: Request<wire::PauseRequest>,
    ) -> Result<Response<hive_proto::v1::Empty>, Status> {
        Err(Status::unimplemented("pause"))
    }
    async fn resume(
        &self,
        _: Request<wire::Handle>,
    ) -> Result<Response<hive_proto::v1::Empty>, Status> {
        Err(Status::unimplemented("resume"))
    }
    async fn trim(&self, _: Request<wire::Handle>) -> Result<Response<wire::Trimmed>, Status> {
        Err(Status::unimplemented("trim"))
    }
    async fn resize(
        &self,
        _: Request<wire::ResizeRequest>,
    ) -> Result<Response<hive_proto::v1::Empty>, Status> {
        Err(Status::unimplemented("resize"))
    }
    async fn stop(&self, _: Request<wire::StopRequest>) -> Result<Response<wire::Exit>, Status> {
        Err(Status::unimplemented("stop"))
    }
    async fn check(&self, _: Request<wire::Handle>) -> Result<Response<wire::Liveness>, Status> {
        Err(Status::unimplemented("check"))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_plugin_of_another_version_or_none_at_all_is_refused() {
    let path = socket("v2");
    let listener = bind(&path).unwrap();
    let incoming = futures::stream::unfold(listener, |l| async move {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    tokio::spawn(
        tonic::transport::Server::builder()
            .serve_with_incoming(DriverServer::new(Future2), incoming),
    );
    let e = PluginDriver::connect(&path).await.unwrap_err();
    assert_eq!(e.reason, Reason::PolicyDenied);
    assert!(e.message.contains("speaks version 2 of the plugin API and this node speaks 1"), "{e}");

    let path = socket("none");
    let e = PluginDriver::connect(&path).await.unwrap_err();
    assert_eq!(e.reason, Reason::Internal);
    assert!(e.message.contains(&path.display().to_string()), "{e}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_plugin_is_found_again() {
    let path = socket("restart");
    let (stop, task) = plugin(Arc::new(Fake::default()), &path);
    let d = PluginDriver::connect(&path).await.unwrap();
    let mut h = CellHandle {
        id: id(),
        backend: Backend::Microvm,
        pid: Some(4242),
        channel: GuestChannel::Unix("/d/drone.sock".into()),
        cgroup: "/c".into(),
        netns: None,
        extra: BTreeMap::new(),
    };
    assert_eq!(d.check(&h).await.unwrap(), Liveness::Alive);

    // A call that goes the same way in process, timed both ways.
    let fake = Fake::default();
    let mut times = [Vec::new(), Vec::new()];
    for _ in 0..2000 {
        let t = Instant::now();
        fake.check(&h).await.unwrap();
        times[0].push(t.elapsed());
        let t = Instant::now();
        d.check(&h).await.unwrap();
        times[1].push(t.elapsed());
    }
    for t in &mut times {
        t.sort();
    }
    println!(
        "check in process p50 {:?}, through the plugin p50 {:?} p99 {:?}",
        times[0][1000], times[1][1000], times[1][1980]
    );

    stop.send(()).unwrap();
    task.await.unwrap();
    let _ = std::fs::remove_file(&path);
    let e = d.check(&h).await.unwrap_err();
    assert_eq!(e.reason, Reason::Internal, "{e}");

    let t = Instant::now();
    let (_stop, _task) = plugin(Arc::new(Fake::default()), &path);
    h.pid = Some(1);
    loop {
        match d.check(&h).await {
            Ok(l) => {
                assert_eq!(l, Liveness::Paused);
                break;
            }
            Err(e) if t.elapsed() < Duration::from_secs(10) => {
                assert_eq!(e.reason, Reason::Internal, "{e}");
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => panic!("the plugin was not found again: {e}"),
        }
    }
    println!("the same driver reached the new plugin {:?} after it bound", t.elapsed());
}
