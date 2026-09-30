//! Scout's gRPC service over a real TCP socket.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use hive_proto::internal as pb;
use hive_proto::internal::scout_client::ScoutClient;
use hive_scout::{NodeReport, Service};
use hive_types::Backend;
use hive_waggle::{BackendSet, LayerBloom};
use tokio_util::sync::CancellationToken;

fn report(node: u16, seq: u64) -> NodeReport {
    NodeReport {
        node,
        epoch: 1,
        seq,
        addr: Arc::from("unix:/run/hivebox/comb.sock"),
        healthy: true,
        backends: BackendSet::of(&[Backend::Container]),
        cpu_milli: 8_000,
        cpu_committed_milli: 2_000,
        mem_admit_mib: 16_384,
        mem_committed_mib: 4_096,
        cells: 8,
        max_cells: 1000,
        pool_depth: 48,
        create_rate: 1.5,
        burst_cap: 300,
        layers: None,
        top_projects: vec![(9, 8)],
    }
}

async fn client(addr: String) -> ScoutClient<tonic::transport::Channel> {
    let channel = tonic::transport::Endpoint::from_shared(addr).unwrap().connect().await.unwrap();
    ScoutClient::new(channel)
}

async fn serve() -> (Service, String, CancellationToken) {
    let (service, addr, stop, _) = serve_until().await;
    (service, addr, stop)
}

/// Like [`serve`], with the server's task, which ends once `stop` is cancelled and every
/// connection is done.
async fn serve_until() -> (Service, String, CancellationToken, tokio::task::JoinHandle<()>) {
    let service = Service::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let stop = CancellationToken::new();
    let incoming = futures::stream::unfold(listener, |l| async move {
        Some((l.accept().await.map(|(s, _)| s), l))
    });
    let server = tonic::transport::Server::builder().add_service(service.server());
    let shutdown = {
        let (stop, service) = (stop.clone(), service.clone());
        async move {
            stop.cancelled().await;
            service.close();
        }
    };
    let task = tokio::spawn(async move {
        server.serve_with_incoming_shutdown(incoming, shutdown).await.unwrap();
    });
    tokio::spawn(service.clone().run(stop.clone()));
    (service, addr, stop, task)
}

#[tokio::test]
async fn reports_over_the_wire_reach_the_snapshot() {
    let (service, addr, stop) = serve().await;
    let mut client = client(addr).await;
    let digest = *blake3::hash(b"layer").as_bytes();
    let mut bloom = LayerBloom::default();
    bloom.insert(&digest);
    let sent = vec![
        pb::NodeReport::from(&NodeReport { layers: Some(bloom), ..report(4, 1) }),
        pb::NodeReport::from(&report(4, 2)),
        // Late, so dropped.
        pb::NodeReport::from(&report(4, 1)),
        pb::NodeReport::from(&NodeReport { cells: 9, top_projects: vec![(9, 9)], ..report(5, 1) }),
    ];
    let acks: Vec<pb::ReportAck> = client
        .report(futures::stream::iter(sent))
        .await
        .unwrap()
        .into_inner()
        .map(|a| a.unwrap())
        .collect()
        .await;
    let got: Vec<(u64, bool)> = acks.iter().map(|a| (a.seq, a.stale)).collect();
    assert_eq!(got, vec![(1, false), (2, false), (1, true), (1, false)]);
    let mut rx = service.subscribe();
    let snap = rx.wait_for(|s| s.view.nodes.len() == 2).await.unwrap().clone();
    let node = &snap.view.nodes[0];
    assert_eq!((node.node, node.cells, node.pool_depth, node.burst_cap), (4, 8, 48, 300));
    assert!(node.layers.contains(&digest), "the filter carried over from the first report");
    assert!(node.backends.has(Backend::Container) && !node.backends.has(Backend::Microvm));
    assert_eq!(snap.totals.cells, 17);
    assert_eq!(snap.projects.get(&9), Some(&17));
    assert_eq!(snap.addr(5).map(|a| &**a), Some("unix:/run/hivebox/comb.sock"));
    let text = service.registry().render();
    assert!(text.contains("hive_scout_reports_total{result=\"stale\"} 1"), "{text}");
    assert!(text.contains("hive_scout_nodes{state=\"healthy\"} 2"), "{text}");
    stop.cancel();
}

#[tokio::test]
async fn a_malformed_report_ends_the_stream_with_an_error() {
    let (_service, addr, stop) = serve().await;
    let mut client = client(addr).await;
    let mut bad = pb::NodeReport::from(&report(1, 1));
    bad.layers = vec![0; 10];
    let mut acks = client.report(futures::stream::iter(vec![bad])).await.unwrap().into_inner();
    let e = tokio::time::timeout(Duration::from_secs(5), acks.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(e.code(), tonic::Code::InvalidArgument);
    assert!(e.message().contains("10 bytes"), "{}", e.message());
    stop.cancel();
}

#[tokio::test]
async fn shutting_down_ends_streams_that_would_report_forever() {
    let (_service, addr, stop, server) = serve_until().await;
    let mut client = client(addr).await;
    let (tx, rx) = tokio::sync::mpsc::channel(1);
    let out = futures::stream::unfold(rx, |mut rx| async move { rx.recv().await.map(|r| (r, rx)) });
    tx.send(pb::NodeReport::from(&report(1, 1))).await.unwrap();
    let mut acks = client.report(out).await.unwrap().into_inner();
    assert_eq!(acks.next().await.unwrap().unwrap().seq, 1);
    // The comb still holds `tx`, so its side of the stream stays open.
    stop.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .expect("the server stopped")
        .unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), acks.next()).await.unwrap();
    assert!(end.is_none_or(|r| r.is_err()), "the stream ended");
    drop(tx);
}
