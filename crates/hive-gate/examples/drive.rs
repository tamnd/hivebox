//! Creates cells, runs a command in each, and stops them, through a gate or straight at a comb,
//! so the two can be timed side by side.
//!
//! ```text
//! cargo run --release -p hive-gate --example drive -- ENDPOINT KEY CELLS BATCH IMAGE
//! drive http://127.0.0.1:7400 hb_... 200 50 python
//! drive unix:/root/hb-tmp/ctl2/comb.sock - 200 50 python
//! ```
//!
//! With a key it calls as that key's project, and with `-` as the project `drive`, which is
//! what a comb without a gate in front takes from the `x-hive-project` header. Creates go out
//! as CELLS / BATCH calls at once, then one `true` runs in every cell, 32 at a time, and a stop
//! by label ends them all.

use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::StreamExt;
use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::exec_client::ExecClient;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

/// Commands in flight at once.
const RUNS: usize = 32;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args.get(1).cloned().unwrap_or_else(|| "http://127.0.0.1:7400".into());
    let key = args.get(2).cloned().unwrap_or_else(|| "-".into());
    let cells: u32 = args.get(3).map_or(100, |a| a.parse().expect("cells"));
    let batch: u32 = args.get(4).map_or(25, |a| a.parse().expect("batch"));
    let image = args.get(5).cloned().unwrap_or_else(|| "python".into());
    let channel = connect(&endpoint).await;
    let key = Arc::new(key);
    let run = format!("drive-{}", std::process::id());
    let spec = v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: image })),
        backend: v1::Backend::Container.into(),
        resources: Some(v1::Resources { mem_mib: 128, ..Default::default() }),
        labels: [("run".to_string(), run.clone())].into(),
        ..Default::default()
    };

    let start = Instant::now();
    let calls = (0..cells.div_ceil(batch)).map(|i| {
        let count = batch.min(cells - i * batch);
        let (mut client, key, spec) = (CellsClient::new(channel.clone()), key.clone(), spec.clone());
        async move {
            let req = v1::CreateRequest { spec: Some(spec), count, ..Default::default() };
            let mut events = client.create(call(&key, req)).await.expect("create").into_inner();
            let mut out = Vec::new();
            while let Some(ev) = events.message().await.expect("create stream") {
                out.push((start.elapsed(), ev));
            }
            out
        }
    });
    let events: Vec<(Duration, v1::CreateEvent)> =
        futures::future::join_all(calls).await.into_iter().flatten().collect();
    let create_wall = start.elapsed();
    let mut made = Vec::new();
    let mut failed = std::collections::BTreeMap::<String, u32>::new();
    let mut ready = Vec::new();
    for (at, ev) in events {
        match ev.result {
            Some(v1::create_event::Result::Cell(c)) => {
                ready.push(at);
                made.push(c);
            }
            Some(v1::create_event::Result::Error(e)) => *failed.entry(e.reason).or_default() += 1,
            None => {}
        }
    }
    let mut nodes = std::collections::BTreeMap::<String, u32>::new();
    for c in &made {
        *nodes.entry(c.node.clone()).or_default() += 1;
    }
    println!(
        "create: {} cells in {:.2} s, {:.1} a second, ready at {}, failed {failed:?}, by node {nodes:?}",
        made.len(),
        create_wall.as_secs_f64(),
        made.len() as f64 / create_wall.as_secs_f64(),
        quantiles(&mut ready),
    );

    let start = Instant::now();
    let mut runs: Vec<Duration> = futures::stream::iter(made.iter().map(|c| {
        let (mut client, key, id) = (ExecClient::new(channel.clone()), key.clone(), c.id.clone());
        async move {
            let t = Instant::now();
            let req = v1::RunRequest { cell_id: id, argv: vec!["true".into()], ..Default::default() };
            let r = client.run(call(&key, req)).await.expect("run").into_inner();
            assert_eq!(r.exit_code, 0);
            t.elapsed()
        }
    }))
    .buffer_unordered(RUNS)
    .collect()
    .await;
    println!(
        "run: {} commands in {:.2} s, took {}",
        runs.len(),
        start.elapsed().as_secs_f64(),
        quantiles(&mut runs)
    );

    let start = Instant::now();
    let sel = v1::CellSelector {
        by: Some(v1::cell_selector::By::Labels(v1::LabelSelector {
            r#match: [("run".to_string(), run)].into(),
        })),
    };
    let req = v1::StopRequest { selector: Some(sel), snapshot: false };
    let r = CellsClient::new(channel).stop(call(&key, req)).await.expect("stop").into_inner();
    println!(
        "stop: {} of {} in {:.2} s, failures {}",
        r.succeeded,
        r.matched,
        start.elapsed().as_secs_f64(),
        r.failures.len()
    );
}

fn call<T>(key: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    if key == "-" {
        r.metadata_mut().insert("x-hive-project", "drive".parse().unwrap());
    } else {
        r.metadata_mut().insert("authorization", format!("Bearer {key}").parse().unwrap());
    }
    r
}

async fn connect(endpoint: &str) -> Channel {
    if let Some(path) = endpoint.strip_prefix("unix:") {
        let path = path.to_owned();
        return Endpoint::from_static("http://comb")
            .connect_with_connector(tower::service_fn(move |_| {
                let path = path.clone();
                async move {
                    let s = tokio::net::UnixStream::connect(path).await?;
                    Ok::<_, std::io::Error>(hyper_util::rt::TokioIo::new(s))
                }
            }))
            .await
            .expect("connect");
    }
    Endpoint::from_shared(endpoint.to_owned())
        .expect("endpoint")
        .tcp_nodelay(true)
        .connect()
        .await
        .expect("connect")
}

fn quantiles(d: &mut [Duration]) -> String {
    d.sort();
    let at = |q: f64| {
        let i = ((d.len().max(1) - 1) as f64 * q) as usize;
        d.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e3)
    };
    format!("p50 {:.1} ms p99 {:.1} ms max {:.1} ms", at(0.5), at(0.99), at(1.0))
}
