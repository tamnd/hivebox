//! Creates cells at a steady rate through one or more gates for a while and counts how many the
//! project's quota let through, so the shares the gates get from the keeper can be checked.
//!
//! ```text
//! cargo run --release -p hive-gate --example quota -- GATES KEY SECONDS RATE IMAGE
//! quota http://127.0.0.1:7401,http://127.0.0.1:7402 hb_... 10 60 python
//! ```
//!
//! One cell per call, `RATE` calls a second in all, taking turns over the gates. The cells are
//! kept until the end, so a quota of cells fills up, and then stopped by label.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use tonic::Request;
use tonic::transport::Endpoint;

#[derive(Default)]
struct Counts {
    ok: u32,
    refused: BTreeMap<String, u32>,
    by_second: BTreeMap<u64, (u32, u32)>,
    took: Vec<Duration>,
}

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [gates, key, secs, rate, image] = &args[..] else {
        eprintln!("usage: quota GATES KEY SECONDS RATE IMAGE");
        std::process::exit(2);
    };
    let secs: u64 = secs.parse().expect("SECONDS");
    let rate: u64 = rate.parse().expect("RATE");
    let mut channels = Vec::new();
    for g in gates.split(',') {
        channels.push(Endpoint::from_shared(g.to_owned()).unwrap().connect().await.unwrap());
    }
    let run = format!("quota-{}", std::process::id());
    let spec = v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: image.clone() })),
        backend: v1::Backend::Container.into(),
        resources: Some(v1::Resources { mem_mib: 64, ..Default::default() }),
        labels: [("run".to_string(), run.clone())].into(),
        ..Default::default()
    };
    let counts = Arc::new(Mutex::new(Counts::default()));
    let start = Instant::now();
    let mut tick = tokio::time::interval(Duration::from_micros(1_000_000 / rate));
    let mut calls = tokio::task::JoinSet::new();
    for i in 0..secs * rate {
        tick.tick().await;
        let mut client = CellsClient::new(channels[i as usize % channels.len()].clone());
        let (key, spec, counts) = (key.clone(), spec.clone(), counts.clone());
        calls.spawn(async move {
            let t = Instant::now();
            let second = start.elapsed().as_secs();
            let req = v1::CreateRequest { spec: Some(spec), count: 1, ..Default::default() };
            let got = match client.create(call(&key, req)).await {
                Ok(r) => match r.into_inner().message().await {
                    Ok(Some(v1::CreateEvent {
                        result: Some(v1::create_event::Result::Cell(_)),
                        ..
                    })) => Ok(()),
                    Ok(Some(v1::CreateEvent {
                        result: Some(v1::create_event::Result::Error(e)),
                        ..
                    })) => Err(e.reason),
                    other => Err(format!("{other:?}")),
                },
                Err(s) => Err(format!("{:?}", s.code())),
            };
            let mut c = counts.lock().unwrap();
            let s = c.by_second.entry(second).or_default();
            match got {
                Ok(()) => {
                    s.0 += 1;
                    c.ok += 1;
                    c.took.push(t.elapsed());
                }
                Err(why) => {
                    s.1 += 1;
                    *c.refused.entry(why).or_default() += 1;
                }
            }
        });
    }
    while calls.join_next().await.is_some() {}
    let wall = start.elapsed().as_secs_f64();
    {
        let mut c = counts.lock().unwrap();
        let per_s: Vec<String> =
            c.by_second.values().map(|(ok, no)| format!("{ok}/{no}")).collect();
        println!("made {} cells in {wall:.1} s, refused {:?}", c.ok, c.refused);
        println!("made/refused by second: {}", per_s.join(" "));
        c.took.sort();
        let at = |q: f64| {
            let i = ((c.took.len().max(1) - 1) as f64 * q) as usize;
            c.took.get(i).map_or(0.0, |d| d.as_secs_f64() * 1e3)
        };
        println!("create took p50 {:.1} ms, p99 {:.1} ms", at(0.5), at(0.99));
    }

    let sel = v1::CellSelector {
        by: Some(v1::cell_selector::By::Labels(v1::LabelSelector {
            r#match: [("run".to_string(), run)].into(),
        })),
    };
    let req = v1::StopRequest { selector: Some(sel), snapshot: false };
    let r = CellsClient::new(channels[0].clone()).stop(call(key, req)).await.unwrap().into_inner();
    println!("stopped {} of {}", r.succeeded, r.matched);
}

fn call<T>(key: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {key}").parse().unwrap());
    r
}
