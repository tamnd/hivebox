//! Drives a keeper group the way a cluster of combs would and prints what it measured.
//!
//! ```text
//! cargo run --release -p hive-keeper --example load -- ADDR NODES SECONDS
//! cargo run --release -p hive-keeper --example load -- leader ADDR
//! cargo run --release -p hive-keeper --example load -- nodes ADDR
//! ```
//!
//! It registers `NODES` combs through the member at `ADDR`, then renews each lease once a second
//! for `SECONDS`, the way combs keep their leases, and also makes a project and a key every
//! 100 ms. At the end it prints the call counts, the failures and the latency of each kind.

use std::time::{Duration, Instant};

use hive_proto::internal as pb;
use hive_proto::internal::keeper_client::KeeperClient;
use tonic::transport::Channel;

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if let [leader, addr] = &args[..]
        && leader == "leader"
    {
        let channel =
            Channel::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap();
        let mut c = KeeperClient::new(channel);
        println!("{}", c.status(pb::StatusRequest {}).await.unwrap().into_inner().leader);
        return;
    }
    if let [nodes, addr] = &args[..]
        && nodes == "nodes"
    {
        let channel =
            Channel::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap();
        let mut c = KeeperClient::new(channel);
        for n in c.list_nodes(pb::ListNodesRequest {}).await.unwrap().into_inner().nodes {
            println!("node {} {} epoch {} lost {} at {}", n.node, n.name, n.epoch, n.lost, n.addr);
        }
        return;
    }
    let [addr, nodes, secs] = &args[..] else {
        eprintln!("usage: load ADDR NODES SECONDS");
        std::process::exit(2);
    };
    let nodes: u32 = nodes.parse().expect("NODES is a number");
    let secs: u64 = secs.parse().expect("SECONDS is a number");
    let channel = Channel::from_shared(format!("http://{addr}")).unwrap().connect().await.unwrap();
    let client = KeeperClient::new(channel);

    let start = Instant::now();
    let mut leases = Vec::new();
    let mut register = Vec::new();
    let run = std::process::id();
    for chunk in (0..nodes).collect::<Vec<_>>().chunks(64) {
        let calls = chunk.iter().map(|i| {
            let mut c = client.clone();
            let req = pb::RegisterRequest {
                name: format!("load-{run}-{i}"),
                addr: format!("http://10.9.{}.{}:7420", i / 250, i % 250),
                epoch: 0,
            };
            async move {
                let t = Instant::now();
                let r = c.register(req).await;
                (t.elapsed(), r)
            }
        });
        for (took, r) in futures::future::join_all(calls).await {
            register.push(took);
            leases.push(r.expect("register").into_inner());
        }
    }
    println!("register: {} combs in {:.2} s", nodes, start.elapsed().as_secs_f64());
    print_latency("register", &mut register);

    let deadline = Instant::now() + Duration::from_secs(secs);
    let renews = leases.into_iter().map(|l| {
        let mut c = client.clone();
        async move {
            let mut took = Vec::new();
            let mut failed = 0u32;
            // Spread the renewals over the second so they do not all land at once.
            tokio::time::sleep(Duration::from_millis(u64::from(l.node) * 1000 / 1024 % 1000)).await;
            let mut tick = tokio::time::interval(Duration::from_secs(1));
            while Instant::now() < deadline {
                tick.tick().await;
                let t = Instant::now();
                match c.renew(pb::RenewRequest { node: l.node, epoch: l.epoch }).await {
                    Ok(_) => took.push(t.elapsed()),
                    Err(_) => failed += 1,
                }
            }
            (took, failed)
        }
    });
    let admin = {
        let mut c = client.clone();
        async move {
            let mut took = Vec::new();
            let mut i = 0;
            while Instant::now() < deadline {
                let t = Instant::now();
                let name = format!("load-{run}-{i}");
                let made = c
                    .create_project(pb::CreateProjectRequest { name: name.clone(), quota: None })
                    .await;
                let made = match made {
                    Ok(_) => c.create_key(pb::CreateKeyRequest { project: name }).await.map(drop),
                    Err(e) => Err(e),
                };
                if let Err(e) = made {
                    eprintln!("project and key {i}: {}", e.message());
                } else {
                    took.push(t.elapsed());
                }
                i += 1;
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            took
        }
    };
    let started = Instant::now();
    let (renewed, mut admin) = tokio::join!(futures::future::join_all(renews), admin);
    let elapsed = started.elapsed().as_secs_f64();
    let mut all = Vec::new();
    let mut failed = 0;
    for (took, f) in renewed {
        all.extend(took);
        failed += f;
    }
    println!(
        "renew: {} ok, {failed} failed, {:.0} a second",
        all.len(),
        all.len() as f64 / elapsed
    );
    print_latency("renew", &mut all);
    println!("project and key: {} pairs", admin.len());
    print_latency("project and key", &mut admin);
}

fn print_latency(what: &str, took: &mut [Duration]) {
    if took.is_empty() {
        return;
    }
    took.sort_unstable();
    let at = |p: usize| took[(took.len() * p / 100).min(took.len() - 1)].as_secs_f64() * 1000.0;
    println!("{what}: p50 {:.2} ms, p99 {:.2} ms, max {:.2} ms", at(50), at(99), at(100));
}
