//! Measures what tokens cost: checking one in the gate's code, and calls through a live gate
//! with a key and with a token held to one cell.
//!
//! ```text
//! cargo run --release -p hive-gate --example token -- GATE KEY IMAGE CALLS
//! token http://127.0.0.1:7401 hb_... python 500
//! ```
//!
//! It first times minting, checking and asking a token in this process with a key of its own.
//! Then it makes a cell through the gate, gets a token from the gate held to that cell and to
//! exec, and runs `true` in the cell `CALLS` times with the key and `CALLS` times with the token,
//! one after another. Last it checks the token can not reach a second cell, and stops both.

use std::time::{Duration, Instant, SystemTime};

use hive_proto::v1;
use hive_proto::v1::cells_client::CellsClient;
use hive_proto::v1::exec_client::ExecClient;
use hive_proto::v1::tokens_client::TokensClient;
use tonic::Request;
use tonic::transport::{Channel, Endpoint};

#[tokio::main]
async fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let [gate, key, image, calls] = &args[..] else {
        eprintln!("usage: token GATE KEY IMAGE CALLS");
        std::process::exit(2);
    };
    let calls: usize = calls.parse().expect("CALLS");
    in_process();

    let ch = Endpoint::from_shared(gate.clone()).unwrap().connect().await.unwrap();
    let a = create(&ch, key, image).await;
    let b = create(&ch, key, image).await;
    let mut tokens = TokensClient::new(ch.clone());
    let ask = v1::MintTokenRequest {
        ttl: Some(hive_proto::convert::duration_to_v1(Duration::from_secs(600))),
        cell_ids: vec![a.clone()],
        ops: vec!["exec".into()],
    };
    let mut mint = Vec::new();
    let mut token = String::new();
    for _ in 0..20 {
        let t = Instant::now();
        token = tokens.mint(call(key, ask.clone())).await.unwrap().into_inner().token;
        mint.push(t.elapsed());
    }
    print("mint through the gate and keeper", &mut mint);
    println!("token is {} bytes", token.len());
    // The gate takes the keeper's public key with its next key list, up to a second later.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    let mut exec = ExecClient::new(ch.clone());
    let run = |id: &str| v1::RunRequest {
        cell_id: id.to_owned(),
        argv: vec!["true".into()],
        ..Default::default()
    };
    for (what, bearer) in [
        ("key", key.as_str()),
        ("token", token.as_str()),
        ("key", key.as_str()),
        ("token", token.as_str()),
    ] {
        let mut took = Vec::with_capacity(calls);
        for _ in 0..calls {
            let t = Instant::now();
            let r = exec.run(call(bearer, run(&a))).await.unwrap().into_inner();
            assert_eq!(r.exit_code, 0);
            took.push(t.elapsed());
        }
        print(&format!("exec run with the {what}"), &mut took);
    }
    let other = exec.run(call(&token, run(&b))).await.map(drop).unwrap_err();
    println!("the token on another cell: {:?}, {}", other.code(), other.message());

    let mut cells = CellsClient::new(ch);
    for id in [a, b] {
        let sel = v1::CellSelector { by: Some(v1::cell_selector::By::Id(id)) };
        let req = v1::StopRequest { selector: Some(sel), snapshot: false };
        cells.stop(call(key, req)).await.unwrap();
    }
}

/// Times minting, checking and asking a token in this process.
fn in_process() {
    let issuer = hive_auth::Issuer::new(&[7; 32]).unwrap();
    let verifier = hive_auth::Verifier::new(&issuer.public()).unwrap();
    let until = SystemTime::now() + Duration::from_secs(600);
    let narrow =
        hive_auth::Narrow { cells: vec!["c1".into()], ops: vec!["exec".into()], until: None };
    let n = 2000;
    let (mut mint, mut verify, mut allows) = (Vec::new(), Vec::new(), Vec::new());
    let mut token = String::new();
    for _ in 0..n {
        let t = Instant::now();
        token = issuer.mint("swe", &[1; 32], until, &narrow).unwrap();
        mint.push(t.elapsed());
    }
    let mut tok = None;
    for _ in 0..n {
        let t = Instant::now();
        tok = Some(verifier.verify(&token).unwrap());
        verify.push(t.elapsed());
    }
    let tok = tok.unwrap();
    for _ in 0..n {
        let t = Instant::now();
        tok.allows("exec", Some("c1"), SystemTime::now()).unwrap();
        allows.push(t.elapsed());
    }
    print("mint in process", &mut mint);
    print("check signatures in process", &mut verify);
    print("ask what it allows in process", &mut allows);
}

async fn create(ch: &Channel, key: &str, image: &str) -> String {
    let spec = v1::CellSpec {
        source: Some(v1::cell_spec::Source::Image(v1::ImageRef { r#ref: image.to_owned() })),
        backend: v1::Backend::Container.into(),
        resources: Some(v1::Resources { mem_mib: 64, ..Default::default() }),
        ..Default::default()
    };
    let req = v1::CreateRequest { spec: Some(spec), count: 1, ..Default::default() };
    let mut events =
        CellsClient::new(ch.clone()).create(call(key, req)).await.unwrap().into_inner();
    match events.message().await.unwrap().and_then(|e| e.result) {
        Some(v1::create_event::Result::Cell(c)) => c.id,
        other => panic!("create: {other:?}"),
    }
}

fn call<T>(bearer: &str, msg: T) -> Request<T> {
    let mut r = Request::new(msg);
    r.metadata_mut().insert("authorization", format!("Bearer {bearer}").parse().unwrap());
    r
}

fn print(what: &str, took: &mut [Duration]) {
    took.sort_unstable();
    let at = |q: f64| took[((took.len() - 1) as f64 * q) as usize].as_secs_f64() * 1e6;
    println!("{what}: p50 {:.0} us, p99 {:.0} us ({} calls)", at(0.5), at(0.99), took.len());
}
