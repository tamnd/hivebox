//! The `hive-pollen` binary: tasks in as lines of JSON, a trajectory out for each sample as a
//! line of JSON.

#![forbid(unsafe_code)]

use hive_pollen::{Task, Trajectory, Worker};
use hive_sdk::Client;
use std::process::ExitCode;
use std::time::Instant;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tokio::sync::mpsc;

const USAGE: &str = "\
usage: hive-pollen [--socket PATH | --endpoint URL] [--project NAME] [--max-inflight N]
                   [--out FILE] TASKS

TASKS is a file with one task per line as JSON, or - for stdin. Each sample's trajectory goes to
FILE, or stdout, as a line of JSON as soon as the sample is done, and a summary goes to stderr at
the end. --max-inflight is how many sample cells may be alive at once, 8 by default.

The endpoint is $HIVE_ENDPOINT, a gate's address like http://10.0.0.5:7400, or the comb socket
$HIVE_SOCKET, /run/hivebox/comb.sock by default. A gate needs the API key or token in $HIVE_TOKEN.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.iter().any(|a| a == "-h" || a == "--help") {
        print!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    if matches!(args.first().map(String::as_str), Some("--version" | "-V")) {
        println!("hive-pollen {}", env!("CARGO_PKG_VERSION"));
        return ExitCode::SUCCESS;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().enable_all().build() {
        Ok(rt) => rt,
        Err(e) => {
            eprintln!("hive-pollen: {e}");
            return ExitCode::FAILURE;
        }
    };
    match rt.block_on(run(args)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("hive-pollen: {e}");
            ExitCode::FAILURE
        }
    }
}

/// The flags and the tasks file.
#[derive(Debug, Default)]
struct Args {
    endpoint: Option<String>,
    project: Option<String>,
    max_inflight: Option<u32>,
    out: Option<String>,
    tasks: Option<String>,
}

fn parse(args: Vec<String>) -> Result<Args, String> {
    let mut out = Args::default();
    let mut it = args.into_iter();
    while let Some(a) = it.next() {
        let mut value = || it.next().ok_or_else(|| format!("{a} needs a value"));
        match a.as_str() {
            "--socket" | "-s" => out.endpoint = Some(format!("unix:{}", value()?)),
            "--endpoint" => out.endpoint = Some(value()?),
            "--project" | "-p" => out.project = Some(value()?),
            "--out" | "-o" => out.out = Some(value()?),
            "--max-inflight" => {
                let v = value()?;
                out.max_inflight = Some(v.parse().map_err(|_| format!("{v} is not a number"))?);
            }
            _ if a.len() > 1 && a.starts_with('-') => return Err(format!("{a} is not a flag")),
            _ if out.tasks.is_none() => out.tasks = Some(a),
            _ => return Err(format!("{a}: only one tasks file")),
        }
    }
    Ok(out)
}

async fn run(args: Vec<String>) -> Result<(), String> {
    let args = parse(args)?;
    let Some(path) = &args.tasks else { return Err(format!("no tasks file\n{USAGE}")) };
    let text = if path == "-" {
        let mut s = String::new();
        tokio::io::AsyncReadExt::read_to_string(&mut tokio::io::stdin(), &mut s)
            .await
            .map_err(|e| format!("stdin: {e}"))?;
        s
    } else {
        tokio::fs::read_to_string(path).await.map_err(|e| format!("{path}: {e}"))?
    };
    let mut tasks = Vec::new();
    for (n, line) in text.lines().enumerate().filter(|(_, l)| !l.trim().is_empty()) {
        tasks.push(Task::parse(line).map_err(|e| format!("{path} line {}: {e}", n + 1))?);
    }
    let endpoint = args.endpoint.clone().unwrap_or_else(|| {
        std::env::var("HIVE_ENDPOINT").unwrap_or_else(|_| {
            let s = std::env::var("HIVE_SOCKET")
                .unwrap_or_else(|_| hive_sdk::DEFAULT_SOCKET.to_string());
            format!("unix:{s}")
        })
    });
    let mut client = Client::connect(&endpoint).await.map_err(|e| e.to_string())?;
    if let Some(p) = args.project.clone().or_else(|| std::env::var("HIVE_PROJECT").ok()) {
        client = client.project(&p).map_err(|e| e.to_string())?;
    }
    if let Ok(t) = std::env::var("HIVE_TOKEN") {
        client = client.token(&t).map_err(|e| e.to_string())?;
    }
    let mut sink: Box<dyn AsyncWrite + Unpin> = match &args.out {
        Some(f) => Box::new(tokio::fs::File::create(f).await.map_err(|e| format!("{f}: {e}"))?),
        None => Box::new(tokio::io::stdout()),
    };
    let worker = Worker::new(client, args.max_inflight.unwrap_or(8));
    let (tx, mut rx) = mpsc::channel::<Trajectory>(64);
    let start = Instant::now();
    let write = async {
        let mut sum = Summary::default();
        while let Some(t) = rx.recv().await {
            sum.add(&t);
            let mut line = serde_json::to_vec(&t).map_err(|e| e.to_string())?;
            line.push(b'\n');
            sink.write_all(&line).await.map_err(|e| e.to_string())?;
            sink.flush().await.map_err(|e| e.to_string())?;
        }
        Ok::<_, String>(sum)
    };
    let ((), sum) = tokio::join!(worker.run(tasks, tx), write);
    let sum = sum?;
    let secs = start.elapsed().as_secs_f64();
    eprintln!(
        "hive-pollen: {} samples in {secs:.1} s ({:.1} a minute), {} passed, {} rewarded, {} masked, {} tampered",
        sum.samples,
        f64::from(sum.samples) * 60.0 / secs.max(0.001),
        sum.passed,
        sum.rewarded,
        sum.masked,
        sum.tampered,
    );
    Ok(())
}

/// Counts for the line at the end.
#[derive(Debug, Default)]
struct Summary {
    samples: u32,
    passed: u32,
    rewarded: u32,
    masked: u32,
    tampered: u32,
}

impl Summary {
    fn add(&mut self, t: &Trajectory) {
        self.samples += 1;
        self.passed += u32::from(t.passed);
        self.rewarded += u32::from(t.reward == Some(1.0));
        self.masked += u32::from(t.reward.is_none());
        self.tampered += u32::from(!t.tampered.is_empty());
    }
}
