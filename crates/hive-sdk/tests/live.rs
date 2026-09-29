//! The SDK against a running comb. Set `HIVE_TEST_SOCKET` to its socket and `HIVE_TEST_IMAGE`
//! to an image it serves to run it, as root on a node that can run container cells.

use hive_sdk::{Backend, CellSpec, Client, Command, Output, Reason, Selector, Source, v1};
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

async fn client() -> Option<(Client, String)> {
    let Some(socket) = std::env::var_os("HIVE_TEST_SOCKET") else {
        eprintln!("skipped: set HIVE_TEST_SOCKET to run it");
        return None;
    };
    let image = std::env::var("HIVE_TEST_IMAGE").unwrap_or_else(|_| "python".into());
    let project = format!("sdk-{}", std::process::id());
    Some((Client::unix(socket).await.unwrap().project(&project).unwrap(), image))
}

fn spec(image: &str, labels: &[(&str, &str)]) -> CellSpec {
    let mut spec = CellSpec::new(Source::Image(image.into()), Backend::Container);
    spec.labels = labels.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect();
    spec
}

#[tokio::test(flavor = "multi_thread")]
async fn one_cell_runs_commands_and_moves_files() {
    let Some((client, image)) = client().await else { return };
    let cell = client.create(&spec(&image, &[("t", "one")])).await.unwrap();
    assert_eq!(cell.state(), v1::CellState::Running);

    let r = cell.run(["python3", "-c", "print(6 * 7)"]).await.unwrap();
    assert_eq!((r.exit_code, r.stdout.as_ref()), (0, b"42\n".as_ref()));
    let r = cell.run(Command::shell("echo $A >&2; exit 3").env("A", "b")).await.unwrap();
    assert_eq!((r.exit_code, r.stderr.as_ref()), (3, b"b\n".as_ref()));
    let r = cell.run(Command::shell("cat").stdin("fed")).await.unwrap();
    assert_eq!(r.stdout.as_ref(), b"fed");
    let r = cell.run(Command::new(["sleep", "5"]).timeout(Duration::from_millis(300))).await;
    assert!(r.unwrap().timed_out);

    // A session keeps the shell's state from one command to the next.
    let session = cell.session().await.unwrap();
    session.run("cd /tmp && X=7").await.unwrap();
    let r = session.run("echo $X $(pwd)").await.unwrap();
    assert_eq!(r.output.as_ref(), b"7 /tmp\n");
    session.close().await.unwrap();

    // A started process takes stdin and streams its output.
    let upper = ["python3", "-c", "import sys; print(sys.stdin.read().upper())"];
    let mut p = cell.start(upper).await.unwrap();
    p.write("hello").await.unwrap();
    p.close_stdin();
    let (mut out, mut code) = (Vec::new(), None);
    while let Some(o) = p.next().await.unwrap() {
        match o {
            Output::Stdout(b) => out.extend_from_slice(&b),
            Output::Stderr(_) => {}
            Output::Exit(r) => code = Some(r.exit_code),
        }
    }
    assert_eq!((out.as_slice(), code), (b"HELLO\n".as_ref(), Some(0)));

    // Files, big enough to be streamed both ways.
    let data: Vec<u8> = (0..3_000_000u32).map(|i| (i * 7 % 251) as u8).collect();
    let info = cell.write("/tmp/sdk/big.bin", data.clone()).await.unwrap();
    assert_eq!(info.size, 3_000_000);
    assert_eq!(cell.read("/tmp/sdk/big.bin").await.unwrap(), data);
    assert_eq!(cell.read_range("/tmp/sdk/big.bin", 10, 5).await.unwrap(), &data[10..15]);
    let r = cell.run(["python3", "-c", "print(len(open('/tmp/sdk/big.bin','rb').read()))"]);
    assert_eq!(r.await.unwrap().stdout.as_ref(), b"3000000\n");
    let names: Vec<_> =
        cell.list("/tmp/sdk", 1).await.unwrap().entries.into_iter().map(|e| e.path).collect();
    assert_eq!(names, ["/tmp/sdk/big.bin"]);
    let e = cell.read("/tmp/sdk/missing").await.unwrap_err();
    assert_eq!((e.reason, e.errno.as_deref()), (Reason::FileError, Some("ENOENT")));
    cell.remove("/tmp/sdk", true).await.unwrap();
    assert!(cell.stat("/tmp/sdk").await.is_err());

    cell.pause().await.unwrap();
    assert_eq!(client.get(cell.id()).await.unwrap().state(), v1::CellState::Paused);
    cell.resume().await.unwrap();
    assert_eq!(cell.run("true").await.unwrap().exit_code, 0);
    cell.stop().await.unwrap();
    let e = cell.run("true").await.unwrap_err();
    assert_eq!(e.reason, Reason::CellNotRunning);
}

#[tokio::test(flavor = "multi_thread")]
async fn many_cells_are_made_and_stopped_by_label() {
    let Some((client, image)) = client().await else { return };
    let n = 16;
    let t = Instant::now();
    let cells = client.create_many(&spec(&image, &[("t", "many")]), n, Some("k")).await.unwrap();
    let made = t.elapsed();
    let cells: Vec<_> = cells.into_iter().map(Result::unwrap).collect();
    assert_eq!(cells.len(), n as usize);
    // The same key gives the same cells back.
    let again = client.create_many(&spec(&image, &[("t", "many")]), n, Some("k")).await.unwrap();
    let ids = |c: &[hive_sdk::Cell]| c.iter().map(|c| c.id().to_string()).collect::<Vec<_>>();
    assert_eq!(ids(&again.into_iter().map(Result::unwrap).collect::<Vec<_>>()), ids(&cells));

    let t = Instant::now();
    let runs = cells.iter().map(|c| c.run(["python3", "-c", "print(1)"]));
    for r in futures::future::join_all(runs).await {
        assert_eq!(r.unwrap().stdout.as_ref(), b"1\n");
    }
    let ran = t.elapsed();

    let labels = BTreeMap::from([("t".to_string(), "many".to_string())]);
    assert_eq!(client.list(&labels, &[v1::CellState::Running]).await.unwrap().len(), n as usize);
    let t = Instant::now();
    let r = client.stop(&Selector::Labels(labels.clone())).await.unwrap();
    let stopped = t.elapsed();
    assert_eq!((r.matched, r.succeeded), (n, n), "{r:?}");
    assert!(client.list(&labels, &[v1::CellState::Running]).await.unwrap().is_empty());
    eprintln!(
        "{n} cells: made in {made:?}, python ran in all of them in {ran:?}, stopped in {stopped:?}"
    );
}
