//! The comb end to end, with a fake driver whose cells are a real drone behind a Unix socket in the
//! cell's directory. Everything but the sandbox is real: the WAL, admission, the handshake, the
//! lifecycle actors and recovery after a restart.

#![cfg(target_os = "linux")]

mod common;

use common::*;

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_runs_commands_and_stops() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut events = comb.subscribe();

    let cell = comb.create(request(spec("python"))).await.unwrap();
    assert_eq!(cell.status.state, CellState::Running);
    assert_eq!(comb.committed(), (1, 256 << 20));
    assert_eq!(echo(&comb, cell.id, "hello").await, "hello\n");
    assert!(s.0.join("cells").join(cell.id.to_string()).is_dir());

    let ended = comb.stop(cell.id, None).await.unwrap();
    assert_eq!(ended.status.state, CellState::Stopped);
    assert_eq!(ended.status.cause, Some(Cause::Requested));
    assert_eq!(comb.committed(), (0, 0));
    assert_eq!(fake.live(), 0);
    assert!(!s.0.join("cells").join(cell.id.to_string()).exists());
    // A stopped cell stays visible, and stopping it again is not an error.
    assert_eq!(comb.list().len(), 1);
    comb.stop(cell.id, None).await.unwrap();
    assert!(comb.drone(cell.id).await.unwrap_err().reason == Reason::CellNotRunning);

    let mut seen = Vec::new();
    while let Ok(e) = events.try_recv() {
        seen.push(e.status.state);
    }
    let want = [
        CellState::Preparing,
        CellState::Starting,
        CellState::Running,
        CellState::Stopping,
        CellState::Stopped,
    ];
    assert_eq!(seen, want);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn one_idempotency_key_makes_one_cell() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = Arc::new(open(config(&s.0), &fake).await);
    let creates: Vec<_> = (0..16)
        .map(|_| {
            let comb = comb.clone();
            tokio::spawn(async move {
                let req = CreateRequest { idem_key: Some("k1".into()), ..request(spec("python")) };
                comb.create(req).await.unwrap().id
            })
        })
        .collect();
    let mut ids = Vec::new();
    for c in creates {
        ids.push(c.await.unwrap());
    }
    ids.dedup();
    assert_eq!(ids.len(), 1);
    assert_eq!(comb.list().len(), 1);
    assert_eq!(comb.committed().0, 1);
    // The same key in another project is another cell.
    let other = CreateRequest {
        project: "q".into(),
        idem_key: Some("k1".into()),
        ..request(spec("python"))
    };
    assert_ne!(comb.create(other).await.unwrap().id, ids[0]);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_key_turned_away_for_room_is_turned_away_again() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(Config { mem_mib: Some(1024), ..config(&s.0) }, &fake).await;
    let sized = |mem_mib| {
        let mut s = spec("python");
        s.resources.mem_mib = mem_mib;
        s.qos = Qos::Latency;
        s
    };
    comb.create(request(sized(768))).await.unwrap();
    let keyed = |mem_mib, anyway| CreateRequest {
        idem_key: Some("k1".into()),
        anyway,
        ..request(sized(mem_mib))
    };
    let e = comb.create(keyed(768, false)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    // A smaller cell fits, but the key went elsewhere, so it is turned away again until the
    // gate says to make it anyway.
    let e = comb.create(keyed(256, false)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    let other = CreateRequest { idem_key: Some("k2".into()), ..request(sized(64)) };
    comb.create(other).await.unwrap();
    let made = comb.create(keyed(128, true)).await.unwrap();
    assert_eq!(comb.create(keyed(128, false)).await.unwrap().id, made.id);
    assert_eq!(comb.list().len(), 3);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn admission_and_bad_requests_leave_nothing_behind() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(Config { mem_mib: Some(1024), ..config(&s.0) }, &fake).await;

    let e = comb.create(request(spec("no-such-image"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::ImageUnavailable);
    let e = comb.create(request(spec("../images"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::InvalidArgument);
    let mut open_net = spec("python");
    open_net.network_profile = "mirrors".into();
    let e = comb.create(request(open_net)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    let microvm = CellSpec::new(Source::Image("python".into()), Backend::Microvm);
    let e = comb.create(request(microvm)).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);

    let mut big = spec("python");
    big.resources.mem_mib = 768;
    big.qos = Qos::Latency;
    comb.create(request(big.clone())).await.unwrap();
    let e = comb.create(request(big.clone())).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);
    // Best effort cells may overcommit.
    big.qos = Qos::BestEffort;
    comb.create(request(big)).await.unwrap();

    assert_eq!(comb.committed(), (2, 1536 << 20));
    let failed: Vec<_> =
        comb.list().into_iter().filter(|c| c.status.state == CellState::Failed).collect();
    assert_eq!(failed.len(), 1, "the missing image fails after it has an id");
    assert_eq!(failed[0].status.cause, Some(Cause::StartFailed));
    assert!(!s.0.join("cells").join(failed[0].id.to_string()).exists());
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_create_that_waits_its_turn_still_gets_the_whole_deadline() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    fake.start_ms.store(1000, Ordering::Relaxed);
    let cfg = Config {
        create_limit: [(Backend::Container, 1)].into(),
        create_deadline: Duration::from_millis(1500),
        ..config(&s.0)
    };
    let comb = Arc::new(open(cfg, &fake).await);
    let creates: Vec<_> = (0..3)
        .map(|i| {
            let comb = comb.clone();
            tokio::spawn(async move {
                // Staggered so they queue in this order, far enough apart for a loaded machine.
                // The third still waits past the deadline: it queues at 0.4 s and starts at 2 s.
                tokio::time::sleep(Duration::from_millis(200 * i)).await;
                comb.create(request(spec("python"))).await.map(|_| ()).map_err(|e| e.reason)
            })
        })
        .collect();
    let mut got = Vec::new();
    for c in creates {
        got.push(c.await.unwrap());
    }
    // The second waits about a second for the first and then takes a second itself, which is
    // past the deadline counted from the request but inside it counted from its turn. The third
    // waits about two seconds for its turn, which is past the deadline.
    assert_eq!(got, [Ok(()), Ok(()), Err(Reason::CapacityUnavailable)]);
    let text = comb.metrics().registry().render();
    assert!(
        text.contains(r#"hive_create_total{backend="container",result="CAPACITY_UNAVAILABLE"} 1"#),
        "{text}"
    );
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_cell_that_dies_while_starting_fails_and_is_cleaned_up() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let started = Instant::now();
    let e = comb.create(request(spec("crash"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::DroneUnreachable, "{e}");
    assert!(started.elapsed() < Duration::from_secs(2), "the death was noticed quickly");
    let cell = &comb.list()[0];
    assert_eq!(cell.status.state, CellState::Failed);
    assert_eq!(fake.stops.load(Ordering::Relaxed), 1);
    assert_eq!(fake.live(), 0);
    assert_eq!(comb.committed(), (0, 0));
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_dropped_channel_reconnects_and_a_dead_cell_is_stopped() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let a = comb.create(request(spec("python"))).await.unwrap().id;
    let b = comb.create(request(spec("python"))).await.unwrap().id;

    // Cut the channel a few times: each reconnect uses the secret from the last handshake.
    for round in 0..3 {
        let before = comb.drone(a).await.unwrap();
        fake.cut(a);
        before.closed().await;
        let until = Instant::now() + Duration::from_secs(5);
        while let Err(e) = comb.drone(a).await {
            assert_eq!(e.reason, Reason::DroneUnreachable);
            assert!(Instant::now() < until, "never reconnected");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert_eq!(echo(&comb, a, &format!("round{round}")).await, format!("round{round}\n"));
    }
    assert_eq!(comb.get(a).unwrap().status.state, CellState::Running);

    fake.kill(b);
    let ended = reaches(&comb, b, CellState::Stopped).await;
    assert_eq!(ended.status.cause, Some(Cause::Exited));
    assert_eq!(comb.committed().0, 1);

    // Every reconnect wrote its new secret down, so a restarted comb still gets in, and so does
    // one restarted after that.
    comb.shutdown().await;
    drop(comb);
    for round in 0..3 {
        let comb = open(config(&s.0), &fake).await;
        assert_eq!(echo(&comb, a, &format!("again{round}")).await, format!("again{round}\n"));
        comb.shutdown().await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn timers_expire_and_pause_cells() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;

    let mut hard = spec("python");
    hard.hard_ttl = Some(Duration::from_millis(300));
    let hard = comb.create(request(hard)).await.unwrap().id;

    // Longer than the others, so a loaded test host that stalls between two requests does not
    // pause it early.
    let mut idle = spec("python");
    idle.idle_ttl = Some(Duration::from_secs(1));
    idle.idle_action = IdleAction::Pause;
    let idle = comb.create(request(idle)).await.unwrap().id;

    let mut idle_stop = spec("python");
    idle_stop.idle_ttl = Some(Duration::from_millis(300));
    idle_stop.idle_action = IdleAction::Stop;
    let idle_stop = comb.create(request(idle_stop)).await.unwrap().id;

    // Keep one idle cell busy past its ttl.
    let busy_until = Instant::now() + Duration::from_millis(1500);
    while Instant::now() < busy_until {
        echo(&comb, idle, "busy").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);

    let expired = reaches(&comb, hard, CellState::Expired).await;
    assert_eq!(expired.status.cause, Some(Cause::HardTtl));
    let stopped = reaches(&comb, idle_stop, CellState::Stopped).await;
    assert_eq!(stopped.status.cause, Some(Cause::Idle));
    reaches(&comb, idle, CellState::Paused).await;
    assert!(fake.with(idle, |g| g.paused));

    // A request wakes it.
    assert_eq!(echo(&comb, idle, "awake").await, "awake\n");
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);
    assert!(!fake.with(idle, |g| g.paused));
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn paused_cells_are_reclaimed_then_stopped_and_ttls_extend() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let cfg = Config {
        reclaim_after: Duration::from_millis(200),
        pause_ttl: Duration::from_secs(1),
        ..config(&s.0)
    };
    let comb = open(cfg.clone(), &fake).await;
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    comb.pause(id).await.unwrap();
    assert!(!fake.with(id, |g| g.reclaimed));
    let until = Instant::now() + Duration::from_secs(5);
    while !fake.with(id, |g| g.reclaimed) {
        assert!(Instant::now() < until, "not reclaimed");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(comb.get(id).unwrap().status.state, CellState::Paused);
    let stopped = reaches(&comb, id, CellState::Stopped).await;
    assert_eq!(stopped.status.cause, Some(Cause::Idle));

    // A hard TTL counted from now, and an idle one, both kept across a restart.
    let mut short = spec("python");
    short.hard_ttl = Some(Duration::from_millis(400));
    let id = comb.create(request(short)).await.unwrap().id;
    let hour = Duration::from_secs(3600);
    let info = comb.extend_ttl(id, Some(hour), Some(hour)).await.unwrap();
    let hard = info.spec.hard_ttl.unwrap();
    assert!(hard >= hour && hard < hour + Duration::from_secs(5), "{hard:?}");
    assert_eq!(info.spec.idle_ttl, Some(hour));
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(comb.get(id).unwrap().status.state, CellState::Running);
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    let info = comb.get(id).unwrap();
    assert_eq!((info.spec.hard_ttl, info.spec.idle_ttl), (Some(hard), Some(hour)));
    assert_eq!(info.status.state, CellState::Running);
    comb.stop(id, None).await.unwrap();
    let e = comb.extend_ttl(id, Some(hour), None).await.unwrap_err();
    assert_eq!(e.reason, Reason::CellNotRunning);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn idle_cells_pause_while_the_node_is_short_of_memory() {
    let s = Scratch::new();
    let psi = s.0.join("memory.pressure");
    let pressure = |avg10: f64| {
        let text = format!(
            "some avg10={avg10:.2} avg60=0.00 avg300=0.00 total=1\nfull avg10=0.00 avg60=0.00 avg300=0.00 total=1\n"
        );
        std::fs::write(&psi, text).unwrap();
    };
    pressure(0.0);
    let fake = Arc::new(Fake::default());
    let cfg = Config {
        psi_stop_admit: 0.20,
        psi_source: Some(psi.clone()),
        pressure_idle: Duration::from_millis(500),
        ..config(&s.0)
    };
    let comb = open(cfg, &fake).await;
    let idle = comb.create(request(spec("python"))).await.unwrap().id;
    let mut warm = spec("python");
    warm.qos = Qos::Latency;
    let warm = comb.create(request(warm)).await.unwrap().id;
    let mut stops = spec("python");
    stops.idle_action = IdleAction::Stop;
    let stops = comb.create(request(stops)).await.unwrap().id;
    let busy = comb.create(request(spec("python"))).await.unwrap().id;

    // With no pressure an idle cell is left alone.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);

    pressure(35.0);
    let until = Instant::now() + Duration::from_secs(10);
    while !fake.with(idle, |g| g.reclaimed) {
        assert!(Instant::now() < until, "the idle cell was not paused and reclaimed");
        echo(&comb, busy, "busy").await;
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Paused);
    // A latency cell, a cell that would be stopped rather than paused, and a cell in use all run.
    for id in [warm, stops, busy] {
        assert_eq!(comb.get(id).unwrap().status.state, CellState::Running, "{id}");
    }
    let text = comb.metrics().registry().render();
    assert!(text.contains("hive_pressure_pauses_total 1"), "{text}");
    let e = comb.create(request(spec("python"))).await.unwrap_err();
    assert_eq!(e.reason, Reason::CapacityUnavailable);

    // Once the pressure is gone the node takes cells again, and idle ones stay running.
    pressure(1.0);
    let until = Instant::now() + Duration::from_secs(10);
    while comb.create(request(spec("python"))).await.is_err() {
        assert!(Instant::now() < until, "admits never came back");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(echo(&comb, idle, "awake").await, "awake\n");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(comb.get(idle).unwrap().status.state, CellState::Running);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn pause_and_resume_by_hand() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(comb.resume(id).await.unwrap().status.state, CellState::Running);
    assert_eq!(comb.resume(id).await.unwrap().status.state, CellState::Running);
    comb.stop(id, None).await.unwrap();
    assert_eq!(comb.pause(id).await.unwrap_err().reason, Reason::CellNotRunning);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_restarted_comb_picks_up_every_cell() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut ids = Vec::new();
    for _ in 0..6 {
        ids.push(comb.create(request(spec("python"))).await.unwrap().id);
    }
    comb.pause(ids[1]).await.unwrap();
    comb.stop(ids[2], None).await.unwrap();
    comb.shutdown().await;
    drop(comb);
    assert_eq!(fake.live(), 5, "a comb going away leaves its cells running");

    // While no comb is watching, one cell dies.
    fake.kill(ids[3]);

    let reopened = Instant::now();
    let comb = open(config(&s.0), &fake).await;
    assert!(reopened.elapsed() < Duration::from_secs(2), "took {:?}", reopened.elapsed());
    assert_eq!(comb.list().len(), 6);
    assert_eq!(comb.get(ids[0]).unwrap().status.state, CellState::Running);
    assert_eq!(comb.get(ids[1]).unwrap().status.state, CellState::Paused);
    assert_eq!(comb.get(ids[2]).unwrap().status.state, CellState::Stopped);
    let dead = reaches(&comb, ids[3], CellState::Stopped).await;
    assert_eq!(dead.status.cause, Some(Cause::Exited));
    assert_eq!(comb.committed(), (4, 4 * (256 << 20)));

    // The secrets came back from the WAL, so the channels work.
    assert_eq!(echo(&comb, ids[0], "again").await, "again\n");
    assert_eq!(echo(&comb, ids[1], "woken").await, "woken\n");
    // New ids never repeat old ones.
    let new = comb.create(request(spec("python"))).await.unwrap().id;
    assert!(!ids.contains(&new));
    assert!(new.seq() > ids[5].seq());

    // And once more, to be sure a recovered comb writes a WAL the next one reads.
    comb.shutdown().await;
    drop(comb);
    let comb = open(config(&s.0), &fake).await;
    assert_eq!(comb.list().len(), 7);
    assert_eq!(echo(&comb, new, "third").await, "third\n");
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_back_in_a_new_epoch_stops_the_old_cells() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(comb.create(request(spec("python"))).await.unwrap().id);
    }
    comb.pause(ids[1]).await.unwrap();
    comb.shutdown().await;
    drop(comb);

    // The node lost its lease and registered again, so its old cells are fenced.
    let cfg = Config { epoch: config(&s.0).epoch + 1, ..config(&s.0) };
    let comb = open(cfg, &fake).await;
    for &id in &ids {
        let c = comb.get(id).unwrap();
        // Losing the node is an infrastructure cause, so the cell failed rather than stopped.
        assert_eq!(c.status.state, CellState::Failed, "{id}");
        assert_eq!(c.status.cause, Some(Cause::NodeLost), "{id}");
    }
    assert_eq!(fake.live(), 0);
    assert_eq!(comb.committed(), (0, 0));
    let new = comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(new.epoch(), ids[0].epoch() + 1);
    assert_eq!(echo(&comb, new, "fresh").await, "fresh\n");

    // An old id the comb has forgotten is lost, and a made up one in this epoch is not found.
    let forgotten = CellId::new(ids[0].unit(), ids[0].node(), ids[0].epoch(), 999, 1).unwrap();
    assert_eq!(comb.get(forgotten).unwrap_err().reason, Reason::CellLost);
    let unknown = CellId::new(new.unit(), new.node(), new.epoch(), 999, 1).unwrap();
    assert_eq!(comb.get(unknown).unwrap_err().reason, Reason::CellNotFound);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_comb_that_loses_its_lease_stops_its_cells() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let comb = open(config(&s.0), &fake).await;
    let mut ids = Vec::new();
    for _ in 0..3 {
        ids.push(comb.create(request(spec("python"))).await.unwrap().id);
    }
    comb.pause(ids[1]).await.unwrap();
    comb.stop(ids[2], None).await.unwrap();
    comb.lose().await;
    assert_eq!(fake.live(), 0);
    let c = comb.get(ids[0]).unwrap();
    assert_eq!((c.status.state, c.status.cause), (CellState::Failed, Some(Cause::NodeLost)));
    let c = comb.get(ids[1]).unwrap();
    assert_eq!((c.status.state, c.status.cause), (CellState::Failed, Some(Cause::NodeLost)));
    // One that had already ended keeps its own end.
    assert_eq!(comb.get(ids[2]).unwrap().status.state, CellState::Stopped);
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn ended_cells_are_forgotten_after_a_while() {
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let cfg = Config { keep_ended: Duration::from_millis(200), ..config(&s.0) };
    let comb = open(cfg.clone(), &fake).await;
    let id = comb.create(request(spec("python"))).await.unwrap().id;
    comb.stop(id, None).await.unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    while comb.get(id).is_ok() {
        assert!(Instant::now() < until, "never forgotten");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(comb.get(id).unwrap_err().reason, Reason::CellNotFound);
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    assert!(comb.list().is_empty());
    comb.shutdown().await;
}

/// Creates and stops cells as fast as the comb allows, for the numbers in the PR. Run with
/// `cargo test --release -p hive-comb --test comb -- --ignored --nocapture`.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn churn() {
    for concurrency in
        std::env::var("HB_C").map_or(vec![1usize, 16, 64, 256], |v| vec![v.parse().unwrap()])
    {
        let s = Scratch::new();
        let fake = Arc::new(Fake::default());
        let mut cfg = Config { mem_mib: Some(1 << 20), ..config(&s.0) };
        // With HB_POOLS set, as root, every cell also gets a real process, in a real cgroup unless
        // it is "netns" and in a real network namespace unless it is "cgroup".
        let pools = std::env::var("HB_POOLS").ok();
        let tree = pools.as_ref().map(|_| Tree::new().expect("needs root"));
        if let (Some(t), Some(p)) = (&tree, &pools) {
            if p != "netns" {
                cfg.cgroup_root = Some(t.0.clone());
            }
            if p != "cgroup" {
                cfg.netns_dir = Some(s.0.join("netns"));
                cfg.netns_depth = Config::default().netns_depth;
            }
        }
        let comb = Arc::new(open(cfg, &fake).await);
        if tree.is_some() {
            // Starts from full pools, as a node that has been up for a while would.
            while comb.spare_netns().is_some_and(|d| d < Config::default().netns_depth)
                || comb.spare_cgroups().is_some_and(|d| d[1] < Config::default().cgroup_depth)
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        let total =
            std::env::var("HB_N").map_or(2048usize.max(concurrency * 8), |v| v.parse().unwrap());
        let started = Instant::now();
        let workers: Vec<_> = (0..concurrency)
            .map(|_| {
                let comb = comb.clone();
                tokio::spawn(async move {
                    let (mut creates, mut failed) = (Vec::new(), 0);
                    for _ in 0..total / concurrency {
                        let t = Instant::now();
                        // A host too busy to start a cell inside its deadline fails the create,
                        // which is counted rather than fatal.
                        match comb.create(request(spec("python"))).await {
                            Ok(cell) => {
                                creates.push(t.elapsed());
                                comb.stop(cell.id, Some(Duration::ZERO)).await.unwrap();
                            }
                            Err(e) => {
                                assert!(
                                    matches!(
                                        e.reason,
                                        Reason::DroneUnreachable | Reason::CapacityUnavailable
                                    ),
                                    "{e:?}"
                                );
                                failed += 1;
                            }
                        }
                    }
                    (creates, failed)
                })
            })
            .collect();
        let (mut creates, mut failed) = (Vec::new(), 0);
        for w in workers {
            let (c, f) = w.await.unwrap();
            creates.extend(c);
            failed += f;
        }
        let took = started.elapsed();
        creates.sort();
        let p = |q: f64| creates[((creates.len() - 1) as f64 * q) as usize];
        let wal = comb.wal_stats();
        println!(
            "concurrency {concurrency:>3}: {total} cells made and stopped in {took:?}, {:.0} per second, {failed} failed, create p50 {:?} p99 {:?}, {} WAL writes in {} syncs",
            total as f64 / took.as_secs_f64(),
            p(0.5),
            p(0.99),
            wal.writes,
            wal.syncs,
        );
        comb.shutdown().await;
        for (_, mut w) in fake.workers.lock().unwrap().drain() {
            let _ = w.kill();
            let _ = w.wait();
        }
    }
}

/// The cgroup a process is in, from `/proc`.
fn cgroup_of(pid: u32) -> PathBuf {
    let line = std::fs::read_to_string(format!("/proc/{pid}/cgroup")).unwrap();
    let rel = line.trim().strip_prefix("0::/").unwrap().to_owned();
    Path::new("/sys/fs/cgroup").join(rel)
}

fn worker(fake: &Fake, id: CellId) -> u32 {
    fake.workers.lock().unwrap()[&id].id()
}

/// Waits for the cell's worker to be killed, and fails the test after five seconds.
async fn killed(fake: &Fake, id: CellId) {
    use std::os::unix::process::ExitStatusExt;
    let mut child = fake.workers.lock().unwrap().remove(&id).unwrap();
    let until = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = child.try_wait().unwrap() {
            assert_eq!(status.signal(), Some(9));
            return;
        }
        assert!(Instant::now() < until, "the worker of {id} is still alive");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn gone(dir: &Path) {
    let until = Instant::now() + Duration::from_secs(5);
    while dir.exists() {
        assert!(Instant::now() < until, "{} is still there", dir.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cells_live_in_cgroups_of_their_own() {
    let Some(tree) = Tree::new() else { return };
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let cfg = Config { cgroup_root: Some(tree.0.clone()), cgroup_depth: 8, ..config(&s.0) };
    let comb = open(cfg.clone(), &fake).await;

    let mut fast = spec("python");
    fast.qos = Qos::Latency;
    fast.resources.vcpu_milli = 500;
    let a = comb.create(request(fast)).await.unwrap().id;
    let leaf = cgroup_of(worker(&fake, a));
    assert!(leaf.starts_with(tree.0.join("latency.slice")), "{}", leaf.display());
    let read = |f: &str| std::fs::read_to_string(leaf.join(f)).unwrap().trim().to_owned();
    assert_eq!(read("memory.max"), (256u64 << 20).to_string());
    assert_eq!(read("cpu.max"), "50000 100000");

    // Stopping the cell kills everything in its cgroup, whatever the driver did.
    comb.stop(a, None).await.unwrap();
    killed(&fake, a).await;
    gone(&leaf).await;

    // A cgroup no record claims is swept on the next start, and a live cell keeps its own.
    let b = comb.create(request(spec("python"))).await.unwrap().id;
    let kept = cgroup_of(worker(&fake, b));
    let stray = tree.0.join("standard.slice").join("cell-999999");
    std::fs::create_dir(&stray).unwrap();
    let mut orphan = std::process::Command::new("sleep").arg("600").spawn().unwrap();
    std::fs::write(stray.join("cgroup.procs"), orphan.id().to_string()).unwrap();
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    assert!(!stray.exists());
    use std::os::unix::process::ExitStatusExt;
    assert_eq!(orphan.wait().unwrap().signal(), Some(9));
    assert_eq!(cgroup_of(worker(&fake, b)), kept);
    reaches(&comb, b, CellState::Running).await;
    comb.stop(b, None).await.unwrap();
    killed(&fake, b).await;
    gone(&kept).await;
    comb.shutdown().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_setup_boost_lifts_the_cpu_quota_until_the_cell_is_ready() {
    let Some(tree) = Tree::new() else { return };
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let cfg = Config { cgroup_root: Some(tree.0.clone()), cgroup_depth: 4, ..config(&s.0) };
    let comb = open(cfg.clone(), &fake).await;
    let boosted = |d: Duration| {
        let mut s = spec("python");
        s.resources.vcpu_milli = 500;
        s.burst_until_ready = Some(d);
        s
    };
    let cpu_max = |leaf: &Path| std::fs::read_to_string(leaf.join("cpu.max")).unwrap();

    // Four times the quota until the caller says the setup is done, then twice it, which is
    // the default burst factor for the standard class.
    let a = comb.create(request(boosted(Duration::from_secs(3600)))).await.unwrap().id;
    let leaf = cgroup_of(worker(&fake, a));
    assert_eq!(cpu_max(&leaf).trim(), "200000 100000");
    comb.ready(a).await.unwrap();
    assert_eq!(cpu_max(&leaf).trim(), "100000 100000");
    // Ready twice is fine, and a comb that starts over does not put the boost back.
    comb.ready(a).await.unwrap();
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    reaches(&comb, a, CellState::Running).await;
    assert_eq!(comb.get(a).unwrap().spec.burst_until_ready, None);
    assert_eq!(cpu_max(&leaf).trim(), "100000 100000");

    // Or until the boost runs out.
    let b = comb.create(request(boosted(Duration::from_millis(300)))).await.unwrap().id;
    let leaf_b = cgroup_of(worker(&fake, b));
    let until = Instant::now() + Duration::from_secs(10);
    while cpu_max(&leaf_b).trim() != "100000 100000" {
        assert!(Instant::now() < until, "still {}", cpu_max(&leaf_b).trim());
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    for id in [a, b] {
        comb.stop(id, None).await.unwrap();
        killed(&fake, id).await;
    }
    comb.shutdown().await;
}

/// The network namespace a process is in, as the inode `stat` gives for a namespace file.
fn netns_of(pid: u32) -> u64 {
    let link = std::fs::read_link(format!("/proc/{pid}/ns/net")).unwrap();
    let link = link.to_str().unwrap();
    link.strip_prefix("net:[").and_then(|l| l.strip_suffix(']')).unwrap().parse().unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn cells_get_a_network_namespace_of_their_own() {
    let Some(tree) = Tree::new() else { return };
    let s = Scratch::new();
    let fake = Arc::new(Fake::default());
    let dir = s.0.join("netns");
    let cfg = Config {
        cgroup_root: Some(tree.0.clone()),
        netns_dir: Some(dir.clone()),
        netns_depth: 4,
        ..config(&s.0)
    };
    let comb = open(cfg.clone(), &fake).await;

    let a = comb.create(request(spec("python"))).await.unwrap().id;
    let b = comb.create(request(spec("python"))).await.unwrap().id;
    let (pa, pb) = (worker(&fake, a), worker(&fake, b));
    let host = netns_of(std::process::id());
    assert_ne!(netns_of(pa), host);
    assert_ne!(netns_of(pa), netns_of(pb));
    let ns_a = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .find(|p| inode(p) == netns_of(pa))
        .expect("a's namespace is a file in the directory");

    comb.stop(a, None).await.unwrap();
    killed(&fake, a).await;
    gone(&ns_a).await;

    // A namespace no record claims is removed on the next start, and a live cell keeps its own.
    let stray = dir.join("cell-999999");
    hive_cell::netns::create(std::slice::from_ref(&stray)).pop().unwrap().unwrap();
    comb.shutdown().await;
    drop(comb);
    let comb = open(cfg, &fake).await;
    assert!(!stray.exists());
    let kept: Vec<PathBuf> = std::fs::read_dir(&dir)
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| inode(p) == netns_of(pb))
        .collect();
    assert_eq!(kept.len(), 1);
    reaches(&comb, b, CellState::Running).await;
    comb.stop(b, None).await.unwrap();
    killed(&fake, b).await;
    gone(&kept[0]).await;
    comb.shutdown().await;
}
