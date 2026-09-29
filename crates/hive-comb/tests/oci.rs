//! Container cells through the whole comb: admission, the cgroup and network namespace pools, the
//! WAL, the OCI driver with its workers started as `hive-comb --oci-worker`, and the drone inside.
//! Needs root, cgroup v2, a static drone in `HIVE_OCI_DRONE`, and an image: either one made by
//! `hive-oci import` in `HIVE_OCI_IMAGE`, or a `hive-nectar` store in `HIVE_NECTAR_STORE` and the id
//! of an image in it in `HIVE_NECTAR_IMAGE`. Passes without doing anything when one is missing.

#![cfg(target_os = "linux")]

mod common;

use common::*;

/// A comb with only the container backend, on pools and a data directory of its own.
struct Node {
    cfg: Config,
    drone: PathBuf,
    // Dropped in this order: the comb before its cgroups and its directory.
    comb: Comb,
    _tree: Tree,
    _scratch: Scratch,
}

impl Node {
    async fn new(depth: usize) -> Option<Self> {
        let Some(drone) = std::env::var_os("HIVE_OCI_DRONE") else {
            eprintln!("skipped: set HIVE_OCI_DRONE to run it");
            return None;
        };
        let nectar =
            std::env::var_os("HIVE_NECTAR_STORE").zip(std::env::var_os("HIVE_NECTAR_IMAGE"));
        let image = std::env::var_os("HIVE_OCI_IMAGE");
        if nectar.is_none() && image.is_none() {
            eprintln!(
                "skipped: set HIVE_OCI_IMAGE, or HIVE_NECTAR_STORE and HIVE_NECTAR_IMAGE, to run it"
            );
            return None;
        }
        let tree = Tree::new()?;
        let scratch = Scratch::new();
        // Scratch makes an empty python image, and this one is the real thing.
        let python = scratch.0.join("images").join("python");
        std::fs::remove_dir_all(&python).unwrap();
        let mut images = Images::default();
        match (nectar, image) {
            (Some((store, id)), _) => {
                std::fs::write(&python, id.as_encoded_bytes()).unwrap();
                images = Images {
                    store: Some(store.into()),
                    cache_dir: scratch.0.join("cache"),
                    layers_dir: scratch.0.join("layers"),
                    ..images
                };
            }
            (None, Some(image)) => std::os::unix::fs::symlink(image, &python).unwrap(),
            (None, None) => unreachable!(),
        }
        let cfg = Config {
            cgroup_root: Some(tree.0.clone()),
            cgroup_depth: depth,
            netns_dir: Some(scratch.0.join("netns")),
            netns_depth: depth,
            create_deadline: Duration::from_secs(30),
            stop_grace: Duration::from_secs(2),
            // Admission counts memory the cells may use, and idle ones use under a MiB.
            mem_mib: Some(1 << 20),
            images,
            ..config(&scratch.0)
        };
        let drone = PathBuf::from(drone);
        let comb = open_oci(&cfg, &drone).await;
        Some(Self { cfg, drone, comb, _tree: tree, _scratch: scratch })
    }

    /// Shuts the comb down, which leaves its cells running, and opens a new one on the same data.
    async fn restart(&mut self) {
        self.comb.shutdown().await;
        self.comb = open_oci(&self.cfg, &self.drone).await;
    }
}

async fn open_oci(cfg: &Config, drone: &Path) -> Comb {
    let oci = hive_cell_oci::Config {
        worker: vec![env!("CARGO_BIN_EXE_hive-comb").into(), "--oci-worker".into()],
        drone: drone.to_path_buf(),
        state_dir: cfg.data_dir.join("oci"),
        ..hive_cell_oci::Config::default()
    };
    let mut drivers = DriverRegistry::new();
    drivers.add(Arc::new(hive_cell_oci::OciDriver::new(oci).unwrap()));
    Comb::open(cfg.clone(), drivers).await.unwrap()
}

async fn sh(comb: &Comb, id: CellId, script: &str) -> String {
    // Right after a restart the comb is still dialling the drone again.
    let until = Instant::now() + Duration::from_secs(5);
    let drone = loop {
        match comb.drone(id).await {
            Ok(d) => break d,
            Err(e) if e.reason == Reason::DroneUnreachable && Instant::now() < until => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => panic!("no drone for {id}: {e}"),
        }
    };
    let req = RunRequest {
        command: Some(Command { shell: script.into(), ..Command::default() }),
        ..RunRequest::default()
    };
    let r = drone.run(&req).await.unwrap();
    assert_eq!(r.exit_code, 0, "{script}: {}", String::from_utf8_lossy(&r.stderr));
    String::from_utf8(r.stdout.to_vec()).unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_container_cell_goes_through_the_comb_and_outlives_it() {
    let Some(mut node) = Node::new(4).await else { return };
    let id = node.comb.create(request(spec("python"))).await.unwrap().id;
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Running);
    assert_eq!(sh(&node.comb, id, "python3 -c 'print(6 * 7)'").await, "42\n");
    // Its own cgroup namespace, rooted at the cell's cgroup, and a network with only loopback.
    assert_eq!(sh(&node.comb, id, "cat /proc/self/cgroup").await, "0::/\n");
    let links = sh(&node.comb, id, "tail -n +3 /proc/net/dev | cut -d: -f1 | tr -d ' '").await;
    assert_eq!(links, "lo\n");

    assert_eq!(node.comb.pause(id).await.unwrap().status.state, CellState::Paused);
    assert_eq!(node.comb.resume(id).await.unwrap().status.state, CellState::Running);
    sh(&node.comb, id, "echo kept > /root/note").await;

    node.restart().await;
    assert_eq!(node.comb.get(id).unwrap().status.state, CellState::Running);
    assert_eq!(sh(&node.comb, id, "cat /root/note").await, "kept\n");

    let info = node.comb.stop(id, None).await.unwrap();
    assert_eq!(info.status.state, CellState::Stopped);
    assert!(!node.cfg.data_dir.join("oci").join(id.to_string()).exists());
    node.comb.shutdown().await;
}

/// What a container cell costs through the comb. Run it with
/// `cargo test --release -p hive-comb --test oci -- --ignored --nocapture`, with `CELLS` to change
/// how many are made at once.
#[tokio::test(flavor = "multi_thread")]
#[ignore = "a measurement, not a test"]
async fn oci_through_the_comb() {
    let cells: usize = std::env::var("CELLS").ok().and_then(|v| v.parse().ok()).unwrap_or(64);
    let Some(node) = Node::new(cells).await else { return };
    let comb = node.comb.clone();
    // Starts from full pools, as a node that has been up a while would.
    while comb.spare_netns().is_some_and(|d| d < cells)
        || comb.spare_cgroups().is_some_and(|d| d[1] < cells)
    {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let mut one = Vec::new();
    for _ in 0..20 {
        let t = Instant::now();
        let id = comb.create(request(spec("python"))).await.unwrap().id;
        one.push(t.elapsed());
        comb.stop(id, None).await.unwrap();
    }
    one.sort();
    println!("one at a time: create p50 {:?} max {:?}", one[one.len() / 2], one[one.len() - 1]);

    let t = Instant::now();
    let made = futures::future::join_all((0..cells).map(|_| {
        let comb = comb.clone();
        tokio::spawn(async move {
            let t = Instant::now();
            let id = comb.create(request(spec("python"))).await.unwrap().id;
            (id, t.elapsed())
        })
    }))
    .await;
    let wall = t.elapsed();
    let (ids, mut times): (Vec<CellId>, Vec<Duration>) =
        made.into_iter().map(Result::unwrap).unzip();
    times.sort();
    println!(
        "{cells} at once: all running in {wall:?}, {:.0} cells/s, create p50 {:?} p99 {:?}",
        cells as f64 / wall.as_secs_f64(),
        times[cells / 2],
        times[cells * 99 / 100]
    );
    let t = Instant::now();
    futures::future::join_all(ids.into_iter().map(|id| {
        let comb = comb.clone();
        tokio::spawn(async move { comb.stop(id, None).await.unwrap() })
    }))
    .await;
    println!("{cells} stopped at once in {:?}", t.elapsed());
    comb.shutdown().await;
}
