//! The `hive-oci` binary, for what the container driver does outside the comb.
//!
//! ```text
//! hive-oci worker
//! hive-oci import DIR [--base 1000000] [--count 65536] [--layer] < rootfs.tar
//! ```
//!
//! `worker` runs one container worker on stdin and stdout, as the comb does with `--oci-worker`.
//! `import` unpacks a root filesystem, such as the output of `docker export`, into `DIR` with
//! every owner shifted to the cells' id range. With `--layer` the stream is one OCI image layer,
//! applied on top of what `DIR` has already with its whiteouts, so an image can be imported layer
//! by layer straight from a registry.

#![forbid(unsafe_code)]

use std::process::ExitCode;

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    const USAGE: &str = "usage: hive-oci worker | hive-oci import DIR [--base N] [--count N] [--layer] < rootfs.tar";
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("worker") if args.len() == 1 => hive_cell_oci::worker::main(),
        Some("import") if args.len() >= 2 => {
            let (mut base, mut count, mut layer) = (1_000_000u32, 65_536u32, false);
            let mut rest = args[2..].iter();
            while let Some(flag) = rest.next() {
                if flag == "--layer" {
                    layer = true;
                    continue;
                }
                let value = rest.next().and_then(|v| v.parse().ok());
                match (flag.as_str(), value) {
                    ("--base", Some(v)) => base = v,
                    ("--count", Some(v)) => count = v,
                    _ => {
                        eprintln!("{USAGE}");
                        return ExitCode::from(2);
                    }
                }
            }
            let dir = std::path::Path::new(&args[1]);
            let started = std::time::Instant::now();
            let from = std::io::stdin().lock();
            let done = if layer {
                hive_cell_oci::import_layer(from, dir, base, count)
            } else {
                hive_cell_oci::import(from, dir, base, count)
            };
            match done {
                Ok(n) => {
                    eprintln!("hive-oci: {n} entries in {:?}", started.elapsed());
                    ExitCode::SUCCESS
                }
                Err(e) => {
                    eprintln!("hive-oci: importing into {}: {e}", dir.display());
                    ExitCode::FAILURE
                }
            }
        }
        _ => {
            eprintln!("{USAGE}");
            ExitCode::from(2)
        }
    }
}

#[cfg(not(target_os = "linux"))]
fn main() -> ExitCode {
    eprintln!("hive-oci runs only on Linux");
    ExitCode::FAILURE
}
