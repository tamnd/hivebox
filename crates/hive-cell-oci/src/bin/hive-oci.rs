//! The `hive-oci` binary, for what the container driver does outside the comb.
//!
//! ```text
//! hive-oci worker
//! hive-oci import DIR [--base 1000000] [--count 65536] < rootfs.tar
//! ```
//!
//! `worker` runs one container worker on stdin and stdout, as the comb does with `--oci-worker`.
//! `import` unpacks a root filesystem, such as the output of `docker export`, into `DIR` with
//! every owner shifted to the cells' id range.

#![forbid(unsafe_code)]

use std::process::ExitCode;

#[cfg(target_os = "linux")]
fn main() -> ExitCode {
    const USAGE: &str =
        "usage: hive-oci worker | hive-oci import DIR [--base N] [--count N] < rootfs.tar";
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("worker") if args.len() == 1 => hive_cell_oci::worker::main(),
        Some("import") if args.len() >= 2 => {
            let (mut base, mut count) = (1_000_000u32, 65_536u32);
            let mut rest = args[2..].iter();
            while let Some(flag) = rest.next() {
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
            match hive_cell_oci::import(std::io::stdin().lock(), dir, base, count) {
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
