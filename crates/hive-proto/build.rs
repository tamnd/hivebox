//! Compiles the `hivebox.v1` and `hivebox.internal.v1` protos. protox parses them in Rust, so building needs no protoc.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    const FILES: &[&str] = &[
        "proto/hivebox/v1/types.proto",
        "proto/hivebox/v1/cells.proto",
        "proto/hivebox/v1/exec.proto",
        "proto/hivebox/v1/files.proto",
        "proto/hivebox/v1/snapshots.proto",
        "proto/hivebox/v1/images.proto",
        "proto/hivebox/v1/verify.proto",
        "proto/hivebox/internal/v1/scout.proto",
    ];
    println!("cargo:rerun-if-changed=proto");
    let fds = protox::compile(FILES, ["proto"])?;
    tonic_prost_build::configure()
        // Output and file contents pass through without a copy.
        .bytes(".hivebox.v1")
        .build_transport(false)
        .compile_fds(fds)?;
    Ok(())
}
