//! The OCI runtime config a cell's container starts from.

use hive_types::CellSpec;
use serde_json::{Value, json};
use std::path::Path;

/// Where the drone binary shows up inside every container.
pub(crate) const DRONE: &str = "/.hive/drone";

/// The PATH a command gets when the cell does not set one. Most images set this one anyway.
const PATH: &str = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin";

/// Docker's default capabilities, which is what images are written to expect.
const CAPS: [&str; 14] = [
    "CAP_CHOWN",
    "CAP_DAC_OVERRIDE",
    "CAP_FSETID",
    "CAP_FOWNER",
    "CAP_MKNOD",
    "CAP_NET_RAW",
    "CAP_SETGID",
    "CAP_SETUID",
    "CAP_SETFCAP",
    "CAP_SETPCAP",
    "CAP_NET_BIND_SERVICE",
    "CAP_SYS_CHROOT",
    "CAP_KILL",
    "CAP_AUDIT_WRITE",
];

/// Kernel files a cell has no business reading. The last three are from DSec, where reading them
/// from a container took a host down.
const MASKED: [&str; 14] = [
    "/proc/acpi",
    "/proc/asound",
    "/proc/interrupts",
    "/proc/kcore",
    "/proc/keys",
    "/proc/latency_stats",
    "/proc/timer_list",
    "/proc/sched_debug",
    "/proc/scsi",
    "/sys/firmware",
    "/sys/devices/virtual/powercap",
    "/proc/kpagecgroup",
    "/proc/kpageflags",
    "/sys/kernel/debug",
];

const READ_ONLY: [&str; 5] =
    ["/proc/bus", "/proc/fs", "/proc/irq", "/proc/sys", "/proc/sysrq-trigger"];

/// What the config needs beyond the cell's spec.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Host<'a> {
    /// The drone binary on the host.
    pub(crate) drone: &'a Path,
    /// The cell's cgroup, as a path under `/sys/fs/cgroup`.
    pub(crate) cgroup: &'a Path,
    /// The first host id that root in the cell maps to.
    pub(crate) uid_base: u32,
    /// How many ids the cell has.
    pub(crate) uid_count: u32,
}

/// The whole `config.json` for a cell.
pub(crate) fn config(spec: &CellSpec, host: &Host<'_>) -> Value {
    let mut args = vec![
        DRONE.to_string(),
        "--init".into(),
        "--listen".into(),
        "fd:3".into(),
        "--secret-stdin".into(),
        "--harden".into(),
    ];
    let mut env = vec![("PATH", PATH), ("HOME", "/root"), ("LANG", "C.UTF-8")];
    env.retain(|(k, _)| !spec.env.contains_key(*k));
    for (k, v) in env.into_iter().chain(spec.env.iter().map(|(k, v)| (k.as_str(), v.as_str()))) {
        args.push("--env".into());
        args.push(format!("{k}={v}"));
    }
    // No network namespace here: the worker joins the cell's before it makes the container, so
    // the container starts in it. Joining it from inside the new user namespace is refused, since
    // the host's user namespace owns it, and that same ownership keeps the cell from changing its
    // own routes or firewall.
    let namespaces = [
        json!({"type": "pid"}),
        json!({"type": "ipc"}),
        json!({"type": "uts"}),
        json!({"type": "mount"}),
        json!({"type": "cgroup"}),
        json!({"type": "user"}),
    ];
    let map = [json!({"containerID": 0, "hostID": host.uid_base, "size": host.uid_count})];
    let files = u64::from(spec.resources.open_files.max(64));
    json!({
        "ociVersion": "1.0.2",
        "root": {"path": "rootfs", "readonly": false},
        "hostname": "cell",
        "process": {
            "terminal": false,
            "user": {"uid": 0, "gid": 0},
            "args": args,
            // The drone builds each command's environment itself, from its --env list.
            "env": [format!("PATH={PATH}")],
            "cwd": "/",
            "capabilities": {"bounding": CAPS, "effective": CAPS, "permitted": CAPS},
            "rlimits": [{"type": "RLIMIT_NOFILE", "hard": files, "soft": files}],
            "noNewPrivileges": true,
        },
        "mounts": [
            {"destination": "/proc", "type": "proc", "source": "proc", "options": ["nosuid", "noexec", "nodev"]},
            {"destination": "/dev", "type": "tmpfs", "source": "tmpfs", "options": ["nosuid", "strictatime", "mode=755", "size=65536k"]},
            {"destination": "/dev/pts", "type": "devpts", "source": "devpts", "options": ["nosuid", "noexec", "newinstance", "ptmxmode=0666", "mode=0620", "gid=5"]},
            {"destination": "/dev/shm", "type": "tmpfs", "source": "shm", "options": ["nosuid", "noexec", "nodev", "mode=1777", "size=65536k"]},
            {"destination": "/dev/mqueue", "type": "mqueue", "source": "mqueue", "options": ["nosuid", "noexec", "nodev"]},
            // A fresh sysfs needs a network namespace the cell's user namespace owns, and the
            // comb makes those outside it, so this is the host's, read only.
            {"destination": "/sys", "type": "none", "source": "/sys", "options": ["rbind", "nosuid", "noexec", "nodev", "ro"]},
            {"destination": "/sys/fs/cgroup", "type": "cgroup", "source": "cgroup", "options": ["nosuid", "noexec", "nodev", "relatime", "ro"]},
            {"destination": DRONE, "type": "bind", "source": host.drone, "options": ["bind", "ro", "nosuid", "nodev"]},
        ],
        "linux": {
            "namespaces": namespaces,
            "uidMappings": map,
            "gidMappings": map,
            "cgroupsPath": host.cgroup,
            "maskedPaths": MASKED,
            "readonlyPaths": READ_ONLY,
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use hive_types::{Backend, Source};

    fn host() -> Host<'static> {
        Host {
            drone: Path::new("/opt/hive/drone"),
            cgroup: Path::new("/hive.slice/std.slice/c1"),
            uid_base: 1_000_000,
            uid_count: 65536,
        }
    }

    #[test]
    fn the_drone_is_pid_one_with_the_cell_env() {
        let mut spec = CellSpec::new(Source::Image("python".into()), Backend::Container);
        spec.env.insert("PATH".into(), "/opt/bin".into());
        spec.env.insert("A".into(), "b c".into());
        let c = config(&spec, &host());
        let args: Vec<&str> =
            c["process"]["args"].as_array().unwrap().iter().map(|a| a.as_str().unwrap()).collect();
        assert_eq!(&args[..6], [DRONE, "--init", "--listen", "fd:3", "--secret-stdin", "--harden"]);
        let env: Vec<&str> = args[6..].chunks(2).map(|p| p[1]).collect();
        assert_eq!(env, ["HOME=/root", "LANG=C.UTF-8", "A=b c", "PATH=/opt/bin"]);
    }

    #[test]
    fn the_cell_is_mapped_and_joins_its_cgroup() {
        let spec = CellSpec::new(Source::Image("python".into()), Backend::Container);
        let c = config(&spec, &host());
        let l = &c["linux"];
        assert_eq!(l["uidMappings"][0]["hostID"], 1_000_000);
        assert_eq!(l["gidMappings"][0]["size"], 65536);
        assert_eq!(l["cgroupsPath"], "/hive.slice/std.slice/c1");
        let ns = l["namespaces"].as_array().unwrap();
        assert_eq!(ns.len(), 6);
        assert!(ns.iter().all(|n| n["type"] != "network"));
        let files = u64::from(spec.resources.open_files);
        assert_eq!(c["process"]["rlimits"][0]["hard"], files);
    }
}
