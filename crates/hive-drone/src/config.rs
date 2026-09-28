use std::path::{Path, PathBuf};
use std::time::Duration;

/// How the drone runs commands. The defaults suit a cell image with a shell at `/bin/sh`.
#[derive(Clone, Debug)]
pub struct Config {
    /// Runs commands given as a string, with `-c`.
    pub shell: PathBuf,
    /// The shell for sessions that do not name one. Bash is started with `--noprofile --norc`.
    pub session_shell: PathBuf,
    /// Where commands run when they do not say.
    pub workdir: PathBuf,
    /// The environment every command starts with. Nothing is inherited from the drone.
    pub base_env: Vec<(String, String)>,
    /// The user commands run as when they do not say. `None` keeps the drone's.
    pub uid: Option<u32>,
    /// The group commands run as when they do not say. `None` keeps the drone's.
    pub gid: Option<u32>,
    /// How long a command may run when it does not say.
    pub default_timeout: Duration,
    /// Output kept per stream when a command does not say.
    pub default_output: usize,
    /// The most output kept per stream, whatever a command asks for.
    pub max_output: usize,
    /// Reported in health answers and the handshake.
    pub build: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            shell: "/bin/sh".into(),
            session_shell: if Path::new("/bin/bash").exists() { "/bin/bash" } else { "/bin/sh" }
                .into(),
            workdir: "/".into(),
            base_env: vec![
                (
                    "PATH".into(),
                    "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".into(),
                ),
                ("HOME".into(), "/root".into()),
                ("LANG".into(), "C.UTF-8".into()),
            ],
            uid: None,
            gid: None,
            default_timeout: Duration::from_secs(600),
            default_output: 1 << 20,
            max_output: 64 << 20,
            build: concat!("hive-drone ", env!("CARGO_PKG_VERSION")).into(),
        }
    }
}

impl Config {
    pub(crate) fn timeout(&self, ms: u64) -> Duration {
        if ms == 0 { self.default_timeout } else { Duration::from_millis(ms) }
    }

    pub(crate) fn output_limit(&self, asked: u64) -> usize {
        if asked == 0 {
            return self.default_output.min(self.max_output);
        }
        usize::try_from(asked).unwrap_or(usize::MAX).min(self.max_output)
    }
}
