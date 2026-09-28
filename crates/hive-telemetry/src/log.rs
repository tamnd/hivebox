//! Log setup.

use tracing_subscriber::EnvFilter;

/// How log lines are written.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum LogFormat {
    /// One JSON object per line, for the log pipeline.
    #[default]
    Json,
    /// Plain text, for a terminal.
    Text,
}

/// Sends `tracing` events to stderr in `format`. The filter comes from `HIVE_LOG` in the usual
/// `target=level` syntax and defaults to `info`. Call it once at startup.
///
/// # Errors
///
/// If `HIVE_LOG` does not parse, or a subscriber is already installed.
pub fn init_logs(format: LogFormat) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let filter = match std::env::var("HIVE_LOG") {
        Ok(s) => EnvFilter::try_new(s)?,
        Err(_) => EnvFilter::new("info"),
    };
    let builder = tracing_subscriber::fmt().with_env_filter(filter).with_writer(std::io::stderr);
    match format {
        LogFormat::Json => builder.json().flatten_event(true).try_init(),
        LogFormat::Text => builder.try_init(),
    }
}
