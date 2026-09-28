//! The stable error reasons from `spec/05_api_sdk.md`, section 2, and the causes a cell can end
//! with.
//!
//! The one property that matters most here is `is_infra`. A trainer masks every sample whose
//! error is the platform's fault and keeps every sample whose error is the policy's fault, so a
//! reason that lands on the wrong side of that line silently biases a training run. That is why
//! the split is written out per reason and tested, rather than derived from the gRPC code.

use std::fmt;

/// Why a request failed. The names are part of the API and never change meaning.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum Reason {
    /// The project is out of quota. Retry after backoff.
    QuotaExceeded,
    /// No node could take the cell right now.
    CapacityUnavailable,
    /// No such cell. It never existed, or it ended longer ago than terminal states are kept.
    CellNotFound,
    /// The cell existed and hivebox lost it, usually with its node.
    CellLost,
    /// The cell is not running and the call needs it to be.
    CellNotRunning,
    /// A command ran past its timeout.
    ExecTimeout,
    /// Output passed its cap. The call succeeded with `truncated` set.
    OutputLimit,
    /// Policy forbids this.
    PolicyDenied,
    /// The request itself is malformed.
    InvalidArgument,
    /// The image could not be fetched or mounted.
    ImageUnavailable,
    /// The guest agent did not answer.
    DroneUnreachable,
    /// A file operation in the cell failed, like reading a path that does not exist. The error's
    /// `errno` names the cause.
    FileError,
    /// A bug or an unexpected failure inside hivebox.
    Internal,
}

/// The gRPC status code a reason is carried under. Kept here rather than in `hive-proto` so that
/// the table in the spec has exactly one copy in code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Code {
    /// `OK`
    Ok,
    /// `INVALID_ARGUMENT`
    InvalidArgument,
    /// `DEADLINE_EXCEEDED`
    DeadlineExceeded,
    /// `NOT_FOUND`
    NotFound,
    /// `PERMISSION_DENIED`
    PermissionDenied,
    /// `RESOURCE_EXHAUSTED`
    ResourceExhausted,
    /// `FAILED_PRECONDITION`
    FailedPrecondition,
    /// `INTERNAL`
    Internal,
    /// `UNAVAILABLE`
    Unavailable,
}

impl Reason {
    /// Every reason.
    pub const ALL: [Self; 13] = [
        Self::QuotaExceeded,
        Self::CapacityUnavailable,
        Self::CellNotFound,
        Self::CellLost,
        Self::CellNotRunning,
        Self::ExecTimeout,
        Self::OutputLimit,
        Self::PolicyDenied,
        Self::InvalidArgument,
        Self::ImageUnavailable,
        Self::DroneUnreachable,
        Self::FileError,
        Self::Internal,
    ];

    /// The wire name, in the `reason` field of `google.rpc.ErrorInfo`.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::QuotaExceeded => "QUOTA_EXCEEDED",
            Self::CapacityUnavailable => "CAPACITY_UNAVAILABLE",
            Self::CellNotFound => "CELL_NOT_FOUND",
            Self::CellLost => "CELL_LOST",
            Self::CellNotRunning => "CELL_NOT_RUNNING",
            Self::ExecTimeout => "EXEC_TIMEOUT",
            Self::OutputLimit => "OUTPUT_LIMIT",
            Self::PolicyDenied => "POLICY_DENIED",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::ImageUnavailable => "IMAGE_UNAVAILABLE",
            Self::DroneUnreachable => "DRONE_UNREACHABLE",
            Self::FileError => "FILE_ERROR",
            Self::Internal => "INTERNAL",
        }
    }

    /// The reason with this wire name.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|r| r.as_str() == name)
    }

    /// The gRPC code it travels under.
    #[must_use]
    pub const fn code(self) -> Code {
        match self {
            Self::QuotaExceeded => Code::ResourceExhausted,
            Self::CapacityUnavailable | Self::ImageUnavailable | Self::DroneUnreachable => {
                Code::Unavailable
            }
            Self::CellNotFound | Self::CellLost => Code::NotFound,
            Self::CellNotRunning | Self::FileError => Code::FailedPrecondition,
            Self::ExecTimeout => Code::DeadlineExceeded,
            Self::OutputLimit => Code::Ok,
            Self::PolicyDenied => Code::PermissionDenied,
            Self::InvalidArgument => Code::InvalidArgument,
            Self::Internal => Code::Internal,
        }
    }

    /// Whether the platform is to blame, so that a trainer should mask the sample rather than
    /// score it.
    #[must_use]
    pub const fn is_infra(self) -> bool {
        matches!(
            self,
            Self::CapacityUnavailable
                | Self::CellLost
                | Self::ImageUnavailable
                | Self::DroneUnreachable
                | Self::Internal
        )
    }

    /// Whether the SDK may retry on its own. It only does so for idempotent calls.
    #[must_use]
    pub const fn is_retryable(self) -> bool {
        matches!(
            self,
            Self::QuotaExceeded
                | Self::CapacityUnavailable
                | Self::ImageUnavailable
                | Self::DroneUnreachable
                | Self::Internal
        )
    }
}

impl fmt::Display for Reason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A failure with its reason and a message for a human. The message is never parsed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Error {
    /// What kind of failure.
    pub reason: Reason,
    /// What happened, for the log and the caller.
    pub message: String,
    /// For [`Reason::FileError`], the name of the Linux errno behind it, like `ENOENT`. SDKs use
    /// it to raise the error their language has for that case.
    pub errno: Option<String>,
}

impl Error {
    /// A failure for `reason`.
    pub fn new(reason: Reason, message: impl Into<String>) -> Self {
        Self { reason, message: message.into(), errno: None }
    }

    /// A [`Reason::FileError`] caused by the errno named `errno`.
    pub fn file(errno: impl Into<String>, message: impl Into<String>) -> Self {
        Self { reason: Reason::FileError, message: message.into(), errno: Some(errno.into()) }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.reason, self.message)
    }
}

impl std::error::Error for Error {}

/// Why a cell reached a terminal state. Recorded in the WAL with the transition.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "serde", serde(rename_all = "snake_case"))]
pub enum Cause {
    /// Someone asked for it to stop.
    Requested,
    /// The idle timer fired with `IdleAction::Stop`.
    Idle,
    /// It reached its hard time to live.
    HardTtl,
    /// The workload's main process exited.
    Exited,
    /// The cell ran out of memory. The workload's fault, not an infra error.
    Oom,
    /// Stopped by a policy decision, such as quarantine.
    Policy,
    /// Failed to start: no pool slot, no image, or the driver refused.
    StartFailed,
    /// The guest agent never finished its handshake, or stopped answering.
    DroneLost,
    /// Its node was lost or its epoch fenced.
    NodeLost,
    /// The node agent found it half made or broken while recovering after a restart.
    Recovery,
}

impl Cause {
    /// Whether the platform is to blame. The same split as [`Reason::is_infra`].
    #[must_use]
    pub const fn is_infra(self) -> bool {
        matches!(self, Self::StartFailed | Self::DroneLost | Self::NodeLost | Self::Recovery)
    }

    /// The name used on the wire and in metrics.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Requested => "requested",
            Self::Idle => "idle",
            Self::HardTtl => "hard_ttl",
            Self::Exited => "exited",
            Self::Oom => "oom",
            Self::Policy => "policy",
            Self::StartFailed => "start_failed",
            Self::DroneLost => "drone_lost",
            Self::NodeLost => "node_lost",
            Self::Recovery => "recovery",
        }
    }
}

impl fmt::Display for Cause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_infra_split_matches_the_spec_table() {
        let infra: Vec<_> = Reason::ALL.into_iter().filter(|r| r.is_infra()).collect();
        assert_eq!(
            infra,
            [
                Reason::CapacityUnavailable,
                Reason::CellLost,
                Reason::ImageUnavailable,
                Reason::DroneUnreachable,
                Reason::Internal
            ]
        );
        assert!(!Reason::QuotaExceeded.is_infra());
        assert!(!Reason::ExecTimeout.is_infra());
        assert!(!Cause::Oom.is_infra());
        assert!(Cause::NodeLost.is_infra());
    }

    #[test]
    fn wire_names_round_trip_and_are_unique() {
        for r in Reason::ALL {
            assert_eq!(Reason::from_name(r.as_str()), Some(r));
        }
        let mut names: Vec<_> = Reason::ALL.map(Reason::as_str).to_vec();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Reason::ALL.len());
    }

    #[test]
    fn only_idempotent_safe_reasons_retry() {
        assert!(!Reason::PolicyDenied.is_retryable());
        assert!(!Reason::CellLost.is_retryable());
        assert!(Reason::DroneUnreachable.is_retryable());
        assert_eq!(Reason::OutputLimit.code(), Code::Ok);
    }
}
