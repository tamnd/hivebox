//! Conversions between `hive-types` and the `hivebox.v1` messages.
//!
//! Requests come in with zero values for anything the caller left out, so turning a request into
//! a `hive-types` value fills in defaults and then validates. Anything that cannot be turned into
//! a valid value becomes an `INVALID_ARGUMENT` error naming the field.

use crate::v1;
use hive_types::{
    Backend, Cause, CellSpec, CellState, Code, Error, IdleAction, Limits, Qos, Reason, Resources,
    Source,
};
use std::time::Duration;
use tonic_types::{ErrorDetails, StatusExt};

/// The domain in the `ErrorInfo` detail of every error status.
pub const ERROR_DOMAIN: &str = "hivebox.dev";

fn invalid(message: impl Into<String>) -> Error {
    Error::new(Reason::InvalidArgument, message)
}

/// A backend on the wire. Unspecified reads as `AUTO`.
#[must_use]
pub fn backend_from_v1(b: v1::Backend) -> Backend {
    match b {
        v1::Backend::Fncall => Backend::Fncall,
        v1::Backend::Container => Backend::Container,
        v1::Backend::Microvm => Backend::Microvm,
        v1::Backend::Fullvm => Backend::Fullvm,
        v1::Backend::Auto | v1::Backend::Unspecified => Backend::Auto,
    }
}

/// A backend for the wire.
#[must_use]
pub fn backend_to_v1(b: Backend) -> v1::Backend {
    match b {
        Backend::Fncall => v1::Backend::Fncall,
        Backend::Container => v1::Backend::Container,
        Backend::Microvm => v1::Backend::Microvm,
        Backend::Fullvm => v1::Backend::Fullvm,
        Backend::Auto => v1::Backend::Auto,
    }
}

/// A QoS class on the wire. Unspecified reads as standard.
#[must_use]
pub fn qos_from_v1(q: v1::Qos) -> Qos {
    match q {
        v1::Qos::Latency => Qos::Latency,
        v1::Qos::BestEffort => Qos::BestEffort,
        v1::Qos::Standard | v1::Qos::Unspecified => Qos::Standard,
    }
}

/// A QoS class for the wire.
#[must_use]
pub fn qos_to_v1(q: Qos) -> v1::Qos {
    match q {
        Qos::Latency => v1::Qos::Latency,
        Qos::Standard => v1::Qos::Standard,
        Qos::BestEffort => v1::Qos::BestEffort,
    }
}

/// A cell state for the wire.
#[must_use]
pub fn state_to_v1(s: CellState) -> v1::CellState {
    match s {
        CellState::Pending => v1::CellState::Pending,
        CellState::Preparing => v1::CellState::Preparing,
        CellState::Starting => v1::CellState::Starting,
        CellState::Running => v1::CellState::Running,
        CellState::Pausing => v1::CellState::Pausing,
        CellState::Paused => v1::CellState::Paused,
        CellState::Stopping => v1::CellState::Stopping,
        CellState::Stopped => v1::CellState::Stopped,
        CellState::Failed => v1::CellState::Failed,
        CellState::Expired => v1::CellState::Expired,
    }
}

/// A cell state from the wire, or `None` for unspecified.
#[must_use]
pub fn state_from_v1(s: v1::CellState) -> Option<CellState> {
    CellState::ALL.into_iter().find(|&c| state_to_v1(c) == s)
}

/// A cause for the wire.
#[must_use]
pub fn cause_to_v1(c: Cause) -> v1::Cause {
    match c {
        Cause::Requested => v1::Cause::Requested,
        Cause::Idle => v1::Cause::Idle,
        Cause::HardTtl => v1::Cause::HardTtl,
        Cause::Exited => v1::Cause::Exited,
        Cause::Oom => v1::Cause::Oom,
        Cause::Policy => v1::Cause::Policy,
        Cause::StartFailed => v1::Cause::StartFailed,
        Cause::DroneLost => v1::Cause::DroneLost,
        Cause::NodeLost => v1::Cause::NodeLost,
        Cause::Recovery => v1::Cause::Recovery,
    }
}

/// A cause from the wire. `None` for `CAUSE_UNSPECIFIED`.
#[must_use]
pub fn cause_from_v1(c: v1::Cause) -> Option<Cause> {
    Cause::ALL.into_iter().find(|&k| cause_to_v1(k) == c)
}

fn duration_from_v1(
    d: Option<prost_types::Duration>,
    field: &str,
) -> Result<Option<Duration>, Error> {
    match d {
        None => Ok(None),
        Some(d) => Duration::try_from(d)
            .map(Some)
            .map_err(|_| invalid(format!("{field} must be a non negative duration"))),
    }
}

/// A duration for the wire. Durations past what protobuf holds are clamped.
#[must_use]
pub fn duration_to_v1(d: Duration) -> prost_types::Duration {
    prost_types::Duration::try_from(d)
        .unwrap_or(prost_types::Duration { seconds: 315_576_000_000, nanos: 0 })
}

/// A cell spec from a request, with defaults filled in and validated.
///
/// # Errors
///
/// `INVALID_ARGUMENT` when there is no source, a duration is negative, or the result fails
/// [`CellSpec::validate`].
pub fn spec_from_v1(mut v: v1::CellSpec) -> Result<CellSpec, Error> {
    let source = match v.source.take() {
        Some(v1::cell_spec::Source::Template(t)) => Source::Template(t),
        Some(v1::cell_spec::Source::Image(i)) => Source::Image(i.r#ref),
        Some(v1::cell_spec::Source::Snapshot(s)) => Source::Snapshot(s.id),
        None => return Err(invalid("the spec needs a template, an image or a snapshot")),
    };
    let backend = backend_from_v1(v.backend());
    let qos = qos_from_v1(v.qos());
    let idle_action = match v.idle_action() {
        v1::IdleAction::Stop => IdleAction::Stop,
        v1::IdleAction::Pause | v1::IdleAction::Unspecified => IdleAction::Pause,
    };
    let mut spec = CellSpec::new(source, backend);
    spec.qos = qos;
    spec.idle_action = idle_action;
    if let Some(r) = v.resources {
        let d = Resources::default();
        let pick = |got: u32, default: u32| if got == 0 { default } else { got };
        spec.resources = Resources {
            vcpu_milli: pick(r.vcpu_milli, d.vcpu_milli),
            mem_mib: pick(r.mem_mib, d.mem_mib),
            disk_gib: pick(r.disk_gib, d.disk_gib),
            pids: pick(r.pids, d.pids),
            open_files: pick(r.open_files, d.open_files),
        };
    }
    if !v.network_profile.is_empty() {
        spec.network_profile = v.network_profile;
    }
    spec.idle_ttl = duration_from_v1(v.idle_ttl, "idle_ttl")?;
    spec.hard_ttl = duration_from_v1(v.hard_ttl, "hard_ttl")?;
    spec.labels = v.labels.into_iter().collect();
    spec.env = v.env.into_iter().collect();
    if let Some(l) = v.limits {
        let d = Limits::default();
        spec.limits = Limits {
            output_bytes: if l.output_bytes == 0 { d.output_bytes } else { l.output_bytes },
            wall_time: duration_from_v1(l.wall_time, "limits.wall_time")?
                .filter(|w| !w.is_zero())
                .unwrap_or(d.wall_time),
        };
    }
    spec.trusted_image = v.trusted_image;
    spec.validate().map_err(|e| invalid(e.to_string()))?;
    Ok(spec)
}

/// A cell spec for the wire.
#[must_use]
pub fn spec_to_v1(s: &CellSpec) -> v1::CellSpec {
    let source = match &s.source {
        Source::Template(t) => v1::cell_spec::Source::Template(t.clone()),
        Source::Image(i) => v1::cell_spec::Source::Image(v1::ImageRef { r#ref: i.clone() }),
        Source::Snapshot(id) => v1::cell_spec::Source::Snapshot(v1::SnapshotRef { id: id.clone() }),
    };
    let r = &s.resources;
    v1::CellSpec {
        source: Some(source),
        backend: backend_to_v1(s.backend).into(),
        resources: Some(v1::Resources {
            vcpu_milli: r.vcpu_milli,
            mem_mib: r.mem_mib,
            disk_gib: r.disk_gib,
            pids: r.pids,
            open_files: r.open_files,
        }),
        qos: qos_to_v1(s.qos).into(),
        network_profile: s.network_profile.clone(),
        idle_ttl: s.idle_ttl.map(duration_to_v1),
        idle_action: match s.idle_action {
            IdleAction::Pause => v1::IdleAction::Pause,
            IdleAction::Stop => v1::IdleAction::Stop,
        }
        .into(),
        hard_ttl: s.hard_ttl.map(duration_to_v1),
        labels: s.labels.clone().into_iter().collect(),
        env: s.env.clone().into_iter().collect(),
        limits: Some(v1::Limits {
            output_bytes: s.limits.output_bytes,
            wall_time: Some(duration_to_v1(s.limits.wall_time)),
        }),
        trusted_image: s.trusted_image,
        checkpoint: None,
    }
}

/// An error as one item of a batch result.
#[must_use]
pub fn error_to_v1(e: &Error) -> v1::Error {
    v1::Error {
        reason: e.reason.as_str().to_string(),
        message: e.message.clone(),
        is_infra_error: e.reason.is_infra(),
        retryable: e.reason.is_retryable(),
        errno: e.errno.clone().unwrap_or_default(),
    }
}

fn code_to_tonic(c: Code) -> tonic::Code {
    match c {
        Code::Ok => tonic::Code::Ok,
        Code::InvalidArgument => tonic::Code::InvalidArgument,
        Code::DeadlineExceeded => tonic::Code::DeadlineExceeded,
        Code::NotFound => tonic::Code::NotFound,
        Code::PermissionDenied => tonic::Code::PermissionDenied,
        Code::ResourceExhausted => tonic::Code::ResourceExhausted,
        Code::FailedPrecondition => tonic::Code::FailedPrecondition,
        Code::Internal => tonic::Code::Internal,
        Code::Unavailable => tonic::Code::Unavailable,
    }
}

/// An error as a gRPC status: the reason's code, with an `ErrorInfo` detail carrying the reason,
/// the domain, `is_infra_error` and `errno` when there is one.
#[must_use]
pub fn error_to_status(e: &Error) -> tonic::Status {
    let mut metadata: std::collections::HashMap<String, String> =
        [("is_infra_error".to_string(), e.reason.is_infra().to_string())].into_iter().collect();
    if let Some(errno) = &e.errno {
        metadata.insert("errno".to_string(), errno.clone());
    }
    let details = ErrorDetails::with_error_info(e.reason.as_str(), ERROR_DOMAIN, metadata);
    tonic::Status::with_error_details(code_to_tonic(e.reason.code()), e.message.clone(), details)
}

/// The error a status carries. A status from something other than hivebox, or with a reason this
/// build does not know, reads as `INTERNAL` with the status message kept.
#[must_use]
pub fn error_from_status(s: &tonic::Status) -> Error {
    let details = s.get_error_details();
    let info = details.error_info().filter(|i| i.domain == ERROR_DOMAIN);
    let reason = info.and_then(|i| Reason::from_name(&i.reason)).unwrap_or(Reason::Internal);
    let mut e = Error::new(reason, s.message().to_string());
    e.errno = info.and_then(|i| i.metadata.get("errno").cloned());
    e
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn causes_round_trip() {
        for c in Cause::ALL {
            assert_eq!(cause_from_v1(cause_to_v1(c)), Some(c));
        }
        assert_eq!(cause_from_v1(v1::Cause::Unspecified), None);
    }

    #[test]
    fn a_bare_request_gets_the_defaults() {
        let v = v1::CellSpec {
            source: Some(v1::cell_spec::Source::Template("swe-py311".into())),
            ..Default::default()
        };
        let s = spec_from_v1(v).unwrap();
        assert_eq!(s, CellSpec::new(Source::Template("swe-py311".into()), Backend::Auto));
    }

    #[test]
    fn a_full_spec_survives_the_round_trip() {
        let mut s = CellSpec::new(
            Source::Image("docker.io/library/python:3.12".into()),
            Backend::Container,
        );
        s.qos = Qos::BestEffort;
        s.idle_action = IdleAction::Stop;
        s.idle_ttl = Some(Duration::from_secs(600));
        s.hard_ttl = Some(Duration::from_millis(3_600_500));
        s.resources.vcpu_milli = 250;
        s.labels.insert("step".into(), "412".into());
        s.env.insert("PYTHONUNBUFFERED".into(), "1".into());
        s.network_profile = "mirrors".into();
        s.trusted_image = true;
        assert_eq!(spec_from_v1(spec_to_v1(&s)).unwrap(), s);
    }

    #[test]
    fn bad_specs_are_invalid_arguments() {
        assert_eq!(
            spec_from_v1(v1::CellSpec::default()).unwrap_err().reason,
            Reason::InvalidArgument
        );
        let v = v1::CellSpec {
            source: Some(v1::cell_spec::Source::Template("t".into())),
            idle_ttl: Some(prost_types::Duration { seconds: -1, nanos: 0 }),
            ..Default::default()
        };
        assert!(spec_from_v1(v).unwrap_err().message.contains("idle_ttl"));
        let v = v1::CellSpec {
            source: Some(v1::cell_spec::Source::Template("t".into())),
            resources: Some(v1::Resources { vcpu_milli: 1, ..Default::default() }),
            ..Default::default()
        };
        assert_eq!(spec_from_v1(v).unwrap_err().reason, Reason::InvalidArgument);
    }

    #[test]
    fn every_reason_survives_a_status() {
        for r in Reason::ALL {
            let e = Error::new(r, "boom");
            let back = error_from_status(&error_to_status(&e));
            assert_eq!(back, e);
        }
        assert_eq!(error_from_status(&tonic::Status::unavailable("x")).reason, Reason::Internal);
        let e = Error::file("ENOENT", "no such file");
        assert_eq!(error_from_status(&error_to_status(&e)), e);
        assert_eq!(error_to_v1(&e).errno, "ENOENT");
    }

    #[test]
    fn every_state_maps_both_ways() {
        for s in CellState::ALL {
            assert_eq!(state_from_v1(state_to_v1(s)), Some(s));
        }
        assert_eq!(state_from_v1(v1::CellState::Unspecified), None);
    }
}
