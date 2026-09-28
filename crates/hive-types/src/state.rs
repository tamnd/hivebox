//! The cell state machine from `spec/05_api_sdk.md`, section 3.
//!
//! The node agent owns every transition and writes each one to its WAL before acting on it. This
//! module is the single answer to whether a transition is allowed, so that the WAL replay, the
//! lifecycle actor and the simulator cannot disagree about it.

use std::fmt;

/// Where a cell is in its life.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum CellState {
    /// Accepted by the gate and waiting for placement.
    Pending,
    /// Admitted by a node. The root filesystem is being attached and a pool slot taken.
    Preparing,
    /// The driver is starting or restoring the cell and waiting for the guest agent's handshake.
    Starting,
    /// Serving requests.
    Running,
    /// A pause was asked for, explicitly or by the idle timer, and is in progress.
    Pausing,
    /// Not running, with memory and disk kept so that it can be resumed.
    Paused,
    /// Being torn down.
    Stopping,
    /// Stopped, on request or because the workload exited.
    Stopped,
    /// Ended by a failure, in hivebox or in the workload.
    Failed,
    /// Ended because it reached its hard time to live.
    Expired,
}

impl CellState {
    /// Every state, in the order a cell normally passes through them.
    pub const ALL: [Self; 10] = [
        Self::Pending,
        Self::Preparing,
        Self::Starting,
        Self::Running,
        Self::Pausing,
        Self::Paused,
        Self::Stopping,
        Self::Stopped,
        Self::Failed,
        Self::Expired,
    ];

    /// A terminal state has no way out. A fork or a restore makes a new cell rather than reviving
    /// an old one.
    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Stopped | Self::Failed | Self::Expired)
    }

    /// Whether the node agent may move a cell from `self` to `next`.
    ///
    /// Any state that is not terminal can fail or be stopped, because a node can lose a driver
    /// and a user can cancel at any point. The other edges are the ones drawn in the spec.
    #[must_use]
    pub const fn can_become(self, next: Self) -> bool {
        use CellState::*;
        if self.is_terminal() {
            return false;
        }
        match next {
            Failed => true,
            Stopping => !matches!(self, Stopping),
            _ => matches!(
                (self, next),
                (Pending, Preparing)
                    | (Preparing, Starting)
                    | (Starting, Running)
                    | (Running, Pausing)
                    | (Pausing, Paused)
                    // The pause did not take, and the cell is still running.
                    | (Pausing, Running)
                    | (Paused, Running)
                    | (Stopping, Stopped)
                    | (Stopping, Expired)
            ),
        }
    }

    /// The name used on the wire and in logs.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Preparing => "PREPARING",
            Self::Starting => "STARTING",
            Self::Running => "RUNNING",
            Self::Pausing => "PAUSING",
            Self::Paused => "PAUSED",
            Self::Stopping => "STOPPING",
            Self::Stopped => "STOPPED",
            Self::Failed => "FAILED",
            Self::Expired => "EXPIRED",
        }
    }
}

impl fmt::Display for CellState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::CellState::{self, *};

    #[test]
    fn the_normal_life_is_allowed() {
        let path =
            [Pending, Preparing, Starting, Running, Pausing, Paused, Running, Stopping, Stopped];
        for pair in path.windows(2) {
            assert!(pair[0].can_become(pair[1]), "{} to {}", pair[0], pair[1]);
        }
    }

    #[test]
    fn terminal_states_go_nowhere() {
        for from in CellState::ALL.into_iter().filter(|s| s.is_terminal()) {
            for to in CellState::ALL {
                assert!(!from.can_become(to), "{from} to {to}");
            }
        }
    }

    #[test]
    fn every_live_state_can_fail_and_can_be_stopped() {
        for from in CellState::ALL.into_iter().filter(|s| !s.is_terminal()) {
            assert!(from.can_become(Failed), "{from}");
            assert!(from.can_become(Stopping) || from == Stopping, "{from}");
        }
    }

    #[test]
    fn shortcuts_are_refused() {
        assert!(!Pending.can_become(Running));
        assert!(!Running.can_become(Paused));
        assert!(!Paused.can_become(Pausing));
        assert!(!Running.can_become(Stopped));
        assert!(!Running.can_become(Expired));
        assert!(!Running.can_become(Running));
    }

    #[test]
    fn a_pause_that_did_not_take_goes_back_to_running() {
        assert!(Pausing.can_become(Running));
    }

    #[test]
    fn only_three_states_are_terminal() {
        let terminal: Vec<_> = CellState::ALL.into_iter().filter(|s| s.is_terminal()).collect();
        assert_eq!(terminal, [Stopped, Failed, Expired]);
    }
}
