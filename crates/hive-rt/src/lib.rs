//! Every service reads time, draws random numbers and opens connections through the traits in this crate. The production implementation is tokio. The simulation implementation is driven by `hive-sim`, and the reason this crate exists is that the second one has to be a drop-in for the first.
//!
//! The design is in `spec/13_observability_testing_bench.md`, section 2. A service takes an [`Rt`] and never calls `tokio::time`, `rand` or `tokio::net` directly. In production that is [`Rt::tokio`]. Under simulation it is [`Sim::rt`], where time only moves when the test says so, every random number comes from one seed, and the network is in memory with switches for taking addresses down.

#![forbid(unsafe_code)]

pub mod clock;
pub mod net;
pub mod rng;

pub use clock::{Clock, Elapsed, Instant, SimClock, Sleep, TokioClock, timeout};
pub use net::{Conn, Io, Listener, Net, SimNet, TokioNet};
pub use rng::{OsRng, Rng, SimRng};

use std::sync::Arc;

/// The clock, randomness and network a service runs on.
#[derive(Clone, Debug)]
pub struct Rt {
    /// Time and timers.
    pub clock: Arc<dyn Clock>,
    /// Random numbers.
    pub rng: Arc<dyn Rng>,
    /// Connections.
    pub net: Arc<dyn Net>,
}

impl Rt {
    /// The production runtime on tokio. It must be used from inside a tokio runtime with time and
    /// I/O enabled.
    #[must_use]
    pub fn tokio() -> Self {
        Self { clock: Arc::new(TokioClock), rng: Arc::new(OsRng), net: Arc::new(TokioNet) }
    }
}

/// A simulated world: one clock, one seeded random source and one in-memory network, shared by
/// every service the test starts in it.
#[derive(Clone, Debug)]
pub struct Sim {
    /// The clock. Time moves only through [`SimClock::advance`] and friends.
    pub clock: SimClock,
    /// The random source every service draws from.
    pub rng: SimRng,
    /// The network.
    pub net: SimNet,
}

impl Sim {
    /// A world whose every random choice follows from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { clock: SimClock::new(), rng: SimRng::new(seed), net: SimNet::new() }
    }

    /// A runtime for one service in this world.
    #[must_use]
    pub fn rt(&self) -> Rt {
        Rt {
            clock: Arc::new(self.clock.clone()),
            rng: Arc::new(self.rng.clone()),
            net: Arc::new(self.net.clone()),
        }
    }
}
