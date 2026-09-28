//! Random numbers.
//!
//! Both sources use xoshiro256** for the fast path. The production one seeds a generator per
//! thread from the OS and reads secrets straight from the OS. The simulated one is a single
//! generator behind a lock, seeded once, so the sequence of draws is the same on every run with
//! the same seed and the same schedule.

use std::cell::RefCell;
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

/// A source of random numbers.
pub trait Rng: Send + Sync + fmt::Debug {
    /// 64 uniformly random bits.
    fn next_u64(&self) -> u64;

    /// Fills `buf` with bytes fit for keys and nonces.
    fn fill_secret(&self, buf: &mut [u8]);

    /// A 32 byte secret.
    fn secret(&self) -> [u8; 32] {
        let mut out = [0; 32];
        self.fill_secret(&mut out);
        out
    }

    /// A uniform number in `0..n`, without modulo bias. Returns 0 when `n` is 0.
    fn below(&self, n: u64) -> u64 {
        if n == 0 {
            return 0;
        }
        // Lemire's multiply and reject.
        let mut m = u128::from(self.next_u64()) * u128::from(n);
        if (m as u64) < n {
            let floor = n.wrapping_neg() % n;
            while (m as u64) < floor {
                m = u128::from(self.next_u64()) * u128::from(n);
            }
        }
        (m >> 64) as u64
    }

    /// `d` scaled by a uniform factor in `[1 - frac, 1 + frac]`, for spreading out retries and
    /// timers. `frac` is clamped to `0..=1`.
    fn jitter(&self, d: Duration, frac: f64) -> Duration {
        let frac = frac.clamp(0.0, 1.0);
        // 53 bits is all an f64 holds.
        let unit = (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64;
        d.mul_f64(1.0 - frac + 2.0 * frac * unit)
    }
}

#[derive(Clone)]
struct Xoshiro([u64; 4]);

impl Xoshiro {
    fn seeded(seed: u64) -> Self {
        // SplitMix64 spreads one word of seed into four, as the xoshiro authors recommend.
        let mut z = seed;
        let mut next = || {
            z = z.wrapping_add(0x9e37_79b9_7f4a_7c15);
            let mut x = z;
            x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
            x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
            x ^ (x >> 31)
        };
        Self([next(), next(), next(), next()])
    }

    fn next(&mut self) -> u64 {
        let s = &mut self.0;
        let out = s[1].wrapping_mul(5).rotate_left(7).wrapping_mul(9);
        let t = s[1] << 17;
        s[2] ^= s[0];
        s[3] ^= s[1];
        s[1] ^= s[2];
        s[0] ^= s[3];
        s[2] ^= t;
        s[3] = s[3].rotate_left(45);
        out
    }

    fn fill(&mut self, buf: &mut [u8]) {
        for chunk in buf.chunks_mut(8) {
            let bytes = self.next().to_le_bytes();
            chunk.copy_from_slice(&bytes[..chunk.len()]);
        }
    }
}

/// The production source.
#[derive(Clone, Copy, Debug, Default)]
pub struct OsRng;

thread_local! {
    static FAST: RefCell<Xoshiro> = RefCell::new(Xoshiro::seeded({
        let mut seed = [0u8; 8];
        os_fill(&mut seed);
        u64::from_le_bytes(seed)
    }));
}

fn os_fill(buf: &mut [u8]) {
    // Without an OS random source nothing about this system is safe, so there is no fallback.
    #[allow(clippy::expect_used)]
    getrandom::fill(buf).expect("the operating system has no random source");
}

impl Rng for OsRng {
    fn next_u64(&self) -> u64 {
        FAST.with(|r| r.borrow_mut().next())
    }

    fn fill_secret(&self, buf: &mut [u8]) {
        os_fill(buf);
    }
}

/// The simulated source. Clones share one generator.
#[derive(Clone)]
pub struct SimRng {
    seed: u64,
    inner: Arc<Mutex<Xoshiro>>,
}

impl SimRng {
    /// A generator whose whole sequence follows from `seed`.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self { seed, inner: Arc::new(Mutex::new(Xoshiro::seeded(seed))) }
    }

    /// The seed, for printing when a simulation fails so the run can be replayed.
    #[must_use]
    pub fn seed(&self) -> u64 {
        self.seed
    }
}

impl fmt::Debug for SimRng {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SimRng").field("seed", &self.seed).finish()
    }
}

impl Rng for SimRng {
    fn next_u64(&self) -> u64 {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).next()
    }

    fn fill_secret(&self, buf: &mut [u8]) {
        self.inner.lock().unwrap_or_else(PoisonError::into_inner).fill(buf);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_same_seed_gives_the_same_sequence() {
        let (a, b, c) = (SimRng::new(42), SimRng::new(42), SimRng::new(43));
        let xs: Vec<u64> = (0..100).map(|_| a.next_u64()).collect();
        let ys: Vec<u64> = (0..100).map(|_| b.next_u64()).collect();
        let zs: Vec<u64> = (0..100).map(|_| c.next_u64()).collect();
        assert_eq!(xs, ys);
        assert_ne!(xs, zs);
        assert_eq!(a.secret(), b.secret());
    }

    #[test]
    fn xoshiro_matches_the_reference_output() {
        // The first outputs of xoshiro256** from state [1, 2, 3, 4], from the reference C code.
        let mut x = Xoshiro([1, 2, 3, 4]);
        assert_eq!([x.next(), x.next(), x.next()], [11520, 0, 1509978240]);
    }

    #[test]
    fn below_stays_in_range_and_covers_it() {
        let r = SimRng::new(7);
        let mut seen = [0u32; 10];
        for _ in 0..100_000 {
            let v = r.below(10);
            seen[v as usize] += 1;
        }
        // Each bucket expects 10000. Five sigma is about 475.
        assert!(seen.iter().all(|&n| (9_500..10_500).contains(&n)), "{seen:?}");
        assert_eq!(r.below(0), 0);
        assert_eq!(r.below(1), 0);
        let big = u64::MAX / 3 * 2;
        assert!((0..1000).all(|_| r.below(big) < big));
    }

    #[test]
    fn jitter_stays_in_its_band() {
        let r = SimRng::new(1);
        let d = Duration::from_millis(1000);
        for _ in 0..10_000 {
            let j = r.jitter(d, 0.2);
            assert!(j >= Duration::from_millis(800) && j <= Duration::from_millis(1200));
        }
        assert_eq!(r.jitter(d, 0.0), d);
    }

    #[test]
    fn os_secrets_differ() {
        assert_ne!(OsRng.secret(), OsRng.secret());
        assert_ne!(OsRng.next_u64(), OsRng.next_u64());
    }
}
