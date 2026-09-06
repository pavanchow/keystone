//! A tiny xorshift64* PRNG for deterministic tests and CLI workloads.

/// Deterministic pseudo random generator seeded by a u64.
#[derive(Clone)]
pub struct Rng {
    state: u64,
}

impl Rng {
    /// Create a generator from `seed` (zero is remapped to a nonzero state).
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Rng {
            state: if seed == 0 { 0x9E37_79B9_7F4A_7C15 } else { seed },
        }
    }

    /// Next 64-bit value.
    pub fn next_u64(&mut self) -> u64 {
        let mut x = self.state;
        x ^= x >> 12;
        x ^= x << 25;
        x ^= x >> 27;
        self.state = x;
        x.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    /// Uniform value in `[0, n)` for `n > 0`.
    pub fn below(&mut self, n: u64) -> u64 {
        self.next_u64() % n
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deterministic_for_seed() {
        let a: Vec<u64> = { let mut r = Rng::new(7); (0..5).map(|_| r.next_u64()).collect() };
        let b: Vec<u64> = { let mut r = Rng::new(7); (0..5).map(|_| r.next_u64()).collect() };
        assert_eq!(a, b);
    }

    #[test]
    fn different_seeds_differ() {
        let mut r1 = Rng::new(1);
        let mut r2 = Rng::new(2);
        assert_ne!(r1.next_u64(), r2.next_u64());
    }
}
