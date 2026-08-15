//! Deterministic splitmix64 pseudo-random source, shared by the query generator
//! and the mutator so both produce reproducible streams from a seed.

pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Self(seed)
    }

    pub(crate) fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Uniform value in `0..n` (returns 0 when `n == 0`).
    pub(crate) fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// True with probability `num / den`.
    pub(crate) fn chance(&mut self, num: u32, den: u32) -> bool {
        (self.next_u64() % u64::from(den)) < u64::from(num)
    }

    pub(crate) fn pick<'a, T>(&mut self, values: &'a [T]) -> &'a T {
        &values[self.below(values.len())]
    }

    /// A random non-empty subset of `values`, preserving order.
    pub(crate) fn subset<'a, T>(&mut self, values: &'a [T]) -> Vec<&'a T> {
        loop {
            let chosen: Vec<&T> = values.iter().filter(|_| self.chance(1, 2)).collect();
            if !chosen.is_empty() {
                return chosen;
            }
        }
    }
}
