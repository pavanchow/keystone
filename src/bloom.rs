//! Bloom filter with double hashing over two FNV-1a base hashes.

use crate::error::{Error, Result};

const FNV_OFFSET_A: u64 = 0xcbf2_9ce4_8422_2325;
const FNV_OFFSET_B: u64 = 0x1000_0000_0000_01b3;
const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

fn fnv1a(data: &[u8], seed: u64) -> u64 {
    let mut hash = seed;
    for &b in data {
        hash ^= u64::from(b);
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

/// A bloom filter that never reports a false negative.
#[derive(Clone)]
pub struct Bloom {
    bits: Vec<u8>,
    num_bits: u64,
    k: u32,
}

impl Bloom {
    /// Build a filter sized for `num_keys` at `bits_per_key`.
    #[must_use]
    pub fn new(num_keys: usize, bits_per_key: usize) -> Self {
        let mut num_bits = (num_keys * bits_per_key.max(1)) as u64;
        if num_bits < 64 {
            num_bits = 64;
        }
        // Optimal probe count k = bits_per_key * ln(2), clamped to a sane range.
        let k = ((bits_per_key as f64 * 0.69) as u32).clamp(1, 30);
        let num_bytes = num_bits.div_ceil(8);
        Bloom {
            bits: vec![0u8; num_bytes as usize],
            num_bits: num_bytes * 8,
            k,
        }
    }

    fn probes(&self, key: &[u8]) -> impl Iterator<Item = u64> + '_ {
        let h1 = fnv1a(key, FNV_OFFSET_A);
        let h2 = fnv1a(key, FNV_OFFSET_B) | 1;
        let num_bits = self.num_bits;
        (0..self.k).map(move |i| h1.wrapping_add(u64::from(i).wrapping_mul(h2)) % num_bits)
    }

    /// Record `key` as a member.
    pub fn add(&mut self, key: &[u8]) {
        let bits: Vec<u64> = self.probes(key).collect();
        for bit in bits {
            self.bits[(bit / 8) as usize] |= 1 << (bit % 8);
        }
    }

    /// Return true if `key` may be present, false if it is definitely absent.
    #[must_use]
    pub fn may_contain(&self, key: &[u8]) -> bool {
        for bit in self.probes(key) {
            if self.bits[(bit / 8) as usize] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    /// Serialize to bytes: [u64 `num_bits`][u32 k][bit bytes].
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(12 + self.bits.len());
        out.extend_from_slice(&self.num_bits.to_le_bytes());
        out.extend_from_slice(&self.k.to_le_bytes());
        out.extend_from_slice(&self.bits);
        out
    }

    /// Reconstruct a filter from `to_bytes` output.
    pub fn from_bytes(buf: &[u8]) -> Result<Self> {
        if buf.len() < 12 {
            return Err(Error::corruption("bloom header truncated"));
        }
        let num_bits = u64::from_le_bytes(buf[0..8].try_into().unwrap());
        let k = u32::from_le_bytes(buf[8..12].try_into().unwrap());
        let bits = buf[12..].to_vec();
        if num_bits != (bits.len() as u64) * 8 {
            return Err(Error::corruption("bloom bit length mismatch"));
        }
        // num_bits == 0 would make the probe modulo divide by zero; an
        // out-of-range probe count would waste unbounded work per lookup.
        if num_bits == 0 {
            return Err(Error::corruption("bloom has zero bits"));
        }
        if k == 0 || k > 64 {
            return Err(Error::corruption("bloom k out of range"));
        }
        Ok(Bloom { bits, num_bits, k })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let mut b = Bloom::new(1000, 10);
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| format!("key-{i}").into_bytes()).collect();
        for k in &keys {
            b.add(k);
        }
        for k in &keys {
            assert!(b.may_contain(k), "false negative for a member");
        }
    }

    #[test]
    fn false_positive_rate_reasonable() {
        let mut b = Bloom::new(2000, 10);
        for i in 0..2000u32 {
            b.add(format!("member-{i}").as_bytes());
        }
        let mut fp = 0;
        let trials = 10000;
        for i in 0..trials {
            if b.may_contain(format!("absent-{i}").as_bytes()) {
                fp += 1;
            }
        }
        let rate = f64::from(fp) / f64::from(trials);
        assert!(rate < 0.05, "false positive rate too high: {rate}");
    }

    #[test]
    fn zero_bits_header_rejected_not_panic() {
        // A crafted 12 byte header with num_bits == 0 must be rejected, never
        // accepted into a filter whose probe modulo would divide by zero.
        let mut buf = Vec::new();
        buf.extend_from_slice(&0u64.to_le_bytes());
        buf.extend_from_slice(&1u32.to_le_bytes());
        assert!(Bloom::from_bytes(&buf).is_err());
    }

    #[test]
    fn out_of_range_k_rejected() {
        let mut buf = Vec::new();
        buf.extend_from_slice(&64u64.to_le_bytes());
        buf.extend_from_slice(&9999u32.to_le_bytes());
        buf.extend_from_slice(&[0u8; 8]);
        assert!(Bloom::from_bytes(&buf).is_err());
    }

    #[test]
    fn serialize_round_trip() {
        let mut b = Bloom::new(500, 12);
        for i in 0..500u32 {
            b.add(format!("k{i}").as_bytes());
        }
        let bytes = b.to_bytes();
        let b2 = Bloom::from_bytes(&bytes).unwrap();
        for i in 0..500u32 {
            assert!(b2.may_contain(format!("k{i}").as_bytes()));
        }
    }
}
