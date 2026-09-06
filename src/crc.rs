//! Table based CRC32 using the IEEE reflected polynomial 0xEDB88320.

const POLY: u32 = 0xEDB8_8320;

struct Table([u32; 256]);

impl Table {
    const fn new() -> Self {
        let mut table = [0u32; 256];
        let mut i = 0;
        while i < 256 {
            let mut crc = i as u32;
            let mut j = 0;
            while j < 8 {
                if crc & 1 == 1 {
                    crc = (crc >> 1) ^ POLY;
                } else {
                    crc >>= 1;
                }
                j += 1;
            }
            table[i] = crc;
            i += 1;
        }
        Table(table)
    }
}

static TABLE: Table = Table::new();

/// Compute the CRC32 (IEEE) checksum of `data`.
#[must_use]
pub fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &b in data {
        let idx = ((crc ^ u32::from(b)) & 0xff) as usize;
        crc = (crc >> 8) ^ TABLE.0[idx];
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vectors() {
        assert_eq!(crc32(b""), 0x0000_0000);
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32(b"The quick brown fox jumps over the lazy dog"), 0x414F_A339);
    }

    #[test]
    fn incremental_differs() {
        assert_ne!(crc32(b"a"), crc32(b"b"));
        assert_ne!(crc32(b"abc"), crc32(b"abd"));
    }
}
