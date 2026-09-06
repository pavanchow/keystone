//! LEB128 variable length integer encoding plus length prefixed byte helpers.

use crate::error::{Error, Result};

/// Append `value` to `out` as an unsigned LEB128 varint.
pub fn encode_u64(value: u64, out: &mut Vec<u8>) {
    let mut v = value;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            break;
        }
    }
}

/// Decode an unsigned LEB128 varint starting at `buf[*pos]`, advancing `pos`.
pub fn decode_u64(buf: &[u8], pos: &mut usize) -> Result<u64> {
    let mut result: u64 = 0;
    let mut shift: u32 = 0;
    loop {
        let byte = *buf
            .get(*pos)
            .ok_or_else(|| Error::corruption("varint truncated"))?;
        *pos += 1;
        if shift >= 64 {
            return Err(Error::corruption("varint too long"));
        }
        // The final byte for a full u64 only carries a single valid bit.
        if shift == 63 && (byte & 0x7e) != 0 {
            return Err(Error::corruption("varint overflow"));
        }
        result |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }
    Ok(result)
}

/// Number of bytes `value` occupies when varint encoded.
#[must_use]
pub fn encoded_len(value: u64) -> usize {
    let mut v = value;
    let mut n = 1;
    while v >= 0x80 {
        v >>= 7;
        n += 1;
    }
    n
}

/// Append a length prefixed byte slice (varint length then bytes).
pub fn encode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    encode_u64(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// Decode a length prefixed byte slice starting at `buf[*pos]`, advancing `pos`.
pub fn decode_bytes<'a>(buf: &'a [u8], pos: &mut usize) -> Result<&'a [u8]> {
    let len = decode_u64(buf, pos)? as usize;
    let start = *pos;
    let end = start
        .checked_add(len)
        .ok_or_else(|| Error::corruption("length overflow"))?;
    if end > buf.len() {
        return Err(Error::corruption("length prefixed slice truncated"));
    }
    *pos = end;
    Ok(&buf[start..end])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip_values() {
        let cases = [
            0u64,
            1,
            127,
            128,
            300,
            16384,
            u64::from(u32::MAX),
            u64::MAX / 2,
            u64::MAX - 1,
            u64::MAX,
        ];
        for &v in &cases {
            let mut buf = Vec::new();
            encode_u64(v, &mut buf);
            assert_eq!(buf.len(), encoded_len(v), "len mismatch for {v}");
            let mut pos = 0;
            let got = decode_u64(&buf, &mut pos).unwrap();
            assert_eq!(got, v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn sequential_encode_decode() {
        let mut buf = Vec::new();
        for v in 0u64..2000 {
            encode_u64(v.wrapping_mul(2_654_435_761), &mut buf);
        }
        let mut pos = 0;
        for v in 0u64..2000 {
            assert_eq!(decode_u64(&buf, &mut pos).unwrap(), v.wrapping_mul(2_654_435_761));
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn truncated_is_error() {
        let buf = [0x80u8, 0x80];
        let mut pos = 0;
        assert!(decode_u64(&buf, &mut pos).is_err());
    }

    #[test]
    fn overflow_is_error() {
        let buf = [0xffu8, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02];
        let mut pos = 0;
        assert!(decode_u64(&buf, &mut pos).is_err());
    }

    #[test]
    fn bytes_round_trip() {
        let payloads: [&[u8]; 3] = [b"", b"hello", &[0u8; 500]];
        let mut buf = Vec::new();
        for p in &payloads {
            encode_bytes(p, &mut buf);
        }
        let mut pos = 0;
        for p in &payloads {
            assert_eq!(decode_bytes(&buf, &mut pos).unwrap(), *p);
        }
        assert_eq!(pos, buf.len());
    }

    #[test]
    fn bytes_truncated_is_error() {
        let mut buf = Vec::new();
        encode_bytes(b"abcdef", &mut buf);
        buf.truncate(buf.len() - 2);
        let mut pos = 0;
        assert!(decode_bytes(&buf, &mut pos).is_err());
    }
}
