//! Internal record types shared across the memtable, SSTables and iterators.

/// Whether a versioned record stores a value or marks a deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ValueType {
    /// A live value.
    Put = 0,
    /// A tombstone that shadows older values for the key.
    Delete = 1,
}

impl ValueType {
    /// Encode as the on-disk tag byte.
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    /// Decode from the on-disk tag byte.
    pub fn from_u8(v: u8) -> Option<ValueType> {
        match v {
            0 => Some(ValueType::Put),
            1 => Some(ValueType::Delete),
            _ => None,
        }
    }
}

/// A single versioned record as produced by merge iteration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Entry {
    /// The user key.
    pub key: Vec<u8>,
    /// The monotonic sequence number of the write.
    pub seqno: u64,
    /// Whether this is a put or a tombstone.
    pub kind: ValueType,
    /// The value bytes, empty for a tombstone.
    pub value: Vec<u8>,
}
