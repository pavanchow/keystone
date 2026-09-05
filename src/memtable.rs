//! In-memory sorted table backed by a std BTreeMap.

use std::collections::BTreeMap;

use crate::types::{Entry, ValueType};

/// A sorted in-memory buffer of the most recent writes.
pub struct MemTable {
    // Only the newest version per user key is retained in memory.
    map: BTreeMap<Vec<u8>, (u64, Option<Vec<u8>>)>,
    approx_size: usize,
}

impl MemTable {
    /// Create an empty memtable.
    pub fn new() -> Self {
        MemTable {
            map: BTreeMap::new(),
            approx_size: 0,
        }
    }

    fn charge(key: &[u8], value: Option<&[u8]>) -> usize {
        // Rough per-entry accounting: keys, value, seqno and map overhead.
        key.len() + value.map(|v| v.len()).unwrap_or(0) + 24
    }

    /// Insert or overwrite a put for `key` at `seqno`.
    pub fn put(&mut self, key: &[u8], value: &[u8], seqno: u64) {
        self.approx_size += Self::charge(key, Some(value));
        if let Some(prev) = self
            .map
            .insert(key.to_vec(), (seqno, Some(value.to_vec())))
        {
            self.approx_size = self
                .approx_size
                .saturating_sub(Self::charge(key, prev.1.as_deref()));
        }
    }

    /// Insert a tombstone for `key` at `seqno`.
    pub fn delete(&mut self, key: &[u8], seqno: u64) {
        self.approx_size += Self::charge(key, None);
        if let Some(prev) = self.map.insert(key.to_vec(), (seqno, None)) {
            self.approx_size = self
                .approx_size
                .saturating_sub(Self::charge(key, prev.1.as_deref()));
        }
    }

    /// Look up the newest version of `key` held in memory.
    ///
    /// Returns `Some(None)` for a tombstone and `Some(Some(v))` for a value.
    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.map.get(key).map(|(_, v)| v.clone())
    }

    /// Approximate byte footprint used against the flush threshold.
    pub fn approx_size(&self) -> usize {
        self.approx_size
    }

    /// True when no writes are buffered.
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Number of distinct keys buffered.
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Iterate entries in ascending key order.
    pub fn iter(&self) -> impl Iterator<Item = Entry> + '_ {
        self.map.iter().map(|(k, (seq, v))| Entry {
            key: k.clone(),
            seqno: *seq,
            kind: if v.is_some() {
                ValueType::Put
            } else {
                ValueType::Delete
            },
            value: v.clone().unwrap_or_default(),
        })
    }
}

impl Default for MemTable {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overwrite_keeps_newest() {
        let mut m = MemTable::new();
        m.put(b"a", b"1", 1);
        m.put(b"a", b"2", 2);
        assert_eq!(m.get(b"a"), Some(Some(b"2".to_vec())));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn delete_is_tombstone() {
        let mut m = MemTable::new();
        m.put(b"a", b"1", 1);
        m.delete(b"a", 2);
        assert_eq!(m.get(b"a"), Some(None));
    }

    #[test]
    fn iter_is_sorted() {
        let mut m = MemTable::new();
        m.put(b"c", b"3", 3);
        m.put(b"a", b"1", 1);
        m.put(b"b", b"2", 2);
        let keys: Vec<Vec<u8>> = m.iter().map(|e| e.key).collect();
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
    }
}
