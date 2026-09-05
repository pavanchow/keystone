//! Tunable engine options with sane defaults and a chainable builder API.

/// Configuration for a Keystone database.
#[derive(Debug, Clone)]
pub struct Options {
    /// Flush the memtable to an L0 SSTable once it exceeds this many bytes.
    pub memtable_size_bytes: usize,
    /// Target size for a packed SSTable data block.
    pub block_size: usize,
    /// Bits allocated per key in each SSTable bloom filter.
    pub bloom_bits_per_key: usize,
    /// Number of L0 files that triggers a compaction into L1.
    pub l0_compaction_trigger: usize,
    /// Byte size multiplier between consecutive levels.
    pub level_size_multiplier: u64,
    /// If true, fsync the WAL on every write for maximum durability.
    pub sync_on_write: bool,
}

impl Default for Options {
    fn default() -> Self {
        Options {
            memtable_size_bytes: 4 * 1024 * 1024,
            block_size: 4 * 1024,
            bloom_bits_per_key: 10,
            l0_compaction_trigger: 4,
            level_size_multiplier: 10,
            sync_on_write: true,
        }
    }
}

impl Options {
    /// Start from the defaults.
    pub fn new() -> Self {
        Options::default()
    }

    /// Set the memtable flush threshold in bytes.
    pub fn memtable_size_bytes(mut self, v: usize) -> Self {
        self.memtable_size_bytes = v;
        self
    }

    /// Set the target SSTable data block size in bytes.
    pub fn block_size(mut self, v: usize) -> Self {
        self.block_size = v;
        self
    }

    /// Set bloom filter bits per key.
    pub fn bloom_bits_per_key(mut self, v: usize) -> Self {
        self.bloom_bits_per_key = v;
        self
    }

    /// Set the L0 file count that triggers compaction.
    pub fn l0_compaction_trigger(mut self, v: usize) -> Self {
        self.l0_compaction_trigger = v;
        self
    }

    /// Set the per level size multiplier.
    pub fn level_size_multiplier(mut self, v: u64) -> Self {
        self.level_size_multiplier = v;
        self
    }

    /// Set whether every write fsyncs the WAL.
    pub fn sync_on_write(mut self, v: bool) -> Self {
        self.sync_on_write = v;
        self
    }
}
