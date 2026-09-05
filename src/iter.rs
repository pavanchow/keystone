//! K-way merge that yields the newest version of each user key.
//!
//! Every source is sorted ascending by key and holds at most one entry per
//! key. When several sources carry the same key the highest seqno wins, which
//! is how a read sees the most recent write across memtable and all levels.

use crate::error::Result;
use crate::types::Entry;

type Source = Box<dyn Iterator<Item = Result<Entry>>>;

/// Merges several sorted entry sources, deduping by key on newest seqno.
pub struct MergeIterator {
    sources: Vec<Source>,
    heads: Vec<Option<Entry>>,
    done: bool,
}

impl MergeIterator {
    /// Build a merge over the given sources.
    pub fn new(sources: Vec<Source>) -> Result<Self> {
        let mut heads = Vec::with_capacity(sources.len());
        let mut sources = sources;
        for s in sources.iter_mut() {
            heads.push(match s.next() {
                Some(Ok(e)) => Some(e),
                Some(Err(e)) => return Err(e),
                None => None,
            });
        }
        Ok(MergeIterator {
            sources,
            heads,
            done: false,
        })
    }

    fn advance(&mut self, i: usize) -> Result<()> {
        self.heads[i] = match self.sources[i].next() {
            Some(Ok(e)) => Some(e),
            Some(Err(e)) => return Err(e),
            None => None,
        };
        Ok(())
    }
}

impl Iterator for MergeIterator {
    type Item = Result<Entry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        // Smallest key currently at any head.
        let mut min_key: Option<Vec<u8>> = None;
        for h in self.heads.iter().flatten() {
            if min_key.as_ref().map(|m| &h.key < m).unwrap_or(true) {
                min_key = Some(h.key.clone());
            }
        }
        let min_key = match min_key {
            Some(k) => k,
            None => {
                self.done = true;
                return None;
            }
        };

        // Among all heads at that key, pick the newest and consume them all.
        let mut winner: Option<Entry> = None;
        let mut to_advance = Vec::new();
        for (i, h) in self.heads.iter().enumerate() {
            if let Some(e) = h {
                if e.key == min_key {
                    to_advance.push(i);
                    if winner.as_ref().map(|w| e.seqno > w.seqno).unwrap_or(true) {
                        winner = Some(e.clone());
                    }
                }
            }
        }
        for i in to_advance {
            if let Err(e) = self.advance(i) {
                self.done = true;
                return Some(Err(e));
            }
        }
        winner.map(Ok)
    }
}
