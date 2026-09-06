//! Leveled compaction driving tables from L0 downward.
//!
//! L0 tables come straight from memtable flushes and may overlap in key range.
//! Once L0 reaches `l0_compaction_trigger` files they merge with the
//! overlapping L1 tables into fresh non-overlapping L1 tables. For deeper
//! levels a single oversized table merges with the overlapping tables one level
//! down. Tombstones and shadowed versions are dropped only when the output is
//! the bottom-most populated level.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::Result;
use crate::iter::MergeIterator;
use crate::manifest::{Manifest, TableMeta};
use crate::options::Options;
use crate::sstable::{SsTableReader, SsTableWriter};
use crate::types::{Entry, ValueType};

/// Path of the `SSTable` file for `id` inside `dir`.
#[must_use]
pub fn sst_path(dir: &Path, id: u64) -> PathBuf {
    dir.join(format!("{id:06}.sst"))
}

fn ranges_overlap(a_lo: &[u8], a_hi: &[u8], b_lo: &[u8], b_hi: &[u8]) -> bool {
    a_lo <= b_hi && b_lo <= a_hi
}

fn target_file_size(opts: &Options) -> u64 {
    (opts.memtable_size_bytes as u64).max(opts.block_size as u64 * 4)
}

fn level_max_bytes(opts: &Options, level: u32) -> u64 {
    // L1 gets a base budget, each deeper level grows by the multiplier.
    let base = (opts.memtable_size_bytes as u64 * opts.l0_compaction_trigger as u64).max(1);
    let mut budget = base;
    for _ in 1..level {
        budget = budget.saturating_mul(opts.level_size_multiplier);
    }
    budget
}

fn combined_range(metas: &[&TableMeta]) -> Option<(Vec<u8>, Vec<u8>)> {
    let mut lo: Option<Vec<u8>> = None;
    let mut hi: Option<Vec<u8>> = None;
    for m in metas {
        if lo.as_ref().is_none_or(|l| &m.smallest_key < l) {
            lo = Some(m.smallest_key.clone());
        }
        if hi.as_ref().is_none_or(|h| &m.largest_key > h) {
            hi = Some(m.largest_key.clone());
        }
    }
    Some((lo?, hi?))
}

/// Run every pending compaction until the shape is stable.
pub fn compact(dir: &Path, manifest: &mut Manifest, opts: &Options) -> Result<()> {
    // The cap only guards against a logic bug looping forever.
    for _ in 0..10_000 {
        if !compact_once(dir, manifest, opts)? {
            return Ok(());
        }
    }
    Ok(())
}

/// Perform a single compaction step if one is due. Returns true if it ran.
pub fn compact_once(dir: &Path, manifest: &mut Manifest, opts: &Options) -> Result<bool> {
    // L0 to L1 by file count.
    let l0: Vec<&TableMeta> = manifest.tables_at(0);
    if l0.len() >= opts.l0_compaction_trigger {
        run_l0_compaction(dir, manifest, opts)?;
        return Ok(true);
    }

    // Deeper levels by size budget.
    let max_level = manifest.max_level();
    for level in 1..=max_level {
        let tables = manifest.tables_at(level);
        let total: u64 = tables.iter().map(|t| t.file_size).sum();
        if total > level_max_bytes(opts, level) && !tables.is_empty() {
            run_level_compaction(dir, manifest, opts, level)?;
            return Ok(true);
        }
    }
    Ok(false)
}

fn run_l0_compaction(dir: &Path, manifest: &mut Manifest, opts: &Options) -> Result<()> {
    let l0_ids: Vec<u64> = manifest
        .tables_at(0)
        .iter()
        .map(|t| t.file_id)
        .collect();
    let l0_metas: Vec<TableMeta> = manifest
        .tables
        .iter()
        .filter(|t| l0_ids.contains(&t.file_id))
        .cloned()
        .collect();
    let l0_refs: Vec<&TableMeta> = l0_metas.iter().collect();
    let (lo, hi) = combined_range(&l0_refs).expect("l0 non-empty");

    let l1_overlap: Vec<TableMeta> = manifest
        .tables_at(1)
        .into_iter()
        .filter(|t| ranges_overlap(&lo, &hi, &t.smallest_key, &t.largest_key))
        .cloned()
        .collect();

    let mut inputs = l0_metas;
    inputs.extend(l1_overlap);
    let input_ids: Vec<u64> = inputs.iter().map(|t| t.file_id).collect();

    let is_bottom = !manifest
        .tables
        .iter()
        .any(|t| t.level > 1 && !input_ids.contains(&t.file_id));

    let new_tables = merge_inputs(dir, manifest, opts, &inputs, 1, is_bottom)?;
    commit(dir, manifest, &input_ids, new_tables)?;
    Ok(())
}

fn run_level_compaction(
    dir: &Path,
    manifest: &mut Manifest,
    opts: &Options,
    level: u32,
) -> Result<()> {
    // Pick the table with the smallest key for determinism.
    let mut level_tables: Vec<TableMeta> = manifest
        .tables_at(level)
        .into_iter()
        .cloned()
        .collect();
    level_tables.sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
    let chosen = level_tables[0].clone();

    let next = level + 1;
    let next_overlap: Vec<TableMeta> = manifest
        .tables_at(next)
        .into_iter()
        .filter(|t| {
            ranges_overlap(
                &chosen.smallest_key,
                &chosen.largest_key,
                &t.smallest_key,
                &t.largest_key,
            )
        })
        .cloned()
        .collect();

    let mut inputs = vec![chosen];
    inputs.extend(next_overlap);
    let input_ids: Vec<u64> = inputs.iter().map(|t| t.file_id).collect();

    let is_bottom = !manifest
        .tables
        .iter()
        .any(|t| t.level > next && !input_ids.contains(&t.file_id));

    let new_tables = merge_inputs(dir, manifest, opts, &inputs, next, is_bottom)?;
    commit(dir, manifest, &input_ids, new_tables)?;
    Ok(())
}

fn merge_inputs(
    dir: &Path,
    manifest: &mut Manifest,
    opts: &Options,
    inputs: &[TableMeta],
    output_level: u32,
    drop_tombstones: bool,
) -> Result<Vec<TableMeta>> {
    let mut sources: Vec<Box<dyn Iterator<Item = Result<Entry>>>> = Vec::new();
    // Keep readers alive for the lifetime of their iterators.
    let mut readers = Vec::new();
    for meta in inputs {
        let reader = SsTableReader::open(&sst_path(dir, meta.file_id))?;
        readers.push(reader);
    }
    for reader in &readers {
        sources.push(Box::new(reader.iter()?));
    }
    let merged = MergeIterator::new(sources)?;

    let target = target_file_size(opts);
    let mut new_tables = Vec::new();
    let mut writer: Option<(u64, SsTableWriter)> = None;
    let mut written: u64 = 0;

    for item in merged {
        let e = item?;
        if drop_tombstones && e.kind == ValueType::Delete {
            continue;
        }
        if writer.is_none() {
            let id = manifest.next_file_id;
            manifest.next_file_id += 1;
            let w = SsTableWriter::create(
                &sst_path(dir, id),
                opts.block_size,
                opts.bloom_bits_per_key,
            )?;
            writer = Some((id, w));
            written = 0;
        }
        let approx = (e.key.len() + e.value.len() + 16) as u64;
        {
            let (_, w) = writer.as_mut().unwrap();
            w.add(&e)?;
        }
        written += approx;
        if written >= target {
            let (id, w) = writer.take().unwrap();
            let stats = w.finish()?;
            new_tables.push(meta_from_stats(output_level, id, stats));
        }
    }
    if let Some((id, w)) = writer.take() {
        let stats = w.finish()?;
        new_tables.push(meta_from_stats(output_level, id, stats));
    }
    Ok(new_tables)
}

fn meta_from_stats(level: u32, id: u64, s: crate::sstable::TableStats) -> TableMeta {
    TableMeta {
        level,
        file_id: id,
        smallest_key: s.smallest_key,
        largest_key: s.largest_key,
        smallest_seqno: s.smallest_seqno,
        largest_seqno: s.largest_seqno,
        file_size: s.file_size,
    }
}

fn commit(
    dir: &Path,
    manifest: &mut Manifest,
    removed_ids: &[u64],
    new_tables: Vec<TableMeta>,
) -> Result<()> {
    manifest
        .tables
        .retain(|t| !removed_ids.contains(&t.file_id));
    manifest.tables.extend(new_tables);
    manifest.save(dir)?;
    // Only unlink old files after the new manifest is durable.
    for id in removed_ids {
        let _ = fs::remove_file(sst_path(dir, *id));
    }
    Ok(())
}
