use super::{Level, LsmState, SSTable};
use crate::{config::CompactionConfig, error::Result};

use super::iterator::{KvIterator, MergeIterator};

/// Check if compaction is needed
pub fn needs_compaction(state: &LsmState, config: &CompactionConfig) -> bool {
    // Don't run if already running
    if !state.needs_compaction() {
        return false;
    }

    // Use the actual compaction decision logic
    find_compaction_level(state, config).is_some()
}

/// Determines which level, if any, requires compaction based on a tiered strategy.
///
/// Compaction is triggered based on a hierarchy of rules:
/// 1. **L0 Compaction:** The highest priority. Triggered if L0 has too many tables,
///    as these tables can have overlapping key ranges and slow down reads.
/// 2. **Size Ratio Compaction (L1+):** Triggered for a level `N` if its total size is
///    significantly larger (by a configurable ratio) than the next level `N+1`. This
///    keeps the size of levels growing exponentially, which is the goal of tiered compaction.
/// 3. **Last Level Compaction:** If a level is the final one, it's compacted only if it
///    accumulates too many individual table files, as there's no "next level" to compare its size against.
pub fn find_compaction_level(state: &LsmState, config: &CompactionConfig) -> Option<u32> {
    let levels = state.levels.read().unwrap();

    // Rule 1: L0 is the highest priority, triggered by the number of tables.
    if !levels.is_empty() && levels[0].table_count() > config.level0_compaction_threshold {
        return Some(0);
    }

    // Rules 2 & 3: Check higher levels for size-ratio or last-level table count triggers.
    for (level_idx, current_level) in levels.iter().enumerate().skip(1) {
        // First, a level must have a minimum number of tables to be considered for compaction.
        // This avoids compacting levels that are mostly empty or have just been compacted.
        if current_level.table_count() < config.max_tables_per_level {
            continue;
        }

        let level_num = level_idx as u32;

        // Rule 3: Handle the special case for the last level and return immediately if it matches.
        let is_last_level = level_idx == levels.len() - 1;
        if is_last_level {
            return Some(level_num);
        }

        // Rule 2: If we are here, it's an intermediate level. Check the size ratio.
        let next_level = &levels[level_idx + 1];
        let current_size = current_level.size();
        let next_size = next_level.size().max(1);

        let is_oversized = current_size / next_size >= config.size_ratio_threshold as u64;
        if is_oversized {
            return Some(level_num);
        }
    }
    None
}

/// Perform tiered compaction if needed
pub async fn compact(store: &super::LsmStore) -> Result<()> {
    let state = &store.state;
    let config = &store.config;
    let _guard = state.start_compaction();

    // Find which level needs compaction
    let source_level = match find_compaction_level(state, &config.compaction) {
        Some(level) => level,
        None => return Ok(()), // No compaction needed
    };

    let target_level = source_level + 1;

    // Get source tables to compact
    let source_tables = {
        let levels = state.levels.read().unwrap();
        if (source_level as usize) >= levels.len() {
            return Ok(());
        }
        levels[source_level as usize].all_tables()
    };

    tracing::info!(
        source_level = source_level,
        target_level = target_level,
        source_tables = source_tables.len(),
        "Starting tiered compaction"
    );

    // 1. Create iterators for all source tables
    let mut iterators = Vec::new();
    let source_table_ids: Vec<u64> = source_tables.iter().map(|t| t.id).collect();

    {
        let levels = state.levels.read().unwrap();
        for table_meta in &source_tables {
            let sstable = levels[source_level as usize]
                .sstables
                .iter()
                .find(|s| s.id == table_meta.id)
                .ok_or_else(|| crate::Error::InvalidOperation("SSTable not found".to_string()))?;

            let scan_iter = sstable.table.scan(..)?;
            let boxed_iter: KvIterator = Box::new(scan_iter);
            iterators.push(boxed_iter);
        }
    }

    // 2. Create merge iterator and write to new SSTable
    let merge_iter = MergeIterator::new(iterators);

    let (table_id, table_path) = store.next_sstable_path();
    let mut writable_table = super::sstable::table::Table::writable(table_path.to_str().unwrap())?;

    let mut builder = super::sstable::block::Builder::new();
    let mut first_key_in_block: Option<Vec<u8>> = None;
    let mut entry_count = 0;
    let mut min_key: Option<Vec<u8>> = None;
    let mut max_key: Option<Vec<u8>> = None;

    for entry_result in merge_iter {
        let (key, value) = entry_result?;

        // Track min/max keys
        if min_key.is_none() {
            min_key = Some(key.clone());
        }
        max_key = Some(key.clone());

        if first_key_in_block.is_none() {
            first_key_in_block = Some(key.clone());
        }

        builder.add_entry(&key, &value);
        entry_count += 1;

        // Flush block when it gets large enough
        if builder.len() >= super::sstable::table::MAX_BLOCK_SIZE {
            let block_data = builder.finish();
            let first_key = first_key_in_block.take().unwrap();
            writable_table.add_block(&block_data, first_key)?;
            builder = super::sstable::block::Builder::new();
        }
    }

    // Write final block if not empty
    if !builder.is_empty() {
        let block_data = builder.finish();
        let first_key = first_key_in_block.take().unwrap();
        writable_table.add_block(&block_data, first_key)?;
    }

    let table = writable_table.finalize()?;

    // 3. Create table metadata
    let table_meta = super::manifest::meta::TableMeta {
        id: table_id,
        level: target_level,
        size: std::fs::metadata(&table_path)?.len(),
        entry_count,
        min_key: min_key.unwrap_or_default(),
        max_key: max_key.unwrap_or_default(),
    };

    // 4. Update manifest with compaction record
    let job_id = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64;

    {
        #[allow(clippy::readonly_write_lock)] // next_seq() mutates the manifest
        let manifest = state.manifest.write().unwrap();
        let begin_seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::BeginCompaction {
            seq: begin_seq,
            job_id,
            source_level,
            target_level,
        })?;

        let commit_seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::CommitCompaction {
            seq: commit_seq,
            job_id,
            source_level,
            deleted_tables: source_table_ids.clone(),
            target_level,
            added_tables: vec![table_meta.clone()],
        })?;
        manifest.sync()?;
    }

    // 5. Update levels data structure
    {
        let mut levels = state.levels.write().unwrap();

        // Remove source tables from source level
        if (source_level as usize) < levels.len() {
            levels[source_level as usize]
                .sstables
                .retain(|t| !source_table_ids.contains(&t.id));
        }

        // Ensure target level exists
        while levels.len() <= target_level as usize {
            let level_num = levels.len() as u32;
            levels.push(Level::new(level_num));
        }

        // Add new table to target level
        let sstable = SSTable::new(table_path, table, &table_meta)?;
        levels[target_level as usize].add_sstable(sstable);
    }

    // 6. Delete old SSTable files
    for table_id in &source_table_ids {
        let old_path = store.sstable_path(*table_id);
        if let Err(e) = std::fs::remove_file(&old_path) {
            tracing::warn!(table_id = table_id, error = %e, "Failed to delete old SSTable file");
        }
    }

    tracing::info!(
        source_level = source_level,
        target_level = target_level,
        source_tables = source_table_ids.len(),
        target_table = table_id,
        entries_compacted = entry_count,
        "Completed tiered compaction"
    );

    // Validate state consistency after compaction
    if let Err(e) = store.state.validate_consistency() {
        tracing::warn!("State inconsistency detected after compaction: {:?}", e);
    }

    // Debug-only comprehensive validation
    #[cfg(debug_assertions)]
    {
        if let Err(e) = store.state.validate_sstable_id_uniqueness() {
            tracing::error!("SSTable ID uniqueness violation after compaction: {:?}", e);
        }

        if let Err(e) = store.state.validate_level_key_ordering() {
            tracing::error!("Level key ordering violation after compaction: {:?}", e);
        }
    }

    Ok(())
}
