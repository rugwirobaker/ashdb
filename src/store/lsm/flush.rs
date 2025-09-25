use super::{Level, LsmState, SSTable};
use crate::{config::LsmConfig, error::Result};

/// Manually flush oldest frozen memtable to SSTable (for testing)
pub async fn flush_memtable(state: &LsmState, config: &LsmConfig) -> Result<bool> {
    // Get oldest frozen memtable
    let memtable = {
        let mut frozen = state.frozen_memtables.write().unwrap();
        match frozen.pop_front() {
            Some(m) => m,
            None => return Ok(false), // Nothing to flush
        }
    };

    let wal_id = memtable.wal_id();

    // Create SSTable (I/O - no locks held)
    let table_id = state.next_sstable_id();
    let table_path = config.dir.join(format!("{}.sst", table_id));
    let mut sstable = super::sstable::table::Table::writable(table_path.to_str().unwrap())?;

    // Extract metadata during flush
    let (min_key, max_key, entry_count) = {
        let mut scan_iter = memtable.scan(..)?;
        let mut min_key: Option<Vec<u8>> = None;
        let mut max_key: Option<Vec<u8>> = None;
        let mut count = 0;

        if let Some(Ok((key, _))) = scan_iter.next() {
            min_key = Some(key.clone());
            max_key = Some(key);
            count = 1;

            // Continue iterating to get max key and count
            for entry_result in scan_iter {
                let (key, _) = entry_result?;
                max_key = Some(key);
                count += 1;
            }
        }

        (
            min_key.unwrap_or_default(),
            max_key.unwrap_or_default(),
            count,
        )
    };

    memtable.flush(&mut sstable)?;
    let table = sstable.finalize()?;

    let table_meta = super::manifest::meta::TableMeta {
        id: table_id,
        level: 0,
        size: std::fs::metadata(&table_path)?.len(),
        entry_count,
        min_key,
        max_key,
    };

    // Update manifest (I/O - no locks held)
    {
        #[allow(clippy::readonly_write_lock)] // next_seq() mutates the header
        let manifest = state.manifest.write().unwrap();
        let seq = manifest.next_seq();
        manifest.append(super::manifest::edit::VersionEdit::Flush {
            seq,
            table: table_meta.clone(),
            wal_id,
        })?;
        manifest.sync()?;
    }

    // Add to level 0
    {
        let mut levels = state.levels.write().unwrap();
        if levels.is_empty() {
            levels.push(Level::new(0));
        }
        levels[0].add_sstable(SSTable {
            id: table_id,
            table,
            path: table_path,
            size: table_meta.size,
            min_key: table_meta.min_key,
            max_key: table_meta.max_key,
        });
    }

    // Delete WAL file (optional cleanup)
    let wal_path = config.dir.join("wal").join(format!("{}.wal", wal_id));
    if let Err(e) = std::fs::remove_file(&wal_path) {
        tracing::warn!(wal_id = wal_id, error = %e, "Failed to delete WAL file");
    }

    tracing::info!(
        table_id = table_id,
        wal_id = wal_id,
        "Manually flushed memtable to SSTable"
    );

    Ok(true)
}
