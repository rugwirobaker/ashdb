use std::{sync::Arc, time::Duration};

use crate::{
    error::Result,
    manifest::{edit::VersionEdit, meta::TableMeta},
    scheduler::{BackgroundTask, Context},
    sstable::table::Table,
    store::{level::SSTable, lsm::LsmStore},
};

pub struct FlushTask {
    store: Arc<LsmStore>,
}

impl FlushTask {
    pub fn new(store: Arc<LsmStore>) -> Self {
        Self { store }
    }

    async fn flush_oldest(&self) -> Result<()> {
        // Get oldest frozen memtable
        let memtable = {
            let mut frozen = self.store.state.frozen_memtables.write().unwrap();
            match frozen.pop_front() {
                Some(m) => m,
                None => return Ok(()), // Nothing to flush
            }
        };

        let wal_id = memtable.wal_id();

        // Create SSTable (I/O - no locks held)
        let table_id = self.store.state.next_sstable_id();
        let table_path = self.store.config.dir.join(format!("{}.sst", table_id));
        let mut sstable = Table::writable(table_path.to_str().unwrap())?;

        memtable.flush(&mut sstable)?;
        let table = sstable.finalize()?;

        let table_meta = TableMeta {
            id: table_id,
            level: 0,
            size: std::fs::metadata(&table_path)?.len(),
            entry_count: 0,  // TODO: get actual count from table
            min_key: vec![], // TODO: get from table
            max_key: vec![], // TODO: get from table
        };

        // Update manifest (I/O - no locks held)
        {
            // TODO: investigate a possibe antipattern. Supposed reads should not mutate underlying data.
            #[allow(clippy::readonly_write_lock)] // next_seq() mutates the header
            let manifest = self.store.state.manifest.write().unwrap();
            let seq = manifest.next_seq();
            manifest.append(VersionEdit::Flush {
                seq,
                table: table_meta.clone(),
                wal_id,
            })?;
            manifest.sync()?;
        }

        // Add to level 0
        {
            use crate::store::level::Level;
            let mut levels = self.store.state.levels.write().unwrap();
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

        // Mark flush complete and delete WAL file
        self.store.state.mark_flush_completed();
        let wal_path = self
            .store
            .config
            .dir
            .join("wal")
            .join(format!("{}.wal", wal_id));
        if let Err(e) = std::fs::remove_file(&wal_path) {
            tracing::warn!(wal_id = wal_id, error = %e, "Failed to delete WAL file");
        }

        tracing::info!(
            table_id = table_id,
            wal_id = wal_id,
            "Flushed memtable to SSTable"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl BackgroundTask for FlushTask {
    fn name(&self) -> &'static str {
        "memtable-flush"
    }

    fn interval(&self) -> Duration {
        self.store.config.scheduler.flush_interval
    }

    async fn execute(&self, _ctx: Context) -> Result<()> {
        // Check if flush needed and try to mark as pending
        if !self.store.state.needs_flush() || !self.store.state.try_mark_flush_pending() {
            return Ok(());
        }

        // Flush oldest memtable
        self.flush_oldest().await?;

        Ok(())
    }
}
