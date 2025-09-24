use std::{sync::Arc, time::Duration};

use crate::{
    error::Result,
    scheduler::{BackgroundTask, Context},
    store::lsm::LsmStore,
};

pub struct CompactionTask {
    store: Arc<LsmStore>,
}

impl CompactionTask {
    pub fn new(store: Arc<LsmStore>) -> Self {
        Self { store }
    }

    async fn compact(&self) -> Result<()> {
        let _guard = self.store.state.start_compaction();

        // Simple compaction: level 0 -> level 1
        let source_tables = {
            let levels = self.store.state.levels.read().unwrap();
            if levels.is_empty()
                || levels[0].table_count()
                    <= self.store.config.scheduler.level0_compaction_threshold
            {
                return Ok(());
            }
            levels[0].all_tables()
        };

        // TODO: Implement actual compaction logic
        // 1. Create merge iterator over all input tables
        // 2. Write to new SSTables
        // 3. Update manifest with compaction record
        // 4. Update levels
        // 5. Delete old SSTable files

        tracing::info!(
            source_tables = source_tables.len(),
            "Would compact level 0 tables (compaction logic not implemented yet)"
        );

        Ok(())
    }
}

#[async_trait::async_trait]
impl BackgroundTask for CompactionTask {
    fn name(&self) -> &'static str {
        "compaction"
    }

    fn interval(&self) -> Duration {
        self.store.config.scheduler.compaction_interval
    }

    async fn execute(&self, _ctx: Context) -> Result<()> {
        // Check if compaction is needed
        if !self.store.state.needs_compaction() {
            return Ok(());
        }

        // Run compaction
        self.compact().await?;

        Ok(())
    }
}
