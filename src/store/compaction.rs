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
        // Check if compaction is needed and run it
        if self.store.needs_compaction() {
            self.store.compact().await?;
        }

        Ok(())
    }
}
