use std::{sync::Arc, time::Duration};

use crate::{
    error::Result,
    scheduler::{BackgroundTask, Context},
    store::lsm::LsmStore,
};

pub struct FlushTask {
    store: Arc<LsmStore>,
}

impl FlushTask {
    pub fn new(store: Arc<LsmStore>) -> Self {
        Self { store }
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

        // Flush oldest memtable using LsmStore method
        self.store.flush_memtable().await?;

        // Mark flush complete
        self.store.state.mark_flush_completed();

        Ok(())
    }
}
