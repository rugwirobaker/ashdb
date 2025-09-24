use std::{sync::Arc, time::Duration};

use crate::{
    error::Result,
    scheduler::{BackgroundTask, Context},
    store::lsm::LsmStore,
};

pub struct MetricsTask {
    store: Arc<LsmStore>,
}

impl MetricsTask {
    pub fn new(store: Arc<LsmStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl BackgroundTask for MetricsTask {
    fn name(&self) -> &'static str {
        "metrics"
    }

    fn interval(&self) -> Duration {
        self.store.config.scheduler.metrics_interval
    }

    async fn execute(&self, _ctx: Context) -> Result<()> {
        // Collect metrics
        let active_size = self.store.state.active_memtable.read().unwrap().size();
        let frozen_count = self.store.state.frozen_memtables.read().unwrap().len();
        let flush_pending = self
            .store
            .state
            .flush_pending
            .load(std::sync::atomic::Ordering::SeqCst);
        let compaction_running = self
            .store
            .state
            .compaction_running
            .load(std::sync::atomic::Ordering::SeqCst);

        let level_counts: Vec<_> = self
            .store
            .state
            .levels
            .read()
            .unwrap()
            .iter()
            .enumerate()
            .map(|(i, l)| (i, l.table_count()))
            .collect();

        tracing::info!(
            active_memtable_size = active_size,
            frozen_memtables = frozen_count,
            flush_pending = flush_pending,
            compaction_running = compaction_running,
            ?level_counts,
            "LSM metrics"
        );

        Ok(())
    }
}
