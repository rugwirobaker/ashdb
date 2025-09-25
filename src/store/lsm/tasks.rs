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
        if !self.store.needs_flush() || !self.store.try_mark_flush_pending() {
            return Ok(());
        }

        // Flush oldest memtable using LsmStore method
        self.store.flush_memtable().await?;

        // Mark flush complete
        self.store.mark_flush_completed();

        Ok(())
    }
}

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
        self.store.collect_metrics()
    }
}

pub struct WalCleanupTask {
    store: Arc<LsmStore>,
}

impl WalCleanupTask {
    pub fn new(store: Arc<LsmStore>) -> Self {
        Self { store }
    }
}

#[async_trait::async_trait]
impl BackgroundTask for WalCleanupTask {
    fn name(&self) -> &'static str {
        "wal-cleanup"
    }

    fn interval(&self) -> Duration {
        self.store.config.scheduler.wal_cleanup_interval
    }

    async fn execute(&self, _ctx: Context) -> Result<()> {
        // Clean up WAL files that are no longer needed
        self.store.cleanup_wals().await
    }
}
