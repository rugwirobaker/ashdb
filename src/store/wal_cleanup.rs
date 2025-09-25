use std::{sync::Arc, time::Duration};

use crate::{
    error::Result,
    scheduler::{BackgroundTask, Context},
    store::lsm::LsmStore,
};

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
        self.store.cleanup_wals().await
    }
}
