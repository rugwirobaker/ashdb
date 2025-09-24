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

    fn deletable_wals(&self) -> Result<Vec<u64>> {
        let _manifest = self.store.state.manifest.read().unwrap();

        // TODO: Implement proper WAL cleanup logic
        // We need to track which WALs have been flushed to SSTables
        // and are safe to delete. This requires extending the manifest
        // to track WAL cleanup or using a separate mechanism.

        // For now, return empty list to avoid deleting active WALs
        Ok(Vec::new())
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
        let deletable = self.deletable_wals()?;

        for wal_id in deletable {
            let wal_path = self
                .store
                .config
                .dir
                .join("wal")
                .join(format!("{}.wal", wal_id));

            match std::fs::remove_file(&wal_path) {
                Ok(_) => {
                    tracing::info!(wal_id = wal_id, "Deleted WAL file");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already deleted, ignore
                }
                Err(e) => {
                    tracing::warn!(
                        wal_id = wal_id,
                        error = %e,
                        "Failed to delete WAL file"
                    );
                }
            }
        }

        Ok(())
    }
}
