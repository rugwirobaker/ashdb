use super::LsmState;
use crate::{config::LsmConfig, error::Result};

/// Get list of deletable WAL file IDs from manifest
pub fn deletable_wals(state: &LsmState) -> Result<Vec<u64>> {
    let manifest = state.manifest.read().unwrap();
    let state = manifest.replay()?;
    Ok(state.deletable_wals)
}

/// Clean up WAL files that are no longer needed
pub async fn cleanup_wals(state: &LsmState, config: &LsmConfig) -> Result<()> {
    let deletable = deletable_wals(state)?;

    if deletable.is_empty() {
        return Ok(()); // No WALs to clean up
    }

    tracing::debug!(
        deletable_wals = ?deletable,
        "Found {} WAL files to clean up",
        deletable.len()
    );

    for wal_id in deletable {
        let wal_path = config.dir.join("wal").join(format!("{}.wal", wal_id));

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
