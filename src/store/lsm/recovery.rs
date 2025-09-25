use super::sstable::table::Table;
use super::wal::recovery::recover_memtables;
use super::{
    manifest::{Manifest, ManifestState},
    Level, LsmState, SSTable,
};
use crate::{config::LsmConfig, error::Result};

use std::path::Path;

const MANIFEST_FILE: &str = "manifest.log";

/// Recover state from manifest and WAL files
pub(crate) fn recover_state(config: &LsmConfig) -> Result<LsmState> {
    let dir = &config.dir;

    // Open or create manifest
    let manifest_path = dir.join(MANIFEST_FILE);
    let manifest = Manifest::new(&manifest_path)?;

    // Recover levels from manifest
    let manifest_state = manifest.replay()?;
    let levels = levels_from_manifest_state(dir, &manifest_state)?;

    // Recover memtables from WAL
    let (active_memtable, frozen_memtables, next_wal_id) = recover_memtables(dir)?;

    Ok(LsmState::new(
        active_memtable,
        frozen_memtables,
        levels,
        manifest,
        manifest_state.next_table_id,
        next_wal_id,
    ))
}

/// Convert manifest state to levels
fn levels_from_manifest_state(dir: &Path, state: &ManifestState) -> Result<Vec<Level>> {
    let mut levels = Vec::new();

    for level_meta in &state.levels {
        while levels.len() <= level_meta.level as usize {
            levels.push(Level::new(levels.len() as u32));
        }

        for table_meta in &level_meta.tables {
            let path = dir.join(format!("{}.sst", table_meta.id));
            let table = Table::readable(path.to_str().unwrap())?;
            levels[level_meta.level as usize].add_sstable(SSTable {
                id: table_meta.id,
                table,
                path,
                size: table_meta.size,
                min_key: table_meta.min_key.clone(),
                max_key: table_meta.max_key.clone(),
            });
        }
    }

    Ok(levels)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmpfs::TempDir;

    #[test]
    fn test_recover_empty_state() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let config = LsmConfig::new(temp_dir.path());

        let state = recover_state(&config)?;

        // Should have empty levels and one active memtable
        assert!(state.levels.read().unwrap().is_empty());
        assert!(state.frozen_memtables.read().unwrap().is_empty());
        assert_eq!(state.active_memtable.read().unwrap().size(), 0);

        Ok(())
    }

    #[test]
    fn test_levels_from_empty_manifest_state() -> Result<()> {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let manifest_state = ManifestState {
            levels: Vec::new(),
            next_table_id: 0,
            deletable_wals: Vec::new(),
        };

        let levels = levels_from_manifest_state(temp_dir.path(), &manifest_state)?;
        assert!(levels.is_empty());

        Ok(())
    }
}
