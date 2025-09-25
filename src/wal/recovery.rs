use std::{collections::VecDeque, path::Path};

use crate::{
    error::Result,
    memtable::{ActiveMemtable, FrozenMemtable},
    wal::Wal,
};

pub type MemtableRecovery = (ActiveMemtable, VecDeque<FrozenMemtable>, u64);

/// Recovers memtables from WAL directory, similar to manifest::replay
pub fn recover_memtables(dir: &Path) -> Result<MemtableRecovery> {
    let wal_dir = dir.join("wal");
    std::fs::create_dir_all(&wal_dir)?;

    let mut wal_files: Vec<_> = std::fs::read_dir(&wal_dir)?
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let path = entry.path();
            if path.extension()?.to_str()? == "wal" {
                Some(path)
            } else {
                None
            }
        })
        .collect();

    // Sort WAL files by ID
    wal_files.sort_by_key(|path| {
        path.file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0)
    });

    let mut frozen_memtables = VecDeque::new();
    let mut next_wal_id = 0;

    // Process all but the last WAL file into frozen memtables
    for wal_path in wal_files.iter().rev().skip(1) {
        let wal_id = wal_path
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(|s| s.parse::<u64>().ok())
            .unwrap_or(0);

        next_wal_id = next_wal_id.max(wal_id + 1);

        let wal = Wal::new(wal_path.to_str().unwrap())?;
        let frozen = FrozenMemtable::from_wal(wal, wal_id)?;
        frozen_memtables.push_back(frozen);
    }

    // Process the last WAL file as active memtable
    let active_memtable = match wal_files.last() {
        Some(active_wal_path) => {
            let wal_id = active_wal_path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);

            next_wal_id = next_wal_id.max(wal_id + 1);

            let wal = Wal::new(active_wal_path.to_str().unwrap())?;
            ActiveMemtable::from_wal(wal, wal_id)?
        }
        None => {
            let wal_id = next_wal_id;
            next_wal_id += 1;
            let wal_path = wal_dir.join(format!("{}.wal", wal_id));
            ActiveMemtable::new(wal_path.to_str().unwrap(), wal_id)?
        }
    };

    Ok((active_memtable, frozen_memtables, next_wal_id))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tmpfs::TempDir;

    #[test]
    fn test_recover_empty_wal_dir() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let (active, frozen, next_id) = recover_memtables(temp_dir.path())?;

        assert!(frozen.is_empty());
        assert_eq!(next_id, 1);
        assert_eq!(active.size(), 0);

        Ok(())
    }

    #[test]
    fn test_recover_single_wal() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        // Create a WAL with some data
        let wal_path = wal_dir.join("0.wal");
        let wal = Wal::new(wal_path.to_str().unwrap())?;
        wal.append(b"key1", Some(b"value1"))?;
        wal.flush()?;

        let (active, frozen, next_id) = recover_memtables(temp_dir.path())?;

        assert!(frozen.is_empty());
        assert_eq!(next_id, 1);
        assert!(active.size() > 0);

        Ok(())
    }

    #[test]
    fn test_recover_multiple_wals() -> Result<()> {
        let temp_dir = TempDir::new()?;
        let wal_dir = temp_dir.path().join("wal");
        std::fs::create_dir_all(&wal_dir)?;

        // Create multiple WALs
        for i in 0..3 {
            let wal_path = wal_dir.join(format!("{}.wal", i));
            let wal = Wal::new(wal_path.to_str().unwrap())?;
            wal.append(
                format!("key{}", i).as_bytes(),
                Some(format!("value{}", i).as_bytes()),
            )?;
            wal.flush()?;
        }

        let (active, frozen, next_id) = recover_memtables(temp_dir.path())?;

        assert_eq!(frozen.len(), 2); // All but last become frozen
        assert_eq!(next_id, 3);
        assert!(active.size() > 0);

        Ok(())
    }
}
