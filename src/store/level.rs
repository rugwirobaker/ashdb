use std::path::PathBuf;

use crate::error::Result;
use crate::sstable::table::{self, Table};

pub struct SSTable {
    pub id: u64,
    pub table: Table,
    pub path: PathBuf,
    pub size: u64, // Size in bytes
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
}

impl SSTable {
    /// Creates a new `SSTable` with a given ID, table, path, and size.
    pub fn new(id: u64, path: String, size: u64) -> Result<Self> {
        let table = table::Table::readable(&path).unwrap();

        let min_key = table.min_key().to_vec();
        let max_key = table.max_key().to_vec();

        Ok(Self {
            id,
            table,
            path: PathBuf::from(&path),
            size,
            min_key,
            max_key,
        })
    }
}

pub struct Level {
    pub level_num: u32,
    pub sstables: Vec<SSTable>,
    // pub max_size: u64, // Maximum size in bytes
}

impl Level {
    /// Creates a new `Level` with a given number and maximum size.
    pub fn new(level_num: u32) -> Self {
        Self {
            level_num,
            sstables: Vec::new(),
            // max_size,
        }
    }

    /// Returns the total size of all SSTables in the level.
    pub fn size(&self) -> u64 {
        self.sstables.iter().map(|sstable| sstable.size).sum()
    }

    /// Returns the number of SSTables in the level.
    pub fn len(&self) -> usize {
        self.sstables.len()
    }

    /// Returns true if the level is empty.
    pub fn is_empty(&self) -> bool {
        self.sstables.is_empty()
    }

    /// Adds an SSTable to the level.
    pub fn add_sstable(&mut self, sstable: SSTable) {
        self.sstables.push(sstable);
    }

    /// Removes an SSTable from the level.
    pub fn remove_sstable(&mut self, id: u64) -> Option<SSTable> {
        let index = self.sstables.iter().position(|sstable| sstable.id == id);
        index.map(|i| self.sstables.remove(i))
    }

    /// Returns the number of tables in the level.
    pub fn table_count(&self) -> usize {
        self.sstables.len()
    }

    /// Returns all tables in the level (for compaction).
    pub fn all_tables(&self) -> Vec<crate::manifest::meta::TableMeta> {
        self.sstables
            .iter()
            .map(|sstable| {
                crate::manifest::meta::TableMeta {
                    id: sstable.id,
                    level: self.level_num,
                    size: sstable.size,
                    entry_count: 0, // TODO: get from table
                    min_key: sstable.min_key.clone(),
                    max_key: sstable.max_key.clone(),
                }
            })
            .collect()
    }

    /// Get value for key from this level's SSTables
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Check each SSTable in the level
        for sstable in &self.sstables {
            // Simple range check - could be optimized with bloom filters
            if key >= sstable.min_key.as_slice() && key <= sstable.max_key.as_slice() {
                if let Ok(Some(value)) = sstable.table.get(key) {
                    return Ok(Some(value));
                }
            }
        }
        Ok(None)
    }
}
