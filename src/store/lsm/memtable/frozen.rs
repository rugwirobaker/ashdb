use super::super::sstable::table;
use super::super::wal::Wal;
use super::{core::Memtable, core::ScanIter};
use crate::error::Result;
use std::{ops::RangeBounds, sync::Arc};

pub struct FrozenMemtable {
    pub(super) memtable: Arc<Memtable>,
    pub(super) wal_id: u64,
}

impl FrozenMemtable {
    pub fn from_wal(wal: Wal, wal_id: u64) -> Result<Self> {
        let memtable = Arc::new(Memtable::from_wal(wal, wal_id)?);
        memtable.freeze()?;

        Ok(Self { memtable, wal_id })
    }

    pub fn wal_id(&self) -> u64 {
        self.wal_id
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.memtable.get(key)
    }

    pub fn flush(&self, table: &mut table::Table) -> Result<()> {
        self.memtable.flush(table)
    }

    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> Result<ScanIter> {
        self.memtable.scan(range)
    }

    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.memtable
    }
}
