use super::super::wal::Wal;
use super::{core::Memtable, core::ScanIterator, frozen::FrozenMemtable};
use crate::error::Result;
use std::{
    ops::RangeBounds,
    sync::{Arc, RwLock},
};

pub struct ActiveMemtable {
    memtable: Arc<Memtable>,
    wal: Arc<RwLock<Wal>>,
    wal_id: u64,
}

impl ActiveMemtable {
    pub fn new(wal_path: &str, wal_id: u64) -> Result<Self> {
        let memtable = Arc::new(Memtable::new(wal_path, wal_id)?);
        let wal = memtable.wal().clone();

        Ok(Self {
            memtable,
            wal,
            wal_id,
        })
    }

    pub fn from_wal(wal: Wal, wal_id: u64) -> Result<Self> {
        let memtable = Arc::new(Memtable::from_wal(wal, wal_id)?);
        let wal = memtable.wal().clone();

        Ok(Self {
            memtable,
            wal,
            wal_id,
        })
    }

    pub fn freeze(&self) -> Result<FrozenMemtable> {
        self.memtable.freeze()?;

        Ok(FrozenMemtable {
            memtable: self.memtable.clone(),
            wal_id: self.wal_id,
        })
    }

    pub fn put(&self, key: Vec<u8>, value: Option<Vec<u8>>) -> Result<()> {
        self.memtable.put(key, value)
    }

    pub fn get(&self, key: &[u8]) -> Option<Option<Vec<u8>>> {
        self.memtable.get(key)
    }

    pub fn size(&self) -> usize {
        self.memtable.size()
    }

    pub fn sync(&self) -> Result<()> {
        self.wal.write().unwrap().flush()
    }

    pub fn scan<R>(&self, range: R) -> Result<ScanIterator<R>>
    where
        R: RangeBounds<Vec<u8>> + Clone + Send + Sync,
    {
        self.memtable.scan(range)
    }

    pub fn memtable(&self) -> &Arc<Memtable> {
        &self.memtable
    }
}
