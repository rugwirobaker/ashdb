use std::sync::{Arc, RwLock};

use crate::memtable::Memtable;

#[derive(Debug)]
pub struct LsmStore {
    _memtable: Arc<RwLock<Memtable>>,
}
