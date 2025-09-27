use crate::error::Result;
use std::{cmp::Ordering, collections::BinaryHeap, ops::RangeBounds, sync::Arc};

/// Type alias for complex iterator used in merge operations
pub type KvIterator<'a> = Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>)>> + Send + Sync + 'a>;

// Iterator implementations
pub struct OwningMemtableIter {
    _memtable: Arc<super::memtable::Memtable>,
    iter: super::memtable::ScanIter<'static>,
}

impl OwningMemtableIter {
    pub fn new(memtable: Arc<super::memtable::Memtable>, range: impl RangeBounds<Vec<u8>>) -> Self {
        let iter = unsafe {
            std::mem::transmute::<super::memtable::ScanIter<'_>, super::memtable::ScanIter<'static>>(
                memtable.scan(range).unwrap(),
            )
        };

        Self {
            _memtable: memtable,
            iter,
        }
    }
}

impl Iterator for OwningMemtableIter {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

pub struct HeapEntry<'a> {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub source: usize,
    pub iterator: KvIterator<'a>,
}

impl std::fmt::Debug for HeapEntry<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("HeapEntry")
            .field("key", &self.key)
            .field("value", &self.value)
            .field("source", &self.source)
            .finish()
    }
}

impl PartialEq for HeapEntry<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for HeapEntry<'_> {}

impl PartialOrd for HeapEntry<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry<'_> {
    fn cmp(&self, other: &Self) -> Ordering {
        match self.key.cmp(&other.key) {
            Ordering::Equal => self.source.cmp(&other.source),
            other => other.reverse(),
        }
    }
}

#[derive(Debug)]
pub struct MergeIterator<'a> {
    heap: BinaryHeap<HeapEntry<'a>>,
    latest_key: Option<Vec<u8>>,
}

impl<'a> MergeIterator<'a> {
    pub fn new(iterators: Vec<KvIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();

        for (source, mut iterator) in iterators.into_iter().enumerate() {
            if let Some(Ok((key, value))) = iterator.next() {
                heap.push(HeapEntry {
                    key,
                    value,
                    source,
                    iterator,
                });
            }
        }

        Self {
            heap,
            latest_key: None,
        }
    }
}

impl Iterator for MergeIterator<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut entry) = self.heap.pop() {
            if self.latest_key.as_ref() == Some(&entry.key) {
                if let Some(Ok((key, value))) = entry.iterator.next() {
                    self.heap.push(HeapEntry {
                        key,
                        value,
                        source: entry.source,
                        iterator: entry.iterator,
                    });
                }
                continue;
            }

            self.latest_key = Some(entry.key.clone());

            if let Some(Ok((key, value))) = entry.iterator.next() {
                self.heap.push(HeapEntry {
                    key,
                    value,
                    source: entry.source,
                    iterator: entry.iterator,
                });
            }
            return Some(Ok((entry.key, entry.value)));
        }

        None
    }
}
