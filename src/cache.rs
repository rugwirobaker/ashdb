use std::{
    cmp::min,
    collections::{linked_list, HashMap, LinkedList},
    fmt::Debug,
    hash::Hash,
    sync::atomic::{AtomicU8, AtomicUsize, Ordering},
};

/// Maximum frequency limit for an entry in the cache.
const MAX_FREQUENCY_LIMIT: u8 = 3;

struct Entry<K, V> {
    key: K,
    value: V,

    freq: AtomicU8,
}

impl<K, V> Entry<K, V> {
    fn new(key: K, value: V) -> Self {
        Entry {
            key,
            value,
            freq: AtomicU8::new(0),
        }
    }
}

impl<K, V> Clone for Entry<K, V>
where
    K: Clone,
    V: Clone,
{
    fn clone(&self) -> Self {
        Entry {
            key: self.key.clone(),
            value: self.value.clone(),
            freq: AtomicU8::new(self.freq.load(Ordering::SeqCst)),
        }
    }
}

type List<K, V> = LinkedList<Entry<K, V>>;

type EvictedFn<K, V> = Box<dyn Fn(K, V) + Send>;

pub struct Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    max_main_size: usize,
    max_cache_size: usize,
    /// new entries are added to this queue
    small: List<K, V>,
    /// entries that have been accessed at least twice are moved to this queue
    main: List<K, V>,
    /// entries that are evicted from small before being accessed twice are moved to this queue
    ghost: LinkedList<K>,
    /// Map of all entries for quick access.
    entries: HashMap<K, Entry<K, V>>,
    /// evicted entries
    evicted_fn: Option<EvictedFn<K, V>>,
    /// stats
    stats: Stats,
}

impl<K, V> Cache<K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    pub fn new(max_cache_size: usize, evicted_fn: Option<EvictedFn<K, V>>) -> Self {
        let max_small_size = max_cache_size / 10;
        let max_main_size = max_cache_size - max_small_size;

        Cache {
            max_main_size,
            max_cache_size,
            small: LinkedList::new(),
            main: LinkedList::new(),
            ghost: LinkedList::new(),
            entries: HashMap::new(),
            evicted_fn,
            stats: Stats::new(),
        }
    }

    /// Returns a reference to the value of the given key if it exists in the cache.
    pub fn get(&self, key: &K) -> Option<&V> {
        if let Some(entry) = self.entries.get(key) {
            let freq = min(entry.freq.load(Ordering::SeqCst) + 1, MAX_FREQUENCY_LIMIT);
            entry.freq.store(freq, Ordering::SeqCst);
            self.stats.hit();
            Some(&entry.value)
        } else {
            self.stats.miss();
            None
        }
    }

    /// Inserts a new entry with the given key and value into the cache.
    pub fn insert(&mut self, key: K, value: V) {
        self.evict();

        if self.entries.contains_key(&key) {
            let entry = Entry::new(key, value);
            self.main.push_back(entry);
        } else {
            let entry = Entry::new(key, value);
            self.entries.insert(entry.key.clone(), entry.clone());
            self.small.push_back(entry);
        }
    }

    pub fn stats(&self) -> (usize, usize) {
        self.stats.get()
    }

    // Add a method to get an iterator
    pub fn iter(&self) -> CacheIterator<K, V> {
        CacheIterator {
            small_iter: self.small.iter(),
            main_iter: self.main.iter(),
        }
    }

    fn insert_m(&mut self, tail: Entry<K, V>) {
        self.main.push_front(tail);
    }

    fn insert_g(&mut self, tail: Entry<K, V>) {
        // call eviction_fn if it exists
        if let Some(evicted_fn) = &self.evicted_fn {
            evicted_fn(tail.key.clone(), tail.value.clone());
        }

        if self.ghost.len() >= self.max_cache_size {
            let key = self.ghost.pop_back().unwrap();
            self.entries.remove(&key);
        }
        self.ghost.push_front(tail.key);
    }

    fn evict(&mut self) {
        if self.small.len() + self.main.len() >= self.max_cache_size {
            if self.main.len() >= self.max_main_size || self.small.is_empty() {
                self.evict_m();
            } else {
                self.evict_s();
            }
        }
    }

    fn evict_m(&mut self) {
        while let Some(tail) = self.main.pop_front() {
            let freq = tail.freq.load(Ordering::SeqCst);
            if freq > 0 {
                tail.freq.store(freq - 1, Ordering::SeqCst);
                self.main.push_back(tail);
            } else {
                self.insert_g(tail);
            }
        }
    }

    fn evict_s(&mut self) {
        while let Some(tail) = self.small.pop_front() {
            if tail.freq.load(Ordering::SeqCst) > 1 {
                self.insert_m(tail);
            } else {
                self.insert_g(tail);
                break;
            }
        }
    }
}

pub struct CacheIterator<'a, K, V>
where
    K: 'a,
    V: 'a,
{
    small_iter: linked_list::Iter<'a, Entry<K, V>>,
    main_iter: linked_list::Iter<'a, Entry<K, V>>,
}

impl<'a, K, V> Iterator for CacheIterator<'a, K, V>
where
    K: PartialEq + Eq + Hash + Clone + Debug,
    V: Clone,
{
    type Item = (&'a K, &'a V);

    fn next(&mut self) -> Option<Self::Item> {
        // Try the `small` list first
        if let Some(entry) = self.small_iter.next() {
            return Some((&entry.key, &entry.value));
        }
        // If `small` is exhausted, move to the `main` list
        self.main_iter
            .next()
            .map(|entry| (&entry.key, &entry.value))
    }
}

struct Stats {
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl Stats {
    fn new() -> Self {
        Stats {
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    fn hit(&self) {
        self.hits.fetch_add(1, Ordering::SeqCst);
    }

    fn miss(&self) {
        self.misses.fetch_add(1, Ordering::SeqCst);
    }

    fn get(&self) -> (usize, usize) {
        (
            self.hits.load(Ordering::SeqCst),
            self.misses.load(Ordering::SeqCst),
        )
    }
}

#[cfg(test)]
mod tests {
    use std::fmt::Debug;
    use std::sync::{
        atomic::{AtomicUsize, Ordering::SeqCst},
        Arc, Mutex,
    };

    use super::*;

    fn assert_opt_eq<V: PartialEq + Debug>(opt: Option<&V>, v: V) {
        assert!(opt.is_some());
        assert_eq!(opt.unwrap(), &v);
    }

    #[test]
    fn test_push_and_read() {
        let mut cache = Cache::new(2, None);

        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_push_removes_oldest() {
        let mut cache = Cache::new(2, None);

        let fruits = vec![
            ("apple", "red"),
            ("banana", "yellow"),
            ("orange", "orange"),
            ("pear", "green"),
            ("peach", "pink"),
        ];

        for (fruit, color) in fruits {
            cache.insert(fruit, color);
        }

        assert!(cache.get(&"apple").is_none());
        assert_opt_eq(cache.get(&"pear"), "green");
        assert_opt_eq(cache.get(&"peach"), "pink");

        // "apple" should been removed from the cache.
        cache.insert("apple", "red");
        cache.insert("banana", "yellow");

        assert_opt_eq(cache.get(&"apple"), "red");
        assert_opt_eq(cache.get(&"banana"), "yellow");
    }

    #[test]
    fn test_no_memory_leaks() {
        static DROP_COUNT: AtomicUsize = AtomicUsize::new(0);

        #[derive(Debug, Clone)]
        struct DropCounter;

        impl Drop for DropCounter {
            fn drop(&mut self) {
                DROP_COUNT.fetch_add(1, SeqCst);
            }
        }

        let n = 100;
        for _ in 0..n {
            let mut cache = Cache::new(20, None);
            for i in 0..n {
                cache.insert(i, DropCounter {});
            }
        }
        assert_eq!(DROP_COUNT.load(SeqCst), 2 * n * n);
    }

    #[test]
    fn test_eviction_fn() {
        let evicted = Arc::new(Mutex::new(Vec::new()));
        let evicted_fn = {
            let evicted = evicted.clone();
            Box::new(move |k: i32, v: i32| {
                evicted.lock().unwrap().push((k, v));
            })
        };

        let mut cache = Cache::new(2, Some(evicted_fn));

        cache.insert(1, 1);
        cache.insert(2, 2);
        cache.insert(3, 3); // expect eviction

        let expected = vec![(1, 1)];

        assert_eq!(*evicted.lock().unwrap(), expected);
    }

    #[test]
    fn test_hit_ratio() {
        let cap = 10;
        let mut cache = Cache::new(cap, None);

        for i in 0..cap {
            cache.insert(i, i);
        }

        for i in 0..cap {
            assert_eq!(cache.get(&i), Some(&i));
        }

        let (hits, misses) = cache.stats.get();

        assert_eq!(hits, cap);
        assert_eq!(misses, 0);
    }
}
