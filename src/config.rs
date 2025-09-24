use std::path::PathBuf;
use std::time::Duration;

/// Configuration for the LSM store
#[derive(Debug, Clone)]
pub struct LsmConfig {
    /// Directory path for the database
    pub dir: PathBuf,

    /// Maximum size for memtables before freezing (default: 64MB)
    pub max_memtable_size: usize,

    /// Enable WAL direct I/O (default: false)
    pub wal_direct_io: bool,

    /// WAL buffer size (default: 64KB)
    pub wal_buffer_size: usize,

    /// Scheduler configuration
    pub scheduler: SchedulerConfig,
}

#[derive(Debug, Clone)]
pub struct SchedulerConfig {
    /// How often to check for flush opportunities (default: 1s)
    pub flush_interval: Duration,

    /// How often to check for compaction opportunities (default: 10s)
    pub compaction_interval: Duration,

    /// How often to clean up old WAL files (default: 30s)
    pub wal_cleanup_interval: Duration,

    /// How often to collect metrics (default: 5s)
    pub metrics_interval: Duration,

    /// Level 0 table count threshold for compaction (default: 4)
    pub level0_compaction_threshold: usize,
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./ashdb"),
            max_memtable_size: 64 * 1024 * 1024, // 64MB
            wal_direct_io: false,
            wal_buffer_size: 64 * 1024, // 64KB
            scheduler: SchedulerConfig::default(),
        }
    }
}

impl Default for SchedulerConfig {
    fn default() -> Self {
        Self {
            flush_interval: Duration::from_secs(1),
            compaction_interval: Duration::from_secs(10),
            wal_cleanup_interval: Duration::from_secs(30),
            metrics_interval: Duration::from_secs(5),
            level0_compaction_threshold: 4,
        }
    }
}

impl LsmConfig {
    /// Create a new config with the given directory
    pub fn new(dir: impl Into<PathBuf>) -> Self {
        Self {
            dir: dir.into(),
            ..Default::default()
        }
    }

    /// Set maximum memtable size
    pub fn max_memtable_size(mut self, size: usize) -> Self {
        self.max_memtable_size = size;
        self
    }

    /// Enable WAL direct I/O
    pub fn wal_direct_io(mut self, enabled: bool) -> Self {
        self.wal_direct_io = enabled;
        self
    }

    /// Set WAL buffer size
    pub fn wal_buffer_size(mut self, size: usize) -> Self {
        self.wal_buffer_size = size;
        self
    }

    /// Configure scheduler settings
    pub fn scheduler(mut self, config: SchedulerConfig) -> Self {
        self.scheduler = config;
        self
    }
}

impl SchedulerConfig {
    /// Set flush check interval
    pub fn flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set compaction check interval
    pub fn compaction_interval(mut self, interval: Duration) -> Self {
        self.compaction_interval = interval;
        self
    }

    /// Set WAL cleanup interval
    pub fn wal_cleanup_interval(mut self, interval: Duration) -> Self {
        self.wal_cleanup_interval = interval;
        self
    }

    /// Set metrics collection interval
    pub fn metrics_interval(mut self, interval: Duration) -> Self {
        self.metrics_interval = interval;
        self
    }

    /// Set level 0 compaction threshold
    pub fn level0_compaction_threshold(mut self, threshold: usize) -> Self {
        self.level0_compaction_threshold = threshold;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = LsmConfig::default();
        assert_eq!(config.dir, PathBuf::from("./ashdb"));
        assert_eq!(config.max_memtable_size, 64 * 1024 * 1024);
        assert!(!config.wal_direct_io);
        assert_eq!(config.wal_buffer_size, 64 * 1024);
    }

    #[test]
    fn test_config_builder() {
        let config = LsmConfig::new("/tmp/test")
            .max_memtable_size(32 * 1024 * 1024)
            .wal_direct_io(true)
            .scheduler(
                SchedulerConfig::default()
                    .flush_interval(Duration::from_millis(500))
                    .compaction_interval(Duration::from_secs(5)),
            );

        assert_eq!(config.dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.max_memtable_size, 32 * 1024 * 1024);
        assert!(config.wal_direct_io);
        assert_eq!(config.scheduler.flush_interval, Duration::from_millis(500));
        assert_eq!(config.scheduler.compaction_interval, Duration::from_secs(5));
    }
}
