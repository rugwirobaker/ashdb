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

    /// Compaction configuration
    pub compaction: CompactionConfig,
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
}

#[derive(Debug, Clone)]
pub struct CompactionConfig {
    /// Level 0 table count threshold for compaction (default: 4)
    pub level0_compaction_threshold: usize,

    /// Size ratio threshold for tiered compaction (default: 10)
    /// When the combined size of tables at level N is >= size_ratio * size of level N+1,
    /// compact level N to level N+1
    pub size_ratio_threshold: u32,

    /// Maximum number of tables per level in tiered compaction (default: 10)
    pub max_tables_per_level: usize,
}

impl Default for CompactionConfig {
    fn default() -> Self {
        Self {
            level0_compaction_threshold: 4,
            size_ratio_threshold: 10,
            max_tables_per_level: 10,
        }
    }
}

impl Default for LsmConfig {
    fn default() -> Self {
        Self {
            dir: PathBuf::from("./ashdb"),
            max_memtable_size: 64 * 1024 * 1024, // 64MB
            wal_direct_io: false,
            wal_buffer_size: 64 * 1024, // 64KB
            scheduler: SchedulerConfig::default(),
            compaction: CompactionConfig::default(),
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

    /// Configure compaction settings
    pub fn compaction(mut self, config: CompactionConfig) -> Self {
        self.compaction = config;
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
}

impl CompactionConfig {
    /// Set level 0 compaction threshold
    pub fn level0_compaction_threshold(mut self, threshold: usize) -> Self {
        self.level0_compaction_threshold = threshold;
        self
    }

    /// Set size ratio threshold for tiered compaction
    pub fn size_ratio_threshold(mut self, ratio: u32) -> Self {
        self.size_ratio_threshold = ratio;
        self
    }

    /// Set maximum tables per level for tiered compaction
    pub fn max_tables_per_level(mut self, max_tables: usize) -> Self {
        self.max_tables_per_level = max_tables;
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

        // Test default compaction config
        assert_eq!(config.compaction.level0_compaction_threshold, 4);
        assert_eq!(config.compaction.size_ratio_threshold, 10);
        assert_eq!(config.compaction.max_tables_per_level, 10);
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
            )
            .compaction(
                CompactionConfig::default()
                    .level0_compaction_threshold(2)
                    .size_ratio_threshold(5)
                    .max_tables_per_level(8),
            );

        assert_eq!(config.dir, PathBuf::from("/tmp/test"));
        assert_eq!(config.max_memtable_size, 32 * 1024 * 1024);
        assert!(config.wal_direct_io);

        // Test scheduler config
        assert_eq!(config.scheduler.flush_interval, Duration::from_millis(500));
        assert_eq!(config.scheduler.compaction_interval, Duration::from_secs(5));

        // Test compaction config
        assert_eq!(config.compaction.level0_compaction_threshold, 2);
        assert_eq!(config.compaction.size_ratio_threshold, 5);
        assert_eq!(config.compaction.max_tables_per_level, 8);
    }
}
