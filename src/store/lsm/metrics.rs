use super::LsmState;
use crate::error::Result;

/// Collect metrics from LSM state and log them
pub fn collect_metrics(state: &LsmState) -> Result<()> {
    // Collect metrics
    let active_size = state.active_memtable.read().unwrap().size();
    let frozen_count = state.frozen_memtables.read().unwrap().len();
    let flush_pending = state
        .flush_pending
        .load(std::sync::atomic::Ordering::SeqCst);
    let compaction_running = state
        .compaction_running
        .load(std::sync::atomic::Ordering::SeqCst);

    let (level_counts, level_sizes): (Vec<_>, Vec<_>) = {
        let levels = state.levels.read().unwrap();
        levels
            .iter()
            .enumerate()
            .map(|(i, l)| ((i, l.table_count()), (i, l.size())))
            .unzip()
    };

    let next_sstable_id = state
        .next_sstable_id
        .load(std::sync::atomic::Ordering::SeqCst);
    let next_wal_id = state.next_wal_id.load(std::sync::atomic::Ordering::SeqCst);

    tracing::info!(
        active_memtable_size = active_size,
        frozen_memtables = frozen_count,
        flush_pending = flush_pending,
        compaction_running = compaction_running,
        next_sstable_id = next_sstable_id,
        next_wal_id = next_wal_id,
        ?level_counts,
        ?level_sizes,
        "LSM metrics"
    );

    Ok(())
}
