use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;

use crate::error::Result;

/// Context provided to background tasks during execution
pub struct Context {
    pub task_name: &'static str,
    pub run_id: u64,
    pub shutdown: broadcast::Receiver<()>,
}

/// Trait for background tasks that run periodically
#[async_trait::async_trait]
pub trait BackgroundTask: Send + Sync {
    /// Task name for logging
    fn name(&self) -> &'static str;

    /// How often to run this task
    fn interval(&self) -> Duration;

    /// Execute the task
    async fn execute(&self, ctx: Context) -> Result<()>;
}

/// Scheduler manages background tasks with graceful shutdown
pub struct Scheduler {
    tasks: RwLock<Vec<JoinHandle<()>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl Scheduler {
    pub fn new() -> Self {
        let (shutdown_tx, _) = broadcast::channel(1);
        Self {
            tasks: RwLock::new(Vec::new()),
            shutdown_tx,
        }
    }

    /// Register a periodic background task
    pub fn register<T: BackgroundTask + 'static>(&self, task: Arc<T>) -> &Self {
        let handle = self.spawn_timer_loop(task);
        self.tasks.write().unwrap().push(handle);
        self
    }

    /// Spawn one-off task
    pub fn spawn<F>(&self, f: F)
    where
        F: std::future::Future<Output = Result<()>> + Send + 'static,
    {
        tokio::spawn(async move {
            if let Err(e) = f.await {
                tracing::error!(error = %e, "One-off task failed");
            }
        });
    }

    fn spawn_timer_loop<T: BackgroundTask + 'static>(&self, task: Arc<T>) -> JoinHandle<()> {
        let interval = task.interval();
        let mut shutdown_rx = self.shutdown_tx.subscribe();
        let mut run_id = 0u64;

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);

            loop {
                tokio::select! {
                    _ = ticker.tick() => {
                        run_id += 1;
                        let ctx = Context {
                            task_name: task.name(),
                            run_id,
                            shutdown: shutdown_rx.resubscribe(),
                        };

                        if let Err(e) = task.execute(ctx).await {
                            tracing::error!(
                                task = task.name(),
                                error = %e,
                                "Task execution failed"
                            );
                        }
                    }

                    _ = shutdown_rx.recv() => {
                        tracing::info!(task = task.name(), "Task shutting down");
                        break;
                    }
                }
            }
        })
    }

    /// Graceful shutdown - wait for all tasks
    pub async fn shutdown(self) -> Result<()> {
        // Signal all tasks to stop
        self.shutdown_tx.send(()).ok();

        // Wait for all tasks to complete
        for task in self.tasks.write().unwrap().drain(..) {
            task.await
                .map_err(|e| crate::Error::InvalidState(format!("Task join error: {}", e)))?;
        }

        Ok(())
    }
}

impl Default for Scheduler {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct TestTask {
        name: &'static str,
        interval: Duration,
        counter: Arc<AtomicUsize>,
    }

    #[async_trait::async_trait]
    impl BackgroundTask for TestTask {
        fn name(&self) -> &'static str {
            self.name
        }

        fn interval(&self) -> Duration {
            self.interval
        }

        async fn execute(&self, _ctx: Context) -> Result<()> {
            self.counter.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_scheduler_basic() -> Result<()> {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let task = Arc::new(TestTask {
            name: "test-task",
            interval: Duration::from_millis(10),
            counter: counter.clone(),
        });

        scheduler.register(task);

        // Let it run for a bit
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Should have executed multiple times
        assert!(counter.load(Ordering::SeqCst) > 0);

        scheduler.shutdown().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_scheduler_graceful_shutdown() -> Result<()> {
        let scheduler = Scheduler::new();
        let counter = Arc::new(AtomicUsize::new(0));

        let task = Arc::new(TestTask {
            name: "test-task",
            interval: Duration::from_millis(10),
            counter: counter.clone(),
        });

        scheduler.register(task);

        // Let it run briefly
        tokio::time::sleep(Duration::from_millis(25)).await;

        let count_before_shutdown = counter.load(Ordering::SeqCst);

        // Shutdown should be fast
        let start = std::time::Instant::now();
        scheduler.shutdown().await?;
        let shutdown_time = start.elapsed();

        // Should shutdown quickly
        assert!(shutdown_time < Duration::from_millis(100));

        // Give a moment to ensure no more tasks run
        tokio::time::sleep(Duration::from_millis(25)).await;
        let count_after_shutdown = counter.load(Ordering::SeqCst);

        // Should not have increased after shutdown
        assert_eq!(count_before_shutdown, count_after_shutdown);

        Ok(())
    }
}
