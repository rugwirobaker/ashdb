//! Workload benchmarking framework for AshDB.
//!
//! This binary provides a flexible framework for benchmarking transactional workloads
//! against the MVCC layer and underlying storage implementations. It supports various
//! workload types with detailed performance metrics.

use ashdb::error::Result;
use ashdb::store::lsm::LsmTree;
use ashdb::store::mvcc::MVCC;
use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::io::Write as _;
use std::time::{Duration, Instant};

/// Workload trait for defining benchmark scenarios.
///
/// Each workload implements this trait to define:
/// - Setup phase (prepare)
/// - Work generation (generate)
/// - Execution logic (execute)
/// - Optional verification (verify)
trait Workload: Send + Sync {
    /// Prepare the workload by setting up initial data.
    fn prepare(&self, mvcc: &MVCC<LsmTree>, rng: &mut StdRng) -> Result<()>;

    /// Generate a single work item.
    fn generate(&self, rng: &mut StdRng) -> WorkItem;

    /// Execute a work item against the MVCC store.
    fn execute(&self, mvcc: &MVCC<LsmTree>, item: &WorkItem) -> Result<()>;

    /// Verify the workload results (optional).
    fn verify(&self, mvcc: &MVCC<LsmTree>) -> Result<()> {
        let _ = mvcc;
        Ok(())
    }

    /// Get workload name for display.
    fn name(&self) -> &str;
}

/// A single unit of work to be executed.
#[derive(Debug, Clone)]
enum WorkItem {
    /// Point write: (key, value)
    Write(Vec<u8>, Vec<u8>),
    /// Point read: key
    Read(Vec<u8>),
    /// Bank transfer: (from_account, to_account, amount)
    Transfer(u64, u64, u64),
}

/// Runner executes a workload with the specified configuration.
struct Runner {
    /// Number of concurrent worker threads
    concurrency: usize,
    /// Total number of transactions to execute (if Some)
    count: Option<u64>,
    /// Duration to run for (if Some)
    duration: Option<Duration>,
    /// Random seed for reproducibility
    seed: u64,
    /// Database path
    path: String,
}

impl Runner {
    fn run<W: Workload + 'static>(&self, workload: W) -> Result<()> {
        let mut rng = StdRng::seed_from_u64(self.seed);
        let mvcc = MVCC::new(LsmTree::open(&self.path)?);

        // Set up a histogram recording txn latencies as nanoseconds.
        // The buckets range from 0.001ms to 10s.
        let mut hist = Histogram::<u64>::new_with_bounds(1_000, 10_000_000_000, 3)
            .expect("Failed to create histogram")
            .into_sync();

        // Prepare the dataset.
        print!("Preparing initial dataset... ");
        std::io::stdout().flush()?;
        let start = Instant::now();
        workload.prepare(&mvcc, &mut rng)?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        // Determine actual count to run
        let count = self
            .count
            .or(self.duration.map(|_| u64::MAX))
            .unwrap_or(1000);

        // Spawn workers using thread scope
        std::thread::scope(|s| -> Result<()> {
            print!("Spawning {} workers... ", self.concurrency);
            std::io::stdout().flush()?;
            let start = Instant::now();

            let (work_tx, work_rx) = crossbeam::channel::bounded(self.concurrency);
            let (done_tx, done_rx) = crossbeam::channel::bounded::<()>(0);

            for _worker_id in 0..self.concurrency {
                let mvcc = &mvcc;
                let workload = &workload;
                let mut recorder = hist.recorder();
                let work_rx = work_rx.clone();
                let done_tx = done_tx.clone();

                s.spawn(move || {
                    while let Ok(item) = work_rx.recv() {
                        let start = Instant::now();
                        let result = workload.execute(mvcc, &item);
                        let elapsed = start.elapsed();

                        // Only record successful transactions
                        if result.is_ok() || matches!(result, Err(ashdb::Error::Serialization)) {
                            recorder
                                .record(elapsed.as_nanos() as u64)
                                .expect("Failed to record");
                        }
                    }
                    drop(done_tx); // disconnects done_rx once all workers exit
                });
            }
            drop(done_tx); // drop local copy

            println!("done ({:.3}s)", start.elapsed().as_secs_f64());

            // Spawn work generator
            {
                let workload = &workload;
                let seed = self.seed;
                println!("Running workload {} (count={})...", workload.name(), count);
                println!();
                s.spawn(move || {
                    let mut rng = StdRng::seed_from_u64(seed);
                    for _ in 0..count {
                        let item = workload.generate(&mut rng);
                        work_tx.send(item).expect("Failed to send work item");
                    }
                });
            }

            // Periodically print stats until all workers are done
            let start = Instant::now();
            let ticker = crossbeam::channel::tick(Duration::from_secs(1));

            println!("Time   Progress     Txns      Rate       p50       p90       p99      pMax");

            while let Err(crossbeam::channel::TryRecvError::Empty) = done_rx.try_recv() {
                crossbeam::select! {
                    recv(ticker) -> _ => {},
                    recv(done_rx) -> _ => {},
                }

                let duration = start.elapsed().as_secs_f64();
                hist.refresh_timeout(Duration::from_secs(1));

                println!(
                    "{:<8} {:>5.1}%  {:>7}  {:>6.0}/s  {:>6.1}ms  {:>6.1}ms  {:>6.1}ms  {:>6.1}ms",
                    format!("{:.1}s", duration),
                    hist.len() as f64 / count as f64 * 100.0,
                    hist.len(),
                    hist.len() as f64 / duration,
                    Duration::from_nanos(hist.value_at_quantile(0.5)).as_secs_f64() * 1000.0,
                    Duration::from_nanos(hist.value_at_quantile(0.9)).as_secs_f64() * 1000.0,
                    Duration::from_nanos(hist.value_at_quantile(0.99)).as_secs_f64() * 1000.0,
                    Duration::from_nanos(hist.max()).as_secs_f64() * 1000.0,
                );
            }

            Ok(())
        })?;

        // Verify the final dataset
        println!();
        print!("Verifying dataset... ");
        std::io::stdout().flush()?;
        let start = Instant::now();
        workload.verify(&mvcc)?;
        println!("done ({:.3}s)", start.elapsed().as_secs_f64());

        Ok(())
    }
}

struct KVWrite {
    keys: u64,
    value_size: usize,
    #[allow(dead_code)]
    sequential: bool,
}

impl Workload for KVWrite {
    fn prepare(&self, _mvcc: &MVCC<LsmTree>, _rng: &mut StdRng) -> Result<()> {
        Ok(())
    }

    fn generate(&self, rng: &mut StdRng) -> WorkItem {
        // TODO: implement sequential key generation properly
        let key = rng.gen_range(0..self.keys);

        let key_bytes = format!("key:{:016}", key).into_bytes();
        let value: Vec<u8> = (0..self.value_size).map(|_| rng.gen()).collect();

        WorkItem::Write(key_bytes, value)
    }

    fn execute(&self, mvcc: &MVCC<LsmTree>, item: &WorkItem) -> Result<()> {
        match item {
            WorkItem::Write(key, value) => {
                let txn = mvcc.begin()?;
                txn.set(key, value.clone())?;
                txn.commit()?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "KVWrite"
    }
}

struct KVRead {
    keys: u64,
}

impl Workload for KVRead {
    fn prepare(&self, mvcc: &MVCC<LsmTree>, _rng: &mut StdRng) -> Result<()> {
        const BATCH_SIZE: u64 = 100;
        for batch_start in (0..self.keys).step_by(BATCH_SIZE as usize) {
            let txn = mvcc.begin()?;
            let batch_end = (batch_start + BATCH_SIZE).min(self.keys);

            for i in batch_start..batch_end {
                let key = format!("key:{:016}", i).into_bytes();
                let value = format!("value:{}", i).into_bytes();
                txn.set(&key, value)?;
            }

            txn.commit()?;
        }

        Ok(())
    }

    fn generate(&self, rng: &mut StdRng) -> WorkItem {
        let key = rng.gen_range(0..self.keys);
        let key_bytes = format!("key:{:016}", key).into_bytes();
        WorkItem::Read(key_bytes)
    }

    fn execute(&self, mvcc: &MVCC<LsmTree>, item: &WorkItem) -> Result<()> {
        match item {
            WorkItem::Read(key) => {
                let txn = mvcc.begin_read_only()?;
                let _ = txn.get(key)?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn name(&self) -> &str {
        "KVRead"
    }
}

struct TxnBank {
    accounts: u64,
    initial_balance: u64,
}

impl Workload for TxnBank {
    fn prepare(&self, mvcc: &MVCC<LsmTree>, _rng: &mut StdRng) -> Result<()> {
        let txn = mvcc.begin()?;
        for i in 0..self.accounts {
            let key = format!("account:{:08}", i).into_bytes();
            let value = self.initial_balance.to_be_bytes().to_vec();
            txn.set(&key, value)?;
        }
        txn.commit()?;

        Ok(())
    }

    fn generate(&self, rng: &mut StdRng) -> WorkItem {
        let from = rng.gen_range(0..self.accounts);
        let mut to = rng.gen_range(0..self.accounts);

        while to == from {
            to = rng.gen_range(0..self.accounts);
        }

        let amount = rng.gen_range(1..=100);
        WorkItem::Transfer(from, to, amount)
    }

    fn execute(&self, mvcc: &MVCC<LsmTree>, item: &WorkItem) -> Result<()> {
        match item {
            WorkItem::Transfer(from, to, amount) => {
                let txn = mvcc.begin()?;

                let from_key = format!("account:{:08}", from).into_bytes();
                let to_key = format!("account:{:08}", to).into_bytes();

                let from_balance = txn
                    .get(&from_key)?
                    .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);

                let to_balance = txn
                    .get(&to_key)?
                    .map(|v| u64::from_be_bytes(v.try_into().unwrap()))
                    .unwrap_or(0);

                if from_balance < *amount {
                    return Ok(());
                }

                let new_from = from_balance - amount;
                let new_to = to_balance + amount;

                txn.set(&from_key, new_from.to_be_bytes().to_vec())?;
                txn.set(&to_key, new_to.to_be_bytes().to_vec())?;

                txn.commit()?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    fn verify(&self, mvcc: &MVCC<LsmTree>) -> Result<()> {
        let txn = mvcc.begin_read_only()?;
        let mut total_balance = 0u64;
        let mut found_accounts = 0;

        for i in 0..self.accounts {
            let key = format!("account:{:08}", i).into_bytes();
            if let Some(value) = txn.get(&key)? {
                let balance = u64::from_be_bytes(value.try_into().unwrap());
                total_balance += balance;
                found_accounts += 1;
            }
        }

        let expected = self.accounts * self.initial_balance;
        if total_balance != expected {
            return Err(ashdb::Error::InvalidData(format!(
                "Balance mismatch: expected {}, got {} (found {} of {} accounts)",
                expected, total_balance, found_accounts, self.accounts
            )));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "TxnBank"
    }
}

#[derive(Parser)]
#[command(name = "workload")]
#[command(about = "AshDB workload benchmarking tool", long_about = None)]
struct Cli {
    /// Number of concurrent workers
    #[arg(short, long, default_value = "1")]
    concurrency: usize,

    /// Total number of transactions to execute
    #[arg(short = 'n', long)]
    count: Option<u64>,

    /// Duration to run (in seconds)
    #[arg(short, long)]
    duration: Option<u64>,

    /// Random seed for reproducibility
    #[arg(short, long, default_value = "42")]
    seed: u64,

    /// Database path
    #[arg(short, long, default_value = "/tmp/ashdb-bench")]
    path: String,

    #[command(subcommand)]
    workload: WorkloadCommand,
}

#[derive(Subcommand)]
enum WorkloadCommand {
    /// Write key-value pairs
    Kvwrite {
        /// Number of unique keys
        #[arg(long, default_value = "10000")]
        keys: u64,

        /// Size of each value in bytes
        #[arg(long, default_value = "128")]
        value_size: usize,

        /// Use sequential key generation
        #[arg(long)]
        sequential: bool,
    },

    /// Read key-value pairs
    Kvread {
        /// Number of keys to populate
        #[arg(long, default_value = "1000")]
        keys: u64,
    },

    /// Simulate bank transfers
    Txnbank {
        /// Number of bank accounts
        #[arg(long, default_value = "100")]
        accounts: u64,

        /// Initial balance per account
        #[arg(long, default_value = "1000")]
        initial_balance: u64,
    },
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    let runner = Runner {
        concurrency: cli.concurrency,
        count: cli.count,
        duration: cli.duration.map(Duration::from_secs),
        seed: cli.seed,
        path: cli.path,
    };

    let runner = if runner.count.is_none() && runner.duration.is_none() {
        Runner {
            count: Some(1000),
            ..runner
        }
    } else {
        runner
    };

    match cli.workload {
        WorkloadCommand::Kvwrite {
            keys,
            value_size,
            sequential,
        } => {
            let workload = KVWrite {
                keys,
                value_size,
                sequential,
            };
            runner.run(workload)?;
        }
        WorkloadCommand::Kvread { keys } => {
            let workload = KVRead { keys };
            runner.run(workload)?;
        }
        WorkloadCommand::Txnbank {
            accounts,
            initial_balance,
        } => {
            let workload = TxnBank {
                accounts,
                initial_balance,
            };
            runner.run(workload)?;
        }
    }

    Ok(())
}
