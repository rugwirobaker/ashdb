# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

AshDB is an SQLite-inspired embedded database for time-series data, built in Rust. The project implements an LSM (Log-Structured Merge) tree-based key-value store with plans to add SQL querying and MVCC transactions.

## Commands

### Build & Test
- **Build**: `cargo build`
- **Build (release)**: `cargo build --release`
- **Run tests**: `cargo test`
- **Run single test**: `cargo test test_name`
- **Run tests with output**: `cargo test -- --nocapture`

### Development
- **Check code**: `cargo check`
- **Format code**: `cargo fmt`
- **Run linter**: `cargo clippy`

### Shell tooks for common operations

**File Operations**

- Find Files: fd
- Find Text: rg (ripgrep)
- Find Code Structure (Rust): ast-grep --lang rust

**Language-Specific Search**

- For Rust code: ast-grep --lang rust -p '<pattern>'
- For TOML/config files: Use rg for text search
- For other languages in the project, set --lang appropriately

**Data Processing**

- Select among matches: pipe to fzf
- JSON: jq
- YAML/XML: yq
- Hex/Binary viewing: hexyl

### Change Rules

- Before making any change plan it out before.
- List the files/modules will be affected before hand.
- Run `cargo check`, `cargo clippy`, `cargo fmt` in that order.
- Make sure we have tests covering this change.

## Architecture

### Core Components

**LSM Store** (`src/store/lsm.rs`)
- Main storage engine implementing the `Store` trait
- Manages active and frozen memtables, SSTable levels, and the manifest log
- Uses file locking (`ashdb.lock`) to ensure single-process access
- Recovery: replays manifest log to restore SSTable state and WAL files to restore memtables
- When active memtable reaches `MAX_MEMTABLE_SIZE` (64MB), it's frozen and a new one is created

**Memtable** (`src/memtable.rs`)
- In-memory sorted structure using `crossbeam-skiplist::SkipMap`
- Each memtable has its own WAL file for durability
- Frozen memtables are flushed to SSTables (Level 0)
- Interior mutability: can freeze atomically via `AtomicBool`

**WAL (Write-Ahead Log)** (`src/wal/`)
- Binary log format with header and checksummed entries
- One WAL file per memtable (in `wal/` subdirectory)
- Format: `[key_len:u32][key][value_len:u32][value][checksum:u64]`
- WAL files are named by ID (e.g., `0.wal`, `1.wal`)
- Removed when memtable is fully flushed to disk

**Manifest** (`src/manifest/`)
- Tracks metadata for SSTables across all levels
- Records operations: `AddTable`, `DeleteTable` with operation types (Flush, Compaction)
- Uses big-endian encoding for portability
- Format: `[length:u32][record_bytes][checksum:u64]`

**SSTable** (`src/sstable/`)
- Sorted String Table for durable on-disk storage (currently in development)
- Components: blocks, index, table
- SSTables are named by ID (e.g., `0.sst`, `1.sst`)

### Data Flow

**Write path**:
1. Write to active memtable's WAL (durability)
2. Insert into active memtable's skiplist
3. If memtable size >= 64MB, freeze it atomically and create new active memtable with new WAL
4. Frozen memtables eventually flush to Level 0 SSTables

**Read path**:
1. Check active memtable
2. Check frozen memtables (newest to oldest)
3. Check SSTables (currently not fully implemented)

**Recovery**:
1. Acquire file lock on `ashdb.lock`
2. Replay manifest log to restore SSTable metadata and levels
3. Recover memtables from WAL directory (all but last become frozen, last becomes active)

### Key Design Patterns

- **Big-endian encoding**: Used throughout for cross-platform compatibility (manifest, WAL)
- **Checksums**: CRC64 checksums on WAL entries and manifest records using `crc64fast`
- **Merge iteration**: `LSMScanIterator` uses a binary heap to merge sorted iterators, deduplicating keys (newer values win)
- **Lazy evaludation**: All iterators are lazily evaluated reducing space(memory) amplification especially on read heavy workloads.
- **Lock-free reads**: Memtable uses `crossbeam-skiplist` for concurrent reads
- **Atomic freezing**: Memtable freeze operation is atomic via `AtomicBool`

## Important Conventions

- Use `?` for error propagation with the custom `Result<T>` type (defined in `src/error.rs`)
- Test utilities use `tempfile::TempDir` for isolated test environments
- Memtable size tracking uses `AtomicUsize` with `SeqCst` ordering
- WAL and manifest IDs are monotonically increasing `u64` values
