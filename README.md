# AshDB

## Introduction

AshDB is an SQLite-inspired, embedded database for time-series data.

## Motivation

The project originated from the need for a database system that combines the
strengths of TimescaleDB's time-series optimization with the simplicity and
portability of SQLite. By embedding this database directly into an application,
AshDB eliminates the need for separate database servers while still offering
scalable and performant data storage.

## Architecture

AshDB is being developed in modular phases, with its architecture inspired by a
layered design:

### 1. LSM-Based Key-Value Store

- **Core Component**: A custom implementation of a key-value store using an LSM
  tree for efficient writes and compactions.
- **Features**:
  - Active and frozen Memtables to manage data in memory.
  - SSTables for durable, sorted, on-disk storage.
  - Optimized compaction and merging logic.
- **Status**: Active development, with Memtable management and basic key-value
  operations implemented.

### 2. SQL Query Engine (Planned)

An extensible SQL engine will allow structured querying of data. This will
include support for relational operations and custom extensions for time-series
queries.

### 3. MVCC Transactions (Planned)

To provide robust data consistency, Multi-Version Concurrency Control (MVCC)
will enable safe concurrent reads and writes, supporting complex transactional
workflows.

## Current Implementation

The project has made significant progress in implementing the LSM tree-based
key-value store:

- **Memtable Management**:
  - An active Memtable handles in-memory writes until a size threshold is
    reached.
  - Memtables are frozen and replaced with new ones as they fill up, ensuring
    write operations continue seamlessly.
- **Scanning and Querying**:
  - Support for range and prefix scans across active and frozen Memtables.
  - Data merging logic ensures that results are correctly ordered.
- **Write-Ahead Log (WAL)**:
  - Logs all changes to disk for durability and crash recovery.
- **Test Coverage**:
  - Comprehensive tests ensure correctness of operations like `put`, `get`,
    `scan`, and Memtable freezing.

## Roadmap

1. Finalize the LSM tree by implementing durable SSTable storage and compaction.
2. Introduce a SQL query layer to provide relational access to stored data.
3. Integrate time-series-specific optimizations such as hypertables and
   time-chunking.
4. Add MVCC-based transactions to support concurrent operations.

---

AshDB is a work in progress, and its modular design ensures each component is
robust before moving to the next phase. If you're interested in following the
journey or contributing, stay tuned!
