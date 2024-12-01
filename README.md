# AshDB

## Introduction

AshDB is an SQLite-inspired, embedded database for time-series data.

## Motivation

I've had an obsession with building a database from scratch for quite some time
now. But with no compelling use case (SQLite is my go-to for all my personal
projects and is quite capable), I only managed to produce a bunch of half-baked
projects. Fortunately for me, I recently acquired another obsession: hardware
and the interface between my comfort zone (software) and the wild, wild world
that is hardware. For my first project(a story for another day) I need to
collect and store sensor data, and what better way than starting a side prject
for your side project? Just like that we have a use case for a database! And so
AshDB was born.

## Architecture

AshDB is being developed in modular phases, with its architecture inspired by a
layered design:

### 1. LSM-Based Key-Value Store

- [x] Memtable for buffering writes in memory.
- [x] A Write-Ahead Log (WAL) per Memtable until it is flushed to disk.
- [ ] SSTables for durable, sorted, on-disk storage.
- [ ] Key/value store interface: `put`, `get`, `scan`, `delete`.

### 2. SQL Query Engine (Planned)

An extensible SQL engine will allow structured querying of data. Just enough to
look like TimeScaleDB and SQLite had a baby.

### 3. MVCC Transactions (Planned)

Database transactions have beat my ass enough times that I decided it was time
to stop reading books and put all tha knowledge to good use.
[MVCC(Multi-Version Concurrency Control)](https://en.wikipedia.org/wiki/Multiversion_concurrency_control)
is one way to skin them that I think will play well with a Key/Value store.

## Roadmap

1. Finalize the LSM tree by implementing durable SSTable storage and compaction.
2. Introduce a SQL query layer to provide relational access to stored data.
3. Integrate time-series-specific optimizations such as hypertables and
   time-chunking.
4. Add MVCC-based transactions to support concurrent operations.

---

AshDB is a work in progress, and its modular design ensures each component can
work on it's own before we move to the next. If you're interested in following
the journey or contributing, stay tuned!
