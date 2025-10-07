use itertools as _;
use std::collections::BTreeSet;
use std::ops::{Bound, RangeBounds};
use std::{borrow::Cow, sync::Arc};

use itertools::Itertools as _;
use serde::{Deserialize, Serialize};

use crate::encoding::{self, bincode, Key as _, Value as _};
use crate::error::Result;
use crate::{errdata, errinput, Error};

use crate::store::Store;

/// An MVCC version represents a logical timestamp. Each version belongs to a
/// separate read/write transaction. The latest version is incremented when a
/// new read-write transaction begins.
pub type Version = u64;

impl encoding::Value for Version {}

/// MVCC keys, using the Keycode encoding which preserves the ordering and
/// grouping of keys.
///
/// Cow byte slices allow encoding borrowed values and decoding owned values.
#[derive(Debug, Deserialize, Serialize)]
enum Key<'a> {
    /// The next available version.
    NextVersion,
    /// Active (uncommitted) transactions by version.
    TxnActive(Version),
    /// A snapshot of the active set at each version. Only written for
    /// versions where the active set is non-empty (excluding itself).
    TxnActiveSnapshot(Version),
    /// A versioned key/value pair.
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
        Version,
    ),
    /// Keeps track of all keys written to by an active transaction (identified
    /// by its version), in case it needs to roll back.
    TxnWrite(
        Version,
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
}

impl<'a> encoding::Key<'a> for Key<'a> {}

/// MVCC key prefixes, for prefix scans. These must match the keys above,
/// including the enum variant index.
#[derive(Debug, Deserialize, Serialize)]
enum KeyPrefix<'a> {
    NextVersion,
    TxnActive,
    TxnActiveSnapshot,
    Version(
        #[serde(with = "serde_bytes")]
        #[serde(borrow)]
        Cow<'a, [u8]>,
    ),
    TxnWrite(Version),
}

impl<'a> encoding::Key<'a> for KeyPrefix<'a> {}

/// An MVCC-based transactional key-value engine. It wraps an underlying storage
/// engine that's used for raw key/value storage.
///
/// While it supports any number of concurrent transactions, individual read or
/// write operations are executed sequentially, serialized via a mutex. There
/// are two reasons for this: the storage engine itself is not thread-safe,
/// requiring serialized access, and the Raft state machine that manages the
/// MVCC engine applies commands one at a time from the Raft log, which will
/// serialize them anyway.
pub struct MVCC<S: Store> {
    pub store: Arc<S>,
}

impl<S: Store> MVCC<S> {
    pub fn new(store: S) -> Self {
        Self {
            store: Arc::new(store),
        }
    }

    // Begins a new write-read transaction
    pub fn begin(&self) -> Result<Transaction<S>> {
        Transaction::begin(self.store.clone())
    }

    /// Begins a new read-only transaction at the latest version.
    pub fn begin_read_only(&self) -> Result<Transaction<S>> {
        Transaction::begin_read_only(self.store.clone(), None)
    }

    /// Begins a new read-only transaction as of the given version.
    pub fn begin_as_of(&self, version: Version) -> Result<Transaction<S>> {
        Transaction::begin_read_only(self.store.clone(), Some(version))
    }
}

#[derive(Clone)]
pub struct TransactionState {
    read_only: bool,
    active: BTreeSet<Version>,
    version: Version,
}

impl TransactionState {
    /// Checks whether the given version is visible to this transaction.
    ///
    /// Future versions, and versions belonging to active transactions as of
    /// the start of this transaction, are never visible.
    ///
    /// Read-write transactions see their own writes at their version.
    ///
    /// Read-only queries only see versions below the transaction's version,
    /// excluding the version itself. This is to ensure time-travel queries see
    /// a consistent version both before and after any active transaction at
    /// that version commits its writes. See the module documentation for
    /// details.
    fn is_visible(&self, version: Version) -> bool {
        if self.active.contains(&version) {
            false
        } else if self.read_only {
            version < self.version
        } else {
            version <= self.version
        }
    }
}

pub struct Transaction<S: Store> {
    store: Arc<S>,
    state: TransactionState,
}

impl<S: Store> Transaction<S> {
    /// Begins a new transaction in read-write mode. This will allocate a new
    /// version that the transaction can write at, add it to the active set, and
    /// record its active snapshot for time-travel queries.
    pub fn begin(store: Arc<S>) -> Result<Self> {
        // Allocate a new version to write at.
        let version = match store.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => 1,
        };

        store.set(&Key::NextVersion.encode(), (version + 1).encode())?;

        let active = Self::scan_active(store.clone())?;
        if !active.is_empty() {
            store.set(&Key::TxnActiveSnapshot(version).encode(), active.encode())?
        }

        store.set(&Key::TxnActive(version).encode(), vec![])?;

        Ok(Self {
            store,
            state: TransactionState {
                version,
                read_only: false,
                active: active.clone(),
            },
        })
    }

    /// Begins a new read-only transaction. If version is given it will see the
    /// state as of the beginning of that version (ignoring writes at that
    /// version). In other words, it sees the same state as the read-write
    /// transaction at that version saw when it began.
    pub fn begin_read_only(store: Arc<S>, as_of: Option<Version>) -> Result<Self> {
        let mut version = match store.get(&Key::NextVersion.encode())? {
            Some(ref v) => Version::decode(v)?,
            None => 1,
        };

        let active = match as_of {
            Some(as_of_version) => {
                if as_of_version >= version {
                    return errinput!("version {as_of_version} does not exist");
                }
                version = as_of_version;
                match store.get(&Key::TxnActiveSnapshot(version).encode())? {
                    Some(value) => BTreeSet::<Version>::decode(&value)?,
                    None => BTreeSet::new(),
                }
            }
            None => Self::scan_active(store.clone())?,
        };

        Ok(Self {
            store,
            state: TransactionState {
                version,
                active,
                read_only: true,
            },
        })
    }

    /// Fetches the set of currently active transactions.
    fn scan_active(store: Arc<S>) -> Result<BTreeSet<Version>> {
        let mut active = BTreeSet::new();

        let prefix = KeyPrefix::TxnActive.encode();
        let mut scan = store.scan_prefix(&prefix);
        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnActive(version) => active.insert(version),
                key => return errdata!("expected TxnActive key, got {key:?}"),
            };
        }
        Ok(active)
    }

    /// Commits the transaction, by removing it from the active set. This will
    /// immediately make its writes visible to subsequent transactions. Also
    /// removes its TxnWrite records, which are no longer needed.
    ///
    /// Unlike toydb's implementation, we call sync() to ensure durability since
    /// we don't rely on a Raft log for persistence. Without Raft, the MVCC
    /// layer must ensure writes are flushed to disk.
    pub fn commit(self) -> Result<()> {
        if self.state.read_only {
            return Ok(());
        }

        let store = self.store;

        let remove: Vec<_> = store
            .scan_prefix(&KeyPrefix::TxnWrite(self.state.version).encode())
            .map_ok(|(k, _)| k)
            .try_collect()?;

        for key in remove {
            store.delete(&key)?
        }
        store.delete(&Key::TxnActive(self.state.version).encode())?;

        store.sync()?;

        Ok(())
    }

    /// Rolls back the transaction, by undoing all written versions and removing
    /// it from the active set. The active set snapshot is left behind, since
    /// this is needed for time travel queries at this version.
    pub fn rollback(self) -> Result<()> {
        if self.state.read_only {
            return Ok(());
        }

        let mut rollback = Vec::new();

        let prefix = KeyPrefix::TxnWrite(self.state.version).encode();
        let mut scan = self.store.scan_prefix(&prefix);

        while let Some((key, _)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::TxnWrite(_, user_key) => {
                    // Add the Version record for deletion
                    rollback.push(Key::Version(user_key, self.state.version).encode());
                }
                key => return errdata!("expected TxnWrite,got {key:?}"),
            };
            // Add the TxnWrite record for deletion
            rollback.push(key);
        }
        drop(scan);

        for key in rollback.into_iter() {
            self.store.delete(&key)?
        }

        // Remove from active set
        self.store
            .delete(&Key::TxnActive(self.state.version).encode())?;

        Ok(())
    }

    /// Sets a value for a key.
    pub fn set(&self, key: &[u8], value: Vec<u8>) -> Result<()> {
        self.write_version(key, Some(value))
    }

    /// Deletes a key.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_version(key, None)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        // Scan from oldest to newest version in natural order
        let from = Key::Version(key.into(), 0).encode();
        let to = Key::Version(key.into(), self.state.version).encode();
        // Reverse to iterate newest to oldest
        let mut scan = self.store.scan(from..=to).rev();
        while let Some((key, value)) = scan.next().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if self.state.is_visible(version) {
                        return bincode::deserialize(&value);
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            };
        }
        Ok(None)
    }

    /// Scans keys in the given range, returning an iterator over visible key-value pairs.
    ///
    /// The scan returns only keys that are visible to this transaction according
    /// to MVCC visibility rules. For each key, only the latest visible version
    /// is returned. Deleted keys (tombstones) are automatically filtered out.
    pub fn scan(&self, range: impl RangeBounds<Vec<u8>>) -> ScanIterator<'_> {
        // Convert user key range to MVCC key range
        let start = match range.start_bound() {
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), 0).encode()),
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), u64::MAX).encode()),
            Bound::Unbounded => Bound::Included(Key::Version(vec![].into(), 0).encode()),
        };

        let end = match range.end_bound() {
            Bound::Included(k) => Bound::Included(Key::Version(k.into(), u64::MAX).encode()),
            Bound::Excluded(k) => Bound::Excluded(Key::Version(k.into(), 0).encode()),
            Bound::Unbounded => Bound::Excluded(KeyPrefix::TxnWrite(0).encode()),
        };

        let store_iter = self.store.scan((start, end));
        VersionIterator::new(self.state.clone(), Box::new(store_iter))
    }

    /// Writes a new version for a key at the transaction's version. None writes
    /// a deletion tombstone. If a write conflict is found (either a newer or
    /// uncommitted version), a serialization error is returned.  Replacing our
    /// own uncommitted write is fine.
    fn write_version(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<()> {
        if self.state.read_only {
            return Err(Error::ReadOnly);
        }

        // Check for write conflicts, i.e. if the latest key is invisible to us
        // (either a newer version, or an uncommitted version in our past). We
        // can only conflict with the latest key, since all transactions enforce
        // the same invariant.
        let from = Key::Version(
            key.into(),
            self.state
                .active
                .first()
                .copied()
                .unwrap_or(self.state.version + 1),
        )
        .encode();

        let to = Key::Version(key.into(), u64::MAX).encode();

        // Scan in natural order then reverse to get the newest version first
        if let Some((key, _)) = self.store.scan(from..=to).next_back().transpose()? {
            match Key::decode(&key)? {
                Key::Version(_, version) => {
                    if !self.state.is_visible(version) {
                        return Err(Error::Serialization);
                    }
                }
                key => return errdata!("expected Key::Version got {key:?}"),
            }
        }

        // Write the new version and its write record.
        //
        // NB: TxnWrite contains the provided user key, not the encoded engine
        // key, since we can construct the engine key using the version.
        self.store.set(
            &Key::TxnWrite(self.state.version, key.into()).encode(),
            vec![],
        )?;

        self.store.set(
            &Key::Version(key.into(), self.state.version).encode(),
            bincode::serialize(&value),
        )?;

        Ok(())
    }
}

/// Iterator adapter that filters and decodes MVCC versioned keys for a transaction.
///
/// Following the same pattern as `RangeFilter`, this is a generic iterator adapter that:
/// - Decodes MVCC `Key::Version(user_key, version)` entries
/// - Filters out versions that aren't visible to the transaction
/// - Deduplicates keys (keeps only the newest visible version)
/// - Skips tombstones (deleted keys)
pub struct VersionIterator<I>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
{
    txn_state: TransactionState,
    inner: std::iter::Peekable<I>,
}

impl<I> VersionIterator<I>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
{
    pub fn new(txn_state: TransactionState, inner: I) -> Self {
        Self {
            txn_state,
            inner: inner.peekable(),
        }
    }
}

impl<I> Iterator for VersionIterator<I>
where
    I: Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>,
{
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(result) = self.inner.next() {
            // Propagate errors immediately
            let (key_bytes, value_bytes) = match result {
                Ok(pair) => pair,
                Err(e) => return Some(Err(e)),
            };

            // Decode the MVCC key
            let (user_key, version) = match Key::decode(&key_bytes) {
                Ok(Key::Version(user_key, version)) => (user_key.into_owned(), version),
                Ok(key) => return Some(errdata!("expected Key::Version, got {key:?}")),
                Err(e) => return Some(Err(e)),
            };

            // Skip if this version is not visible to our transaction
            if !self.txn_state.is_visible(version) {
                continue;
            }

            // Peek ahead to see if there's a newer version of this key
            // If the next entry has the same user_key, skip this older version
            if let Some(peek_result) = self.inner.peek() {
                match peek_result {
                    Ok((next_key_bytes, _)) => {
                        match Key::decode(next_key_bytes) {
                            Ok(Key::Version(next_user_key, _)) => {
                                if next_user_key.as_ref() == user_key.as_slice() {
                                    // Next entry is a newer version of same key, skip this one
                                    continue;
                                }
                            }
                            Ok(key) => return Some(errdata!("expected Key::Version, got {key:?}")),
                            Err(e) => return Some(Err(e)),
                        }
                    }
                    Err(e) => return Some(Err(e.clone())),
                }
            }

            // This is the latest visible version of this user_key

            // Decode the value (which is bincode-encoded Option<Vec<u8>>)
            let value_option: Option<Vec<u8>> = match bincode::deserialize(&value_bytes) {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };

            // Skip tombstones (deleted keys)
            if let Some(value) = value_option {
                return Some(Ok((user_key, value)));
            }
            // If tombstone, continue to next user key
        }

        None
    }
}

/// An iterator over the latest live and visible key/value pairs for a transaction.
///
/// This is a type alias for `VersionIterator` specialized to work with boxed
/// `ScanIterator` trait objects from the store layer.
pub type ScanIterator<'a> = VersionIterator<Box<dyn crate::store::ScanIterator + Send + Sync + 'a>>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::lsm::LsmTree;
    use tempfile::TempDir;

    fn create_test_mvcc() -> (TempDir, MVCC<LsmTree>) {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let path_str = temp_dir.path().to_str().expect("Invalid path");
        let store = LsmTree::open(path_str).expect("Failed to open store");
        let mvcc = MVCC::new(store);
        (temp_dir, mvcc)
    }

    #[test]
    fn test_banking_transaction_saga() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // === ACT 1: ACCOUNT SETUP ===
        // The story begins: Alice and Bob open bank accounts
        let setup_txn = mvcc.begin()?;
        setup_txn.set(b"account:alice:balance", b"1000".to_vec())?;
        setup_txn.set(b"account:bob:balance", b"500".to_vec())?;

        // Read-write transactions can see their own writes
        assert_eq!(
            setup_txn.get(b"account:alice:balance")?,
            Some(b"1000".to_vec()),
            "Alice should be visible within the setup transaction"
        );
        setup_txn.commit()?;

        // After commit, new read-only transactions can see the data
        let verify_txn = mvcc.begin_read_only()?;
        assert_eq!(
            verify_txn.get(b"account:alice:balance")?,
            Some(b"1000".to_vec()),
            "Alice should start with $1000"
        );
        assert_eq!(
            verify_txn.get(b"account:bob:balance")?,
            Some(b"500".to_vec()),
            "Bob should start with $500"
        );

        // === ACT 2: CONCURRENT TRANSFERS BEGIN ===
        // Two tellers try to process transfers simultaneously
        // T1: Transfer $100 from Alice to Bob
        // T2: Transfer $200 from Bob to Alice
        let txn1 = mvcc.begin()?;
        let txn2 = mvcc.begin()?;

        // T1 reads Alice's balance and prepares transfer
        let alice_balance_t1 = txn1.get(b"account:alice:balance")?;
        assert_eq!(
            alice_balance_t1,
            Some(b"1000".to_vec()),
            "T1 should see Alice's initial balance"
        );

        // T2 reads Bob's balance and prepares transfer
        let bob_balance_t2 = txn2.get(b"account:bob:balance")?;
        assert_eq!(
            bob_balance_t2,
            Some(b"500".to_vec()),
            "T2 should see Bob's initial balance"
        );

        // === ACT 3: ISOLATION VERIFICATION ===
        // While both transactions are active, a third party checks balances
        let observer_txn = mvcc.begin_read_only()?;
        assert_eq!(
            observer_txn.get(b"account:alice:balance")?,
            Some(b"1000".to_vec()),
            "Observer should see original Alice balance (snapshot isolation)"
        );
        assert_eq!(
            observer_txn.get(b"account:bob:balance")?,
            Some(b"500".to_vec()),
            "Observer should see original Bob balance (snapshot isolation)"
        );

        // === ACT 4: FIRST TRANSACTION COMMITS ===
        // T1 completes: Alice sends $100 to Bob
        txn1.set(b"account:alice:balance", b"900".to_vec())?;
        txn1.set(b"account:bob:balance", b"600".to_vec())?;
        txn1.commit()?;

        // Verify T1's changes are now visible to new transactions
        let check_txn = mvcc.begin_read_only()?;
        assert_eq!(
            check_txn.get(b"account:alice:balance")?,
            Some(b"900".to_vec()),
            "Alice should have $900 after T1 commits"
        );
        assert_eq!(
            check_txn.get(b"account:bob:balance")?,
            Some(b"600".to_vec()),
            "Bob should have $600 after T1 commits"
        );

        // === ACT 5: SERIALIZATION CONFLICT ===
        // T2 tries to commit, but should detect conflict since T1 modified Bob's account
        let result = txn2.set(b"account:bob:balance", b"300".to_vec());
        assert!(
            result.is_err(),
            "T2 should fail with serialization error when trying to update Bob's balance"
        );
        assert!(
            matches!(result.unwrap_err(), Error::Serialization),
            "Error should be specifically a Serialization error"
        );

        // === ACT 6: FINAL CONSISTENCY ===
        // After the dust settles, verify only T1's changes persisted
        let final_txn = mvcc.begin_read_only()?;
        assert_eq!(
            final_txn.get(b"account:alice:balance")?,
            Some(b"900".to_vec()),
            "Alice should still have $900 (only T1's transfer succeeded)"
        );
        assert_eq!(
            final_txn.get(b"account:bob:balance")?,
            Some(b"600".to_vec()),
            "Bob should still have $600 (only T1's transfer succeeded)"
        );

        Ok(())
    }

    #[test]
    fn test_time_travelers_history() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // === ACT 1: VERSION 1 - THE BEGINNING ===
        // Alice joins the community as a member
        let v1_txn = mvcc.begin()?;
        v1_txn.set(b"user:alice:role", b"member".to_vec())?;
        let v1_version = v1_txn.state.version;

        // Read-write transaction can see its own writes
        assert_eq!(
            v1_txn.get(b"user:alice:role")?,
            Some(b"member".to_vec()),
            "Alice should be visible within v1 transaction"
        );

        v1_txn.commit()?;

        // After commit, verify version 1 state from a new transaction
        let check_v1 = mvcc.begin_read_only()?;
        assert_eq!(
            check_v1.get(b"user:alice:role")?,
            Some(b"member".to_vec()),
            "Alice should be a member after v1 commits"
        );
        assert_eq!(
            check_v1.get(b"user:bob:role")?,
            None,
            "Bob doesn't exist yet"
        );

        // === ACT 2: VERSION 2 - GROWTH ===
        // Alice gets promoted to moderator, Bob joins as member
        let v2_txn = mvcc.begin()?;
        v2_txn.set(b"user:alice:role", b"moderator".to_vec())?;
        v2_txn.set(b"user:bob:role", b"member".to_vec())?;
        let v2_version = v2_txn.state.version;
        v2_txn.commit()?;

        // Verify version 2 state
        let check_v2 = mvcc.begin_read_only()?;
        assert_eq!(
            check_v2.get(b"user:alice:role")?,
            Some(b"moderator".to_vec()),
            "Alice should be a moderator at version 2"
        );
        assert_eq!(
            check_v2.get(b"user:bob:role")?,
            Some(b"member".to_vec()),
            "Bob should be a member at version 2"
        );

        // === ACT 3: VERSION 3 - LEADERSHIP ===
        // Alice becomes admin, Bob becomes moderator
        let v3_txn = mvcc.begin()?;
        v3_txn.set(b"user:alice:role", b"admin".to_vec())?;
        v3_txn.set(b"user:bob:role", b"moderator".to_vec())?;
        v3_txn.set(b"user:charlie:role", b"member".to_vec())?;
        v3_txn.commit()?;

        // Verify current state (version 3)
        let check_v3 = mvcc.begin_read_only()?;
        assert_eq!(
            check_v3.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Alice should be admin at version 3"
        );
        assert_eq!(
            check_v3.get(b"user:bob:role")?,
            Some(b"moderator".to_vec()),
            "Bob should be moderator at version 3"
        );
        assert_eq!(
            check_v3.get(b"user:charlie:role")?,
            Some(b"member".to_vec()),
            "Charlie should be member at version 3"
        );

        // === ACT 4: TIME TRAVEL - VISITING VERSION 1 ===
        // An auditor wants to see the state as it was at version 1
        let time_travel_v1 = mvcc.begin_as_of(v1_version)?;
        assert_eq!(
            time_travel_v1.get(b"user:alice:role")?,
            None,
            "Time travel to v1: Alice's version 1 write should not be visible to as-of query (read-only sees before version)"
        );
        assert_eq!(
            time_travel_v1.get(b"user:bob:role")?,
            None,
            "Time travel to v1: Bob didn't exist before version 1"
        );
        assert_eq!(
            time_travel_v1.get(b"user:charlie:role")?,
            None,
            "Time travel to v1: Charlie didn't exist before version 1"
        );

        // === ACT 5: TIME TRAVEL - VISITING VERSION 2 ===
        // Auditor checks the state as it was at version 2
        let time_travel_v2 = mvcc.begin_as_of(v2_version)?;
        assert_eq!(
            time_travel_v2.get(b"user:alice:role")?,
            Some(b"member".to_vec()),
            "Time travel to v2: Alice should be a member (seeing version 1's write)"
        );
        assert_eq!(
            time_travel_v2.get(b"user:bob:role")?,
            None,
            "Time travel to v2: Bob didn't exist before version 2"
        );
        assert_eq!(
            time_travel_v2.get(b"user:charlie:role")?,
            None,
            "Time travel to v2: Charlie didn't exist before version 2"
        );

        // === ACT 6: ACTIVE TRANSACTION VISIBILITY ===
        // Start version 4 transaction but don't commit
        let v4_txn = mvcc.begin()?;
        let v4_version = v4_txn.state.version;
        v4_txn.set(b"user:alice:role", b"superadmin".to_vec())?;
        // Don't commit yet!

        // A time-travel query at version 4 should NOT see version 4's uncommitted writes
        let time_travel_v4 = mvcc.begin_as_of(v4_version)?;
        assert_eq!(
            time_travel_v4.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Time travel to v4: Should see version 3's write (not v4's uncommitted write)"
        );

        // Current readers should also not see v4's uncommitted changes
        let concurrent_reader = mvcc.begin_read_only()?;
        assert_eq!(
            concurrent_reader.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Concurrent reader should see version 3's committed state (not v4's uncommitted)"
        );

        // === ACT 7: AFTER COMMIT, HISTORY IS COMPLETE ===
        v4_txn.commit()?;

        // Now version 4's changes are visible
        let final_check = mvcc.begin_read_only()?;
        assert_eq!(
            final_check.get(b"user:alice:role")?,
            Some(b"superadmin".to_vec()),
            "After v4 commits, Alice should be superadmin"
        );

        // But time travel still works to see old versions
        let replay_v2 = mvcc.begin_as_of(v2_version)?;
        assert_eq!(
            replay_v2.get(b"user:alice:role")?,
            Some(b"member".to_vec()),
            "Time travel to v2 still shows Alice as member (history preserved)"
        );

        Ok(())
    }

    #[test]
    fn test_rollback_recovery() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // === ACT 1: BASELINE STATE ===
        // Establish a stable baseline with two users
        let setup_txn = mvcc.begin()?;
        setup_txn.set(b"user:alice:role", b"admin".to_vec())?;
        setup_txn.set(b"user:bob:role", b"member".to_vec())?;

        // Verify within transaction
        assert_eq!(
            setup_txn.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Alice should be visible within setup transaction"
        );

        setup_txn.commit()?;

        // Verify baseline after commit
        let verify_baseline = mvcc.begin_read_only()?;
        assert_eq!(
            verify_baseline.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Baseline: Alice is admin"
        );
        assert_eq!(
            verify_baseline.get(b"user:bob:role")?,
            Some(b"member".to_vec()),
            "Baseline: Bob is member"
        );

        // === ACT 2: BATCH IMPORT BEGINS ===
        // System admin starts importing 5 new users
        let import_txn = mvcc.begin()?;
        import_txn.set(b"user:charlie:role", b"member".to_vec())?;
        import_txn.set(b"user:david:role", b"member".to_vec())?;
        import_txn.set(b"user:eve:role", b"moderator".to_vec())?;
        import_txn.set(b"user:frank:role", b"member".to_vec())?;
        import_txn.set(b"user:grace:role", b"member".to_vec())?;

        // === ACT 3: ISOLATION DURING IMPORT ===
        // While import is in progress, other transactions shouldn't see uncommitted data
        let concurrent_check = mvcc.begin_read_only()?;
        assert_eq!(
            concurrent_check.get(b"user:charlie:role")?,
            None,
            "Charlie should not be visible during uncommitted import"
        );
        assert_eq!(
            concurrent_check.get(b"user:eve:role")?,
            None,
            "Eve should not be visible during uncommitted import"
        );
        // But original data should still be visible
        assert_eq!(
            concurrent_check.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Alice should still be visible during import"
        );

        // === ACT 4: ERROR & ROLLBACK ===
        // Import encounters an error and needs to rollback
        import_txn.rollback()?;

        // === ACT 5: CLEAN SLATE VERIFICATION ===
        // After rollback, all imported users should be gone
        let post_rollback = mvcc.begin_read_only()?;
        assert_eq!(
            post_rollback.get(b"user:charlie:role")?,
            None,
            "Charlie should not exist after rollback"
        );
        assert_eq!(
            post_rollback.get(b"user:david:role")?,
            None,
            "David should not exist after rollback"
        );
        assert_eq!(
            post_rollback.get(b"user:eve:role")?,
            None,
            "Eve should not exist after rollback"
        );
        assert_eq!(
            post_rollback.get(b"user:frank:role")?,
            None,
            "Frank should not exist after rollback"
        );
        assert_eq!(
            post_rollback.get(b"user:grace:role")?,
            None,
            "Grace should not exist after rollback"
        );

        // Original data should still be intact
        assert_eq!(
            post_rollback.get(b"user:alice:role")?,
            Some(b"admin".to_vec()),
            "Alice should still be admin after rollback (baseline preserved)"
        );
        assert_eq!(
            post_rollback.get(b"user:bob:role")?,
            Some(b"member".to_vec()),
            "Bob should still be member after rollback (baseline preserved)"
        );

        // === ACT 6: SUCCESSFUL RETRY ===
        // Try again with a smaller batch that succeeds
        // Note: Charlie, David, and Eve were in the rolled-back batch, so they're safe to retry
        let retry_txn = mvcc.begin()?;
        retry_txn.set(b"user:charlie:role", b"member".to_vec())?;
        retry_txn.set(b"user:david:role", b"member".to_vec())?;
        retry_txn.set(b"user:eve:role", b"moderator".to_vec())?;
        retry_txn.commit()?;

        // Verify successful import
        let final_check = mvcc.begin_read_only()?;
        assert_eq!(
            final_check.get(b"user:charlie:role")?,
            Some(b"member".to_vec()),
            "Charlie should exist after successful import"
        );
        assert_eq!(
            final_check.get(b"user:david:role")?,
            Some(b"member".to_vec()),
            "David should exist after successful import"
        );
        assert_eq!(
            final_check.get(b"user:eve:role")?,
            Some(b"moderator".to_vec()),
            "Eve should exist after successful import"
        );
        // Failed users should still not exist
        assert_eq!(
            final_check.get(b"user:frank:role")?,
            None,
            "Frank should not exist (was only in failed import)"
        );
        assert_eq!(
            final_check.get(b"user:grace:role")?,
            None,
            "Grace should not exist (was only in failed import)"
        );

        Ok(())
    }

    #[test]
    fn test_scan_basic() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Setup: Create some keys
        let txn = mvcc.begin()?;
        txn.set(b"apple", b"red".to_vec())?;
        txn.set(b"banana", b"yellow".to_vec())?;
        txn.set(b"cherry", b"red".to_vec())?;
        txn.set(b"date", b"brown".to_vec())?;
        txn.commit()?;

        // Scan all keys
        let read_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = read_txn.scan(..).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 4);
        assert_eq!(pairs[0], (b"apple".to_vec(), b"red".to_vec()));
        assert_eq!(pairs[1], (b"banana".to_vec(), b"yellow".to_vec()));
        assert_eq!(pairs[2], (b"cherry".to_vec(), b"red".to_vec()));
        assert_eq!(pairs[3], (b"date".to_vec(), b"brown".to_vec()));

        Ok(())
    }

    #[test]
    fn test_scan_range_bounds() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Setup
        let txn = mvcc.begin()?;
        txn.set(b"a", b"1".to_vec())?;
        txn.set(b"b", b"2".to_vec())?;
        txn.set(b"c", b"3".to_vec())?;
        txn.set(b"d", b"4".to_vec())?;
        txn.set(b"e", b"5".to_vec())?;
        txn.commit()?;

        let read_txn = mvcc.begin_read_only()?;

        // Test inclusive range
        let results: Result<Vec<_>> = read_txn.scan(b"b".to_vec()..=b"d".to_vec()).collect();
        let pairs = results?;
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"b");
        assert_eq!(pairs[1].0, b"c");
        assert_eq!(pairs[2].0, b"d");

        // Test exclusive range
        let results: Result<Vec<_>> = read_txn.scan(b"b".to_vec()..b"d".to_vec()).collect();
        let pairs = results?;
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"b");
        assert_eq!(pairs[1].0, b"c");

        // Test from start
        let results: Result<Vec<_>> = read_txn.scan(..=b"c".to_vec()).collect();
        let pairs = results?;
        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0].0, b"a");
        assert_eq!(pairs[2].0, b"c");

        Ok(())
    }

    #[test]
    fn test_scan_tombstone_filtering() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Setup: Create keys then delete some
        let txn = mvcc.begin()?;
        txn.set(b"key1", b"value1".to_vec())?;
        txn.set(b"key2", b"value2".to_vec())?;
        txn.set(b"key3", b"value3".to_vec())?;
        txn.commit()?;

        // Delete key2
        let txn2 = mvcc.begin()?;
        txn2.delete(b"key2")?;
        txn2.commit()?;

        // Scan should not return deleted key
        let read_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = read_txn.scan(..).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[0].0, b"key1");
        assert_eq!(pairs[1].0, b"key3");
        // key2 should be filtered out

        Ok(())
    }

    #[test]
    fn test_scan_version_deduplication() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Create initial version
        let txn1 = mvcc.begin()?;
        txn1.set(b"user:alice", b"v1".to_vec())?;
        txn1.commit()?;

        // Update to version 2
        let txn2 = mvcc.begin()?;
        txn2.set(b"user:alice", b"v2".to_vec())?;
        txn2.commit()?;

        // Update to version 3
        let txn3 = mvcc.begin()?;
        txn3.set(b"user:alice", b"v3".to_vec())?;
        txn3.commit()?;

        // Scan should return only the latest version
        let read_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = read_txn.scan(..).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"user:alice");
        assert_eq!(pairs[0].1, b"v3"); // Should be latest version only

        Ok(())
    }

    #[test]
    fn test_scan_visibility_filtering() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Version 1: Create initial data
        let txn1 = mvcc.begin()?;
        txn1.set(b"alice", b"v1".to_vec())?;
        let v1_version = txn1.state.version;
        txn1.commit()?;

        // Version 2: Update alice
        let txn2 = mvcc.begin()?;
        txn2.set(b"alice", b"v2".to_vec())?;
        txn2.commit()?;

        // Time travel to version 1 - should not see version 1's write
        let time_travel_txn = mvcc.begin_as_of(v1_version)?;
        let results: Result<Vec<_>> = time_travel_txn.scan(..).collect();
        let pairs = results?;

        // Should see nothing (read-only sees before the version)
        assert_eq!(pairs.len(), 0);

        Ok(())
    }

    #[test]
    fn test_scan_active_transaction_isolation() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Setup baseline
        let txn1 = mvcc.begin()?;
        txn1.set(b"key1", b"committed".to_vec())?;
        txn1.commit()?;

        // Start an active transaction that writes but doesn't commit
        let active_txn = mvcc.begin()?;
        active_txn.set(b"key2", b"uncommitted".to_vec())?;
        // Don't commit yet!

        // A concurrent reader should not see uncommitted writes
        let reader_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = reader_txn.scan(..).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].0, b"key1");
        assert_eq!(pairs[0].1, b"committed");
        // key2 should not be visible

        // Active transaction should see its own write
        let self_results: Result<Vec<_>> = active_txn.scan(..).collect();
        let self_pairs = self_results?;

        assert_eq!(self_pairs.len(), 2);
        assert_eq!(self_pairs[0].0, b"key1");
        assert_eq!(self_pairs[1].0, b"key2");
        assert_eq!(self_pairs[1].1, b"uncommitted");

        Ok(())
    }

    #[test]
    fn test_scan_empty_range() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Setup
        let txn = mvcc.begin()?;
        txn.set(b"a", b"1".to_vec())?;
        txn.set(b"z", b"26".to_vec())?;
        txn.commit()?;

        // Scan a range with no keys
        let read_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = read_txn.scan(b"m".to_vec()..b"n".to_vec()).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 0);

        Ok(())
    }

    #[test]
    fn test_scan_multiple_keys_mixed_operations() -> Result<()> {
        let (_temp_dir, mvcc) = create_test_mvcc();

        // Complex scenario with creates, updates, and deletes
        let txn1 = mvcc.begin()?;
        txn1.set(b"key1", b"v1".to_vec())?;
        txn1.set(b"key2", b"v1".to_vec())?;
        txn1.set(b"key3", b"v1".to_vec())?;
        txn1.commit()?;

        let txn2 = mvcc.begin()?;
        txn2.set(b"key2", b"v2_updated".to_vec())?; // Update
        txn2.delete(b"key3")?; // Delete
        txn2.set(b"key4", b"v2_new".to_vec())?; // New key
        txn2.commit()?;

        // Scan should show: key1(v1), key2(v2_updated), key4(v2_new)
        let read_txn = mvcc.begin_read_only()?;
        let results: Result<Vec<_>> = read_txn.scan(..).collect();
        let pairs = results?;

        assert_eq!(pairs.len(), 3);
        assert_eq!(pairs[0], (b"key1".to_vec(), b"v1".to_vec()));
        assert_eq!(pairs[1], (b"key2".to_vec(), b"v2_updated".to_vec()));
        assert_eq!(pairs[2], (b"key4".to_vec(), b"v2_new".to_vec()));

        Ok(())
    }
}
