# P0 Implementation Plan - Production Readiness

**Date:** 2025-11-13
**Goal:** Implement critical features to make zvdb production-ready
**Estimated Total Effort:** 4-6 weeks (1 developer)

---

## Executive Summary

This plan addresses the three critical gaps (P0) identified in the architecture review:

1. **B-tree Indexes** - Eliminate O(n) table scans for WHERE clauses
2. **WAL + Crash Recovery** - Ensure durability and data safety
3. **Basic Transactions** - ACID guarantees for multi-statement operations

**Dependencies:** Features must be implemented in order due to dependencies:
- WAL first (foundation for durability)
- Transactions second (requires WAL for rollback)
- Indexes can be parallel to transactions

---

## P0-1: B-tree Indexes for WHERE Performance

### Current State
- All WHERE clauses perform full table scans: O(n)
- No index structures exist
- Example: `SELECT * FROM users WHERE id = 123` scans all rows

### Target State
- B-tree indexes for fast lookups: O(log n)
- `CREATE INDEX idx_name ON table(column)` support
- Query executor automatically uses indexes when available
- Index persistence to disk

### Estimated Effort: 1.5-2 weeks

---

### Phase 1.1: B-tree Data Structure (4-5 days)

**File:** Create `src/btree.zig`

**Tasks:**
- [ ] Implement B-tree node structure
  ```zig
  pub const BTreeNode = struct {
      keys: ArrayList(ColumnValue),      // Sorted keys
      children: ArrayList(?*BTreeNode),  // Child pointers (n+1 for n keys)
      values: ArrayList(u64),            // Row IDs for leaf nodes
      is_leaf: bool,
      parent: ?*BTreeNode,
  };
  ```

- [ ] Implement B-tree operations
  - `insert(key: ColumnValue, row_id: u64)` - O(log n)
  - `search(key: ColumnValue)` → []u64 - O(log n)
  - `range(min: ColumnValue, max: ColumnValue)` → []u64 - O(log n + k)
  - `delete(key: ColumnValue, row_id: u64)` - O(log n)

- [ ] Implement node splitting/merging
  - Split when node full (> 2*ORDER keys)
  - Merge when node too empty (< ORDER keys)
  - Maintain balance invariant

- [ ] Implement persistence
  - `save(path: []const u8)` - Serialize tree to disk
  - `load(allocator, path: []const u8)` - Deserialize from disk

- [ ] Write comprehensive tests
  - Basic insert/search/delete
  - Large dataset tests (100k+ rows)
  - Range query tests
  - Persistence round-trip tests

**Acceptance Criteria:**
- All B-tree operations pass tests
- Handles all ColumnValue types (int, float, text, bool)
- Persistence works correctly

---

### Phase 1.2: Index Manager (2-3 days)

**File:** Create `src/index_manager.zig`

**Tasks:**
- [ ] Implement Index metadata structure
  ```zig
  pub const Index = struct {
      name: []const u8,
      table_name: []const u8,
      column_name: []const u8,
      btree: BTree,
      allocator: Allocator,
  };
  ```

- [ ] Add index registry to Database
  ```zig
  // database.zig
  pub const Database = struct {
      tables: StringHashMap(*Table),
      hnsw: ?*HNSW(f32),
      indexes: StringHashMap(*Index),  // NEW
      allocator: Allocator,
  };
  ```

- [ ] Implement index lifecycle
  - `createIndex(table: []const u8, column: []const u8)` - Build index
  - `dropIndex(name: []const u8)` - Remove index
  - `rebuildIndex(name: []const u8)` - Rebuild from table data

- [ ] Implement automatic index updates
  - On INSERT: Add row to relevant indexes
  - On DELETE: Remove row from relevant indexes
  - On UPDATE: Update affected indexes

- [ ] Add index persistence
  - Save/load all indexes with database
  - Index files: `{data_dir}/{table_name}.{column_name}.idx`

**Acceptance Criteria:**
- Can create/drop indexes
- Indexes update automatically on data changes
- Indexes persist across saves/loads

---

### Phase 1.3: SQL Parser for CREATE INDEX (1 day)

**File:** Modify `src/sql.zig`

**Tasks:**
- [ ] Add CREATE INDEX command structure
  ```zig
  pub const CreateIndexCmd = struct {
      index_name: []const u8,
      table_name: []const u8,
      column_name: []const u8,
  };
  ```

- [ ] Add DROP INDEX command
  ```zig
  pub const DropIndexCmd = struct {
      index_name: []const u8,
  };
  ```

- [ ] Update SqlCommand union
  ```zig
  pub const SqlCommand = union(enum) {
      create_table: CreateTableCmd,
      create_index: CreateIndexCmd,  // NEW
      drop_index: DropIndexCmd,      // NEW
      insert: InsertCmd,
      // ...
  };
  ```

- [ ] Implement parser for CREATE INDEX
  - `CREATE INDEX idx_users_age ON users(age)`
  - `CREATE UNIQUE INDEX idx_users_email ON users(email)`

- [ ] Implement parser for DROP INDEX
  - `DROP INDEX idx_users_age`

**Acceptance Criteria:**
- Parser handles CREATE/DROP INDEX syntax
- Syntax errors return helpful messages

---

### Phase 1.4: Query Executor Integration (2-3 days)

**File:** Modify `src/database.zig`

**Tasks:**
- [ ] Implement index-aware WHERE evaluation
  ```zig
  fn executeSelect() {
      // Check if WHERE clause can use an index
      if (hasIndexFor(cmd.where_column)) {
          // Use index lookup: O(log n)
          row_ids = index.search(cmd.where_value);
      } else {
          // Fall back to table scan: O(n)
          row_ids = table.getAllRows();
      }
  }
  ```

- [ ] Add query planning logic
  - Detect indexed columns in WHERE clause
  - Choose index vs table scan based on selectivity
  - Support compound WHERE with multiple indexes

- [ ] Implement range queries with indexes
  - `WHERE age > 18 AND age < 65` → index.range(18, 65)
  - `WHERE name >= "A" AND name < "M"` → index.range("A", "M")

- [ ] Add index statistics
  - Track index hit/miss rate
  - Log when indexes are used
  - Performance metrics

**Acceptance Criteria:**
- Queries automatically use indexes when available
- Performance improves from O(n) → O(log n) for indexed columns
- Falls back gracefully when no index exists

---

### Phase 1.5: Testing & Benchmarking (1-2 days)

**File:** Create `src/test_indexes.zig` and `benchmarks/index_benchmarks.zig`

**Tasks:**
- [ ] Write integration tests
  - Create table → Create index → Insert data → Query with WHERE
  - Verify correct results with/without index
  - Test index updates on INSERT/DELETE/UPDATE
  - Test index persistence

- [ ] Write performance benchmarks
  - Compare table scan vs index lookup
  - Test with 10k, 100k, 1M rows
  - Measure query time improvement

- [ ] Document performance characteristics
  - Index build time: O(n log n)
  - Index lookup time: O(log n)
  - Index space overhead: O(n)

**Acceptance Criteria:**
- 10x+ speedup for indexed queries on 100k+ rows
- All tests pass
- Documentation updated

---

## P0-2: Write-Ahead Logging (WAL) + Crash Recovery

### Current State
- No crash recovery
- Data lost if process crashes between writes
- Auto-save only on clean shutdown

### Target State
- Write-Ahead Logging for all mutations
- Crash recovery replays WAL on startup
- Guaranteed durability (data safe after commit)

### Estimated Effort: 2-3 weeks

---

### Phase 2.1: WAL File Format Design (2-3 days)

**File:** Create `src/wal.zig`

**Tasks:**
- [ ] Define WAL record format
  ```zig
  pub const WalRecordType = enum(u8) {
      begin_tx,      // Transaction start
      insert_row,    // INSERT operation
      delete_row,    // DELETE operation
      update_row,    // UPDATE operation
      commit_tx,     // Transaction commit
      rollback_tx,   // Transaction rollback
      checkpoint,    // Checkpoint marker
  };

  pub const WalRecord = struct {
      type: WalRecordType,
      tx_id: u64,              // Transaction ID
      table_name: []const u8,  // Target table
      row_id: u64,             // Row ID
      data: []const u8,        // Serialized operation data
      checksum: u32,           // CRC32 checksum
  };
  ```

- [ ] Define WAL file header
  ```zig
  pub const WalHeader = struct {
      magic: u32,      // 0x5741_4C00 ("WAL\0")
      version: u32,    // Format version
      page_size: u32,  // Page size (4KB recommended)
      sequence: u64,   // Sequence number for file rotation
  };
  ```

- [ ] Implement WAL record serialization
  - `serializeRecord(record: WalRecord)` → []u8
  - `deserializeRecord(data: []u8)` → WalRecord
  - Include CRC32 checksum for corruption detection

**Acceptance Criteria:**
- WAL format documented
- Serialization/deserialization works
- Checksums detect corruption

---

### Phase 2.2: WAL Writer Implementation (3-4 days)

**File:** `src/wal.zig` continued

**Tasks:**
- [ ] Implement WAL writer
  ```zig
  pub const WalWriter = struct {
      file: std.fs.File,
      buffer: ArrayList(u8),  // Write buffer
      allocator: Allocator,
      current_tx_id: u64,

      pub fn init(path: []const u8) !WalWriter;
      pub fn writeRecord(record: WalRecord) !void;
      pub fn flush() !void;  // fsync to disk
      pub fn close() !void;
  };
  ```

- [ ] Implement buffered writes
  - Buffer records in memory (default: 4KB)
  - Flush on buffer full or explicit sync
  - Use `fsync()` for durability guarantee

- [ ] Implement WAL file rotation
  - Rotate when file exceeds size limit (default: 16MB)
  - Create new file: `wal.000001`, `wal.000002`, etc.
  - Keep old WAL files until checkpoint

- [ ] Implement checkpoint mechanism
  - Checkpoint = flush all dirty pages to table files
  - Write checkpoint record to WAL
  - Delete old WAL files before checkpoint

**Acceptance Criteria:**
- Can write WAL records to disk
- Flush/fsync works correctly
- File rotation works
- Checkpoint creates safe truncation point

---

### Phase 2.3: Integrate WAL with Database Operations (4-5 days)

**File:** Modify `src/database.zig`

**Tasks:**
- [ ] Add WAL to Database struct
  ```zig
  pub const Database = struct {
      tables: StringHashMap(*Table),
      hnsw: ?*HNSW(f32),
      indexes: StringHashMap(*Index),
      wal: ?*WalWriter,  // NEW
      allocator: Allocator,
  };
  ```

- [ ] Modify INSERT to write WAL
  ```zig
  fn executeInsert() {
      // 1. Write WAL record
      if (self.wal) |w| {
          try w.writeRecord(.{
              .type = .insert_row,
              .tx_id = current_tx_id,
              .table_name = cmd.table_name,
              .row_id = row_id,
              .data = serialized_row,
          });
          try w.flush();  // Ensure durable
      }

      // 2. Apply to table (now safe)
      const row_id = try table.insert(values_map);
  }
  ```

- [ ] Modify DELETE to write WAL
- [ ] Modify UPDATE to write WAL

- [ ] Implement WAL-ahead protocol
  - **CRITICAL:** WAL record must hit disk BEFORE in-memory change
  - Order: Write WAL → fsync → Apply change → Return to user
  - This ensures recoverability

**Acceptance Criteria:**
- All mutations write to WAL first
- WAL is flushed to disk before returning success
- Database remains consistent

---

### Phase 2.4: Crash Recovery Implementation (5-6 days)

**File:** Create `src/recovery.zig`

**Tasks:**
- [ ] Implement WAL reader
  ```zig
  pub const WalReader = struct {
      file: std.fs.File,
      allocator: Allocator,

      pub fn init(path: []const u8) !WalReader;
      pub fn readRecord() !?WalRecord;  // Returns null at EOF
      pub fn close() void;
  };
  ```

- [ ] Implement recovery manager
  ```zig
  pub const RecoveryManager = struct {
      pub fn recover(db: *Database, wal_dir: []const u8) !void {
          // 1. Find all WAL files
          // 2. Read records in sequence
          // 3. Replay committed transactions
          // 4. Discard uncommitted transactions
      }
  };
  ```

- [ ] Implement redo logic
  - Replay INSERT/DELETE/UPDATE operations
  - Skip operations already in table files (idempotent)
  - Handle partial transactions (no commit record)

- [ ] Implement transaction detection
  - Track begin_tx → commit_tx pairs
  - Only replay committed transactions
  - Discard records from uncommitted transactions

- [ ] Add recovery to Database initialization
  ```zig
  pub fn init(allocator: Allocator) Database {
      var db = Database{ /* ... */ };

      // NEW: Run recovery on startup
      if (wal_exists()) {
          try RecoveryManager.recover(&db, wal_dir);
      }

      return db;
  }
  ```

- [ ] Implement WAL cleanup after recovery
  - After successful recovery, create checkpoint
  - Delete old WAL files
  - Start fresh WAL for new operations

**Acceptance Criteria:**
- Recovery correctly replays committed transactions
- Uncommitted transactions are discarded
- Database state matches last commit after crash
- Recovery is idempotent (can run multiple times safely)

---

### Phase 2.5: WAL Testing (3-4 days)

**File:** Create `src/test_wal.zig`

**Tasks:**
- [ ] Write WAL unit tests
  - Record serialization/deserialization
  - Checksum validation
  - File rotation

- [ ] Write recovery integration tests
  - Simulate crash during INSERT (before commit)
  - Simulate crash after COMMIT
  - Verify data consistency after recovery

- [ ] Write crash simulation tests
  ```zig
  test "recovery after crash during INSERT" {
      var db = Database.init(allocator);
      try db.enableWAL("./test_wal");

      // Insert some data
      _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");

      // Simulate crash (close without checkpoint)
      db.wal.?.close();

      // Restart database
      var db2 = Database.init(allocator);
      try db2.enableWAL("./test_wal");

      // Verify data recovered
      var result = try db2.execute("SELECT * FROM users WHERE id = 1");
      try expectEqual(1, result.rows.items.len);
  }
  ```

- [ ] Write performance tests
  - Measure WAL write overhead
  - Measure recovery time for various database sizes

**Acceptance Criteria:**
- All WAL tests pass
- Recovery works correctly in all crash scenarios
- Performance overhead is acceptable (< 20% slowdown)

---

## P0-3: Basic Transactions (BEGIN/COMMIT/ROLLBACK)

### Current State
- No transaction support
- Each statement auto-commits
- No atomicity for multi-statement operations

### Target State
- Explicit transactions with BEGIN/COMMIT/ROLLBACK
- Atomic multi-statement operations
- Integration with WAL for durability

### Estimated Effort: 1.5-2 weeks

---

### Phase 3.1: Transaction State Management (3-4 days)

**File:** Create `src/transaction.zig`

**Tasks:**
- [ ] Define transaction structure
  ```zig
  pub const TransactionState = enum {
      active,      // Transaction in progress
      committed,   // Committed (final)
      aborted,     // Rolled back
  };

  pub const Transaction = struct {
      id: u64,
      state: TransactionState,
      operations: ArrayList(Operation),  // For rollback
      allocator: Allocator,

      pub fn init(id: u64, allocator: Allocator) Transaction;
      pub fn addOperation(op: Operation) !void;
      pub fn commit() !void;
      pub fn rollback() !void;
  };
  ```

- [ ] Define operation log for rollback
  ```zig
  pub const Operation = union(enum) {
      insert: struct {
          table_name: []const u8,
          row_id: u64,
      },
      delete: struct {
          table_name: []const u8,
          row_id: u64,
          saved_row: Row,  // For undo
      },
      update: struct {
          table_name: []const u8,
          row_id: u64,
          old_values: Row,  // For undo
      },
  };
  ```

- [ ] Implement transaction manager
  ```zig
  pub const TransactionManager = struct {
      current_tx: ?Transaction,
      next_tx_id: u64,
      allocator: Allocator,

      pub fn begin() !u64;  // Returns tx_id
      pub fn commit(tx_id: u64) !void;
      pub fn rollback(tx_id: u64) !void;
      pub fn getCurrentTx() ?*Transaction;
  };
  ```

**Acceptance Criteria:**
- Can create/commit/rollback transactions
- Transaction state tracked correctly
- Operations logged for rollback

---

### Phase 3.2: SQL Parser for Transaction Commands (1 day)

**File:** Modify `src/sql.zig`

**Tasks:**
- [ ] Add transaction commands
  ```zig
  pub const SqlCommand = union(enum) {
      begin: void,
      commit: void,
      rollback: void,
      // ... existing commands
  };
  ```

- [ ] Implement parser for transaction commands
  - `BEGIN` or `BEGIN TRANSACTION`
  - `COMMIT` or `COMMIT TRANSACTION`
  - `ROLLBACK` or `ROLLBACK TRANSACTION`

**Acceptance Criteria:**
- Parser handles BEGIN/COMMIT/ROLLBACK
- Case-insensitive keywords work

---

### Phase 3.3: Integrate Transactions with Database (4-5 days)

**File:** Modify `src/database.zig`

**Tasks:**
- [ ] Add transaction manager to Database
  ```zig
  pub const Database = struct {
      tables: StringHashMap(*Table),
      hnsw: ?*HNSW(f32),
      indexes: StringHashMap(*Index),
      wal: ?*WalWriter,
      tx_manager: TransactionManager,  // NEW
      allocator: Allocator,
  };
  ```

- [ ] Implement BEGIN command
  ```zig
  fn executeBegin() {
      const tx_id = try self.tx_manager.begin();

      // Write BEGIN to WAL
      if (self.wal) |w| {
          try w.writeRecord(.{ .type = .begin_tx, .tx_id = tx_id });
      }
  }
  ```

- [ ] Implement COMMIT command
  ```zig
  fn executeCommit() {
      const tx = self.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;

      // Write COMMIT to WAL
      if (self.wal) |w| {
          try w.writeRecord(.{ .type = .commit_tx, .tx_id = tx.id });
          try w.flush();  // Ensure durable
      }

      // Mark transaction committed
      try self.tx_manager.commit(tx.id);
  }
  ```

- [ ] Implement ROLLBACK command
  ```zig
  fn executeRollback() {
      const tx = self.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;

      // Undo operations in reverse order
      var i = tx.operations.items.len;
      while (i > 0) {
          i -= 1;
          const op = tx.operations.items[i];
          try undoOperation(op);
      }

      // Write ROLLBACK to WAL
      if (self.wal) |w| {
          try w.writeRecord(.{ .type = .rollback_tx, .tx_id = tx.id });
      }

      // Mark transaction aborted
      try self.tx_manager.rollback(tx.id);
  }
  ```

- [ ] Modify data operations to track in transaction
  - INSERT: Log operation for rollback
  - DELETE: Save old row for undo
  - UPDATE: Save old values for undo

- [ ] Implement auto-commit mode
  - If no explicit transaction, wrap each statement in implicit BEGIN/COMMIT

**Acceptance Criteria:**
- BEGIN/COMMIT/ROLLBACK work correctly
- Multi-statement transactions are atomic
- ROLLBACK correctly undoes all operations
- Auto-commit works for non-transactional queries

---

### Phase 3.4: Transaction Testing (2-3 days)

**File:** Create `src/test_transactions.zig`

**Tasks:**
- [ ] Write basic transaction tests
  ```zig
  test "basic transaction commit" {
      var db = Database.init(allocator);

      _ = try db.execute("BEGIN");
      _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
      _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
      _ = try db.execute("COMMIT");

      var result = try db.execute("SELECT * FROM users");
      try expectEqual(2, result.rows.items.len);
  }

  test "transaction rollback" {
      var db = Database.init(allocator);

      _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");

      _ = try db.execute("BEGIN");
      _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
      _ = try db.execute("DELETE FROM users WHERE id = 1");
      _ = try db.execute("ROLLBACK");

      var result = try db.execute("SELECT * FROM users");
      try expectEqual(1, result.rows.items.len);  // Only Alice remains
  }
  ```

- [ ] Write complex transaction tests
  - Multiple INSERT/UPDATE/DELETE in one transaction
  - Nested operations
  - Rollback with indexes (ensure indexes rollback too)

- [ ] Write crash recovery with transactions
  - Crash during transaction (before commit) → data discarded
  - Crash after commit → data recovered

**Acceptance Criteria:**
- All transaction tests pass
- Rollback works correctly for all operation types
- Transactions integrate with WAL recovery

---

## Implementation Order & Dependencies

### Recommended Implementation Sequence:

**Week 1-2: WAL Foundation**
1. Implement WAL file format (Phase 2.1)
2. Implement WAL writer (Phase 2.2)
3. Integrate WAL with database operations (Phase 2.3)

**Week 3-4: Crash Recovery**
4. Implement crash recovery (Phase 2.4)
5. Test WAL and recovery thoroughly (Phase 2.5)

**Week 5-6: Transactions**
6. Implement transaction state management (Phase 3.1)
7. Add transaction SQL parsing (Phase 3.2)
8. Integrate transactions with database (Phase 3.3)
9. Test transactions (Phase 3.4)

**Week 7-8: B-tree Indexes (Can overlap with transactions)**
10. Implement B-tree data structure (Phase 1.1)
11. Implement index manager (Phase 1.2)
12. Add CREATE INDEX SQL (Phase 1.3)
13. Integrate indexes with query executor (Phase 1.4)
14. Test and benchmark indexes (Phase 1.5)

**Rationale:**
- WAL must come first (foundation for both transactions and durability)
- Transactions depend on WAL (for commit/rollback logging)
- Indexes are mostly independent and can be done in parallel to transactions

---

## Risk Mitigation

### Technical Risks

1. **WAL Performance Overhead**
   - Risk: fsync() on every commit may be too slow
   - Mitigation: Implement group commit (batch multiple transactions)
   - Fallback: Make fsync optional for testing/development

2. **B-tree Complexity**
   - Risk: B-tree implementation bugs are hard to debug
   - Mitigation: Extensive testing, reference existing implementations
   - Fallback: Start with simpler hash index for equality lookups

3. **Transaction Rollback Complexity**
   - Risk: Complex operations (e.g., with HNSW) hard to undo
   - Mitigation: Start with simple operations, add complexity gradually
   - Fallback: Disable HNSW updates during transactions initially

4. **Recovery Edge Cases**
   - Risk: Incomplete WAL records, corrupted files
   - Mitigation: Checksums, extensive crash testing
   - Fallback: Manual recovery tools for corrupted databases

### Timeline Risks

1. **Scope Creep**
   - Risk: Features expand beyond P0
   - Mitigation: Strict adherence to plan, defer P1/P2 features

2. **Underestimated Complexity**
   - Risk: Features take longer than estimated
   - Mitigation: 20% buffer included in estimates, weekly reviews

---

## Success Metrics

### Performance Metrics
- [ ] Indexed queries are 10x+ faster than table scans (100k+ rows)
- [ ] WAL overhead is < 20% for write-heavy workloads
- [ ] Recovery time is < 1 second per 10k transactions

### Correctness Metrics
- [ ] All tests pass (unit, integration, crash simulation)
- [ ] Zero data loss after crash during transaction
- [ ] Rollback correctly undoes all operations
- [ ] Indexes always consistent with table data

### Code Quality Metrics
- [ ] Test coverage > 80% for new code
- [ ] All public APIs documented
- [ ] No memory leaks (valgrind/asan clean)

---

## Documentation Deliverables

- [ ] Update README.md with transaction examples
- [ ] Create TRANSACTIONS.md explaining transaction semantics
- [ ] Create INDEXES.md explaining index usage and performance
- [ ] Update SQL_FEATURES.md with new commands
- [ ] Create RECOVERY.md explaining crash recovery process
- [ ] Add inline code documentation for all public APIs

---

## Future Considerations (Post-P0)

These are explicitly out of scope for P0 but should be considered:

1. **MVCC (Multi-Version Concurrency Control)**
   - Allow concurrent reads during writes
   - Snapshot isolation

2. **Savepoints**
   - `SAVEPOINT name` / `ROLLBACK TO SAVEPOINT name`
   - Partial rollback within transaction

3. **2-Phase Commit**
   - Distributed transactions
   - XA protocol support

4. **Advanced Indexes**
   - Composite indexes (multiple columns)
   - Unique constraints
   - Full-text search indexes

5. **Query Optimizer**
   - Cost-based query planning
   - Join order optimization
   - Index selection heuristics

---

## Conclusion

This plan provides a structured approach to implementing P0 features that will make zvdb production-ready. The estimated 4-6 weeks accounts for:
- Design and implementation
- Comprehensive testing
- Documentation
- Bug fixes and iteration

The features are prioritized by dependency, ensuring a solid foundation (WAL) before building higher-level features (transactions, indexes).

**Next Steps:**
1. Review and approve this plan
2. Set up project tracking (GitHub issues/milestones)
3. Begin with Phase 2.1 (WAL File Format Design)
