# MVCC Implementation Status

## Overview

This document describes the Multi-Version Concurrency Control (MVCC) implementation in zvdb. MVCC enables multiple transactions to read and write to the database concurrently without blocking each other, providing snapshot isolation.

**Status**: Phase 4 Complete ✅
**Isolation Level**: Snapshot Isolation
**Test Coverage**: 150+ tests passing (includes VACUUM tests)
**Features**: Full MVCC with VACUUM garbage collection and auto-cleanup

---

## Implementation Phases

### ✅ Phase 1: Transaction Infrastructure (Complete)

**Components**:
- **Transaction IDs**: Atomic counter with thread-safe allocation
- **Snapshots**: Capture point-in-time database state for consistent reads
- **Commit Log (CLOG)**: Track transaction status (in_progress, committed, aborted)

**Files**:
- `src/transaction.zig`: Core transaction management (484 lines)
- `src/test_mvcc_phase1.zig`: Comprehensive tests (38 tests, all passing)

**Key Features**:
- Multiple concurrent transactions supported
- Each transaction gets a unique ID and snapshot on BEGIN
- Snapshot captures list of active transactions at start time
- Thread-safe via mutexes and atomic operations

### ✅ Phase 2: Multi-Version Storage (Complete)

**Components**:
- **RowVersion**: Each row can have multiple versions forming a linked list
- **Version Chains**: Maps row_id → newest version pointer
- **Visibility Logic**: Core MVCC rule implementation

**Files**:
- `src/table.zig`: RowVersion struct and visibility logic (lines 380-449)
- `src/test_mvcc_storage.zig`: Storage layer tests (14 tests, all passing)

**Key Features**:
```zig
pub const RowVersion = struct {
    row_id: u64,        // Shared across all versions
    xmin: u64,          // Transaction that created this version
    xmax: u64,          // Transaction that deleted/updated (0 = active)
    data: Row,          // Actual row data
    next: ?*RowVersion, // Pointer to older version
};
```

**Visibility Rules** (PostgreSQL-compatible):
1. Version created after snapshot → NOT visible
2. Version created by active transaction → NOT visible
3. Version created by aborted transaction → NOT visible
4. Version deleted before snapshot → NOT visible
5. Otherwise → VISIBLE

### ✅ Phase 3: Executor Integration & Conflict Detection (Complete)

**Recent Changes** (November 2025):

#### 3.1 Fixed Critical Visibility Bug
**Problem**: Transactions couldn't see their own changes
- `src/table.zig:415`: Changed `xmin >= snapshot.txid` to `xmin > snapshot.txid`
- **Impact**: Transactions can now SELECT rows they just INSERTed

**Root Cause**:
```zig
// BEFORE (WRONG):
if (self.xmin >= snapshot.txid) return false;  // Excluded own changes!

// AFTER (CORRECT):
if (self.xmin > snapshot.txid) return false;   // Can see own changes
```

#### 3.2 Eliminated Null Snapshot Workarounds
**Problem**: Code used `null, null` to bypass MVCC visibility
- `src/database/executor/command_executor.zig:251`: INSERT now uses proper snapshot
- **Impact**: All production code now uses MVCC consistently

**Changes**:
```zig
// BEFORE:
const row = table.get(final_row_id, null, null).?;  // Bypass MVCC

// AFTER:
const snapshot = db.getCurrentSnapshot();
const clog = db.getClog();
const row = table.get(final_row_id, snapshot, clog).?;  // Use MVCC
```

#### 3.3 Added Write-Write Conflict Detection
**Problem**: Concurrent updates could cause lost updates
- `src/table.zig:620-651`: Added conflict detection to `update()`
- `src/table.zig:563-579`: Added conflict detection to `delete()`

**Implementation**:
```zig
// Check if another transaction is modifying this row
if (old_version.xmax != 0 and old_version.xmax != tx_id) {
    return error.SerializationFailure;  // Conflict detected!
}
```

**Prevents**:
- Lost updates (T1 and T2 both update same row)
- Delete-update conflicts (T1 deletes while T2 updates)
- Update-delete conflicts (T1 updates while T2 deletes)

### ✅ Phase 4: Garbage Collection & Advanced Features (Complete)

**Implemented November 2025**:

#### 4.1 VACUUM Command ✅
**Purpose**: Remove old versions no longer visible to any transaction

**SQL Syntax**:
```sql
VACUUM;                    -- Clean all tables
VACUUM table_name;         -- Clean specific table
```

**Implementation** (`src/table.zig:757-847`):
```zig
pub fn vacuum(self: *Table, min_visible_txid: u64, clog: *CommitLog) !VacuumStats
```

**Algorithm**:
1. Get minimum visible transaction ID from all active transactions
2. For each version chain:
   - Walk from newest to oldest
   - Keep: head version (never remove newest)
   - Keep: versions visible to min_visible_txid
   - Remove: versions created and deleted before min_visible_txid
   - Remove: versions from aborted transactions
3. Free memory for removed versions
4. Return statistics (versions_removed, total_chains, etc.)

**Safety Features**:
- Never removes head (newest version) of any chain
- Preserves versions visible to active transactions
- Thread-safe (called with transaction manager mutex)
- Properly handles aborted transactions

**Executor** (`src/database/executor/command_executor.zig:645-726`):
- Supports both VACUUM and VACUUM table_name
- Returns detailed statistics for monitoring
- Automatically finds oldest active transaction

#### 4.2 Auto-VACUUM ✅
**Purpose**: Automatic background cleanup

**Configuration** (`src/database/core.zig:99-109`):
```zig
pub const VacuumConfig = struct {
    enabled: bool = true,                   // Auto-vacuum on/off
    max_chain_length: usize = 10,          // Trigger threshold
    txn_interval: usize = 1000,            // Cleanup frequency
};
```

**Triggers**:
- ✅ Version chain length > max_chain_length (default: 10)
- ✅ Transaction count > txn_interval (default: 1000)
- ✅ Runs after UPDATE and DELETE operations

**Implementation** (`src/database/core.zig:222-288`):
- `maybeAutoVacuum()`: Check thresholds and trigger if needed
- `runAutoVacuum()`: Execute vacuum on all tables
- Called automatically from UPDATE/DELETE executors
- Can be disabled via config

**Usage**:
```zig
// Configure auto-VACUUM
db.vacuum_config.enabled = true;
db.vacuum_config.max_chain_length = 5;   // Trigger on 5+ versions
db.vacuum_config.txn_interval = 500;     // Every 500 transactions

// Or disable it
db.vacuum_config.enabled = false;
```

#### 4.3 Enhanced WAL Integration
**Status**: Deferred to future work

**Current State**:
- ✅ BEGIN/COMMIT/ROLLBACK logged
- ✅ INSERT/UPDATE/DELETE data logged
- ⏳ Version metadata (xmin/xmax) in WAL records (future)
- ⏳ Recovery rebuilds version chains (future)

**Rationale for Deferral**:
- Current WAL implementation works for crash recovery
- MVCC metadata can be inferred from transaction log
- Full version chain recovery is an optimization, not critical
- Can be added in future enhancement

---

## Isolation Level Details

### Snapshot Isolation

**Guarantees Provided**:
- ✅ **Read Committed**: See all committed changes
- ✅ **Repeatable Read**: Same query returns same results within transaction
- ✅ **Phantom Read Prevention**: Range queries are stable
- ✅ **No Dirty Reads**: Can't see uncommitted changes from other transactions

**Not Provided**:
- ❌ **Full Serializability**: Some anomalies possible with concurrent writes
  - Example: Write skew (T1 reads X, T2 reads Y, T1 writes Y, T2 writes X)
  - Can be added later with predicate locking or serialization graph testing

**How It Works**:
1. Transaction T1 begins → Snapshot captures active transactions
2. T1 reads row → Walks version chain to find visible version
3. T1 writes row → Creates new version with xmin = T1's ID
4. Concurrent T2 begins → Gets different snapshot
5. T2 reads same row → Sees different version (based on its snapshot)
6. T1 commits → New versions become visible to future snapshots
7. T2 still sees old version (snapshot isolation!)

---

## API Usage

### Basic Transaction Lifecycle

```zig
const Database = @import("zvdb").Database;

var db = try Database.init(allocator);
defer db.deinit();

// Create table
_ = try db.execute("CREATE TABLE users (id INT, name TEXT)");

// Transaction 1: Insert data
_ = try db.execute("BEGIN");
_ = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
_ = try db.execute("COMMIT");

// Transaction 2: Update data (isolated from T1 until T1 commits)
_ = try db.execute("BEGIN");
_ = try db.execute("UPDATE users SET name = 'Bob' WHERE id = 1");
_ = try db.execute("ROLLBACK");  // Undo changes
```

### Concurrent Transactions

```zig
// Transaction 1: Long-running query
_ = try db.execute("BEGIN");
const tx1 = db.tx_manager.getCurrentTx().?;
const result1 = try db.execute("SELECT * FROM large_table");
// ... process results slowly ...

// Transaction 2: Quick update (runs concurrently with T1)
// T1 won't see T2's changes because it has an older snapshot
_ = try db.execute("BEGIN");
_ = try db.execute("UPDATE large_table SET status = 'processed'");
_ = try db.execute("COMMIT");

// T1 continues to see old data (snapshot isolation)
const result2 = try db.execute("SELECT * FROM large_table");
// result2 shows same data as result1 (repeatable read)

_ = try db.execute("COMMIT");  // End T1
```

### Handling Write Conflicts

```zig
// Transaction 1: Update account
_ = try db.execute("BEGIN");
_ = try db.execute("UPDATE accounts SET balance = 1500 WHERE id = 1");
// Don't commit yet...

// Transaction 2: Try to update same account
_ = try db.execute("BEGIN");
const result = db.execute("UPDATE accounts SET balance = 2000 WHERE id = 1");
// Returns error.SerializationFailure (conflict detected)

if (result) |_| {
    // Success - other transaction released lock
} else |err| {
    if (err == error.SerializationFailure) {
        // Conflict! Rollback and retry
        _ = try db.execute("ROLLBACK");
        // ... retry logic ...
    }
}
```

### Using VACUUM for Memory Management

```zig
// Manual VACUUM - clean up old versions
const result = try db.execute("VACUUM");
// Or vacuum specific table
const result2 = try db.execute("VACUUM users");

// Check statistics
std.debug.print("Table: {s}\n", .{result.rows.items[0].items[0].text});
std.debug.print("Versions removed: {}\n", .{result.rows.items[0].items[1].int});
std.debug.print("Total versions: {}\n", .{result.rows.items[0].items[3].int});

// Configure auto-VACUUM
db.vacuum_config.enabled = true;
db.vacuum_config.max_chain_length = 5;  // Trigger on 5+ versions per row
db.vacuum_config.txn_interval = 1000;   // Or every 1000 transactions

// Auto-VACUUM runs automatically after UPDATE/DELETE
_ = try db.execute("UPDATE products SET price = price * 1.1");
// Auto-VACUUM may have run if thresholds exceeded

// Disable auto-VACUUM for bulk operations
db.vacuum_config.enabled = false;
for (0..10000) |i| {
    // ... bulk updates ...
}
db.vacuum_config.enabled = true;
_ = try db.execute("VACUUM");  // Manual cleanup after bulk operation
```

---

## Performance Characteristics

### Reads
- **No Blocking**: Readers never block writers, writers never block readers
- **No Locks**: Uses snapshots instead of read locks
- **Cost**: Must walk version chain to find visible version (usually 1-2 versions)

### Writes
- **Conflict Detection**: O(1) check if row already locked
- **Version Creation**: O(1) append to version chain
- **Cost**: Allocates new RowVersion for each update

### Memory
- **Growth**: Each update creates a new version (memory grows until VACUUM)
- **Chain Length**: Typically 1-3 versions per row
- **Garbage**: Old versions accumulate until VACUUM runs

**Example**:
```
Row 1:
  v3 (newest) → v2 → v1 (oldest)

After VACUUM (assuming v1 and v2 no longer visible):
  v3 (only version remaining)
```

---

## Testing

### Test Files
1. `src/test_mvcc_phase1.zig` - Transaction infrastructure (38 tests)
2. `src/test_mvcc_storage.zig` - Multi-version storage (14 tests)
3. `src/test_mvcc_concurrent.zig` - Concurrent transaction scenarios (9 tests)
4. `src/test_sql.zig` - Integration tests (includes MVCC tests)

### Running Tests
```bash
zig build test
```

### Test Coverage
- ✅ Transaction lifecycle (BEGIN/COMMIT/ROLLBACK)
- ✅ Snapshot isolation
- ✅ Version chain creation and traversal
- ✅ Visibility rules for all scenarios
- ✅ Write-write conflict detection
- ✅ Repeatable reads
- ✅ Phantom read prevention
- ✅ Rollback and undo operations

---

## Known Limitations

### Current Limitations

1. **Simple Conflict Detection** → Always aborts on conflict
   - **Current**: Returns error.SerializationFailure immediately
   - **Better**: Could wait for other transaction to commit/rollback
   - **Fix**: Add lock manager with wait queues

3. **Single Active Transaction per Connection** → getCurrentTx() returns first active
   - **Current**: Works for single-threaded execution
   - **Limitation**: Can't easily test true concurrent scenarios
   - **Fix**: Add transaction ID to Database context

4. **No Serializable Isolation** → Write skew anomalies possible
   - **Current**: Snapshot isolation only
   - **Missing**: Predicate locks or serialization graph testing
   - **Fix**: Implement SSI (Serializable Snapshot Isolation)

### Memory Leaks
- ✅ All tests pass with no memory leaks detected
- ✅ Proper cleanup in deinit() methods
- ✅ Version chains freed correctly

---

## Architecture Diagrams

### Transaction Flow
```
BEGIN
  ↓
1. Allocate unique TX_ID (atomic)
2. Capture active_txs → SNAPSHOT
3. Register in TransactionManager
4. Mark as IN_PROGRESS in CLOG
  ↓
OPERATIONS (INSERT/UPDATE/DELETE)
  ↓
  → INSERT: Create RowVersion with xmin = TX_ID
  → UPDATE: Set xmax on old, create new with xmin = TX_ID
  → DELETE: Set xmax = TX_ID on current version
  ↓
COMMIT or ROLLBACK
  ↓
5. Mark as COMMITTED/ABORTED in CLOG
6. Remove from active_txs
7. Cleanup transaction
```

### Version Chain Example
```
UPDATE users SET name = 'v3' WHERE id = 1;  -- T3
UPDATE users SET name = 'v2' WHERE id = 1;  -- T2
INSERT INTO users VALUES (1, 'v1');         -- T1

version_chains[1] →
  ┌─────────────────────────┐
  │ RowVersion              │
  │  xmin: 3                │ ← Newest (visible to TX_ID ≥ 4)
  │  xmax: 0                │
  │  data: name = 'v3'      │
  │  next: ─────────────────┼───┐
  └─────────────────────────┘   │
                                ↓
  ┌─────────────────────────┐
  │ RowVersion              │
  │  xmin: 2                │ ← Older (visible to TX_ID = 3 if T2 committed)
  │  xmax: 3                │
  │  data: name = 'v2'      │
  │  next: ─────────────────┼───┐
  └─────────────────────────┘   │
                                ↓
  ┌─────────────────────────┐
  │ RowVersion              │
  │  xmin: 1                │ ← Oldest (visible to TX_ID = 2 if T1 committed)
  │  xmax: 2                │
  │  data: name = 'v1'      │
  │  next: null             │
  └─────────────────────────┘
```

---

## Future Enhancements

### Short Term (Phase 4)
1. **VACUUM Command** - Manual garbage collection
2. **Auto-VACUUM** - Automatic cleanup on thresholds
3. **WAL Metadata** - Full crash recovery for version chains

### Medium Term
1. **Connection-Scoped Transactions** - Proper multi-connection support
2. **Lock Manager** - Wait for locks instead of immediate abort
3. **Better Conflict Resolution** - Retry logic and backoff strategies

### Long Term
1. **Serializable Isolation** - Full SSI implementation
2. **Parallel VACUUM** - Background cleanup workers
3. **Index Support for Old Versions** - Allow index scans on historical data
4. **MVCC for Indexes** - B-tree and HNSW index versioning

---

## References

### PostgreSQL MVCC Documentation
- [PostgreSQL MVCC Intro](https://www.postgresql.org/docs/current/mvcc-intro.html)
- [Transaction Isolation](https://www.postgresql.org/docs/current/transaction-iso.html)

### Academic Papers
- "A Critique of ANSI SQL Isolation Levels" (Berenson et al., 1995)
- "Serializable Snapshot Isolation in PostgreSQL" (Ports & Grittner, 2012)

### Implementation References
- PostgreSQL src/backend/access/heap (MVCC implementation)
- MySQL InnoDB (Alternative MVCC approach with undo logs)

---

## Changelog

### November 16, 2025 - Phase 4 Completion (VACUUM & Auto-Cleanup)
- Implemented VACUUM command for manual garbage collection
- Added auto-VACUUM with configurable thresholds
- Created VacuumStats struct for monitoring version chains
- Integrated auto-VACUUM triggers in UPDATE/DELETE executors
- Added comprehensive VACUUM tests (10 new tests)
- Updated documentation with Phase 4 implementation details
- **Status**: Full MVCC implementation complete!

### November 16, 2025 - Phase 3 Completion
- Fixed visibility bug: Transactions can now see own changes
- Removed null snapshot workarounds in INSERT executor
- Added write-write conflict detection to UPDATE and DELETE
- Created comprehensive concurrent transaction tests
- Documented full MVCC implementation status

### Previous Work
- Phase 1: Transaction infrastructure (38 tests)
- Phase 2: Multi-version storage (14 tests)
- Initial executor integration with TODO markers
