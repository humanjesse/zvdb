# MVCC Implementation Plan - Production-Grade Concurrency

**Date:** 2025-11-14
**Goal:** Implement Multi-Version Concurrency Control (MVCC) for high-concurrency workloads
**Estimated Total Effort:** 6-8 weeks (1 developer)

---

## Executive Summary

This plan implements Multi-Version Concurrency Control (MVCC) to enable:
- **Concurrent reads and writes** without blocking
- **Snapshot isolation** for consistent reads
- **Higher throughput** for multi-user scenarios
- **No read locks** - readers never block writers, writers never block readers

**Current State:**
- Single-version storage (one version per row)
- Pessimistic locking (would need to be added for concurrency)
- No isolation between concurrent transactions

**Target State:**
- Multi-version storage (multiple versions per row)
- Optimistic concurrency control
- Snapshot isolation level
- Automatic garbage collection of old versions

---

## Background: Why MVCC?

### The Concurrency Problem

Traditional databases use locks:
```
Transaction 1: BEGIN
Transaction 1: UPDATE users SET balance = 100 WHERE id = 1
                ↓
            [LOCK acquired on row 1]
                ↓
Transaction 2: SELECT * FROM users WHERE id = 1
                ↓
            [BLOCKED - waiting for lock]
                ↓
Transaction 1: COMMIT
                ↓
            [LOCK released]
                ↓
Transaction 2: [Unblocked - can now read]
```

**Problem:** Readers block on writers, throughput suffers.

### The MVCC Solution

With MVCC:
```
Transaction 1 (tx_id=10): BEGIN
Transaction 1: UPDATE users SET balance = 100 WHERE id = 1
                ↓
            [Creates version with tx_id=10]
                ↓
Transaction 2 (tx_id=11): SELECT * FROM users WHERE id = 1
                ↓
            [Reads version visible to tx_id=11]
            [NOT BLOCKED - reads old version]
                ↓
Transaction 1: COMMIT
                ↓
Transaction 2: [Still reads old version - consistent snapshot]
```

**Benefit:** Readers and writers don't block each other.

---

## MVCC Architecture Design

### Core Concepts

#### 1. Transaction IDs (TxID)
- Every transaction gets a monotonically increasing ID
- TxID determines visibility of data versions
- Global counter: `next_tx_id: AtomicU64`

#### 2. Data Versions
Every row has multiple versions, each with:
```zig
pub const RowVersion = struct {
    row_id: u64,              // Row identifier
    tx_id_min: u64,           // Transaction that created this version
    tx_id_max: u64,           // Transaction that deleted/updated (or MAX if active)
    data: Row,                // The actual row data
    next: ?*RowVersion,       // Linked list of versions
};
```

#### 3. Visibility Rules
A version is visible to transaction T if:
```
version.tx_id_min < T.tx_id AND
(version.tx_id_max == MAX OR version.tx_id_max > T.tx_id)
```

#### 4. Transaction Snapshots
```zig
pub const Snapshot = struct {
    tx_id: u64,              // This transaction's ID
    active_txs: []u64,       // Transactions active at snapshot time

    pub fn isVisible(self: Snapshot, version: *RowVersion) bool {
        // Version created by uncommitted transaction? Not visible
        if (isActive(version.tx_id_min)) return false;

        // Version created after snapshot? Not visible
        if (version.tx_id_min >= self.tx_id) return false;

        // Version deleted before snapshot? Not visible
        if (version.tx_id_max < self.tx_id and !isActive(version.tx_id_max)) {
            return false;
        }

        return true;
    }
};
```

---

## Implementation Phases

### Phase 1: Foundation - Transaction Manager Redesign (1.5 weeks)

#### Phase 1.1: Enhanced Transaction Manager (4-5 days)

**File:** Modify `src/transaction.zig`

**Tasks:**

1. **Add global transaction ID counter**
```zig
pub const TransactionManager = struct {
    next_tx_id: std.atomic.Value(u64),  // Thread-safe counter
    active_txs: std.AutoHashMap(u64, *Transaction),  // Active transactions
    mutex: std.Thread.Mutex,  // Protects active_txs map
    allocator: Allocator,

    pub fn init(allocator: Allocator) TransactionManager {
        return .{
            .next_tx_id = std.atomic.Value(u64).init(1),
            .active_txs = std.AutoHashMap(u64, *Transaction).init(allocator),
            .mutex = .{},
            .allocator = allocator,
        };
    }
};
```

2. **Implement snapshot creation**
```zig
pub const Snapshot = struct {
    tx_id: u64,
    active_tx_ids: []u64,  // Copy of all active transaction IDs
    allocator: Allocator,

    pub fn create(tx_manager: *TransactionManager, tx_id: u64) !Snapshot {
        // Capture active transactions at snapshot time
        var active = std.ArrayList(u64).init(allocator);

        tx_manager.mutex.lock();
        defer tx_manager.mutex.unlock();

        var iter = tx_manager.active_txs.iterator();
        while (iter.next()) |entry| {
            if (entry.key_ptr.* != tx_id) {  // Exclude self
                try active.append(entry.key_ptr.*);
            }
        }

        return Snapshot{
            .tx_id = tx_id,
            .active_tx_ids = try active.toOwnedSlice(),
            .allocator = allocator,
        };
    }

    pub fn isActive(self: Snapshot, tx_id: u64) bool {
        for (self.active_tx_ids) |active_id| {
            if (active_id == tx_id) return true;
        }
        return false;
    }

    pub fn deinit(self: *Snapshot) void {
        self.allocator.free(self.active_tx_ids);
    }
};
```

3. **Update Transaction structure**
```zig
pub const Transaction = struct {
    id: u64,
    snapshot: Snapshot,  // NEW: Snapshot for consistent reads
    state: TransactionState,
    operations: ArrayList(Operation),
    allocator: Allocator,

    pub fn begin(tx_manager: *TransactionManager, allocator: Allocator) !*Transaction {
        // Get next transaction ID (atomic increment)
        const tx_id = tx_manager.next_tx_id.fetchAdd(1, .monotonic);

        // Create transaction
        var tx = try allocator.create(Transaction);
        tx.* = .{
            .id = tx_id,
            .snapshot = try Snapshot.create(tx_manager, tx_id),
            .state = .active,
            .operations = ArrayList(Operation).init(allocator),
            .allocator = allocator,
        };

        // Register as active
        tx_manager.mutex.lock();
        defer tx_manager.mutex.unlock();
        try tx_manager.active_txs.put(tx_id, tx);

        return tx;
    }

    pub fn commit(self: *Transaction, tx_manager: *TransactionManager) !void {
        self.state = .committed;

        // Unregister from active transactions
        tx_manager.mutex.lock();
        defer tx_manager.mutex.unlock();
        _ = tx_manager.active_txs.remove(self.id);
    }

    pub fn rollback(self: *Transaction, tx_manager: *TransactionManager) !void {
        self.state = .aborted;

        // Unregister from active transactions
        tx_manager.mutex.lock();
        defer tx_manager.mutex.unlock();
        _ = tx_manager.active_txs.remove(self.id);
    }
};
```

**Acceptance Criteria:**
- ✅ Transaction IDs assigned sequentially
- ✅ Snapshots capture active transaction list
- ✅ Thread-safe transaction registration

---

#### Phase 1.2: Testing Transaction Manager (2-3 days)

**File:** Create `src/test_mvcc_tx.zig`

**Tests:**
```zig
test "transaction ID assignment" {
    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1 = try Transaction.begin(&tx_mgr, allocator);
    const tx2 = try Transaction.begin(&tx_mgr, allocator);

    try expect(tx2.id > tx1.id);
    try expect(tx1.snapshot.tx_id == tx1.id);
}

test "snapshot captures active transactions" {
    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1 = try Transaction.begin(&tx_mgr, allocator);
    const tx2 = try Transaction.begin(&tx_mgr, allocator);
    const tx3 = try Transaction.begin(&tx_mgr, allocator);

    // tx3's snapshot should see tx1 and tx2 as active
    try expect(tx3.snapshot.isActive(tx1.id));
    try expect(tx3.snapshot.isActive(tx2.id));
    try expect(!tx3.snapshot.isActive(tx3.id));  // Not self
}

test "commit removes from active list" {
    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1 = try Transaction.begin(&tx_mgr, allocator);
    try tx1.commit(&tx_mgr);

    const tx2 = try Transaction.begin(&tx_mgr, allocator);
    try expect(!tx2.snapshot.isActive(tx1.id));  // tx1 committed
}
```

**Acceptance Criteria:**
- ✅ All transaction manager tests pass
- ✅ Thread-safety verified with concurrent tests

---

### Phase 2: Multi-Version Storage Layer (2 weeks)

#### Phase 2.1: Row Version Data Structure (4-5 days)

**File:** Create `src/mvcc_storage.zig`

**Tasks:**

1. **Implement RowVersion structure**
```zig
pub const RowVersion = struct {
    row_id: u64,
    tx_id_min: u64,        // Transaction that created this version
    tx_id_max: u64,        // Transaction that invalidated (or MAX_TX_ID)
    data: Row,
    next: ?*RowVersion,    // Next older version

    pub const MAX_TX_ID = std.math.maxInt(u64);

    pub fn init(allocator: Allocator, row_id: u64, tx_id: u64, data: Row) !*RowVersion {
        var version = try allocator.create(RowVersion);
        version.* = .{
            .row_id = row_id,
            .tx_id_min = tx_id,
            .tx_id_max = MAX_TX_ID,
            .data = try data.clone(allocator),
            .next = null,
        };
        return version;
    }

    pub fn isVisible(self: *RowVersion, snapshot: *Snapshot) bool {
        // Created by uncommitted transaction? Not visible
        if (snapshot.isActive(self.tx_id_min)) return false;

        // Created after snapshot? Not visible
        if (self.tx_id_min >= snapshot.tx_id) return false;

        // Deleted/updated before snapshot? Not visible
        if (self.tx_id_max != MAX_TX_ID) {
            if (self.tx_id_max < snapshot.tx_id and !snapshot.isActive(self.tx_id_max)) {
                return false;
            }
        }

        return true;
    }

    pub fn deinit(self: *RowVersion, allocator: Allocator) void {
        self.data.deinit(allocator);
        allocator.destroy(self);
    }
};
```

2. **Implement Version Chain**
```zig
pub const VersionChain = struct {
    head: ?*RowVersion,    // Most recent version
    mutex: std.Thread.Mutex,
    allocator: Allocator,

    pub fn init(allocator: Allocator) VersionChain {
        return .{
            .head = null,
            .mutex = .{},
            .allocator = allocator,
        };
    }

    /// Add new version at head of chain
    pub fn addVersion(self: *VersionChain, version: *RowVersion) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        version.next = self.head;
        self.head = version;
    }

    /// Find visible version for given snapshot
    pub fn getVisible(self: *VersionChain, snapshot: *Snapshot) ?*RowVersion {
        self.mutex.lock();
        defer self.mutex.unlock();

        var current = self.head;
        while (current) |version| {
            if (version.isVisible(snapshot)) {
                return version;
            }
            current = version.next;
        }
        return null;  // No visible version
    }

    pub fn deinit(self: *VersionChain) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var current = self.head;
        while (current) |version| {
            const next = version.next;
            version.deinit(self.allocator);
            current = next;
        }
    }
};
```

3. **Implement MVCC Table Storage**
```zig
pub const MVCCTable = struct {
    name: []const u8,
    columns: []Column,
    version_chains: std.AutoHashMap(u64, *VersionChain),  // row_id -> chain
    next_row_id: std.atomic.Value(u64),
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, columns: []Column) !MVCCTable {
        return .{
            .name = try allocator.dupe(u8, name),
            .columns = try allocator.dupe(Column, columns),
            .version_chains = std.AutoHashMap(u64, *VersionChain).init(allocator),
            .next_row_id = std.atomic.Value(u64).init(1),
            .allocator = allocator,
        };
    }

    /// Insert new row (creates first version)
    pub fn insert(self: *MVCCTable, tx: *Transaction, data: Row) !u64 {
        const row_id = self.next_row_id.fetchAdd(1, .monotonic);

        // Create version
        const version = try RowVersion.init(self.allocator, row_id, tx.id, data);

        // Create version chain
        var chain = try self.allocator.create(VersionChain);
        chain.* = VersionChain.init(self.allocator);
        try chain.addVersion(version);

        // Add to table
        try self.version_chains.put(row_id, chain);

        return row_id;
    }

    /// Update row (creates new version)
    pub fn update(self: *MVCCTable, tx: *Transaction, row_id: u64, new_data: Row) !void {
        const chain = self.version_chains.get(row_id) orelse return error.RowNotFound;

        // Mark current version as superseded
        chain.mutex.lock();
        if (chain.head) |current_version| {
            current_version.tx_id_max = tx.id;
        }
        chain.mutex.unlock();

        // Create new version
        const new_version = try RowVersion.init(self.allocator, row_id, tx.id, new_data);
        try chain.addVersion(new_version);
    }

    /// Delete row (marks current version as deleted)
    pub fn delete(self: *MVCCTable, tx: *Transaction, row_id: u64) !void {
        const chain = self.version_chains.get(row_id) orelse return error.RowNotFound;

        chain.mutex.lock();
        defer chain.mutex.unlock();

        if (chain.head) |current_version| {
            current_version.tx_id_max = tx.id;
        }
    }

    /// Read row (returns visible version)
    pub fn read(self: *MVCCTable, tx: *Transaction, row_id: u64) ?Row {
        const chain = self.version_chains.get(row_id) orelse return null;
        const version = chain.getVisible(&tx.snapshot) orelse return null;
        return version.data;
    }

    /// Scan all visible rows
    pub fn scan(self: *MVCCTable, tx: *Transaction, allocator: Allocator) ![]Row {
        var results = std.ArrayList(Row).init(allocator);

        var iter = self.version_chains.iterator();
        while (iter.next()) |entry| {
            const chain = entry.value_ptr.*;
            if (chain.getVisible(&tx.snapshot)) |version| {
                try results.append(try version.data.clone(allocator));
            }
        }

        return results.toOwnedSlice();
    }
};
```

**Acceptance Criteria:**
- ✅ Version chains maintain history correctly
- ✅ Visibility rules work for all scenarios
- ✅ Thread-safe concurrent access

---

#### Phase 2.2: Integration with Existing Table Structure (3-4 days)

**File:** Modify `src/table.zig`

**Tasks:**

1. **Add MVCC mode to Table**
```zig
pub const Table = struct {
    name: []const u8,
    columns: []Column,

    // Storage backends
    storage_mode: enum { simple, mvcc },
    simple_storage: ?SimpleStorage,  // Original implementation
    mvcc_storage: ?MVCCTable,        // New MVCC storage

    allocator: Allocator,

    pub fn initSimple(allocator: Allocator, name: []const u8, columns: []Column) !Table {
        // Existing implementation
    }

    pub fn initMVCC(allocator: Allocator, name: []const u8, columns: []Column) !Table {
        return .{
            .name = try allocator.dupe(u8, name),
            .columns = try allocator.dupe(Column, columns),
            .storage_mode = .mvcc,
            .simple_storage = null,
            .mvcc_storage = try MVCCTable.init(allocator, name, columns),
            .allocator = allocator,
        };
    }

    pub fn insert(self: *Table, tx: *Transaction, data: Row) !u64 {
        return switch (self.storage_mode) {
            .simple => self.simple_storage.?.insert(data),
            .mvcc => self.mvcc_storage.?.insert(tx, data),
        };
    }

    // Similar wrappers for update, delete, read, scan...
};
```

2. **Update Database to support MVCC mode**
```zig
pub const Database = struct {
    tables: StringHashMap(*Table),
    hnsw: ?*HNSW(f32),
    indexes: StringHashMap(*Index),
    wal: ?*WalWriter,
    tx_manager: TransactionManager,

    mvcc_enabled: bool,  // NEW: MVCC mode flag

    allocator: Allocator,

    pub fn enableMVCC(self: *Database) void {
        self.mvcc_enabled = true;
    }

    pub fn createTable(self: *Database, name: []const u8, columns: []Column) !void {
        const table = if (self.mvcc_enabled)
            try Table.initMVCC(self.allocator, name, columns)
        else
            try Table.initSimple(self.allocator, name, columns);

        try self.tables.put(name, table);
    }
};
```

**Acceptance Criteria:**
- ✅ Both simple and MVCC modes work
- ✅ Backward compatibility maintained
- ✅ Mode can be toggled at database creation

---

#### Phase 2.3: Testing Multi-Version Storage (2-3 days)

**File:** Create `src/test_mvcc_storage.zig`

**Tests:**
```zig
test "basic MVCC insert and read" {
    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    var table = try MVCCTable.init(allocator, "users", &[_]Column{...});
    defer table.deinit();

    var tx = try Transaction.begin(&tx_mgr, allocator);
    defer tx.deinit();

    const row_id = try table.insert(tx, row_data);
    const read_data = table.read(tx, row_id);

    try expect(read_data != null);
}

test "concurrent transactions see different versions" {
    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    var table = try MVCCTable.init(allocator, "users", &[_]Column{...});
    defer table.deinit();

    // T1: Insert row
    var tx1 = try Transaction.begin(&tx_mgr, allocator);
    const row_id = try table.insert(tx1, .{ .name = "Alice", .age = 25 });
    try tx1.commit(&tx_mgr);

    // T2: Start and read
    var tx2 = try Transaction.begin(&tx_mgr, allocator);
    const data_before = table.read(tx2, row_id);
    try expect(data_before.?.age == 25);

    // T3: Update row (while T2 still active)
    var tx3 = try Transaction.begin(&tx_mgr, allocator);
    try table.update(tx3, row_id, .{ .name = "Alice", .age = 30 });
    try tx3.commit(&tx_mgr);

    // T2: Read again - should still see old version (snapshot isolation)
    const data_after = table.read(tx2, row_id);
    try expect(data_after.?.age == 25);  // NOT 30!
}

test "version chain traversal" {
    // Test that version chains are walked correctly
    // Insert -> Update -> Update -> Read should find right version
}

test "deleted row invisible to new transactions" {
    // Delete row in T1, commit
    // T2 should not see the row
}
```

**Acceptance Criteria:**
- ✅ Snapshot isolation verified
- ✅ Version chains traversed correctly
- ✅ Concurrent reads/writes work

---

### Phase 3: Garbage Collection (1.5 weeks)

Old versions accumulate over time and must be cleaned up.

#### Phase 3.1: Vacuum Process Design (3-4 days)

**File:** Create `src/mvcc_vacuum.zig`

**Concepts:**
- **Oldest Active Transaction**: Minimum TxID of all active transactions
- **Safe Horizon**: Versions older than oldest active transaction can be deleted
- **Vacuum**: Process that removes dead versions

**Tasks:**

1. **Implement Vacuum Manager**
```zig
pub const VacuumManager = struct {
    allocator: Allocator,

    /// Find oldest active transaction ID
    pub fn findOldestActiveTx(tx_mgr: *TransactionManager) u64 {
        tx_mgr.mutex.lock();
        defer tx_mgr.mutex.unlock();

        var min_tx_id: u64 = std.math.maxInt(u64);

        var iter = tx_mgr.active_txs.iterator();
        while (iter.next()) |entry| {
            const tx = entry.value_ptr.*;
            if (tx.id < min_tx_id) {
                min_tx_id = tx.id;
            }
        }

        return min_tx_id;
    }

    /// Vacuum a single version chain
    pub fn vacuumChain(chain: *VersionChain, horizon: u64) !usize {
        chain.mutex.lock();
        defer chain.mutex.unlock();

        var removed: usize = 0;
        var current = &chain.head;

        while (current.*) |version| {
            // Keep version if:
            // 1. It's the head (most recent)
            // 2. It might be visible to active transactions
            const keep = (version == chain.head) or
                         (version.tx_id_max >= horizon);

            if (keep) {
                current = &version.next;
            } else {
                // Remove this version
                const next = version.next;
                version.deinit(chain.allocator);
                current.* = next;
                removed += 1;
            }
        }

        return removed;
    }

    /// Vacuum entire table
    pub fn vacuumTable(table: *MVCCTable, tx_mgr: *TransactionManager) !usize {
        const horizon = findOldestActiveTx(tx_mgr);
        var total_removed: usize = 0;

        var iter = table.version_chains.iterator();
        while (iter.next()) |entry| {
            const chain = entry.value_ptr.*;
            total_removed += try vacuumChain(chain, horizon);
        }

        return total_removed;
    }

    /// Vacuum entire database
    pub fn vacuumDatabase(db: *Database) !VacuumStats {
        var stats = VacuumStats{};

        var iter = db.tables.iterator();
        while (iter.next()) |entry| {
            const table = entry.value_ptr.*;
            if (table.storage_mode == .mvcc) {
                const removed = try vacuumTable(&table.mvcc_storage.?, &db.tx_manager);
                stats.versions_removed += removed;
                stats.tables_vacuumed += 1;
            }
        }

        return stats;
    }
};

pub const VacuumStats = struct {
    versions_removed: usize = 0,
    tables_vacuumed: usize = 0,
};
```

2. **Add VACUUM SQL command**
```zig
// In sql.zig
pub const SqlCommand = union(enum) {
    // ... existing commands
    vacuum: VacuumCmd,
};

pub const VacuumCmd = struct {
    table_name: ?[]const u8,  // null = vacuum all tables
};
```

**Acceptance Criteria:**
- ✅ Vacuum correctly identifies removable versions
- ✅ Vacuum doesn't remove versions needed by active transactions
- ✅ VACUUM SQL command works

---

#### Phase 3.2: Automatic Vacuum (2-3 days)

**File:** Modify `src/mvcc_vacuum.zig`

**Tasks:**

1. **Implement auto-vacuum trigger**
```zig
pub const AutoVacuumConfig = struct {
    enabled: bool = true,
    threshold_transactions: u64 = 1000,  // Vacuum after N transactions
    threshold_dead_tuples: usize = 10000,  // Vacuum after N dead versions
};

pub const AutoVacuumManager = struct {
    config: AutoVacuumConfig,
    tx_count_since_vacuum: std.atomic.Value(u64),

    pub fn shouldVacuum(self: *AutoVacuumManager) bool {
        if (!self.config.enabled) return false;

        const tx_count = self.tx_count_since_vacuum.load(.monotonic);
        return tx_count >= self.config.threshold_transactions;
    }

    pub fn onTransactionCommit(self: *AutoVacuumManager, db: *Database) !void {
        _ = self.tx_count_since_vacuum.fetchAdd(1, .monotonic);

        if (self.shouldVacuum()) {
            // Run vacuum in background thread
            const thread = try std.Thread.spawn(.{}, vacuumWorker, .{db});
            thread.detach();

            // Reset counter
            self.tx_count_since_vacuum.store(0, .monotonic);
        }
    }

    fn vacuumWorker(db: *Database) void {
        _ = VacuumManager.vacuumDatabase(db) catch |err| {
            std.log.err("Auto-vacuum failed: {}", .{err});
        };
    }
};
```

2. **Integrate with Database**
```zig
pub const Database = struct {
    // ... existing fields
    auto_vacuum: AutoVacuumManager,

    pub fn commit(self: *Database, tx: *Transaction) !void {
        // Existing commit logic...
        try tx.commit(&self.tx_manager);

        // Trigger auto-vacuum check
        try self.auto_vacuum.onTransactionCommit(self);
    }
};
```

**Acceptance Criteria:**
- ✅ Auto-vacuum triggers after configured threshold
- ✅ Vacuum runs in background without blocking
- ✅ Can disable auto-vacuum for testing

---

#### Phase 3.3: Testing Garbage Collection (2-3 days)

**File:** Create `src/test_mvcc_vacuum.zig`

**Tests:**
```zig
test "vacuum removes old versions" {
    // Create versions, commit, start new transaction
    // Vacuum should remove versions not visible to any active tx
}

test "vacuum preserves versions needed by active transactions" {
    // Long-running transaction T1
    // Many updates in T2, T3, T4
    // Vacuum should keep versions visible to T1
}

test "auto-vacuum triggers after threshold" {
    // Run 1000+ transactions
    // Verify auto-vacuum was triggered
}

test "vacuum performance on large version chains" {
    // Create 100k versions
    // Measure vacuum time
}
```

**Acceptance Criteria:**
- ✅ Vacuum tests pass
- ✅ No memory leaks in vacuum
- ✅ Performance acceptable for large datasets

---

### Phase 4: WAL Integration (1 week)

MVCC requires WAL to record transaction IDs and version metadata.

#### Phase 4.1: WAL Record Extensions (3-4 days)

**File:** Modify `src/wal.zig`

**Tasks:**

1. **Add version metadata to WAL records**
```zig
pub const WalRecord = struct {
    type: WalRecordType,
    tx_id: u64,
    table_name: []const u8,
    row_id: u64,

    // NEW MVCC fields
    tx_id_min: u64,  // For version tracking
    tx_id_max: u64,  // For version tracking

    data: []const u8,
    checksum: u32,
};
```

2. **Update recovery to restore versions**
```zig
// In recovery.zig
pub fn recover(db: *Database, wal_dir: []const u8) !void {
    // Pass 1: Identify committed transactions (same as before)

    // Pass 2: Replay operations with version metadata
    while (try reader.readRecord()) |record| {
        if (isCommitted(record.tx_id)) {
            switch (record.type) {
                .insert_row => {
                    // Restore version with original tx_id_min
                    const version = try RowVersion.init(
                        allocator,
                        record.row_id,
                        record.tx_id_min,  // Use from WAL
                        deserialized_data,
                    );
                    // Add to table...
                },
                .update_row => {
                    // Mark old version with tx_id_max
                    // Create new version with tx_id_min
                },
                // ...
            }
        }
    }
}
```

**Acceptance Criteria:**
- ✅ WAL records include version metadata
- ✅ Recovery restores version chains correctly
- ✅ Backward compatibility with non-MVCC WAL

---

#### Phase 4.2: Testing WAL + MVCC (2-3 days)

**File:** Create `src/test_mvcc_wal.zig`

**Tests:**
```zig
test "MVCC recovery after crash" {
    // T1: Insert version A
    // T2: Update to version B
    // [CRASH]
    // Recovery should restore both versions correctly
}

test "MVCC snapshot isolation after recovery" {
    // Create complex version history
    // Crash and recover
    // Verify snapshots still work correctly
}
```

---

### Phase 5: Index Integration (1 week)

B-tree indexes must handle multiple versions.

#### Phase 5.1: MVCC-aware Indexes (4-5 days)

**File:** Modify `src/btree.zig` and `src/index_manager.zig`

**Design Decision:**
- Indexes point to ALL versions of a row
- Visibility filtering happens at read time

**Tasks:**

1. **Change index entries to store version lists**
```zig
pub const BTree = struct {
    // OLD: keys -> row_id
    // NEW: keys -> list of (row_id, tx_id_min, tx_id_max)

    pub const IndexEntry = struct {
        row_id: u64,
        tx_id_min: u64,
        tx_id_max: u64,
    };

    pub fn search(self: *BTree, key: ColumnValue, snapshot: *Snapshot) ![]IndexEntry {
        // Find all versions for this key
        const all_entries = try self.searchInternal(key);

        // Filter by visibility
        var visible = std.ArrayList(IndexEntry).init(self.allocator);
        for (all_entries) |entry| {
            if (isVisibleVersion(entry, snapshot)) {
                try visible.append(entry);
            }
        }

        return visible.toOwnedSlice();
    }
};
```

2. **Update index on INSERT/UPDATE/DELETE**
```zig
// On INSERT: Add entry with tx_id_min
try index.insert(key_value, .{
    .row_id = row_id,
    .tx_id_min = tx.id,
    .tx_id_max = RowVersion.MAX_TX_ID,
});

// On UPDATE: Mark old entry with tx_id_max, add new entry
try index.markDeleted(key_value, row_id, tx.id);
try index.insert(new_key_value, .{
    .row_id = row_id,
    .tx_id_min = tx.id,
    .tx_id_max = RowVersion.MAX_TX_ID,
});

// On DELETE: Mark entry as deleted
try index.markDeleted(key_value, row_id, tx.id);
```

**Acceptance Criteria:**
- ✅ Indexes return correct versions for snapshots
- ✅ Queries with WHERE use indexes correctly in MVCC mode
- ✅ Index vacuum removes old entries

---

### Phase 6: Conflict Detection (1 week)

Detect write-write conflicts for serializable isolation.

#### Phase 6.1: Write Conflict Detection (4-5 days)

**File:** Create `src/mvcc_conflicts.zig`

**Concepts:**
- **Write-Write Conflict**: Two transactions update the same row
- **First-Committer Wins**: Second transaction aborts

**Tasks:**

1. **Implement write lock tracking**
```zig
pub const WriteSet = struct {
    rows: std.AutoHashMap(RowId, void),  // Rows modified by this tx
    allocator: Allocator,

    pub fn add(self: *WriteSet, table: []const u8, row_id: u64) !void {
        try self.rows.put(.{ .table = table, .row_id = row_id }, {});
    }

    pub fn contains(self: *WriteSet, table: []const u8, row_id: u64) bool {
        return self.rows.contains(.{ .table = table, .row_id = row_id });
    }
};

pub const RowId = struct {
    table: []const u8,
    row_id: u64,
};
```

2. **Add conflict detection to UPDATE/DELETE**
```zig
pub fn update(self: *MVCCTable, tx: *Transaction, row_id: u64, new_data: Row) !void {
    const chain = self.version_chains.get(row_id) orelse return error.RowNotFound;

    chain.mutex.lock();
    defer chain.mutex.unlock();

    const current_version = chain.head orelse return error.RowNotFound;

    // Check for conflict: has another tx modified this row?
    if (current_version.tx_id_min != tx.snapshot.tx_id and
        current_version.tx_id_min >= tx.snapshot.tx_id) {
        // Row was modified by a concurrent transaction
        return error.WriteConflict;
    }

    // Proceed with update...
}
```

3. **Handle conflicts in executor**
```zig
// In database.zig
fn executeUpdate(self: *Database, cmd: UpdateCmd) !QueryResult {
    const tx = self.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;

    table.update(tx, row_id, new_data) catch |err| {
        if (err == error.WriteConflict) {
            // Abort transaction
            try self.executeRollback();
            return error.SerializationFailure;
        }
        return err;
    };
}
```

**Acceptance Criteria:**
- ✅ Write-write conflicts detected
- ✅ First committer wins
- ✅ Clear error messages for conflicts

---

### Phase 7: Testing & Benchmarking (1 week)

#### Phase 7.1: Comprehensive MVCC Tests (3-4 days)

**File:** Create `src/test_mvcc_integration.zig`

**Scenarios to Test:**

1. **Snapshot Isolation**
```zig
test "phantom reads prevented" {
    // T1: SELECT COUNT(*) FROM users -> 10
    // T2: INSERT INTO users ...
    // T2: COMMIT
    // T1: SELECT COUNT(*) FROM users -> should still be 10
}

test "repeatable reads" {
    // T1: SELECT * FROM users WHERE id = 1
    // T2: UPDATE users SET name = 'Bob' WHERE id = 1
    // T2: COMMIT
    // T1: SELECT * FROM users WHERE id = 1 -> should see original value
}
```

2. **Write Conflicts**
```zig
test "concurrent updates conflict" {
    // T1: UPDATE users SET balance = 100 WHERE id = 1
    // T2: UPDATE users SET balance = 200 WHERE id = 1
    // One should fail with WriteConflict
}
```

3. **Long-running Transactions**
```zig
test "long transaction doesn't block short transactions" {
    // T1: BEGIN, SELECT * (keeps snapshot)
    // T2: INSERT, COMMIT
    // T3: SELECT * (should see T2's insert)
    // T1: SELECT * (should NOT see T2's insert)
}
```

**Acceptance Criteria:**
- ✅ All isolation tests pass
- ✅ No data corruption under concurrent load
- ✅ Conflict detection works correctly

---

#### Phase 7.2: Performance Benchmarking (3-4 days)

**File:** Create `benchmarks/mvcc_benchmarks.zig`

**Benchmarks:**

1. **Read throughput**
```zig
// Measure: reads/second with concurrent writers
// Compare: Simple storage vs MVCC
```

2. **Write throughput**
```zig
// Measure: writes/second with concurrent readers
// Compare: Simple storage vs MVCC
```

3. **Vacuum overhead**
```zig
// Measure: vacuum time vs database size
// Measure: impact on concurrent operations
```

4. **Memory overhead**
```zig
// Measure: memory per version
// Measure: memory growth over time
```

**Acceptance Criteria:**
- ✅ Read throughput improves with MVCC (no lock contention)
- ✅ Write throughput acceptable (< 30% overhead)
- ✅ Vacuum completes in reasonable time
- ✅ Memory overhead documented

---

## Success Metrics

### Functional Requirements
- ✅ Snapshot isolation level achieved
- ✅ Readers never block writers
- ✅ Writers never block readers
- ✅ Write conflicts detected and handled
- ✅ Vacuum reclaims old versions
- ✅ WAL recovery restores version chains

### Performance Requirements
- ✅ Read throughput: 2x improvement with concurrent writes
- ✅ Write overhead: < 30% compared to simple mode
- ✅ Memory overhead: < 3x per row on average
- ✅ Vacuum time: < 5 seconds per million versions

### Code Quality
- ✅ 100+ new tests for MVCC
- ✅ Zero memory leaks under valgrind
- ✅ Thread-safe concurrent operations
- ✅ Comprehensive inline documentation

---

## Documentation Deliverables

- [ ] **MVCC_DESIGN.md** - Architecture and design decisions
- [ ] **MVCC_USAGE.md** - How to use MVCC features
- [ ] **VACUUM.md** - Vacuum process and tuning
- [ ] Update **README.md** with MVCC examples
- [ ] Update **SQL_FEATURES.md** with VACUUM command
- [ ] Add **ISOLATION_LEVELS.md** explaining guarantees

---

## Risk Mitigation

### Technical Risks

1. **Memory Growth from Versions**
   - Risk: Old versions accumulate faster than vacuum can clean
   - Mitigation: Aggressive auto-vacuum defaults, monitoring tools
   - Fallback: Manual VACUUM command, configurable thresholds

2. **Vacuum Performance**
   - Risk: Vacuum takes too long, blocking operations
   - Mitigation: Incremental vacuum, background processing
   - Fallback: Vacuum during off-peak hours

3. **Index Bloat**
   - Risk: Indexes grow with multiple versions per key
   - Mitigation: Index-specific vacuum, periodic rebuild
   - Fallback: Drop and recreate indexes

4. **Lock Contention on Version Chains**
   - Risk: Per-chain mutex becomes bottleneck
   - Mitigation: Fine-grained locking, lock-free reads
   - Fallback: Partition version chains by hash

5. **WAL Size Growth**
   - Risk: More metadata in WAL increases file size
   - Mitigation: Compression, more frequent checkpoints
   - Fallback: Larger WAL rotation threshold

### Complexity Risks

1. **Correctness Hard to Verify**
   - Risk: Subtle bugs in visibility logic
   - Mitigation: Extensive testing, formal verification of visibility rules
   - Fallback: Conservative visibility (may over-retain versions)

2. **Integration with Existing Code**
   - Risk: Breaking changes to table/database APIs
   - Mitigation: Dual-mode support (simple/MVCC), gradual migration
   - Fallback: MVCC as opt-in feature

---

## Implementation Timeline

### Week 1-2: Foundation
- Phase 1.1: Enhanced Transaction Manager (4-5 days)
- Phase 1.2: Testing Transaction Manager (2-3 days)

### Week 3-4: Storage Layer
- Phase 2.1: Row Version Data Structure (4-5 days)
- Phase 2.2: Integration with Table (3-4 days)
- Phase 2.3: Testing Storage (2-3 days)

### Week 5-6: Garbage Collection
- Phase 3.1: Vacuum Process (3-4 days)
- Phase 3.2: Auto-vacuum (2-3 days)
- Phase 3.3: Testing Vacuum (2-3 days)

### Week 7: WAL Integration
- Phase 4.1: WAL Record Extensions (3-4 days)
- Phase 4.2: Testing WAL + MVCC (2-3 days)

### Week 8: Indexes & Conflicts
- Phase 5.1: MVCC-aware Indexes (4-5 days)
- Phase 6.1: Conflict Detection (4-5 days)

### Week 9: Testing & Polish
- Phase 7.1: Integration Tests (3-4 days)
- Phase 7.2: Benchmarking (3-4 days)

**Total: 6-8 weeks** (includes buffer for iteration and bug fixes)

---

## Future Enhancements (Post-MVCC)

1. **Serializable Snapshot Isolation (SSI)**
   - Detect read-write conflicts
   - Prevent write skew anomalies
   - True serializability

2. **Hot Standby / Replication**
   - Stream version changes to replicas
   - Read replicas with snapshot isolation

3. **Time-Travel Queries**
   - `SELECT * FROM users AS OF TIMESTAMP '2024-01-01'`
   - Query historical data

4. **Vacuum Optimizations**
   - Parallel vacuum
   - Incremental vacuum
   - Vacuum progress tracking

5. **Lock-Free Data Structures**
   - Replace mutexes with atomic operations
   - Even better concurrent performance

---

## Conclusion

MVCC is the most sophisticated concurrency control mechanism, enabling:
- **True concurrent access** without blocking
- **Consistent snapshots** for long-running queries
- **Production-grade isolation** (snapshot isolation level)

This plan provides a structured 6-8 week path to implementing industrial-strength MVCC in zvdb. The phased approach ensures:
- Incremental progress with testable milestones
- Backward compatibility with simple storage mode
- Comprehensive testing at each phase
- Clear success metrics and risk mitigation

**Recommendation**: Start with Phase 1 (Transaction Manager redesign) as it's the foundation for everything else. Each phase is independently testable, allowing for course correction if needed.

Ready to begin implementation! 🚀
