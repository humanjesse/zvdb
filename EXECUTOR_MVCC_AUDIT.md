# Executor.zig MVCC Audit Report
**Date:** 2025-11-16
**File:** src/database/executor.zig (2,970 lines)
**Purpose:** Identify all Table access points for MVCC migration

## Executive Summary

### Statistics
- **Table Retrievals:** 11 locations
- **Table Method Calls:** 46+ locations
- **Direct Field Access:** 100+ locations
- **Write Operations:** 5 critical paths
- **Read Operations:** 30+ query types

### Key Findings

✅ **Good News for MVCC:**
- No direct `table.rows` HashMap access - all through methods
- Atomic `next_id` already in place (Priority 1 complete)
- WAL-Ahead protocol established
- Transaction tracking infrastructure exists
- Clean method-based API

⚠️ **Critical Challenges:**
- **executeUpdate (lines 2582-2810):** Mutates rows in-place at line 2775
- **executeDelete (lines 2504-2580):** Physically removes data
- **executeRollback (lines 2871-2969):** Uses physical undo operations
- **21 `table.get()` call sites:** Must add visibility checks
- **11 `table.getAllRows()` call sites:** Must filter by snapshot

## Most Critical Functions

### 1. executeUpdate (Lines 2582-2810) - **HIGHEST PRIORITY**
**Current Behavior:**
```zig
var row = table.get(row_id) orelse continue;  // Mutable reference
try row.set(db.allocator, column, new_value);  // In-place mutation
```

**Required Change:**
```zig
const old_version = table.get(row_id, snapshot) orelse continue;  // Immutable
const new_version = try old_version.clone();
try new_version.set(db.allocator, column, new_value);
try table.createVersion(row_id, new_version, txid);  // Link versions
```

**Impact:** Core MVCC requirement - cannot mutate in-place

---

### 2. table.get() - 21 Call Sites
**Locations:**
- Line 898, 910: executeInsert (row retrieval for indexes)
- Line 2157: executeSelect (WHERE evaluation)
- Line 2294: executeAggregateSelect
- Line 2403: executeGroupBySelect
- Lines 1563, 1567, 1592, 1613, 1655, 1676: executeTwoTableJoin
- Lines 1751, 1830, 1866, 1908, 1945: executeJoinStage
- Line 2512: executeDelete
- Line 2627: executeUpdate
- Lines 2919, 2941, 2949: executeRollback

**Required Change:**
```zig
// BEFORE:
const row = table.get(row_id) orelse continue;

// AFTER:
const snapshot = try db.tx_manager.getSnapshot();
const row = table.get(row_id, snapshot) orelse continue;
```

**Implementation:**
```zig
pub fn get(self: *Table, row_id: u64, snapshot: *const Snapshot) ?*Row {
    const version_chain = self.version_chains.get(row_id) orelse return null;
    var current = version_chain;

    // Walk chain from newest to oldest
    while (current) |version| {
        if (snapshot.isVisible(version)) {
            return &version.asRow();
        }
        current = version.next;
    }
    return null; // No visible version
}
```

---

### 3. table.getAllRows() - 11 Call Sites
**Locations:**
- Line 2125, 2138: executeSelect (full table scan)
- Line 1552, 1555: executeTwoTableJoin
- Line 1747: executeMultiTableJoin
- Line 1813: executeJoinStage
- Line 2290: executeAggregateSelect
- Line 2399: executeGroupBySelect
- Line 2508: executeDelete (find rows to delete)
- Line 2623: executeUpdate (find rows to update)

**Required Change:**
```zig
// BEFORE:
const row_ids = try table.getAllRows(db.allocator);

// AFTER:
const row_ids = try table.getAllRows(db.allocator, snapshot);
```

**Implementation:**
```zig
pub fn getAllRows(self: *Table, allocator: Allocator, snapshot: *const Snapshot) ![]u64 {
    var visible_ids = ArrayList(u64).init(allocator);

    var it = self.version_chains.iterator();
    while (it.next()) |entry| {
        const row_id = entry.key_ptr.*;
        var version = entry.value_ptr.*;

        // Check if any version is visible
        while (version) |v| {
            if (snapshot.isVisible(v)) {
                try visible_ids.append(row_id);
                break;
            }
            version = v.next;
        }
    }

    return visible_ids.toOwnedSlice();
}
```

---

### 4. executeInsert (Lines 845-932)
**Current Sequence:**
1. Reserve ID: `table.next_id.fetchAdd(1, .monotonic)` ✅ Already atomic
2. Write WAL: Log operation
3. Insert: `table.insertWithId(row_id, values_map)`
4. Update indexes

**Required Change:**
```zig
pub fn insertWithId(
    self: *Table,
    row_id: u64,
    values: StringHashMap(ColumnValue),
    txid: u64  // NEW: Transaction ID
) !void {
    const new_version = try self.allocator.create(RowVersion);
    new_version.* = .{
        .row_id = row_id,
        .xmin = txid,      // Created by this transaction
        .xmax = 0,         // Not deleted yet
        .next = null,      // First version
        .values = values,
    };

    try self.version_chains.put(row_id, new_version);
}
```

---

### 5. executeDelete (Lines 2504-2580)
**Current Behavior:**
```zig
try table.delete(row_id);  // Physically removes row
```

**Required Change:**
```zig
try table.markDeleted(row_id, txid);  // Logical delete

pub fn markDeleted(self: *Table, row_id: u64, txid: u64) !void {
    const current_version = self.version_chains.get(row_id) orelse return error.RowNotFound;
    current_version.xmax = txid;  // Mark as deleted
    // Don't remove from version_chains - old snapshots need it
}
```

---

## MVCC Data Structures

### RowVersion Structure
```zig
pub const RowVersion = struct {
    row_id: u64,
    xmin: u64,              // Transaction that created this version
    xmax: u64,              // Transaction that deleted/updated (0 = current)
    next: ?*RowVersion,     // Link to older version
    values: StringHashMap(ColumnValue),

    // Optional: Performance optimization
    hint_committed: bool = false,
    hint_aborted: bool = false,
};
```

### Snapshot Structure
```zig
pub const Snapshot = struct {
    txid: u64,                    // Snapshot taken at this TX ID
    active_txids: []const u64,    // Active when snapshot created
    commit_timestamp: i64,

    pub fn isVisible(self: *const Snapshot, version: *const RowVersion) bool {
        // Too new
        if (version.xmin > self.txid) return false;

        // Created by aborted transaction
        if (isTransactionAborted(version.xmin)) return false;

        // Not yet committed
        if (!isTransactionCommitted(version.xmin) and version.xmin != self.txid) {
            return false;
        }

        // Check deletion
        if (version.xmax != 0) {
            if (version.xmax <= self.txid and isTransactionCommitted(version.xmax)) {
                return false;
            }
        }

        return true;
    }
};
```

### Modified Table Structure
```zig
pub const Table = struct {
    name: []const u8,
    columns: ArrayList(Column),
    version_chains: AutoHashMap(u64, *RowVersion),  // row_id -> newest version
    next_id: std.atomic.Value(u64),  // ✅ Already done
    allocator: Allocator,
};
```

---

## Migration Phases

### Phase 1: Foundation (No Breaking Changes)
- [x] Add atomic next_id (Priority 1 - DONE)
- [ ] Create RowVersion struct
- [ ] Create Snapshot struct
- [ ] Add Transaction status tracking (CLOG)

### Phase 2: Read Path (Snapshot Isolation)
- [ ] Add snapshot parameter to table.get()
- [ ] Add snapshot parameter to table.getAllRows()
- [ ] Update 21 get() call sites
- [ ] Update 11 getAllRows() call sites
- [ ] Implement visibility checks

### Phase 3: Write Path - INSERT
- [ ] Modify executeInsert to create versions
- [ ] Set xmin = txid on new versions
- [ ] Test concurrent INSERT + SELECT

### Phase 4: Write Path - DELETE
- [ ] Modify executeDelete to set xmax instead of removing
- [ ] Keep physical data for old snapshots
- [ ] Test concurrent DELETE + SELECT

### Phase 5: Write Path - UPDATE (Most Complex)
- [ ] Rewrite executeUpdate to create new versions
- [ ] Link new version to old via version chain
- [ ] Set old.xmax = txid, new.xmin = txid
- [ ] Test concurrent UPDATE + SELECT

### Phase 6: Transaction Simplification
- [ ] Replace executeRollback physical undo with status update
- [ ] Mark aborted transactions in CLOG
- [ ] Test rollback visibility

### Phase 7: VACUUM & Cleanup
- [ ] Implement VACUUM to remove old versions
- [ ] SQL: VACUUM command
- [ ] Auto-vacuum background process
- [ ] Remove old single-version code

---

## Critical Access Point Inventory

### Write Operations (Must Create Versions)
| Function | Line | Operation | MVCC Change |
|----------|------|-----------|-------------|
| executeInsert | 893 | `table.insertWithId()` | Add txid, create version with xmin |
| executeUpdate | 2775 | `row.set()` | CREATE new version, don't mutate |
| executeDelete | 2555 | `table.delete()` | Set xmax, don't remove |
| executeRollback | 2916, 2938, 2963 | Physical undo | Mark transaction aborted |

### Read Operations (Must Check Visibility)
| Function | Lines | Count | MVCC Change |
|----------|-------|-------|-------------|
| executeInsert | 898, 910 | 2 | Add snapshot param |
| executeSelect | 2157 | 1 | Add snapshot param |
| executeAggregateSelect | 2294 | 1 | Add snapshot param |
| executeGroupBySelect | 2403 | 1 | Add snapshot param |
| executeTwoTableJoin | 1563, 1567, 1592, 1613, 1655, 1676 | 6 | Add snapshot param |
| executeJoinStage | 1751, 1830, 1866, 1908, 1945 | 5 | Add snapshot param |
| executeDelete | 2512 | 1 | Add snapshot param |
| executeUpdate | 2627 | 1 | Add snapshot param |
| executeRollback | 2919, 2941, 2949 | 3 | Add snapshot param |

### Table Scans (Must Filter Visible Rows)
| Function | Lines | MVCC Change |
|----------|-------|-------------|
| executeSelect | 2125, 2138 | Filter by snapshot |
| executeTwoTableJoin | 1552, 1555 | Filter by snapshot |
| executeMultiTableJoin | 1747 | Filter by snapshot |
| executeJoinStage | 1813 | Filter by snapshot |
| executeAggregateSelect | 2290 | Filter by snapshot |
| executeGroupBySelect | 2399 | Filter by snapshot |
| executeDelete | 2508 | Filter by snapshot |
| executeUpdate | 2623 | Filter by snapshot |

---

## Testing Requirements

### Unit Tests
- [ ] RowVersion.isVisible() with various transaction states
- [ ] Snapshot.create() captures active transactions correctly
- [ ] Version chain walking finds correct version
- [ ] table.get() with multiple versions

### Integration Tests
- [ ] Concurrent INSERT + SELECT see consistent data
- [ ] UPDATE creates new version, SELECT sees old or new based on snapshot
- [ ] DELETE marks deleted, old snapshots still see row
- [ ] Rollback makes changes invisible
- [ ] Multi-table JOIN uses consistent snapshot

### Stress Tests
- [ ] 100 concurrent readers during writes
- [ ] 1000 versions in single version chain
- [ ] Long-running query with many concurrent updates
- [ ] VACUUM with active transactions

---

## Recommended Order of Implementation

1. **Week 1-2:** RowVersion + Snapshot structs, visibility logic
2. **Week 3:** Update table.get() and table.getAllRows() (Phase 2)
3. **Week 4:** Migrate executeInsert (simplest write operation)
4. **Week 5:** Migrate executeDelete (logical deletion)
5. **Week 6:** Migrate executeUpdate (most complex, save for last)
6. **Week 7:** Simplify executeRollback
7. **Week 8:** VACUUM implementation
8. **Week 9:** Testing, optimization, bug fixes

---

## Performance Considerations

### Visibility Check Optimization
- **Hint bits:** Cache transaction status on versions
- **Visibility map:** Bitmap of which pages have all-visible tuples
- **HOT updates:** Heap-Only Tuples for updates that don't change indexed columns

### Memory Management
- **Version pruning:** VACUUM removes old versions
- **Memory limits:** Cap version chain length per row
- **Index bloat:** Lazy index updates to reduce garbage

---

## Files That Need Changes

| File | Changes Required | Priority |
|------|-----------------|----------|
| `src/table.zig` | Add version chains, MVCC methods | **CRITICAL** |
| `src/database/executor.zig` | Update all 150+ access points | **CRITICAL** |
| `src/transaction.zig` | Add CLOG, snapshot management | **HIGH** |
| `src/database/core.zig` | Snapshot capture in execute() | **HIGH** |
| `src/wal.zig` | Add version metadata to records | **MEDIUM** |
| `src/index_manager.zig` | MVCC-aware indexing strategy | **MEDIUM** |
| `src/database/recovery.zig` | Replay with version chains | **MEDIUM** |

---

**Audit Complete** - Ready for MVCC implementation with clear roadmap.
