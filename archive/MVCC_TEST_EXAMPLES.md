# MVCC Phase 1 Test Examples

This document provides concrete examples of what the tests verify, with visual timelines to illustrate key MVCC concepts.

## Example 1: Basic Snapshot Isolation

### Scenario
```
Timeline:
T1: BEGIN ───────────────────────────────→
T2:           BEGIN ─────────────────────→
```

### What the test verifies
```zig
// T1 begins
const tx1_id = try tx_mgr.begin();  // tx1_id = 1

// T2 begins (T1 is still active)
const tx2_id = try tx_mgr.begin();  // tx2_id = 2

// Get T2's snapshot
const tx2 = tx_mgr.getTransaction(tx2_id).?;
const snapshot2 = &tx2.snapshot.?;

// T2's snapshot should include T1 as active
assert(snapshot2.wasActive(tx1_id) == true);
assert(snapshot2.wasActive(tx2_id) == false);  // T2 doesn't see itself
```

**Why this matters**: In MVCC, when T2 takes a snapshot, it records which transactions were active at that moment. T1 was active, so T2's snapshot includes it. This means T2 won't see any changes made by T1 (even if T1 commits later).

---

## Example 2: Committed Transaction Not Visible

### Scenario
```
Timeline:
T1: BEGIN ───→ COMMIT
T2:                    BEGIN ───────────→
```

### What the test verifies
```zig
// T1 begins and commits
const tx1_id = try tx_mgr.begin();  // tx1_id = 1
try tx_mgr.commit(tx1_id);

// T2 begins AFTER T1 committed
const tx2_id = try tx_mgr.begin();  // tx2_id = 2

// Get T2's snapshot
const tx2 = tx_mgr.getTransaction(tx2_id).?;
const snapshot2 = &tx2.snapshot.?;

// T2's snapshot should NOT include T1 (it was already committed)
assert(snapshot2.wasActive(tx1_id) == false);
assert(snapshot2.active_txids.len == 0);  // No active transactions
```

**Why this matters**: Since T1 already committed before T2 began, T1 is not in T2's active set. This means T2 CAN see T1's changes (they're committed and visible).

---

## Example 3: Multiple Overlapping Transactions

### Scenario
```
Timeline:
T1: BEGIN ───────────→ COMMIT
T2:      BEGIN ──────────────────────────→
T3:                    BEGIN ─────────────→
T4:                           BEGIN ──────→
```

### What the test verifies
```zig
// T1 begins
const tx1_id = try tx_mgr.begin();  // tx1_id = 1

// T2 begins (sees T1 as active)
const tx2_id = try tx_mgr.begin();  // tx2_id = 2
const tx2 = tx_mgr.getTransaction(tx2_id).?;
assert(tx2.snapshot.?.wasActive(tx1_id) == true);

// T1 commits
try tx_mgr.commit(tx1_id);

// T3 begins (T1 already committed, T2 still active)
const tx3_id = try tx_mgr.begin();  // tx3_id = 3
const tx3 = tx_mgr.getTransaction(tx3_id).?;
assert(tx3.snapshot.?.wasActive(tx1_id) == false);  // T1 committed
assert(tx3.snapshot.?.wasActive(tx2_id) == true);   // T2 active

// T4 begins (T1 committed, T2 and T3 active)
const tx4_id = try tx_mgr.begin();  // tx4_id = 4
const tx4 = tx_mgr.getTransaction(tx4_id).?;
assert(tx4.snapshot.?.wasActive(tx1_id) == false);  // T1 committed
assert(tx4.snapshot.?.wasActive(tx2_id) == true);   // T2 active
assert(tx4.snapshot.?.wasActive(tx3_id) == true);   // T3 active
```

**Why this matters**: This demonstrates how snapshots correctly track which transactions are active vs. committed. Each new transaction sees an accurate point-in-time view of the database state.

---

## Example 4: Transaction ID Uniqueness

### Scenario
```
Timeline:
T1: BEGIN
T2:       BEGIN
T3:             BEGIN
T4:                   BEGIN
```

### What the test verifies
```zig
// Begin 4 transactions simultaneously
const tx1_id = try tx_mgr.begin();  // tx1_id = 1
const tx2_id = try tx_mgr.begin();  // tx2_id = 2
const tx3_id = try tx_mgr.begin();  // tx3_id = 3
const tx4_id = try tx_mgr.begin();  // tx4_id = 4

// All IDs must be unique
assert(tx1_id != tx2_id);
assert(tx1_id != tx3_id);
assert(tx1_id != tx4_id);
assert(tx2_id != tx3_id);
assert(tx2_id != tx4_id);
assert(tx3_id != tx4_id);

// All should be retrievable
assert(tx_mgr.getTransaction(tx1_id) != null);
assert(tx_mgr.getTransaction(tx2_id) != null);
assert(tx_mgr.getTransaction(tx3_id) != null);
assert(tx_mgr.getTransaction(tx4_id) != null);

// Active count should be 4
assert(tx_mgr.activeCount() == 4);
```

**Why this matters**: In the old single-transaction system, trying to BEGIN twice would error with "TransactionAlreadyActive". Now, multiple concurrent transactions work correctly, each with a unique ID.

---

## Example 5: CommitLog Status Tracking

### Scenario
```
Timeline:
T1: BEGIN ───→ COMMIT
T2: BEGIN ───→ ROLLBACK
T3: BEGIN ───────────────────→ (still active)
```

### What the test verifies
```zig
// T1: Begin and commit
const tx1_id = try tx_mgr.begin();
try tx_mgr.commit(tx1_id);

// T2: Begin and rollback
const tx2_id = try tx_mgr.begin();
try tx_mgr.rollback(tx2_id);

// T3: Begin and leave active
const tx3_id = try tx_mgr.begin();

// Check CLOG statuses
assert(tx_mgr.clog.isCommitted(tx1_id) == true);
assert(tx_mgr.clog.isAborted(tx2_id) == true);
assert(tx_mgr.clog.isInProgress(tx3_id) == true);

// These should be false
assert(tx_mgr.clog.isCommitted(tx2_id) == false);
assert(tx_mgr.clog.isAborted(tx1_id) == false);
assert(tx_mgr.clog.isInProgress(tx1_id) == false);
```

**Why this matters**: The CommitLog (CLOG) maintains the permanent status of all transactions. Even after a transaction ends and is removed from the active map, its final status (committed or aborted) remains queryable. This is essential for MVCC visibility checking.

---

## Example 6: Transaction Lifecycle and Active Count

### Scenario
```
Timeline:
Count: 0
  ↓ BEGIN T1
Count: 1
  ↓ BEGIN T2
Count: 2
  ↓ BEGIN T3
Count: 3
  ↓ COMMIT T1
Count: 2
  ↓ ROLLBACK T2
Count: 1
  ↓ COMMIT T3
Count: 0
```

### What the test verifies
```zig
// Initially no active transactions
assert(tx_mgr.activeCount() == 0);

// Begin T1
const tx1_id = try tx_mgr.begin();
assert(tx_mgr.activeCount() == 1);

// Begin T2
const tx2_id = try tx_mgr.begin();
assert(tx_mgr.activeCount() == 2);

// Begin T3
const tx3_id = try tx_mgr.begin();
assert(tx_mgr.activeCount() == 3);

// Commit T1 (count decreases)
try tx_mgr.commit(tx1_id);
assert(tx_mgr.activeCount() == 2);

// Rollback T2 (count decreases)
try tx_mgr.rollback(tx2_id);
assert(tx_mgr.activeCount() == 1);

// Commit T3 (back to 0)
try tx_mgr.commit(tx3_id);
assert(tx_mgr.activeCount() == 0);
```

**Why this matters**: Demonstrates that TransactionManager correctly tracks the number of active transactions, incrementing on BEGIN and decrementing on COMMIT/ROLLBACK.

---

## Example 7: Independent Transaction Commits

### Scenario
```
Timeline:
T1: BEGIN ────────────────────────────────→
T2:      BEGIN ───→ COMMIT
T3:                    BEGIN ──────────────→
```

### What the test verifies
```zig
// Begin three transactions
const tx1_id = try tx_mgr.begin();
const tx2_id = try tx_mgr.begin();
const tx3_id = try tx_mgr.begin();

assert(tx_mgr.activeCount() == 3);

// Commit T2 in the middle
try tx_mgr.commit(tx2_id);

// T1 and T3 should still be active
assert(tx_mgr.activeCount() == 2);
assert(tx_mgr.getTransaction(tx1_id) != null);
assert(tx_mgr.getTransaction(tx2_id) == null);  // T2 is gone
assert(tx_mgr.getTransaction(tx3_id) != null);

// Verify CLOG states
assert(tx_mgr.clog.isInProgress(tx1_id) == true);
assert(tx_mgr.clog.isCommitted(tx2_id) == true);
assert(tx_mgr.clog.isInProgress(tx3_id) == true);
```

**Why this matters**: Committing one transaction doesn't affect others. Each transaction is independent and can commit/rollback without interfering with other active transactions.

---

## Example 8: Memory Safety with Leak Detection

### Scenario
Multiple transactions with snapshots, all cleaned up properly.

### What the test verifies
```zig
// Use GeneralPurposeAllocator with leak detection
var gpa = std.heap.GeneralPurposeAllocator(.{}){};
defer {
    const leaked = gpa.deinit();
    if (leaked == .leak) {
        @panic("Memory leak detected!");
    }
}
const allocator = gpa.allocator();

var tx_mgr = TransactionManager.init(allocator);
defer tx_mgr.deinit();

// Run 100 transaction cycles
var i: usize = 0;
while (i < 100) : (i += 1) {
    const tx_id = try tx_mgr.begin();
    // Each begin() allocates:
    // - Transaction struct
    // - Snapshot struct
    // - Active txids array in snapshot

    try tx_mgr.commit(tx_id);
    // commit() must free all of the above
}

// If ANY memory leaked, GPA.deinit() will catch it
```

**Why this matters**: Ensures that all memory allocated for transactions and snapshots is properly freed. This includes:
- Transaction heap allocation
- Snapshot structure
- Active transaction ID array within snapshot
- All cleaned up on commit/rollback

---

## Example 9: Complex Multi-Transaction Integration

### Scenario
```
Timeline:
T1: BEGIN ─────────→ COMMIT
T2:      BEGIN ─────────────────→ ROLLBACK
T3:           BEGIN ─────────────────────────────→
T4:                        BEGIN ─────────────────→
T5:                                      BEGIN ───→

State at each point:
- T1 begins:  active={}, T1_snapshot=[]
- T2 begins:  active={T1}, T2_snapshot=[T1]
- T3 begins:  active={T1,T2}, T3_snapshot=[T1,T2]
- T1 commits: active={T2,T3}
- T4 begins:  active={T2,T3}, T4_snapshot=[T2,T3]  (NOT T1!)
- T2 aborts:  active={T3,T4}
- T5 begins:  active={T3,T4}, T5_snapshot=[T3,T4]  (NOT T1 or T2!)
```

### What the test verifies
```zig
// T1 begins (sees nothing)
const tx1_id = try tx_mgr.begin();

// T2 begins (sees T1)
const tx2_id = try tx_mgr.begin();
const tx2 = tx_mgr.getTransaction(tx2_id).?;
assert(tx2.snapshot.?.wasActive(tx1_id) == true);

// T3 begins (sees T1, T2)
const tx3_id = try tx_mgr.begin();
const tx3 = tx_mgr.getTransaction(tx3_id).?;
assert(tx3.snapshot.?.wasActive(tx1_id) == true);
assert(tx3.snapshot.?.wasActive(tx2_id) == true);

// T1 commits
try tx_mgr.commit(tx1_id);
assert(tx_mgr.clog.isCommitted(tx1_id) == true);

// T4 begins (sees T2, T3 but NOT T1)
const tx4_id = try tx_mgr.begin();
const tx4 = tx_mgr.getTransaction(tx4_id).?;
assert(tx4.snapshot.?.wasActive(tx1_id) == false);  // T1 committed
assert(tx4.snapshot.?.wasActive(tx2_id) == true);
assert(tx4.snapshot.?.wasActive(tx3_id) == true);

// T2 rollbacks
try tx_mgr.rollback(tx2_id);
assert(tx_mgr.clog.isAborted(tx2_id) == true);

// T5 begins (sees T3, T4 but NOT T1 or T2)
const tx5_id = try tx_mgr.begin();
const tx5 = tx_mgr.getTransaction(tx5_id).?;
assert(tx5.snapshot.?.wasActive(tx1_id) == false);  // T1 committed
assert(tx5.snapshot.?.wasActive(tx2_id) == false);  // T2 aborted
assert(tx5.snapshot.?.wasActive(tx3_id) == true);
assert(tx5.snapshot.?.wasActive(tx4_id) == true);

// Active count should be 3 (T3, T4, T5)
assert(tx_mgr.activeCount() == 3);
```

**Why this matters**: This comprehensive scenario demonstrates that all components work together correctly:
- Snapshots capture correct active sets
- CLOG tracks all states
- Active count is accurate
- Transaction independence is maintained

---

## Example 10: Snapshot with Many Active Transactions

### Scenario
```
Timeline:
T1-T20: All BEGIN simultaneously
T21:    BEGIN (should see all 20 as active)
```

### What the test verifies
```zig
// Start 20 transactions
var tx_ids: [20]u64 = undefined;
var i: usize = 0;
while (i < 20) : (i += 1) {
    tx_ids[i] = try tx_mgr.begin();
}

// Start one more - should see all previous 20 as active
const final_tx_id = try tx_mgr.begin();
const final_tx = tx_mgr.getTransaction(final_tx_id).?;
const snapshot = &final_tx.snapshot.?;

// Snapshot should have 20 active transactions
assert(snapshot.active_txids.len == 20);

// Verify all are marked as active
i = 0;
while (i < 20) : (i += 1) {
    assert(snapshot.wasActive(tx_ids[i]) == true);
}

// But NOT the snapshot's own ID
assert(snapshot.wasActive(final_tx_id) == false);
```

**Why this matters**: Tests that snapshots can handle many concurrent active transactions without issues. The active_txids array correctly stores all active transaction IDs.

---

## Key MVCC Concepts Demonstrated

### 1. Snapshot Isolation
- Each transaction sees a consistent view of the database
- Snapshot is taken at BEGIN time
- Snapshot includes all currently active transactions
- Changes by active transactions are invisible

### 2. Transaction ID Management
- Monotonically increasing IDs
- Atomic allocation (thread-safe)
- Unique per transaction

### 3. Commit Log (CLOG)
- Permanent record of transaction status
- Three states: in_progress, committed, aborted
- Queryable even after transaction ends
- Used for visibility checking

### 4. Concurrent Transactions
- Multiple transactions can run simultaneously
- Each has independent state
- Committing/aborting one doesn't affect others
- No more single-transaction limitation

### 5. Memory Management
- All allocations properly tracked
- Clean cleanup on commit/rollback
- No leaks even with many transactions
- Safe cleanup on TransactionManager deinit

---

## How This Enables Phase 2

These tests verify that Phase 1 provides the foundation for Phase 2:

1. **Visibility Checking**: With snapshots and CLOG working, Phase 2 can implement tuple visibility rules
2. **Version Chains**: Transaction IDs can be used as xmin/xmax in tuple headers
3. **Garbage Collection**: CLOG status helps identify when old versions can be cleaned up
4. **Read Committed**: Can implement by taking new snapshot on each statement
5. **Repeatable Read**: Can implement by keeping same snapshot for entire transaction

The infrastructure is solid and ready for storage layer integration!
