const std = @import("std");
const testing = std.testing;
const transaction = @import("transaction.zig");
const TransactionManager = transaction.TransactionManager;
const Transaction = transaction.Transaction;
const Snapshot = transaction.Snapshot;
const CommitLog = transaction.CommitLog;
const TxStatus = transaction.TxStatus;

// ============================================================================
// Transaction ID Assignment Tests
// ============================================================================

test "transaction IDs are assigned sequentially" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin three transactions and verify IDs are sequential
    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();
    const tx3_id = try tx_mgr.begin();

    try testing.expectEqual(@as(u64, 1), tx1_id);
    try testing.expectEqual(@as(u64, 2), tx2_id);
    try testing.expectEqual(@as(u64, 3), tx3_id);

    // Clean up
    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
}

test "transaction IDs are monotonically increasing" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    var previous_id: u64 = 0;

    // Create and commit 10 transactions
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        const tx_id = try tx_mgr.begin();

        // Verify ID is greater than previous
        try testing.expect(tx_id > previous_id);
        previous_id = tx_id;

        try tx_mgr.commit(tx_id);
    }
}

test "concurrent transactions get unique IDs" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin multiple transactions simultaneously (without committing)
    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();
    const tx3_id = try tx_mgr.begin();
    const tx4_id = try tx_mgr.begin();

    // All IDs should be unique
    try testing.expect(tx1_id != tx2_id);
    try testing.expect(tx1_id != tx3_id);
    try testing.expect(tx1_id != tx4_id);
    try testing.expect(tx2_id != tx3_id);
    try testing.expect(tx2_id != tx4_id);
    try testing.expect(tx3_id != tx4_id);

    // Clean up
    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
    try tx_mgr.commit(tx4_id);
}

// ============================================================================
// Snapshot Creation Tests
// ============================================================================

test "snapshot excludes own transaction ID" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    defer tx_mgr.commit(tx_id) catch {};

    const tx = tx_mgr.getTransaction(tx_id).?;
    const snapshot = &tx.snapshot.?;

    // Snapshot should NOT include its own transaction ID
    try testing.expect(!snapshot.wasActive(tx_id));
}

test "snapshot captures active transactions correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Start T1
    const tx1_id = try tx_mgr.begin();

    // Start T2 - should see T1 as active
    const tx2_id = try tx_mgr.begin();

    // Start T3 - should see T1 and T2 as active
    const tx3_id = try tx_mgr.begin();

    const tx2 = tx_mgr.getTransaction(tx2_id).?;
    const tx3 = tx_mgr.getTransaction(tx3_id).?;

    const snapshot2 = &tx2.snapshot.?;
    const snapshot3 = &tx3.snapshot.?;

    // T2's snapshot should see T1 as active
    try testing.expect(snapshot2.wasActive(tx1_id));
    try testing.expect(!snapshot2.wasActive(tx2_id)); // Not itself

    // T3's snapshot should see T1 and T2 as active
    try testing.expect(snapshot3.wasActive(tx1_id));
    try testing.expect(snapshot3.wasActive(tx2_id));
    try testing.expect(!snapshot3.wasActive(tx3_id)); // Not itself

    // Clean up
    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
}

test "snapshot captures correct timestamp" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const before = std.time.timestamp();
    const tx_id = try tx_mgr.begin();
    const after = std.time.timestamp();

    const tx = tx_mgr.getTransaction(tx_id).?;
    const snapshot = &tx.snapshot.?;

    // Timestamp should be within reasonable range
    try testing.expect(snapshot.timestamp >= before);
    try testing.expect(snapshot.timestamp <= after + 1); // Allow 1 second tolerance

    try tx_mgr.commit(tx_id);
}

test "snapshot captures empty active set when first transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // First transaction should see no other active transactions
    const tx_id = try tx_mgr.begin();

    const tx = tx_mgr.getTransaction(tx_id).?;
    const snapshot = &tx.snapshot.?;

    // Active txids list should be empty
    try testing.expectEqual(@as(usize, 0), snapshot.active_txids.len);

    try tx_mgr.commit(tx_id);
}

// ============================================================================
// Snapshot Isolation Tests
// ============================================================================

test "snapshot isolation: T1 begins, T2 begins, T1 sees T2 as active" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1 begins
    const tx1_id = try tx_mgr.begin();

    // T2 begins (T1 is active)
    const tx2_id = try tx_mgr.begin();

    const tx2 = tx_mgr.getTransaction(tx2_id).?;
    const snapshot2 = &tx2.snapshot.?;

    // T2's snapshot should see T1 as active
    try testing.expect(snapshot2.wasActive(tx1_id));

    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
}

test "snapshot isolation: T1 commits before T2 begins, T2 does not see T1" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1 begins and commits
    const tx1_id = try tx_mgr.begin();
    try tx_mgr.commit(tx1_id);

    // T2 begins after T1 committed
    const tx2_id = try tx_mgr.begin();

    const tx2 = tx_mgr.getTransaction(tx2_id).?;
    const snapshot2 = &tx2.snapshot.?;

    // T2's snapshot should NOT see T1 (it's already committed)
    try testing.expect(!snapshot2.wasActive(tx1_id));

    try tx_mgr.commit(tx2_id);
}

test "snapshot isolation: multiple overlapping transactions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1 begins
    const tx1_id = try tx_mgr.begin();

    // T2 begins (sees T1)
    const tx2_id = try tx_mgr.begin();

    // T1 commits
    try tx_mgr.commit(tx1_id);

    // T3 begins (should NOT see T1, should see T2)
    const tx3_id = try tx_mgr.begin();

    // T4 begins (should NOT see T1, should see T2 and T3)
    const tx4_id = try tx_mgr.begin();

    const tx3 = tx_mgr.getTransaction(tx3_id).?;
    const tx4 = tx_mgr.getTransaction(tx4_id).?;

    const snapshot3 = &tx3.snapshot.?;
    const snapshot4 = &tx4.snapshot.?;

    // T3 should NOT see T1 (committed), should see T2
    try testing.expect(!snapshot3.wasActive(tx1_id));
    try testing.expect(snapshot3.wasActive(tx2_id));

    // T4 should NOT see T1 (committed), should see T2 and T3
    try testing.expect(!snapshot4.wasActive(tx1_id));
    try testing.expect(snapshot4.wasActive(tx2_id));
    try testing.expect(snapshot4.wasActive(tx3_id));

    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
    try tx_mgr.commit(tx4_id);
}

// ============================================================================
// CommitLog (CLOG) Tests
// ============================================================================

test "CLOG: setStatus and getStatus work correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Set and get committed status
    try clog.setStatus(1, .committed);
    try testing.expectEqual(TxStatus.committed, clog.getStatus(1));

    // Set and get aborted status
    try clog.setStatus(2, .aborted);
    try testing.expectEqual(TxStatus.aborted, clog.getStatus(2));

    // Set and get in_progress status
    try clog.setStatus(3, .in_progress);
    try testing.expectEqual(TxStatus.in_progress, clog.getStatus(3));
}

test "CLOG: default status is in_progress for unknown transactions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Query status of transaction that was never set
    const status = clog.getStatus(999);
    try testing.expectEqual(TxStatus.in_progress, status);
}

test "CLOG: isCommitted helper works correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);
    try clog.setStatus(2, .aborted);
    try clog.setStatus(3, .in_progress);

    try testing.expect(clog.isCommitted(1));
    try testing.expect(!clog.isCommitted(2));
    try testing.expect(!clog.isCommitted(3));
}

test "CLOG: isAborted helper works correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);
    try clog.setStatus(2, .aborted);
    try clog.setStatus(3, .in_progress);

    try testing.expect(!clog.isAborted(1));
    try testing.expect(clog.isAborted(2));
    try testing.expect(!clog.isAborted(3));
}

test "CLOG: isInProgress helper works correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);
    try clog.setStatus(2, .aborted);
    try clog.setStatus(3, .in_progress);

    try testing.expect(!clog.isInProgress(1));
    try testing.expect(!clog.isInProgress(2));
    try testing.expect(clog.isInProgress(3));
}

test "CLOG: can update status of same transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Start as in_progress
    try clog.setStatus(1, .in_progress);
    try testing.expectEqual(TxStatus.in_progress, clog.getStatus(1));

    // Update to committed
    try clog.setStatus(1, .committed);
    try testing.expectEqual(TxStatus.committed, clog.getStatus(1));
}

// ============================================================================
// TransactionManager Tests
// ============================================================================

test "TransactionManager: begin() registers transaction in active_txs" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Initially no active transactions
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());

    // Begin transaction
    const tx_id = try tx_mgr.begin();

    // Should now have 1 active transaction
    try testing.expectEqual(@as(usize, 1), tx_mgr.activeCount());

    // Should be able to retrieve the transaction
    const tx = tx_mgr.getTransaction(tx_id);
    try testing.expect(tx != null);

    try tx_mgr.commit(tx_id);
}

test "TransactionManager: commit() removes transaction and updates CLOG" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    try testing.expectEqual(@as(usize, 1), tx_mgr.activeCount());

    // Commit the transaction
    try tx_mgr.commit(tx_id);

    // Should be removed from active transactions
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());
    try testing.expect(tx_mgr.getTransaction(tx_id) == null);

    // Should be marked as committed in CLOG
    try testing.expect(tx_mgr.clog.isCommitted(tx_id));
}

test "TransactionManager: rollback() removes transaction and updates CLOG" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    try testing.expectEqual(@as(usize, 1), tx_mgr.activeCount());

    // Rollback the transaction
    try tx_mgr.rollback(tx_id);

    // Should be removed from active transactions
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());
    try testing.expect(tx_mgr.getTransaction(tx_id) == null);

    // Should be marked as aborted in CLOG
    try testing.expect(tx_mgr.clog.isAborted(tx_id));
}

test "TransactionManager: getTransaction() returns correct transaction by ID" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();
    const tx3_id = try tx_mgr.begin();

    // Get each transaction and verify IDs
    const tx1 = tx_mgr.getTransaction(tx1_id).?;
    const tx2 = tx_mgr.getTransaction(tx2_id).?;
    const tx3 = tx_mgr.getTransaction(tx3_id).?;

    try testing.expectEqual(tx1_id, tx1.id);
    try testing.expectEqual(tx2_id, tx2.id);
    try testing.expectEqual(tx3_id, tx3.id);

    // Non-existent transaction should return null
    try testing.expect(tx_mgr.getTransaction(999) == null);

    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
}

test "TransactionManager: activeCount() returns correct count" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Start with 0
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());

    // Add transactions
    const tx1_id = try tx_mgr.begin();
    try testing.expectEqual(@as(usize, 1), tx_mgr.activeCount());

    const tx2_id = try tx_mgr.begin();
    try testing.expectEqual(@as(usize, 2), tx_mgr.activeCount());

    const tx3_id = try tx_mgr.begin();
    try testing.expectEqual(@as(usize, 3), tx_mgr.activeCount());

    // Commit one
    try tx_mgr.commit(tx1_id);
    try testing.expectEqual(@as(usize, 2), tx_mgr.activeCount());

    // Rollback one
    try tx_mgr.rollback(tx2_id);
    try testing.expectEqual(@as(usize, 1), tx_mgr.activeCount());

    // Commit last one
    try tx_mgr.commit(tx3_id);
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());
}

test "TransactionManager: getSnapshot() returns snapshot for transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();

    // Get snapshots
    const snapshot1 = tx_mgr.getSnapshot(tx1_id);
    const snapshot2 = tx_mgr.getSnapshot(tx2_id);

    try testing.expect(snapshot1 != null);
    try testing.expect(snapshot2 != null);

    // Verify snapshot IDs match transaction IDs
    try testing.expectEqual(tx1_id, snapshot1.?.txid);
    try testing.expectEqual(tx2_id, snapshot2.?.txid);

    // Non-existent transaction returns null
    try testing.expect(tx_mgr.getSnapshot(999) == null);

    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
}

test "TransactionManager: commit/rollback non-existent transaction returns error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Try to commit non-existent transaction
    const commit_result = tx_mgr.commit(999);
    try testing.expectError(error.NoActiveTransaction, commit_result);

    // Try to rollback non-existent transaction
    const rollback_result = tx_mgr.rollback(999);
    try testing.expectError(error.NoActiveTransaction, rollback_result);
}

// ============================================================================
// Concurrent Transaction Tests
// ============================================================================

test "multiple transactions can be active simultaneously" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin 5 transactions simultaneously
    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();
    const tx3_id = try tx_mgr.begin();
    const tx4_id = try tx_mgr.begin();
    const tx5_id = try tx_mgr.begin();

    // All should be active
    try testing.expectEqual(@as(usize, 5), tx_mgr.activeCount());

    // All should be retrievable
    try testing.expect(tx_mgr.getTransaction(tx1_id) != null);
    try testing.expect(tx_mgr.getTransaction(tx2_id) != null);
    try testing.expect(tx_mgr.getTransaction(tx3_id) != null);
    try testing.expect(tx_mgr.getTransaction(tx4_id) != null);
    try testing.expect(tx_mgr.getTransaction(tx5_id) != null);

    // Clean up
    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx2_id);
    try tx_mgr.commit(tx3_id);
    try tx_mgr.commit(tx4_id);
    try tx_mgr.commit(tx5_id);
}

test "committing one transaction does not affect others" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx1_id = try tx_mgr.begin();
    const tx2_id = try tx_mgr.begin();
    const tx3_id = try tx_mgr.begin();

    try testing.expectEqual(@as(usize, 3), tx_mgr.activeCount());

    // Commit tx2 in the middle
    try tx_mgr.commit(tx2_id);

    // tx1 and tx3 should still be active
    try testing.expectEqual(@as(usize, 2), tx_mgr.activeCount());
    try testing.expect(tx_mgr.getTransaction(tx1_id) != null);
    try testing.expect(tx_mgr.getTransaction(tx2_id) == null);
    try testing.expect(tx_mgr.getTransaction(tx3_id) != null);

    // Verify tx1 and tx3 are still marked as in_progress in CLOG
    try testing.expect(tx_mgr.clog.isInProgress(tx1_id));
    try testing.expect(tx_mgr.clog.isCommitted(tx2_id));
    try testing.expect(tx_mgr.clog.isInProgress(tx3_id));

    try tx_mgr.commit(tx1_id);
    try tx_mgr.commit(tx3_id);
}

test "ten concurrent transactions coexist correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin 10 transactions
    var tx_ids: [10]u64 = undefined;
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        tx_ids[i] = try tx_mgr.begin();
    }

    // All 10 should be active
    try testing.expectEqual(@as(usize, 10), tx_mgr.activeCount());

    // Commit odd-numbered transactions
    i = 0;
    while (i < 10) : (i += 1) {
        if (i % 2 == 1) {
            try tx_mgr.commit(tx_ids[i]);
        }
    }

    // Should have 5 active transactions left
    try testing.expectEqual(@as(usize, 5), tx_mgr.activeCount());

    // Rollback even-numbered transactions
    i = 0;
    while (i < 10) : (i += 1) {
        if (i % 2 == 0) {
            try tx_mgr.rollback(tx_ids[i]);
        }
    }

    // All should be gone
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());
}

// ============================================================================
// Memory Management Tests
// ============================================================================

test "snapshots are properly cleaned up on transaction commit" {
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

    const tx_id = try tx_mgr.begin();

    // Transaction has a snapshot
    try testing.expect(tx_mgr.getSnapshot(tx_id) != null);

    // Commit should clean up snapshot
    try tx_mgr.commit(tx_id);

    // GPA deinit will catch any leaks
}

test "snapshots are properly cleaned up on transaction rollback" {
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

    const tx_id = try tx_mgr.begin();

    // Transaction has a snapshot
    try testing.expect(tx_mgr.getSnapshot(tx_id) != null);

    // Rollback should clean up snapshot
    try tx_mgr.rollback(tx_id);

    // GPA deinit will catch any leaks
}

test "no memory leaks with many begin/commit cycles" {
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
        try tx_mgr.commit(tx_id);
    }

    // GPA deinit will catch any leaks
}

test "no memory leaks with many begin/rollback cycles" {
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
        try tx_mgr.rollback(tx_id);
    }

    // GPA deinit will catch any leaks
}

test "no memory leaks with overlapping transactions" {
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

    // Create overlapping transactions
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        const tx1_id = try tx_mgr.begin();
        const tx2_id = try tx_mgr.begin();
        const tx3_id = try tx_mgr.begin();

        try tx_mgr.commit(tx1_id);
        try tx_mgr.rollback(tx2_id);
        try tx_mgr.commit(tx3_id);
    }

    // GPA deinit will catch any leaks
}

test "TransactionManager cleans up all active transactions on deinit" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            @panic("Memory leak detected!");
        }
    }
    const allocator = gpa.allocator();

    {
        var tx_mgr = TransactionManager.init(allocator);

        // Begin several transactions but don't commit them
        _ = try tx_mgr.begin();
        _ = try tx_mgr.begin();
        _ = try tx_mgr.begin();

        // deinit should clean up all active transactions
        tx_mgr.deinit();
    }

    // GPA deinit will catch any leaks
}

// ============================================================================
// Snapshot Edge Cases
// ============================================================================

test "snapshot with many active transactions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

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
    try testing.expectEqual(@as(usize, 20), snapshot.active_txids.len);

    // Verify all are marked as active
    i = 0;
    while (i < 20) : (i += 1) {
        try testing.expect(snapshot.wasActive(tx_ids[i]));
    }

    // Clean up
    i = 0;
    while (i < 20) : (i += 1) {
        try tx_mgr.commit(tx_ids[i]);
    }
    try tx_mgr.commit(final_tx_id);
}

test "snapshot wasActive returns false for non-active transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    const tx = tx_mgr.getTransaction(tx_id).?;
    const snapshot = &tx.snapshot.?;

    // Check for transaction IDs that were never created
    try testing.expect(!snapshot.wasActive(9999));
    try testing.expect(!snapshot.wasActive(10000));
    try testing.expect(!snapshot.wasActive(10001));

    try tx_mgr.commit(tx_id);
}

// ============================================================================
// Transaction State Tests
// ============================================================================

test "transaction state transitions correctly on commit" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    const tx = tx_mgr.getTransaction(tx_id).?;

    // Should start as active
    try testing.expectEqual(transaction.TransactionState.active, tx.state);

    // Commit should change state
    try tx_mgr.commit(tx_id);

    // Note: Can't check state after commit since transaction is freed
    // But CLOG should reflect committed state
    try testing.expect(tx_mgr.clog.isCommitted(tx_id));
}

test "transaction state transitions correctly on rollback" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    const tx_id = try tx_mgr.begin();
    const tx = tx_mgr.getTransaction(tx_id).?;

    // Should start as active
    try testing.expectEqual(transaction.TransactionState.active, tx.state);

    // Rollback should change state
    try tx_mgr.rollback(tx_id);

    // Note: Can't check state after rollback since transaction is freed
    // But CLOG should reflect aborted state
    try testing.expect(tx_mgr.clog.isAborted(tx_id));
}

// ============================================================================
// Integration Tests
// ============================================================================

test "integration: complex multi-transaction scenario" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Scenario:
    // T1 begins
    const tx1_id = try tx_mgr.begin();

    // T2 begins (sees T1)
    const tx2_id = try tx_mgr.begin();
    const tx2 = tx_mgr.getTransaction(tx2_id).?;
    try testing.expect(tx2.snapshot.?.wasActive(tx1_id));

    // T3 begins (sees T1, T2)
    const tx3_id = try tx_mgr.begin();
    const tx3 = tx_mgr.getTransaction(tx3_id).?;
    try testing.expect(tx3.snapshot.?.wasActive(tx1_id));
    try testing.expect(tx3.snapshot.?.wasActive(tx2_id));

    // T1 commits
    try tx_mgr.commit(tx1_id);
    try testing.expect(tx_mgr.clog.isCommitted(tx1_id));

    // T4 begins (should NOT see T1, should see T2, T3)
    const tx4_id = try tx_mgr.begin();
    const tx4 = tx_mgr.getTransaction(tx4_id).?;
    try testing.expect(!tx4.snapshot.?.wasActive(tx1_id));
    try testing.expect(tx4.snapshot.?.wasActive(tx2_id));
    try testing.expect(tx4.snapshot.?.wasActive(tx3_id));

    // T2 rollbacks
    try tx_mgr.rollback(tx2_id);
    try testing.expect(tx_mgr.clog.isAborted(tx2_id));

    // T5 begins (should NOT see T1 or T2, should see T3, T4)
    const tx5_id = try tx_mgr.begin();
    const tx5 = tx_mgr.getTransaction(tx5_id).?;
    try testing.expect(!tx5.snapshot.?.wasActive(tx1_id));
    try testing.expect(!tx5.snapshot.?.wasActive(tx2_id));
    try testing.expect(tx5.snapshot.?.wasActive(tx3_id));
    try testing.expect(tx5.snapshot.?.wasActive(tx4_id));

    // Active count should be 3 (T3, T4, T5)
    try testing.expectEqual(@as(usize, 3), tx_mgr.activeCount());

    // Clean up remaining transactions
    try tx_mgr.commit(tx3_id);
    try tx_mgr.commit(tx4_id);
    try tx_mgr.commit(tx5_id);

    // All should be done
    try testing.expectEqual(@as(usize, 0), tx_mgr.activeCount());
}

test "integration: verify CLOG persistence across begin/commit cycles" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin and commit several transactions
    const tx1_id = try tx_mgr.begin();
    try tx_mgr.commit(tx1_id);

    const tx2_id = try tx_mgr.begin();
    try tx_mgr.rollback(tx2_id);

    const tx3_id = try tx_mgr.begin();
    try tx_mgr.commit(tx3_id);

    // Verify CLOG maintains all statuses
    try testing.expect(tx_mgr.clog.isCommitted(tx1_id));
    try testing.expect(tx_mgr.clog.isAborted(tx2_id));
    try testing.expect(tx_mgr.clog.isCommitted(tx3_id));

    // Start new transaction and verify old statuses still accessible
    const tx4_id = try tx_mgr.begin();
    try testing.expect(tx_mgr.clog.isCommitted(tx1_id));
    try testing.expect(tx_mgr.clog.isAborted(tx2_id));
    try testing.expect(tx_mgr.clog.isCommitted(tx3_id));
    try testing.expect(tx_mgr.clog.isInProgress(tx4_id));

    try tx_mgr.commit(tx4_id);
}
