const std = @import("std");
const testing = std.testing;
const Table = @import("table.zig").Table;
const Row = @import("table.zig").Row;
const RowVersion = @import("table.zig").RowVersion;
const ColumnValue = @import("table.zig").ColumnValue;
const ColumnType = @import("table.zig").ColumnType;
const StringHashMap = std.StringHashMap;
const transaction = @import("transaction.zig");
const TransactionManager = transaction.TransactionManager;
const Snapshot = transaction.Snapshot;
const CommitLog = transaction.CommitLog;

// ============================================================================
// Test 1: Basic Insert Creates Version
// ============================================================================

test "basic insert creates version" {
    const allocator = testing.allocator;

    // Setup table and transaction manager
    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Begin transaction
    const tx_id = try tx_mgr.begin();
    defer {
        tx_mgr.commit(tx_id) catch {};
    }

    // Create values
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("name", ColumnValue{ .text = "Alice" });
    try values.put("age", ColumnValue{ .int = 30 });

    // Insert row
    try table.insertWithId(1, values, tx_id);

    // Verify version chain exists
    const version = table.version_chains.get(1).?;
    try testing.expectEqual(tx_id, version.xmin);
    try testing.expectEqual(@as(u64, 0), version.xmax);
    try testing.expectEqual(@as(u64, 1), version.row_id);
    try testing.expect(version.next == null);
}

// ============================================================================
// Test 2: Update Creates New Version with Chain
// ============================================================================

test "update creates new version with chain" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Transaction 1: Insert row
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.commit(tx1);

    // Transaction 2: Update row
    const tx2 = try tx_mgr.begin();
    try table.update(1, "value", ColumnValue{ .int = 200 }, tx2, null);
    try tx_mgr.commit(tx2);

    // Verify version chain
    const new_version = table.version_chains.get(1).?;
    try testing.expectEqual(tx2, new_version.xmin);
    try testing.expectEqual(@as(u64, 0), new_version.xmax); // Current version

    // Verify old version is linked
    const old_version = new_version.next.?;
    try testing.expectEqual(tx1, old_version.xmin);
    try testing.expectEqual(tx2, old_version.xmax); // Superseded by tx2

    // Verify values
    const new_value = new_version.data.get("value").?;
    try testing.expectEqual(@as(i64, 200), new_value.int);

    const old_value = old_version.data.get("value").?;
    try testing.expectEqual(@as(i64, 100), old_value.int);
}

// ============================================================================
// Test 3: Delete Marks xmax, Doesn't Remove
// ============================================================================

test "delete marks xmax, doesn't remove" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Insert and commit
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("data", ColumnValue{ .text = "test" });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.commit(tx1);

    // Delete row
    const tx2 = try tx_mgr.begin();
    try table.delete(1, tx2, null);
    try tx_mgr.commit(tx2);

    // Verify version still exists in version_chains
    const version = table.version_chains.get(1).?;
    try testing.expectEqual(tx2, version.xmax);
    try testing.expectEqual(tx1, version.xmin);
}

// ============================================================================
// Test 4: Visibility - Snapshot Sees Correct Version
// ============================================================================

test "visibility: snapshot sees correct version" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1: Insert row (value=100) and commit
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.commit(tx1);

    // T2: Begin (snapshot taken - should see value=100)
    const tx2 = try tx_mgr.begin();
    const snapshot2 = tx_mgr.getSnapshot(tx2).?;

    // T3: Update row (value=200) and commit
    const tx3 = try tx_mgr.begin();
    try table.update(1, "value", ColumnValue{ .int = 200 }, tx3, null);
    try tx_mgr.commit(tx3);

    // T2 should still see value=100 (snapshot isolation!)
    const row = table.get(1, snapshot2, &tx_mgr.clog).?;
    const value = row.get("value").?;
    try testing.expectEqual(@as(i64, 100), value.int);

    // Clean up T2
    try tx_mgr.commit(tx2);

    // New snapshot should see value=200
    const tx4 = try tx_mgr.begin();
    defer tx_mgr.commit(tx4) catch {};
    const snapshot4 = tx_mgr.getSnapshot(tx4).?;
    const row2 = table.get(1, snapshot4, &tx_mgr.clog).?;
    const value2 = row2.get("value").?;
    try testing.expectEqual(@as(i64, 200), value2.int);
}

// ============================================================================
// Test 5: Visibility - Aborted Transaction
// ============================================================================

test "visibility: aborted transaction not visible" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1: Insert row and ABORT
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.rollback(tx1); // ABORT!

    // T2: Should not see the row
    const tx2 = try tx_mgr.begin();
    defer tx_mgr.commit(tx2) catch {};
    const snapshot = tx_mgr.getSnapshot(tx2).?;
    const row = table.get(1, snapshot, &tx_mgr.clog);
    try testing.expect(row == null);
}

// ============================================================================
// Test 6: Visibility - Active Transaction Not Visible
// ============================================================================

test "visibility: active transaction not visible to concurrent snapshot" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // T1: Begin and insert (but don't commit yet)
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, tx1);

    // T2: Begin AFTER T1 inserted (T1 is in active list)
    const tx2 = try tx_mgr.begin();
    const snapshot2 = tx_mgr.getSnapshot(tx2).?;

    // T2 should NOT see T1's insert (T1 is in active_txids)
    const row = table.get(1, snapshot2, &tx_mgr.clog);
    try testing.expect(row == null);

    // Clean up
    try tx_mgr.commit(tx1);
    try tx_mgr.commit(tx2);
}

// ============================================================================
// Test 7: getAllRows Filters by Visibility
// ============================================================================

test "getAllRows filters by visibility" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Insert 3 rows
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("data", ColumnValue{ .int = 1 });
    try table.insertWithId(1, values, tx1);
    try table.insertWithId(2, values, tx1);
    try table.insertWithId(3, values, tx1);
    try tx_mgr.commit(tx1);

    // Take snapshot (should see all 3 rows)
    const tx2 = try tx_mgr.begin();
    const snapshot2 = tx_mgr.getSnapshot(tx2).?;

    // Delete row 2
    const tx3 = try tx_mgr.begin();
    try table.delete(2, tx3, null);
    try tx_mgr.commit(tx3);

    // Old snapshot should still see all 3 rows
    const rows_old = try table.getAllRows(allocator, snapshot2, &tx_mgr.clog);
    defer allocator.free(rows_old);
    try testing.expectEqual(@as(usize, 3), rows_old.len);

    // New snapshot should see only 2 rows
    try tx_mgr.commit(tx2);
    const tx4 = try tx_mgr.begin();
    defer tx_mgr.commit(tx4) catch {};
    const snapshot4 = tx_mgr.getSnapshot(tx4).?;
    const rows_new = try table.getAllRows(allocator, snapshot4, &tx_mgr.clog);
    defer allocator.free(rows_new);
    try testing.expectEqual(@as(usize, 2), rows_new.len);
}

// ============================================================================
// Test 8: Row.clone() Works Correctly
// ============================================================================

test "Row.clone() works correctly" {
    const allocator = testing.allocator;

    var original = Row.init(allocator, 42);
    defer original.deinit(allocator);

    try original.set(allocator, "name", ColumnValue{ .text = "Alice" });
    try original.set(allocator, "age", ColumnValue{ .int = 30 });
    try original.set(allocator, "score", ColumnValue{ .float = 95.5 });

    // Clone
    var cloned = try original.clone(allocator);
    defer cloned.deinit(allocator);

    // Verify clone has same values
    try testing.expectEqual(@as(u64, 42), cloned.id);
    try testing.expectEqualStrings("Alice", cloned.get("name").?.text);
    try testing.expectEqual(@as(i64, 30), cloned.get("age").?.int);
    try testing.expectEqual(@as(f64, 95.5), cloned.get("score").?.float);

    // Modify clone shouldn't affect original
    try cloned.set(allocator, "age", ColumnValue{ .int = 31 });
    try testing.expectEqual(@as(i64, 30), original.get("age").?.int);
    try testing.expectEqual(@as(i64, 31), cloned.get("age").?.int);
}

// ============================================================================
// Test 9: Multiple Updates Create Long Chain
// ============================================================================

test "multiple updates create long version chain" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Insert initial version
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("counter", ColumnValue{ .int = 0 });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.commit(tx1);

    // Update 5 times
    var i: i64 = 1;
    while (i <= 5) : (i += 1) {
        const tx = try tx_mgr.begin();
        try table.update(1, "counter", ColumnValue{ .int = i }, tx, null);
        try tx_mgr.commit(tx);
    }

    // Walk the chain and verify
    const head = table.version_chains.get(1).?;
    var current: ?*RowVersion = head;
    var chain_length: usize = 0;
    var expected_value: i64 = 5;

    while (current) |version| {
        chain_length += 1;
        const val = version.data.get("counter").?;
        try testing.expectEqual(expected_value, val.int);
        expected_value -= 1;
        current = version.next;
    }

    try testing.expectEqual(@as(usize, 6), chain_length); // Initial + 5 updates
}

// ============================================================================
// Test 10: Non-MVCC Mode (Backward Compatibility)
// ============================================================================

test "non-MVCC mode returns newest version" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    // Insert without MVCC (tx_id = 0)
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, 0);

    // Update without MVCC
    try table.update(1, "value", ColumnValue{ .int = 200 }, 0, null);

    // Get without snapshot should return newest version
    const row = table.get(1, null, null).?;
    try testing.expectEqual(@as(i64, 200), row.get("value").?.int);

    // getAllRows without snapshot should return all IDs
    const ids = try table.getAllRows(allocator, null, null);
    defer allocator.free(ids);
    try testing.expectEqual(@as(usize, 1), ids.len);
}

// ============================================================================
// Test 11: Deleted Row Not Visible After Commit
// ============================================================================

test "deleted row not visible after commit" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Insert
    const tx1 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("data", ColumnValue{ .text = "test" });
    try table.insertWithId(1, values, tx1);
    try tx_mgr.commit(tx1);

    // Delete
    const tx2 = try tx_mgr.begin();
    try table.delete(1, tx2, null);
    try tx_mgr.commit(tx2);

    // New snapshot should not see the row
    const tx3 = try tx_mgr.begin();
    defer tx_mgr.commit(tx3) catch {};
    const snapshot = tx_mgr.getSnapshot(tx3).?;
    const row = table.get(1, snapshot, &tx_mgr.clog);
    try testing.expect(row == null);
}

// ============================================================================
// Test 12: Concurrent Readers Don't Block Each Other
// ============================================================================

test "concurrent readers see consistent snapshots" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    var tx_mgr = TransactionManager.init(allocator);
    defer tx_mgr.deinit();

    // Insert initial data
    const tx0 = try tx_mgr.begin();
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 100 });
    try table.insertWithId(1, values, tx0);
    try tx_mgr.commit(tx0);

    // Reader 1 takes snapshot
    const reader1 = try tx_mgr.begin();
    const snap1 = tx_mgr.getSnapshot(reader1).?;

    // Writer updates
    const writer = try tx_mgr.begin();
    try table.update(1, "value", ColumnValue{ .int = 200 }, writer, null);
    try tx_mgr.commit(writer);

    // Reader 2 takes snapshot
    const reader2 = try tx_mgr.begin();
    const snap2 = tx_mgr.getSnapshot(reader2).?;

    // Reader 1 should see 100
    const row1 = table.get(1, snap1, &tx_mgr.clog).?;
    try testing.expectEqual(@as(i64, 100), row1.get("value").?.int);

    // Reader 2 should see 200
    const row2 = table.get(1, snap2, &tx_mgr.clog).?;
    try testing.expectEqual(@as(i64, 200), row2.get("value").?.int);

    // Clean up
    try tx_mgr.commit(reader1);
    try tx_mgr.commit(reader2);
}

// ============================================================================
// Test 13: Version Chain Cleanup (deinitChain)
// ============================================================================

test "version chain cleanup works" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    // Insert and update multiple times
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("value", ColumnValue{ .int = 0 });
    try table.insertWithId(1, values, 1);

    try table.update(1, "value", ColumnValue{ .int = 1 }, 2, null);
    try table.update(1, "value", ColumnValue{ .int = 2 }, 3, null);
    try table.update(1, "value", ColumnValue{ .int = 3 }, 4, null);

    // Table.deinit() will call deinitChain on all version chains
    // This test just ensures no memory leaks occur (verified by allocator)
}

// ============================================================================
// Test 14: Save and Load Preserves Data (Non-MVCC)
// ============================================================================

test "save and load preserves newest version" {
    const allocator = testing.allocator;

    const temp_path = "test_mvcc_save_load.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create and populate table
    {
        var table = try Table.init(allocator, "test_table");
        defer table.deinit();

        try table.addColumn("name", ColumnType.text);
        try table.addColumn("age", ColumnType.int);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("name", ColumnValue{ .text = "Alice" });
        try values.put("age", ColumnValue{ .int = 30 });
        try table.insertWithId(1, values, 1);

        // Update to create version chain
        try table.update(1, "age", ColumnValue{ .int = 31 }, 2, null);

        try table.save(temp_path);
    }

    // Load and verify (should load newest version only)
    {
        var loaded = try Table.load(allocator, temp_path);
        defer loaded.deinit();

        try testing.expectEqualStrings("test_table", loaded.name);
        try testing.expectEqual(@as(usize, 2), loaded.columns.items.len);

        const row = loaded.get(1, null, null).?;
        try testing.expectEqualStrings("Alice", row.get("name").?.text);
        try testing.expectEqual(@as(i64, 31), row.get("age").?.int); // Newest version
    }
}
