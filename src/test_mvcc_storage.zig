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

// ============================================================================
// Phase 4B: MVCC Version Chain Persistence Tests
// ============================================================================

test "saveMvcc and loadMvcc: preserves single version chain" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_single_chain.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    const checkpoint_txid: u64 = 5;

    // Create and save table with version chain
    {
        var table = try Table.init(allocator, "mvcc_test");
        defer table.deinit();

        try table.addColumn("name", ColumnType.text);
        try table.addColumn("value", ColumnType.int);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("name", ColumnValue{ .text = "Test" });
        try values.put("value", ColumnValue{ .int = 1 });
        try table.insertWithId(1, values, 1);

        // Create version chain with updates
        try table.update(1, "value", ColumnValue{ .int = 2 }, 2, null);
        try table.update(1, "value", ColumnValue{ .int = 3 }, 3, null);

        try table.saveMvcc(temp_path, checkpoint_txid);
    }

    // Load and verify all versions preserved
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        try testing.expectEqualStrings("mvcc_test", loaded.name);

        // Get version chain head
        const head = loaded.version_chains.get(1).?;

        // Verify newest version (tx 3)
        try testing.expectEqual(@as(u64, 3), head.xmin);
        try testing.expectEqual(@as(u64, 0), head.xmax);
        try testing.expectEqual(@as(i64, 3), head.data.get("value").?.int);

        // Verify middle version (tx 2)
        const v2 = head.next.?;
        try testing.expectEqual(@as(u64, 2), v2.xmin);
        try testing.expectEqual(@as(u64, 3), v2.xmax); // Marked as deleted by tx 3
        try testing.expectEqual(@as(i64, 2), v2.data.get("value").?.int);

        // Verify oldest version (tx 1)
        const v1 = v2.next.?;
        try testing.expectEqual(@as(u64, 1), v1.xmin);
        try testing.expectEqual(@as(u64, 2), v1.xmax); // Marked as deleted by tx 2
        try testing.expectEqual(@as(i64, 1), v1.data.get("value").?.int);

        // Verify chain ends
        try testing.expect(v1.next == null);
    }
}

test "saveMvcc and loadMvcc: multiple rows with different chain lengths" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_multi_chains.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create table with multiple version chains
    {
        var table = try Table.init(allocator, "multi_chain");
        defer table.deinit();

        try table.addColumn("name", ColumnType.text);
        try table.addColumn("count", ColumnType.int);

        // Row 1: 3 versions
        var values1 = StringHashMap(ColumnValue).init(allocator);
        defer values1.deinit();
        try values1.put("name", ColumnValue{ .text = "Row1" });
        try values1.put("count", ColumnValue{ .int = 1 });
        try table.insertWithId(1, values1, 1);
        try table.update(1, "count", ColumnValue{ .int = 2 }, 2, null);
        try table.update(1, "count", ColumnValue{ .int = 3 }, 3, null);

        // Row 2: 1 version (no updates)
        var values2 = StringHashMap(ColumnValue).init(allocator);
        defer values2.deinit();
        try values2.put("name", ColumnValue{ .text = "Row2" });
        try values2.put("count", ColumnValue{ .int = 10 });
        try table.insertWithId(2, values2, 4);

        // Row 3: 2 versions
        var values3 = StringHashMap(ColumnValue).init(allocator);
        defer values3.deinit();
        try values3.put("name", ColumnValue{ .text = "Row3" });
        try values3.put("count", ColumnValue{ .int = 100 });
        try table.insertWithId(3, values3, 5);
        try table.update(3, "count", ColumnValue{ .int = 200 }, 6, null);

        try table.saveMvcc(temp_path, 6);
    }

    // Load and verify
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        // Verify row count
        try testing.expectEqual(@as(u32, 3), loaded.version_chains.count());

        // Check Row 1: 3 versions
        const chain1 = loaded.version_chains.get(1).?;
        var count1: u32 = 0;
        var curr1: ?*RowVersion = chain1;
        while (curr1) |v| : (curr1 = v.next) {
            count1 += 1;
        }
        try testing.expectEqual(@as(u32, 3), count1);

        // Check Row 2: 1 version
        const chain2 = loaded.version_chains.get(2).?;
        var count2: u32 = 0;
        var curr2: ?*RowVersion = chain2;
        while (curr2) |v| : (curr2 = v.next) {
            count2 += 1;
        }
        try testing.expectEqual(@as(u32, 1), count2);
        try testing.expectEqual(@as(i64, 10), chain2.data.get("count").?.int);

        // Check Row 3: 2 versions
        const chain3 = loaded.version_chains.get(3).?;
        var count3: u32 = 0;
        var curr3: ?*RowVersion = chain3;
        while (curr3) |v| : (curr3 = v.next) {
            count3 += 1;
        }
        try testing.expectEqual(@as(u32, 2), count3);
    }
}

test "saveMvcc and loadMvcc: preserves xmin and xmax correctly" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_tx_metadata.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create table with specific transaction IDs
    {
        var table = try Table.init(allocator, "tx_test");
        defer table.deinit();

        try table.addColumn("data", ColumnType.int);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("data", ColumnValue{ .int = 100 });
        try table.insertWithId(1, values, 42); // tx 42

        try table.update(1, "data", ColumnValue{ .int = 200 }, 123, null); // tx 123

        try table.saveMvcc(temp_path, 123);
    }

    // Load and verify transaction metadata
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        const head = loaded.version_chains.get(1).?;

        // Newest version created by tx 123
        try testing.expectEqual(@as(u64, 123), head.xmin);
        try testing.expectEqual(@as(u64, 0), head.xmax); // Still current

        // Old version created by tx 42, deleted by tx 123
        const old = head.next.?;
        try testing.expectEqual(@as(u64, 42), old.xmin);
        try testing.expectEqual(@as(u64, 123), old.xmax);
    }
}

test "saveMvcc and loadMvcc: empty table" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_empty.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create and save empty table
    {
        var table = try Table.init(allocator, "empty");
        defer table.deinit();

        try table.addColumn("col1", ColumnType.int);
        try table.addColumn("col2", ColumnType.text);

        try table.saveMvcc(temp_path, 0);
    }

    // Load and verify
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        try testing.expectEqualStrings("empty", loaded.name);
        try testing.expectEqual(@as(usize, 2), loaded.columns.items.len);
        try testing.expectEqual(@as(u32, 0), loaded.version_chains.count());
    }
}

test "saveMvcc and loadMvcc: preserves all column types" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_all_types.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create table with all column types
    {
        var table = try Table.init(allocator, "all_types");
        defer table.deinit();

        try table.addColumn("col_int", ColumnType.int);
        try table.addColumn("col_float", ColumnType.float);
        try table.addColumn("col_text", ColumnType.text);
        try table.addColumn("col_bool", ColumnType.bool);
        try table.addColumn("col_embedding", ColumnType.embedding);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("col_int", ColumnValue{ .int = 42 });
        try values.put("col_float", ColumnValue{ .float = 3.14 });
        try values.put("col_text", ColumnValue{ .text = "Hello MVCC" });
        try values.put("col_bool", ColumnValue{ .bool = true });

        const embedding = try allocator.alloc(f32, 3);
        defer allocator.free(embedding);
        embedding[0] = 1.0;
        embedding[1] = 2.0;
        embedding[2] = 3.0;
        try values.put("col_embedding", ColumnValue{ .embedding = embedding });

        try table.insertWithId(1, values, 1);

        try table.saveMvcc(temp_path, 1);
    }

    // Load and verify all types preserved
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        const head = loaded.version_chains.get(1).?;
        const row = head.data;

        try testing.expectEqual(@as(i64, 42), row.get("col_int").?.int);
        try testing.expectEqual(@as(f64, 3.14), row.get("col_float").?.float);
        try testing.expectEqualStrings("Hello MVCC", row.get("col_text").?.text);
        try testing.expectEqual(true, row.get("col_bool").?.bool);

        const emb = row.get("col_embedding").?.embedding;
        try testing.expectEqual(@as(usize, 3), emb.len);
        try testing.expectEqual(@as(f32, 1.0), emb[0]);
        try testing.expectEqual(@as(f32, 2.0), emb[1]);
        try testing.expectEqual(@as(f32, 3.0), emb[2]);
    }
}

test "loadMvcc: rejects non-v3 files" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_wrong_version.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Create a v2 file (using old save method)
    {
        var table = try Table.init(allocator, "v2_table");
        defer table.deinit();

        try table.addColumn("data", ColumnType.int);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("data", ColumnValue{ .int = 1 });
        try table.insertWithId(1, values, 1);

        try table.save(temp_path); // Saves as v2
    }

    // Try to load with loadMvcc - should fail
    const result = Table.loadMvcc(allocator, temp_path);
    try testing.expectError(error.UnsupportedVersion, result);
}

test "saveMvcc and loadMvcc: long version chains (10+ versions)" {
    const allocator = testing.allocator;

    const temp_path = "/tmp/test_mvcc_long_chain.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    const num_updates: u64 = 15;

    // Create long version chain
    {
        var table = try Table.init(allocator, "long_chain");
        defer table.deinit();

        try table.addColumn("counter", ColumnType.int);

        var values = StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("counter", ColumnValue{ .int = 0 });
        try table.insertWithId(1, values, 1);

        // Create many versions
        var tx: u64 = 2;
        while (tx <= num_updates + 1) : (tx += 1) {
            try table.update(1, "counter", ColumnValue{ .int = @intCast(tx - 1) }, tx, null);
        }

        try table.saveMvcc(temp_path, num_updates + 1);
    }

    // Load and verify chain length
    {
        var loaded = try Table.loadMvcc(allocator, temp_path);
        defer loaded.deinit();

        var head = loaded.version_chains.get(1).?;

        // Count versions
        var count: u32 = 0;
        var curr: ?*RowVersion = head;
        while (curr) |v| : (curr = v.next) {
            count += 1;
        }

        try testing.expectEqual(@as(u32, num_updates + 1), count);

        // Verify newest has highest value
        try testing.expectEqual(@as(i64, num_updates), head.data.get("counter").?.int);
    }
}
