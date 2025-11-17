const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const QueryResult = @import("database.zig").QueryResult;

// ============================================================================
// Basic Transaction Tests
// ============================================================================

test "basic transaction commit" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    // Start transaction
    var result2 = try db.execute("BEGIN");
    defer result2.deinit();

    // Insert data within transaction
    var result3 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result3.deinit();

    var result4 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result4.deinit();

    // Commit transaction
    var result5 = try db.execute("COMMIT");
    defer result5.deinit();

    // Verify data is persisted
    var result6 = try db.execute("SELECT * FROM users");
    defer result6.deinit();

    try testing.expectEqual(@as(usize, 2), result6.rows.items.len);
}

test "basic transaction rollback" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table and insert initial data
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result2.deinit();

    // Start transaction
    var result3 = try db.execute("BEGIN");
    defer result3.deinit();

    // Insert data within transaction
    var result4 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result4.deinit();

    var result5 = try db.execute("DELETE FROM users WHERE id = 1");
    defer result5.deinit();

    // Rollback transaction
    var result6 = try db.execute("ROLLBACK");
    defer result6.deinit();

    // Verify only Alice remains (rollback worked)
    var result7 = try db.execute("SELECT * FROM users");
    defer result7.deinit();

    try testing.expectEqual(@as(usize, 1), result7.rows.items.len);
}

test "transaction rollback undo insert" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text)");
    defer result1.deinit();

    // Start transaction and insert
    var result2 = try db.execute("BEGIN");
    defer result2.deinit();

    var result3 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer result3.deinit();

    var result4 = try db.execute("INSERT INTO users VALUES (2, \"Bob\")");
    defer result4.deinit();

    // Rollback
    var result5 = try db.execute("ROLLBACK");
    defer result5.deinit();

    // Verify table is empty
    var result6 = try db.execute("SELECT * FROM users");
    defer result6.deinit();

    try testing.expectEqual(@as(usize, 0), result6.rows.items.len);
}

test "transaction rollback undo delete" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table and insert data
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result2.deinit();

    var result3 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result3.deinit();

    // Start transaction and delete
    var result4 = try db.execute("BEGIN");
    defer result4.deinit();

    var result5 = try db.execute("DELETE FROM users WHERE id = 1");
    defer result5.deinit();

    // Rollback
    var result6 = try db.execute("ROLLBACK");
    defer result6.deinit();

    // Verify both users are still there
    var result7 = try db.execute("SELECT * FROM users");
    defer result7.deinit();

    try testing.expectEqual(@as(usize, 2), result7.rows.items.len);
}

test "transaction rollback undo update" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table and insert data
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result2.deinit();

    // Start transaction and update
    var result3 = try db.execute("BEGIN");
    defer result3.deinit();

    var result4 = try db.execute("UPDATE users SET age = 30 WHERE id = 1");
    defer result4.deinit();

    // Rollback
    var result5 = try db.execute("ROLLBACK");
    defer result5.deinit();

    // Verify age is still 25
    var result6 = try db.execute("SELECT * FROM users WHERE id = 1");
    defer result6.deinit();

    try testing.expectEqual(@as(usize, 1), result6.rows.items.len);
    const age_value = result6.rows.items[0].items[2]; // age column
    try testing.expectEqual(@as(i64, 25), age_value.int);
}

// ============================================================================
// Complex Transaction Tests
// ============================================================================

test "transaction with multiple operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result2.deinit();

    var result3 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result3.deinit();

    // Start transaction with mixed operations
    var result4 = try db.execute("BEGIN");
    defer result4.deinit();

    var result5 = try db.execute("INSERT INTO users VALUES (3, \"Charlie\", 35)");
    defer result5.deinit();

    var result6 = try db.execute("UPDATE users SET age = 26 WHERE id = 1");
    defer result6.deinit();

    var result7 = try db.execute("DELETE FROM users WHERE id = 2");
    defer result7.deinit();

    // Rollback all changes
    var result8 = try db.execute("ROLLBACK");
    defer result8.deinit();

    // Verify state is back to initial (Alice 25, Bob 30)
    var result9 = try db.execute("SELECT * FROM users");
    defer result9.deinit();

    try testing.expectEqual(@as(usize, 2), result9.rows.items.len);

    // Verify Alice's age is still 25
    var result10 = try db.execute("SELECT * FROM users WHERE id = 1");
    defer result10.deinit();
    const alice_age = result10.rows.items[0].items[2];
    try testing.expectEqual(@as(i64, 25), alice_age.int);

    // Verify Bob is still present
    var result11 = try db.execute("SELECT * FROM users WHERE id = 2");
    defer result11.deinit();
    try testing.expectEqual(@as(usize, 1), result11.rows.items.len);
}

test "transaction commit after multiple operations" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result2.deinit();

    // Start transaction
    var result3 = try db.execute("BEGIN");
    defer result3.deinit();

    var result4 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result4.deinit();

    var result5 = try db.execute("UPDATE users SET age = 26 WHERE id = 1");
    defer result5.deinit();

    // Commit changes
    var result6 = try db.execute("COMMIT");
    defer result6.deinit();

    // Verify all changes are persisted
    var result7 = try db.execute("SELECT * FROM users");
    defer result7.deinit();

    try testing.expectEqual(@as(usize, 2), result7.rows.items.len);

    // Verify Alice's age is updated to 26
    var result8 = try db.execute("SELECT * FROM users WHERE id = 1");
    defer result8.deinit();
    const alice_age = result8.rows.items[0].items[2];
    try testing.expectEqual(@as(i64, 26), alice_age.int);
}

test "auto-commit mode without explicit transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text)");
    defer result1.deinit();

    // Insert without explicit transaction (auto-commit)
    var result2 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer result2.deinit();

    // Verify data is immediately visible
    var result3 = try db.execute("SELECT * FROM users");
    defer result3.deinit();

    try testing.expectEqual(@as(usize, 1), result3.rows.items.len);
}

// ============================================================================
// Error Handling Tests
// ============================================================================

test "commit without active transaction returns error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Try to commit without BEGIN
    const result = db.execute("COMMIT");
    try testing.expectError(error.NoActiveTransaction, result);
}

test "rollback without active transaction returns error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Try to rollback without BEGIN
    const result = db.execute("ROLLBACK");
    try testing.expectError(error.NoActiveTransaction, result);
}

test "concurrent transactions allowed" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Start first transaction
    var result1 = try db.execute("BEGIN");
    defer result1.deinit();

    // Extract first transaction ID from result
    try testing.expect(result1.rows.items.len == 1);
    const tx1_msg = result1.rows.items[0].items[0].text;

    // Start second transaction (should succeed with MVCC)
    var result2 = try db.execute("BEGIN");
    defer result2.deinit();

    // Extract second transaction ID from result
    try testing.expect(result2.rows.items.len == 1);
    const tx2_msg = result2.rows.items[0].items[0].text;

    // Verify different transaction IDs
    try testing.expect(!std.mem.eql(u8, tx1_msg, tx2_msg));

    // Both transactions can commit independently
    var result3 = try db.execute("COMMIT");
    defer result3.deinit();

    var result4 = try db.execute("COMMIT");
    defer result4.deinit();
}

// ============================================================================
// Transaction + Index Tests
// ============================================================================

test "transaction rollback with indexes" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Create table
    var result1 = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result1.deinit();

    // Create index
    var result2 = try db.execute("CREATE INDEX idx_age ON users(age)");
    defer result2.deinit();

    // Insert initial data
    var result3 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer result3.deinit();

    // Start transaction
    var result4 = try db.execute("BEGIN");
    defer result4.deinit();

    // Insert more data
    var result5 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer result5.deinit();

    // Rollback
    var result6 = try db.execute("ROLLBACK");
    defer result6.deinit();

    // Verify index is consistent (only Alice should be found)
    var result7 = try db.execute("SELECT * FROM users WHERE age = 25");
    defer result7.deinit();

    try testing.expectEqual(@as(usize, 1), result7.rows.items.len);

    // Verify Bob is not in index
    var result8 = try db.execute("SELECT * FROM users WHERE age = 30");
    defer result8.deinit();

    try testing.expectEqual(@as(usize, 0), result8.rows.items.len);
}

// ============================================================================
// Transaction State Tests
// ============================================================================

test "transaction state transitions" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Initially no active transaction
    try testing.expect(!db.tx_manager.hasActiveTx());

    // Start transaction
    var result1 = try db.execute("BEGIN");
    defer result1.deinit();

    // Now has active transaction
    try testing.expect(db.tx_manager.hasActiveTx());

    // Commit transaction
    var result2 = try db.execute("COMMIT");
    defer result2.deinit();

    // No longer has active transaction
    try testing.expect(!db.tx_manager.hasActiveTx());

    // Start new transaction
    var result3 = try db.execute("BEGIN");
    defer result3.deinit();

    try testing.expect(db.tx_manager.hasActiveTx());

    // Rollback transaction
    var result4 = try db.execute("ROLLBACK");
    defer result4.deinit();

    // No longer has active transaction
    try testing.expect(!db.tx_manager.hasActiveTx());
}

// ============================================================================
// Edge Cases
// ============================================================================

test "empty transaction commit" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Start and immediately commit empty transaction
    var result1 = try db.execute("BEGIN");
    defer result1.deinit();

    var result2 = try db.execute("COMMIT");
    defer result2.deinit();

    // Should succeed without errors
    try testing.expect(true);
}

test "empty transaction rollback" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = Database.init(allocator);
    defer db.deinit();

    // Start and immediately rollback empty transaction
    var result1 = try db.execute("BEGIN");
    defer result1.deinit();

    var result2 = try db.execute("ROLLBACK");
    defer result2.deinit();

    // Should succeed without errors
    try testing.expect(true);
}

// ============================================================================
// CommitLog Persistence Tests (Phase 4A)
// ============================================================================

const transaction = @import("transaction.zig");
const CommitLog = transaction.CommitLog;
const TxStatus = transaction.TxStatus;
const AutoHashMap = std.AutoHashMap;

test "CommitLog: save and load empty CLOG" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create empty CLOG and save
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    const temp_path = "/tmp/test_clog_empty.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    try clog.save(temp_path);

    // Load and verify
    var loaded_clog = try CommitLog.load(allocator, temp_path);
    defer loaded_clog.deinit();

    try testing.expectEqual(@as(u32, 0), loaded_clog.status_map.count());
}

test "CommitLog: save and load single transaction" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create CLOG with one committed transaction
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);

    const temp_path = "/tmp/test_clog_single.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    try clog.save(temp_path);

    // Load and verify
    var loaded_clog = try CommitLog.load(allocator, temp_path);
    defer loaded_clog.deinit();

    try testing.expectEqual(@as(u32, 1), loaded_clog.status_map.count());
    try testing.expect(loaded_clog.isCommitted(1));
}

test "CommitLog: save and load multiple transactions with different states" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create CLOG with multiple transactions in different states
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);
    try clog.setStatus(2, .aborted);
    try clog.setStatus(3, .in_progress);
    try clog.setStatus(10, .committed);
    try clog.setStatus(25, .aborted);

    const temp_path = "/tmp/test_clog_multiple.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    try clog.save(temp_path);

    // Load and verify all states
    var loaded_clog = try CommitLog.load(allocator, temp_path);
    defer loaded_clog.deinit();

    try testing.expectEqual(@as(u32, 5), loaded_clog.status_map.count());
    try testing.expect(loaded_clog.isCommitted(1));
    try testing.expect(loaded_clog.isAborted(2));
    try testing.expect(loaded_clog.isInProgress(3));
    try testing.expect(loaded_clog.isCommitted(10));
    try testing.expect(loaded_clog.isAborted(25));
}

test "CommitLog: invalid file format returns error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const temp_path = "/tmp/test_clog_invalid.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Write a file with wrong magic number
    {
        const file = try std.fs.cwd().createFile(temp_path, .{});
        defer file.close();

        const utils = @import("utils.zig");
        try utils.writeInt(file, u32, 0xDEADBEEF); // Wrong magic
        try utils.writeInt(file, u32, 1); // Version
    }

    // Try to load - should fail
    const result = CommitLog.load(allocator, temp_path);
    try testing.expectError(error.InvalidFileFormat, result);
}

test "CommitLog: unsupported version returns error" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const temp_path = "/tmp/test_clog_version.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    // Write a file with future version
    {
        const file = try std.fs.cwd().createFile(temp_path, .{});
        defer file.close();

        const utils = @import("utils.zig");
        try utils.writeInt(file, u32, 0x434C_4F47); // Correct magic "CLOG"
        try utils.writeInt(file, u32, 999); // Future version
    }

    // Try to load - should fail
    const result = CommitLog.load(allocator, temp_path);
    try testing.expectError(error.UnsupportedVersion, result);
}

test "CommitLog: mergeRecoveredState combines states correctly" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create checkpoint CLOG with some transactions
    var checkpoint_clog = CommitLog.init(allocator);
    defer checkpoint_clog.deinit();

    try checkpoint_clog.setStatus(1, .committed);
    try checkpoint_clog.setStatus(2, .committed);

    // Create recovered state from WAL with new and overlapping transactions
    var recovered_state = AutoHashMap(u64, TxStatus).init(allocator);
    defer recovered_state.deinit();

    try recovered_state.put(2, .aborted); // Overlapping - should override
    try recovered_state.put(3, .committed); // New transaction
    try recovered_state.put(4, .in_progress); // New transaction

    // Merge recovered state into checkpoint
    try checkpoint_clog.mergeRecoveredState(recovered_state);

    // Verify merged state
    try testing.expectEqual(@as(u32, 4), checkpoint_clog.status_map.count());
    try testing.expect(checkpoint_clog.isCommitted(1)); // Unchanged from checkpoint
    try testing.expect(checkpoint_clog.isAborted(2)); // Overridden by recovery
    try testing.expect(checkpoint_clog.isCommitted(3)); // New from recovery
    try testing.expect(checkpoint_clog.isInProgress(4)); // New from recovery
}

test "CommitLog: save/load preserves large transaction IDs" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Create CLOG with very large transaction IDs
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    const large_txid: u64 = 1_000_000_000_000; // 1 trillion
    try clog.setStatus(large_txid, .committed);
    try clog.setStatus(large_txid + 1, .aborted);

    const temp_path = "/tmp/test_clog_large_ids.zvdb";
    defer std.fs.cwd().deleteFile(temp_path) catch {};

    try clog.save(temp_path);

    // Load and verify
    var loaded_clog = try CommitLog.load(allocator, temp_path);
    defer loaded_clog.deinit();

    try testing.expect(loaded_clog.isCommitted(large_txid));
    try testing.expect(loaded_clog.isAborted(large_txid + 1));
}

test "CommitLog: bootstrap transaction (txid=0) behavior" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Bootstrap transaction should always be committed, never in progress, never aborted
    try testing.expect(clog.isCommitted(0));
    try testing.expect(!clog.isInProgress(0));
    try testing.expect(!clog.isAborted(0));

    // This should be true even if we explicitly set status (though we shouldn't)
    try clog.setStatus(0, .in_progress);

    // Bootstrap txid=0 special handling should still apply
    try testing.expect(clog.isCommitted(0));
}

test "CommitLog: save creates parent directory if needed" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    try clog.setStatus(1, .committed);

    const temp_dir = "/tmp/test_clog_subdir";
    const temp_path = "/tmp/test_clog_subdir/nested/clog.zvdb";
    defer std.fs.cwd().deleteTree(temp_dir) catch {};

    // Should create nested directory
    try clog.save(temp_path);

    // Verify file was created
    const file = try std.fs.cwd().openFile(temp_path, .{});
    file.close();
}
