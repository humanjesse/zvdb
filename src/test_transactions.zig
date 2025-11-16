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
