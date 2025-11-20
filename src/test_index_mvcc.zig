const std = @import("std");
const testing = std.testing;
const Database = @import("database/core.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;
const StringHashMap = std.StringHashMap;

// ============================================================================
// Index MVCC Tests - Verify indexes respect transaction isolation
// ============================================================================

// Test that index queries respect MVCC snapshot isolation
// TX1 inserts a row, TX2 (started before TX1 commits) should NOT see it via index
test "Index MVCC: query respects snapshot isolation" {
    const allocator = testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table with index
    _ = try db.execute("CREATE TABLE users (id INT, email TEXT)");
    _ = try db.execute("CREATE INDEX idx_email ON users(email)");

    // TX1: Begin and insert a row
    _ = try db.execute("BEGIN");
    const tx1_id = db.tx_manager.getCurrentTx().?.id;
    _ = try db.execute("INSERT INTO users (id, email) VALUES (1, 'alice@example.com')");

    // TX2: Begin (snapshot taken BEFORE TX1 commits)
    var db2 = Database.init(allocator);
    defer db2.deinit();
    db2.tables = db.tables; // Share tables
    db2.index_manager = db.index_manager; // Share indexes
    db2.tx_manager = db.tx_manager; // Share transaction manager
    db2.clog = db.clog; // Share CLOG

    _ = try db2.execute("BEGIN");

    // TX1: Commit
    _ = try db.execute("COMMIT");

    // TX2: Query via index - should NOT see alice@example.com (committed after snapshot)
    var result2 = try db2.execute("SELECT * FROM users WHERE email = 'alice@example.com'");
    defer result2.deinit();

    try testing.expectEqual(@as(usize, 0), result2.rows.items.len);

    // TX2: Commit
    _ = try db2.execute("COMMIT");

    // TX3: New transaction should see the committed row
    _ = try db.execute("BEGIN");
    var result3 = try db.execute("SELECT * FROM users WHERE email = 'alice@example.com'");
    defer result3.deinit();

    try testing.expectEqual(@as(usize, 1), result3.rows.items.len);

    _ = try db.execute("COMMIT");
}

// Test that index range scans filter out rows from aborted transactions
test "Index MVCC: range scan filters aborted transaction rows" {
    const allocator = testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table with index
    _ = try db.execute("CREATE TABLE products (id INT, price INT)");
    _ = try db.execute("CREATE INDEX idx_price ON products(price)");

    // Insert some committed data
    _ = try db.execute("INSERT INTO products (id, price) VALUES (1, 100)");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (2, 200)");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (3, 300)");

    // TX1: Insert rows 4-6 and then rollback
    _ = try db.execute("BEGIN");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (4, 150)");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (5, 250)");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (6, 350)");

    // Before rollback, TX1 can see all 6 rows
    var result_before = try db.execute("SELECT COUNT(*) FROM products");
    defer result_before.deinit();
    try testing.expectEqual(@as(i64, 6), result_before.rows.items[0].items[0].int);

    // Rollback TX1
    _ = try db.execute("ROLLBACK");

    // TX2: Query after rollback - should only see original 3 rows
    _ = try db.execute("BEGIN");
    var result_after = try db.execute("SELECT COUNT(*) FROM products WHERE price >= 0");
    defer result_after.deinit();
    try testing.expectEqual(@as(i64, 3), result_after.rows.items[0].items[0].int);

    // Range scan should also filter properly
    var result_range = try db.execute("SELECT * FROM products WHERE price >= 100 AND price <= 300");
    defer result_range.deinit();
    try testing.expectEqual(@as(usize, 3), result_range.rows.items.len);

    _ = try db.execute("COMMIT");
}

// Test that concurrent transactions see correct data via indexes
test "Index MVCC: concurrent transactions with different snapshots" {
    const allocator = testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table with index
    _ = try db.execute("CREATE TABLE accounts (id INT, balance INT)");
    _ = try db.execute("CREATE INDEX idx_balance ON accounts(balance)");

    // Initial data
    _ = try db.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)");
    _ = try db.execute("INSERT INTO accounts (id, balance) VALUES (2, 2000)");

    // TX1: Begin
    _ = try db.execute("BEGIN");
    var result1_before = try db.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000");
    defer result1_before.deinit();
    try testing.expectEqual(@as(i64, 2), result1_before.rows.items[0].items[0].int);

    // TX2: Insert new account and commit (in a separate "session")
    var db2 = Database.init(allocator);
    defer db2.deinit();
    db2.tables = db.tables;
    db2.index_manager = db.index_manager;
    db2.tx_manager = db.tx_manager;
    db2.clog = db.clog;

    _ = try db2.execute("BEGIN");
    _ = try db2.execute("INSERT INTO accounts (id, balance) VALUES (3, 3000)");
    _ = try db2.execute("COMMIT");

    // TX1: Should still see only 2 accounts (snapshot isolation)
    var result1_after = try db.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000");
    defer result1_after.deinit();
    try testing.expectEqual(@as(i64, 2), result1_after.rows.items[0].items[0].int);

    // TX1: Commit
    _ = try db.execute("COMMIT");

    // TX3: New transaction should see all 3 accounts
    _ = try db.execute("BEGIN");
    var result3 = try db.execute("SELECT COUNT(*) FROM accounts WHERE balance >= 1000");
    defer result3.deinit();
    try testing.expectEqual(@as(i64, 3), result3.rows.items[0].items[0].int);

    _ = try db.execute("COMMIT");
}

// Test that deleted rows are filtered correctly in index queries
test "Index MVCC: deleted rows filtered in index queries" {
    const allocator = testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table with index
    _ = try db.execute("CREATE TABLE messages (id INT, content TEXT)");
    _ = try db.execute("CREATE INDEX idx_content ON messages(content)");

    // Insert data
    _ = try db.execute("INSERT INTO messages (id, content) VALUES (1, 'hello')");
    _ = try db.execute("INSERT INTO messages (id, content) VALUES (2, 'world')");
    _ = try db.execute("INSERT INTO messages (id, content) VALUES (3, 'hello')");

    // TX1: Begin
    _ = try db.execute("BEGIN");

    // TX2: Delete a row
    var db2 = Database.init(allocator);
    defer db2.deinit();
    db2.tables = db.tables;
    db2.index_manager = db.index_manager;
    db2.tx_manager = db.tx_manager;
    db2.clog = db.clog;

    _ = try db2.execute("BEGIN");
    _ = try db2.execute("DELETE FROM messages WHERE id = 1");

    // TX1: Should still see all 3 messages (deletion not committed yet)
    var result1 = try db.execute("SELECT COUNT(*) FROM messages WHERE content = 'hello'");
    defer result1.deinit();
    try testing.expectEqual(@as(i64, 2), result1.rows.items[0].items[0].int);

    // TX2: Commit deletion
    _ = try db2.execute("COMMIT");

    // TX1: Should still see both 'hello' messages (snapshot isolation)
    var result1_after = try db.execute("SELECT COUNT(*) FROM messages WHERE content = 'hello'");
    defer result1_after.deinit();
    try testing.expectEqual(@as(i64, 2), result1_after.rows.items[0].items[0].int);

    // TX1: Commit
    _ = try db.execute("COMMIT");

    // TX3: Should see only 1 'hello' message
    _ = try db.execute("BEGIN");
    var result3 = try db.execute("SELECT COUNT(*) FROM messages WHERE content = 'hello'");
    defer result3.deinit();
    try testing.expectEqual(@as(i64, 1), result3.rows.items[0].items[0].int);

    _ = try db.execute("COMMIT");
}

// Test that updated rows are handled correctly in index queries
test "Index MVCC: updated rows filtered correctly" {
    const allocator = testing.allocator;
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table with index
    _ = try db.execute("CREATE TABLE users (id INT, status TEXT)");
    _ = try db.execute("CREATE INDEX idx_status ON users(status)");

    // Insert data
    _ = try db.execute("INSERT INTO users (id, status) VALUES (1, 'active')");
    _ = try db.execute("INSERT INTO users (id, status) VALUES (2, 'active')");
    _ = try db.execute("INSERT INTO users (id, status) VALUES (3, 'inactive')");

    // TX1: Begin
    _ = try db.execute("BEGIN");
    var result1_before = try db.execute("SELECT COUNT(*) FROM users WHERE status = 'active'");
    defer result1_before.deinit();
    try testing.expectEqual(@as(i64, 2), result1_before.rows.items[0].items[0].int);

    // TX2: Update user 2 to inactive
    var db2 = Database.init(allocator);
    defer db2.deinit();
    db2.tables = db.tables;
    db2.index_manager = db.index_manager;
    db2.tx_manager = db.tx_manager;
    db2.clog = db.clog;

    _ = try db2.execute("BEGIN");
    _ = try db2.execute("UPDATE users SET status = 'inactive' WHERE id = 2");
    _ = try db2.execute("COMMIT");

    // TX1: Should still see 2 active users (snapshot isolation)
    var result1_after = try db.execute("SELECT COUNT(*) FROM users WHERE status = 'active'");
    defer result1_after.deinit();
    try testing.expectEqual(@as(i64, 2), result1_after.rows.items[0].items[0].int);

    // TX1: Commit
    _ = try db.execute("COMMIT");

    // TX3: Should see only 1 active user
    _ = try db.execute("BEGIN");
    var result3_active = try db.execute("SELECT COUNT(*) FROM users WHERE status = 'active'");
    defer result3_active.deinit();
    try testing.expectEqual(@as(i64, 1), result3_active.rows.items[0].items[0].int);

    var result3_inactive = try db.execute("SELECT COUNT(*) FROM users WHERE status = 'inactive'");
    defer result3_inactive.deinit();
    try testing.expectEqual(@as(i64, 2), result3_inactive.rows.items[0].items[0].int);

    _ = try db.execute("COMMIT");
}
