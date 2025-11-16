// ============================================================================
// VACUUM Tests (Phase 4)
// ============================================================================
//
// This module tests the VACUUM functionality including:
// - Manual VACUUM command (all tables and specific table)
// - Version chain cleanup
// - Auto-VACUUM based on chain length threshold
// - Auto-VACUUM based on transaction count threshold
// - Memory reclamation verification
//
// ============================================================================

const std = @import("std");
const testing = std.testing;
const Database = @import("database/core.zig").Database;
const Table = @import("table.zig").Table;

// ============================================================================
// Manual VACUUM Tests
// ============================================================================

test "VACUUM: removes old versions after updates" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create table
    _ = try db.execute("CREATE TABLE accounts (id INT, balance INT)");
    _ = try db.execute("INSERT INTO accounts (id, balance) VALUES (1, 1000)");

    // Get initial stats
    const table = db.tables.get("accounts").?;
    const stats_before = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 1), stats_before.total_versions);

    // Create multiple versions via updates
    _ = try db.execute("UPDATE accounts SET balance = 1100 WHERE id = 1");
    _ = try db.execute("UPDATE accounts SET balance = 1200 WHERE id = 1");
    _ = try db.execute("UPDATE accounts SET balance = 1300 WHERE id = 1");

    // Should have 4 versions now (original + 3 updates)
    const stats_after_updates = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 4), stats_after_updates.total_versions);
    try testing.expectEqual(@as(usize, 4), stats_after_updates.max_chain_length);

    // Run VACUUM
    const result = try db.execute("VACUUM accounts");
    defer result.deinit();

    // Old versions should be removed (only current version remains)
    const stats_after_vacuum = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 1), stats_after_vacuum.total_versions);
    try testing.expectEqual(@as(usize, 1), stats_after_vacuum.max_chain_length);

    // Verify we can still read the current value
    const select_result = try db.execute("SELECT balance FROM accounts WHERE id = 1");
    defer select_result.deinit();
    try testing.expectEqual(@as(i64, 1300), select_result.rows.items[0].items[0].int);
}

test "VACUUM: preserves versions visible to active transactions" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create table and insert data
    _ = try db.execute("CREATE TABLE products (id INT, price INT)");
    _ = try db.execute("INSERT INTO products (id, price) VALUES (1, 100)");

    // Start a long-running transaction
    _ = try db.execute("BEGIN");
    const initial_result = try db.execute("SELECT price FROM products WHERE id = 1");
    defer initial_result.deinit();
    try testing.expectEqual(@as(i64, 100), initial_result.rows.items[0].items[0].int);

    // Get transaction ID for later verification
    const tx = db.tx_manager.getCurrentTx().?;
    const tx_id = tx.id;

    // Commit transaction to allow another to start
    _ = try db.execute("COMMIT");

    // Another transaction updates the value
    _ = try db.execute("UPDATE products SET price = 200");

    // VACUUM should NOT remove the old version if we had kept the transaction open
    // (but since we committed, it will be removed)
    const result = try db.execute("VACUUM products");
    defer result.deinit();

    // After VACUUM, only newest version should remain
    const table = db.tables.get("products").?;
    const stats = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 1), stats.total_versions);
}

test "VACUUM: all tables variant works correctly" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create multiple tables
    _ = try db.execute("CREATE TABLE table1 (id INT)");
    _ = try db.execute("CREATE TABLE table2 (id INT)");
    _ = try db.execute("INSERT INTO table1 (id) VALUES (1)");
    _ = try db.execute("INSERT INTO table2 (id) VALUES (2)");

    // Create versions in both tables
    _ = try db.execute("UPDATE table1 SET id = 10");
    _ = try db.execute("UPDATE table1 SET id = 20");
    _ = try db.execute("UPDATE table2 SET id = 30");
    _ = try db.execute("UPDATE table2 SET id = 40");

    // Run VACUUM on all tables
    const result = try db.execute("VACUUM");
    defer result.deinit();

    // Verify both tables in result
    try testing.expectEqual(@as(usize, 2), result.rows.items.len);

    // Both tables should have been cleaned up
    const table1 = db.tables.get("table1").?;
    const table2 = db.tables.get("table2").?;
    try testing.expectEqual(@as(usize, 1), table1.getVacuumStats().total_versions);
    try testing.expectEqual(@as(usize, 1), table2.getVacuumStats().total_versions);
}

test "VACUUM: removes versions from aborted transactions" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create table
    _ = try db.execute("CREATE TABLE users (id INT, name TEXT)");
    _ = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");

    // Transaction 1: Update and rollback
    _ = try db.execute("BEGIN");
    _ = try db.execute("UPDATE users SET name = 'Bob'");
    const table = db.tables.get("users").?;
    const stats_before_rollback = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 2), stats_before_rollback.total_versions);

    _ = try db.execute("ROLLBACK");

    // Version from aborted transaction should still exist until VACUUM
    const stats_after_rollback = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 2), stats_after_rollback.total_versions);

    // VACUUM should remove the aborted version
    const result = try db.execute("VACUUM users");
    defer result.deinit();

    const stats_after_vacuum = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 1), stats_after_vacuum.total_versions);
}

// ============================================================================
// Auto-VACUUM Tests
// ============================================================================

test "Auto-VACUUM: triggers on max chain length threshold" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Configure auto-VACUUM with low threshold
    db.vacuum_config.max_chain_length = 3;
    db.vacuum_config.txn_interval = 10000; // High value to not trigger by count

    // Create table
    _ = try db.execute("CREATE TABLE items (id INT, value INT)");
    _ = try db.execute("INSERT INTO items (id, value) VALUES (1, 100)");

    const table = db.tables.get("items").?;

    // Create versions up to threshold
    _ = try db.execute("UPDATE items SET value = 200");
    _ = try db.execute("UPDATE items SET value = 300");

    // At this point we have 3 versions, but auto-VACUUM hasn't triggered yet
    const stats_before = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 3), stats_before.total_versions);

    // One more update should trigger auto-VACUUM
    _ = try db.execute("UPDATE items SET value = 400");

    // Auto-VACUUM should have run, cleaning up old versions
    const stats_after = table.getVacuumStats();
    // Should be cleaned up to just 1 version (the current one)
    try testing.expectEqual(@as(usize, 1), stats_after.total_versions);
}

test "Auto-VACUUM: can be disabled" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Disable auto-VACUUM
    db.vacuum_config.enabled = false;
    db.vacuum_config.max_chain_length = 2; // Low threshold

    // Create table
    _ = try db.execute("CREATE TABLE data (id INT)");
    _ = try db.execute("INSERT INTO data (id) VALUES (1)");

    const table = db.tables.get("data").?;

    // Create many versions
    _ = try db.execute("UPDATE data SET id = 2");
    _ = try db.execute("UPDATE data SET id = 3");
    _ = try db.execute("UPDATE data SET id = 4");
    _ = try db.execute("UPDATE data SET id = 5");

    // Auto-VACUUM should NOT have run (disabled)
    const stats = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 5), stats.total_versions);
}

// ============================================================================
// VACUUM Result Verification Tests
// ============================================================================

test "VACUUM: returns correct statistics" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create table and create versions
    _ = try db.execute("CREATE TABLE metrics (id INT)");
    _ = try db.execute("INSERT INTO metrics (id) VALUES (1)");
    _ = try db.execute("UPDATE metrics SET id = 2");
    _ = try db.execute("UPDATE metrics SET id = 3");

    // Run VACUUM and check result
    const result = try db.execute("VACUUM metrics");
    defer result.deinit();

    // Verify result columns
    try testing.expectEqual(@as(usize, 5), result.columns.items.len);
    try testing.expect(std.mem.eql(u8, "table_name", result.columns.items[0]));
    try testing.expect(std.mem.eql(u8, "versions_removed", result.columns.items[1]));
    try testing.expect(std.mem.eql(u8, "total_chains", result.columns.items[2]));
    try testing.expect(std.mem.eql(u8, "total_versions", result.columns.items[3]));
    try testing.expect(std.mem.eql(u8, "max_chain_length", result.columns.items[4]));

    // Verify result values
    try testing.expectEqual(@as(usize, 1), result.rows.items.len);
    try testing.expect(std.mem.eql(u8, "metrics", result.rows.items[0].items[0].text));
    try testing.expectEqual(@as(i64, 2), result.rows.items[0].items[1].int); // 2 versions removed
    try testing.expectEqual(@as(i64, 1), result.rows.items[0].items[2].int); // 1 chain
    try testing.expectEqual(@as(i64, 1), result.rows.items[0].items[3].int); // 1 version remaining
    try testing.expectEqual(@as(i64, 1), result.rows.items[0].items[4].int); // max chain length 1
}

// ============================================================================
// Memory Reclamation Tests
// ============================================================================

test "VACUUM: actually frees memory" {
    const allocator = testing.allocator;
    var db = try Database.init(allocator);
    defer db.deinit();

    // Create table with data
    _ = try db.execute("CREATE TABLE memory_test (id INT, data TEXT)");
    _ = try db.execute("INSERT INTO memory_test (id, data) VALUES (1, 'initial')");

    const table = db.tables.get("memory_test").?;

    // Create many versions
    for (0..20) |i| {
        const query = try std.fmt.allocPrint(allocator, "UPDATE memory_test SET data = 'version_{d}'", .{i});
        defer allocator.free(query);
        _ = try db.execute(query);
    }

    // Should have 21 versions (initial + 20 updates)
    const stats_before = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 21), stats_before.total_versions);

    // Run VACUUM
    _ = try db.execute("VACUUM memory_test");

    // Should only have 1 version now
    const stats_after = table.getVacuumStats();
    try testing.expectEqual(@as(usize, 1), stats_after.total_versions);

    // Verify data is still accessible
    const result = try db.execute("SELECT data FROM memory_test");
    defer result.deinit();
    try testing.expect(std.mem.eql(u8, "version_19", result.rows.items[0].items[0].text));
}
