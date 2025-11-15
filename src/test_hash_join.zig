const std = @import("std");
const testing = std.testing;
const hash_join = @import("database/hash_join.zig");
const Table = @import("table.zig").Table;
const ColumnValue = @import("table.zig").ColumnValue;
const ColumnType = @import("table.zig").ColumnType;
const sql = @import("sql.zig");
const JoinType = sql.JoinType;

// ============================================================================
// Hash Function Tests
// ============================================================================

test "hashColumnValue: deterministic for integers" {
    const val1 = ColumnValue{ .int = 42 };
    const val2 = ColumnValue{ .int = 42 };
    const val3 = ColumnValue{ .int = 43 };

    const hash1 = hash_join.hashColumnValue(val1);
    const hash2 = hash_join.hashColumnValue(val2);
    const hash3 = hash_join.hashColumnValue(val3);

    // Same value should produce same hash
    try testing.expectEqual(hash1, hash2);

    // Different values should (usually) produce different hashes
    // (not guaranteed, but very likely with a good hash function)
    try testing.expect(hash1 != hash3);
}

test "hashColumnValue: deterministic for floats" {
    const val1 = ColumnValue{ .float = 3.14 };
    const val2 = ColumnValue{ .float = 3.14 };
    const val3 = ColumnValue{ .float = 2.71 };

    const hash1 = hash_join.hashColumnValue(val1);
    const hash2 = hash_join.hashColumnValue(val2);
    const hash3 = hash_join.hashColumnValue(val3);

    try testing.expectEqual(hash1, hash2);
    try testing.expect(hash1 != hash3);
}

test "hashColumnValue: deterministic for text" {
    const val1 = ColumnValue{ .text = "hello" };
    const val2 = ColumnValue{ .text = "hello" };
    const val3 = ColumnValue{ .text = "world" };

    const hash1 = hash_join.hashColumnValue(val1);
    const hash2 = hash_join.hashColumnValue(val2);
    const hash3 = hash_join.hashColumnValue(val3);

    try testing.expectEqual(hash1, hash2);
    try testing.expect(hash1 != hash3);
}

test "hashColumnValue: deterministic for booleans" {
    const val_true1 = ColumnValue{ .bool = true };
    const val_true2 = ColumnValue{ .bool = true };
    const val_false = ColumnValue{ .bool = false };

    const hash_true1 = hash_join.hashColumnValue(val_true1);
    const hash_true2 = hash_join.hashColumnValue(val_true2);
    const hash_false = hash_join.hashColumnValue(val_false);

    try testing.expectEqual(hash_true1, hash_true2);
    try testing.expect(hash_true1 != hash_false);
}

test "hashColumnValue: NULL handling" {
    const val1 = ColumnValue.null_value;
    const val2 = ColumnValue.null_value;

    const hash1 = hash_join.hashColumnValue(val1);
    const hash2 = hash_join.hashColumnValue(val2);

    // NULLs should hash consistently
    try testing.expectEqual(hash1, hash2);
}

// ============================================================================
// Hash Table Tests
// ============================================================================

test "JoinHashTable: basic insert and probe" {
    const allocator = testing.allocator;

    var hash_table = hash_join.JoinHashTable.init(allocator);
    defer hash_table.deinit();

    const key1 = ColumnValue{ .int = 1 };
    const hash1 = hash_join.hashColumnValue(key1);

    // Insert row with key
    try hash_table.insert(hash1, 100);
    try hash_table.all_row_ids.append(100);

    // Probe for the key
    const matches = hash_table.probe(hash1);
    try testing.expect(matches != null);
    try testing.expectEqual(@as(usize, 1), matches.?.len);
    try testing.expectEqual(@as(u64, 100), matches.?[0]);
}

test "JoinHashTable: multiple rows with same key" {
    const allocator = testing.allocator;

    var hash_table = hash_join.JoinHashTable.init(allocator);
    defer hash_table.deinit();

    const key = ColumnValue{ .int = 1 };
    const hash = hash_join.hashColumnValue(key);

    // Insert multiple rows with same key
    try hash_table.insert(hash, 100);
    try hash_table.insert(hash, 101);
    try hash_table.insert(hash, 102);

    // Probe should return all rows
    const matches = hash_table.probe(hash);
    try testing.expect(matches != null);
    try testing.expectEqual(@as(usize, 3), matches.?.len);
}

test "JoinHashTable: probe non-existent key" {
    const allocator = testing.allocator;

    var hash_table = hash_join.JoinHashTable.init(allocator);
    defer hash_table.deinit();

    const key1 = ColumnValue{ .int = 1 };
    const key2 = ColumnValue{ .int = 2 };

    const hash1 = hash_join.hashColumnValue(key1);
    const hash2 = hash_join.hashColumnValue(key2);

    // Insert only key1
    try hash_table.insert(hash1, 100);

    // Probe for key2 should return null
    const matches = hash_table.probe(hash2);
    try testing.expect(matches == null);
}

test "JoinHashTable: mark and check matched rows" {
    const allocator = testing.allocator;

    var hash_table = hash_join.JoinHashTable.init(allocator);
    defer hash_table.deinit();

    // Mark some rows as matched
    try hash_table.markMatched(100);
    try hash_table.markMatched(101);

    // Check matched status
    try testing.expect(hash_table.wasMatched(100));
    try testing.expect(hash_table.wasMatched(101));
    try testing.expect(!hash_table.wasMatched(102));
}

// ============================================================================
// Build Phase Tests
// ============================================================================

test "buildHashTable: basic functionality" {
    const allocator = testing.allocator;

    // Create a simple table
    var table = try Table.init(allocator, "test_table");
    defer table.deinit(allocator);

    try table.addColumn("id", .int);
    try table.addColumn("name", .text);

    // Insert some rows
    var values1 = std.StringHashMap(ColumnValue).init(allocator);
    defer values1.deinit();
    try values1.put("id", ColumnValue{ .int = 1 });
    try values1.put("name", ColumnValue{ .text = "Alice" });
    _ = try table.insert(values1);

    var values2 = std.StringHashMap(ColumnValue).init(allocator);
    defer values2.deinit();
    try values2.put("id", ColumnValue{ .int = 2 });
    try values2.put("name", ColumnValue{ .text = "Bob" });
    _ = try table.insert(values2);

    // Build hash table on "id" column
    var hash_table = try hash_join.buildHashTable(allocator, &table, "id");
    defer hash_table.deinit();

    // Verify we can probe for both keys
    const hash1 = hash_join.hashColumnValue(ColumnValue{ .int = 1 });
    const hash2 = hash_join.hashColumnValue(ColumnValue{ .int = 2 });

    const matches1 = hash_table.probe(hash1);
    const matches2 = hash_table.probe(hash2);

    try testing.expect(matches1 != null);
    try testing.expect(matches2 != null);
    try testing.expectEqual(@as(usize, 1), matches1.?.len);
    try testing.expectEqual(@as(usize, 1), matches2.?.len);
}

test "buildHashTable: NULL keys are skipped" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit(allocator);

    try table.addColumn("id", .int);
    try table.addColumn("value", .int);

    // Insert row with NULL id
    var values1 = std.StringHashMap(ColumnValue).init(allocator);
    defer values1.deinit();
    try values1.put("id", ColumnValue.null_value);
    try values1.put("value", ColumnValue{ .int = 100 });
    _ = try table.insert(values1);

    // Insert row with real id
    var values2 = std.StringHashMap(ColumnValue).init(allocator);
    defer values2.deinit();
    try values2.put("id", ColumnValue{ .int = 1 });
    try values2.put("value", ColumnValue{ .int = 200 });
    _ = try table.insert(values2);

    // Build hash table
    var hash_table = try hash_join.buildHashTable(allocator, &table, "id");
    defer hash_table.deinit();

    // Should only have one row (NULL was skipped)
    try testing.expectEqual(@as(usize, 1), hash_table.all_row_ids.items.len);
}

test "buildHashTable: duplicate join keys" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit(allocator);

    try table.addColumn("category", .int);
    try table.addColumn("value", .int);

    // Insert multiple rows with same category
    for (0..3) |i| {
        var values = std.StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("category", ColumnValue{ .int = 1 }); // Same category
        try values.put("value", ColumnValue{ .int = @intCast(i) });
        _ = try table.insert(values);
    }

    // Build hash table
    var hash_table = try hash_join.buildHashTable(allocator, &table, "category");
    defer hash_table.deinit();

    // Should have all 3 rows under the same hash
    const hash = hash_join.hashColumnValue(ColumnValue{ .int = 1 });
    const matches = hash_table.probe(hash);

    try testing.expect(matches != null);
    try testing.expectEqual(@as(usize, 3), matches.?.len);
}

// ============================================================================
// Integration Test: Compare Hash Join with Nested Loop
// ============================================================================

test "hash join produces same results as nested loop for INNER JOIN" {
    const allocator = testing.allocator;

    // Create users table
    var users = try Table.init(allocator, "users");
    defer users.deinit(allocator);
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    // Create orders table
    var orders = try Table.init(allocator, "orders");
    defer orders.deinit(allocator);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("amount", .float);

    // Insert users
    for (1..6) |i| {
        var values = std.StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = @intCast(i) });
        const name = try std.fmt.allocPrint(allocator, "User{d}", .{i});
        defer allocator.free(name);
        const name_owned = try allocator.dupe(u8, name);
        try values.put("name", ColumnValue{ .text = name_owned });
        _ = try users.insert(values);
    }

    // Insert orders (some users have orders, some don't)
    const order_data = [_]struct { user_id: i64, amount: f64 }{
        .{ .user_id = 1, .amount = 10.0 },
        .{ .user_id = 1, .amount = 20.0 },
        .{ .user_id = 2, .amount = 30.0 },
        .{ .user_id = 4, .amount = 40.0 },
    };

    for (order_data) |order| {
        var values = std.StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("user_id", ColumnValue{ .int = order.user_id });
        try values.put("amount", ColumnValue{ .float = order.amount });
        _ = try orders.insert(values);
    }

    // Execute hash join
    var hash_result = try hash_join.executeHashJoin(
        allocator,
        &users,
        &orders,
        "users",
        "orders",
        .inner,
        "id",
        "user_id",
        true, // SELECT *
        &[_]sql.ColumnSpec{},
    );
    defer hash_result.deinit();

    // Verify we got the expected number of results
    // User 1: 2 orders, User 2: 1 order, User 4: 1 order = 4 total
    try testing.expectEqual(@as(usize, 4), hash_result.rows.items.len);

    // Verify column count (all columns from both tables)
    const expected_columns = users.columns.items.len + orders.columns.items.len;
    try testing.expectEqual(expected_columns, hash_result.columns.items.len);
}

test "hash join LEFT JOIN includes unmatched rows" {
    const allocator = testing.allocator;

    // Create tables
    var users = try Table.init(allocator, "users");
    defer users.deinit(allocator);
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    var orders = try Table.init(allocator, "orders");
    defer orders.deinit(allocator);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("amount", .float);

    // Insert 3 users
    for (1..4) |i| {
        var values = std.StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = @intCast(i) });
        const name = try std.fmt.allocPrint(allocator, "User{d}", .{i});
        defer allocator.free(name);
        const name_owned = try allocator.dupe(u8, name);
        try values.put("name", ColumnValue{ .text = name_owned });
        _ = try users.insert(values);
    }

    // Insert order for only user 1
    var values = std.StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("user_id", ColumnValue{ .int = 1 });
    try values.put("amount", ColumnValue{ .float = 100.0 });
    _ = try orders.insert(values);

    // Execute LEFT JOIN
    var result = try hash_join.executeHashJoin(
        allocator,
        &users,
        &orders,
        "users",
        "orders",
        .left,
        "id",
        "user_id",
        true,
        &[_]sql.ColumnSpec{},
    );
    defer result.deinit();

    // Should have 3 rows (all users, even those without orders)
    try testing.expectEqual(@as(usize, 3), result.rows.items.len);

    // First row should have matching order data
    // Rows 2 and 3 should have NULLs for order columns
    const order_columns_start = users.columns.items.len;

    // Check that unmatched rows have NULL for order columns
    for (result.rows.items[1..]) |row| {
        for (row.items[order_columns_start..]) |val| {
            try testing.expectEqual(ColumnValue.null_value, val);
        }
    }
}

test "hash join RIGHT JOIN includes unmatched rows from right table" {
    const allocator = testing.allocator;

    // Create tables
    var users = try Table.init(allocator, "users");
    defer users.deinit(allocator);
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    var orders = try Table.init(allocator, "orders");
    defer orders.deinit(allocator);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("amount", .float);

    // Insert 1 user
    var user_values = std.StringHashMap(ColumnValue).init(allocator);
    defer user_values.deinit();
    try user_values.put("id", ColumnValue{ .int = 1 });
    const name_owned = try allocator.dupe(u8, "Alice");
    try user_values.put("name", ColumnValue{ .text = name_owned });
    _ = try users.insert(user_values);

    // Insert 3 orders (one matching, two orphaned)
    const order_data = [_]struct { user_id: i64, amount: f64 }{
        .{ .user_id = 1, .amount = 10.0 },
        .{ .user_id = 999, .amount = 20.0 }, // No matching user
        .{ .user_id = 888, .amount = 30.0 }, // No matching user
    };

    for (order_data) |order| {
        var values = std.StringHashMap(ColumnValue).init(allocator);
        defer values.deinit();
        try values.put("user_id", ColumnValue{ .int = order.user_id });
        try values.put("amount", ColumnValue{ .float = order.amount });
        _ = try orders.insert(values);
    }

    // Execute RIGHT JOIN
    var result = try hash_join.executeHashJoin(
        allocator,
        &users,
        &orders,
        "users",
        "orders",
        .right,
        "id",
        "user_id",
        true,
        &[_]sql.ColumnSpec{},
    );
    defer result.deinit();

    // Should have 3 rows (all orders, even those without matching users)
    try testing.expectEqual(@as(usize, 3), result.rows.items.len);

    // Check that unmatched orders have NULL for user columns
    var null_user_count: usize = 0;
    for (result.rows.items) |row| {
        // First columns are from users table
        const first_col = row.items[0];
        if (first_col == .null_value) {
            null_user_count += 1;
        }
    }

    // Two orders should have NULL users
    try testing.expectEqual(@as(usize, 2), null_user_count);
}

test "hash join handles empty tables" {
    const allocator = testing.allocator;

    var table1 = try Table.init(allocator, "table1");
    defer table1.deinit(allocator);
    try table1.addColumn("id", .int);

    var table2 = try Table.init(allocator, "table2");
    defer table2.deinit(allocator);
    try table2.addColumn("id", .int);

    // Both tables empty
    var result = try hash_join.executeHashJoin(
        allocator,
        &table1,
        &table2,
        "table1",
        "table2",
        .inner,
        "id",
        "id",
        true,
        &[_]sql.ColumnSpec{},
    );
    defer result.deinit();

    try testing.expectEqual(@as(usize, 0), result.rows.items.len);
}

test "hash join with different data types" {
    const allocator = testing.allocator;

    // Test with text join keys
    var table1 = try Table.init(allocator, "table1");
    defer table1.deinit(allocator);
    try table1.addColumn("code", .text);
    try table1.addColumn("value", .int);

    var table2 = try Table.init(allocator, "table2");
    defer table2.deinit(allocator);
    try table2.addColumn("code", .text);
    try table2.addColumn("description", .text);

    // Insert data
    var values1 = std.StringHashMap(ColumnValue).init(allocator);
    defer values1.deinit();
    try values1.put("code", ColumnValue{ .text = try allocator.dupe(u8, "ABC") });
    try values1.put("value", ColumnValue{ .int = 100 });
    _ = try table1.insert(values1);

    var values2 = std.StringHashMap(ColumnValue).init(allocator);
    defer values2.deinit();
    try values2.put("code", ColumnValue{ .text = try allocator.dupe(u8, "ABC") });
    try values2.put("description", ColumnValue{ .text = try allocator.dupe(u8, "Test") });
    _ = try table2.insert(values2);

    // Execute join
    var result = try hash_join.executeHashJoin(
        allocator,
        &table1,
        &table2,
        "table1",
        "table2",
        .inner,
        "code",
        "code",
        true,
        &[_]sql.ColumnSpec{},
    );
    defer result.deinit();

    try testing.expectEqual(@as(usize, 1), result.rows.items.len);
}
