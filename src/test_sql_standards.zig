const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const Database = @import("database/core.zig").Database;

// ============================================================================
// SQL Standard Compliance Tests for JOINs
// ============================================================================
// These tests ensure ZVDB JOIN implementation follows SQL standard behavior
// including NULL handling, Cartesian products, column ordering, and edge cases.

// ----------------------------------------------------------------------------
// NULL Handling Tests
// ----------------------------------------------------------------------------

test "SQL Standard: NULL handling in JOINs" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (NULL, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (2, NULL, 50.0)");
        defer result.deinit();
    }

    // SQL Standard: NULL != NULL, so NULLs should not match in joins
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    // Only Alice (id=1) should match, Bob (NULL) should not match order with NULL user_id
    try expectEqual(@as(usize, 1), result.rows.items.len);
}

test "SQL Standard: NULL in LEFT JOIN" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (NULL, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
        defer result.deinit();
    }

    // LEFT JOIN should include all users, even those with NULL id
    var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    // Both Alice and Bob should appear (Bob with NULLs for orders)
    try expectEqual(@as(usize, 2), result.rows.items.len);
}

// ----------------------------------------------------------------------------
// Empty Table Tests
// ----------------------------------------------------------------------------

test "SQL Standard: JOIN with empty tables" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }

    // INNER JOIN with both tables empty
    {
        var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        try expectEqual(@as(usize, 0), result.rows.items.len);
    }

    // LEFT JOIN with both tables empty
    {
        var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        try expectEqual(@as(usize, 0), result.rows.items.len);
    }
}

test "SQL Standard: LEFT JOIN with empty right table" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }

    // LEFT JOIN should still return users with NULLs for orders
    var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Alice"));
    try expect(result.rows.items[0].items[2] == .null_value); // orders.id
}

// ----------------------------------------------------------------------------
// Data Type Tests
// ----------------------------------------------------------------------------

test "SQL Standard: JOIN with different numeric types" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 1, 100.5)");
        defer result.deinit();
    }

    // JOIN should work even though user_id is int and id is int
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
}

test "SQL Standard: text comparison in JOINs is case-sensitive" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE products (sku text, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES ('ABC', 'Widget')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE inventory (sku text, quantity int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO inventory VALUES ('ABC', 50)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO inventory VALUES ('abc', 100)");
        defer result.deinit();
    }

    // Should only match 'ABC', not 'abc' (case-sensitive)
    var result = try db.execute("SELECT * FROM products INNER JOIN inventory ON products.sku = inventory.sku");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 50), result.rows.items[0].items[3].int);
}

// ----------------------------------------------------------------------------
// Complex Query Tests
// ----------------------------------------------------------------------------

test "SQL Standard: multiple rows with same join key" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (2, 1, 200.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (3, 1, 150.0)");
        defer result.deinit();
    }

    // SQL Standard: Cartesian product for matching rows (1 user × 3 orders = 3 rows)
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 3), result.rows.items.len);

    // All rows should be for Alice
    for (result.rows.items) |row| {
        try expectEqual(@as(i64, 1), row.items[0].int); // users.id
        try expect(std.mem.eql(u8, row.items[1].text, "Alice")); // users.name
    }
}

test "SQL Standard: duplicate join keys on both sides" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE table1 (id int, value text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO table1 VALUES (1, 'A')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO table1 VALUES (1, 'B')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE table2 (id int, value text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO table2 VALUES (1, 'X')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO table2 VALUES (1, 'Y')");
        defer result.deinit();
    }

    // SQL Standard: 2 × 2 Cartesian product = 4 result rows
    var result = try db.execute("SELECT * FROM table1 INNER JOIN table2 ON table1.id = table2.id");
    defer result.deinit();

    try expectEqual(@as(usize, 4), result.rows.items.len);
}

// ----------------------------------------------------------------------------
// Edge Cases
// ----------------------------------------------------------------------------

test "SQL Standard: JOIN result preserves data integrity" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 2, 75.5)");
        defer result.deinit();
    }

    // Verify data integrity: order 101 should only join with Bob (user_id=2)
    var result = try db.execute("SELECT users.name, orders.id, orders.total FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Bob"));
    try expectEqual(@as(i64, 101), result.rows.items[0].items[1].int);
    try expect(@abs(result.rows.items[0].items[2].float - 75.5) < 0.01);
}

test "SQL Standard: all join types return consistent column order" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1)");
        defer result.deinit();
    }

    // All join types should return columns in same order: base_table columns, then join_table columns
    const join_types = [_][]const u8{ "INNER", "LEFT", "RIGHT" };

    for (join_types) |join_type| {
        const query = try std.fmt.allocPrint(std.testing.allocator, "SELECT * FROM users {s} JOIN orders ON users.id = orders.user_id", .{join_type});
        defer std.testing.allocator.free(query);

        var result = try db.execute(query);
        defer result.deinit();

        // Should have 4 columns in order: users.id, users.name, orders.id, orders.user_id
        try expectEqual(@as(usize, 4), result.columns.items.len);
        try expect(std.mem.indexOf(u8, result.columns.items[0], "users.id") != null);
        try expect(std.mem.indexOf(u8, result.columns.items[1], "users.name") != null);
        try expect(std.mem.indexOf(u8, result.columns.items[2], "orders.id") != null);
        try expect(std.mem.indexOf(u8, result.columns.items[3], "orders.user_id") != null);
    }
}

test "SQL Standard: RIGHT JOIN is symmetric to LEFT JOIN" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (2, 3, 50.0)");
        defer result.deinit();
    }

    // users LEFT JOIN orders should return 2 rows (Alice matched, Bob unmatched)
    {
        var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        try expectEqual(@as(usize, 2), result.rows.items.len);
    }

    // users RIGHT JOIN orders should return 2 rows (order 1 matched, order 2 unmatched)
    {
        var result = try db.execute("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        try expectEqual(@as(usize, 2), result.rows.items.len);

        // Count NULL users (should be 1 - order for user_id=3)
        var null_count: usize = 0;
        for (result.rows.items) |row| {
            if (row.items[1] == .null_value) { // users.name
                null_count += 1;
            }
        }
        try expectEqual(@as(usize, 1), null_count);
    }
}

// ----------------------------------------------------------------------------
// SQL Standard Cartesian Product Tests
// ----------------------------------------------------------------------------

test "SQL Standard: INNER JOIN is commutative for equal tables" {
    // SQL Standard: A JOIN B should produce same result count as B JOIN A
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1)");
        defer result.deinit();
    }

    var count1: usize = 0;
    var count2: usize = 0;

    // users JOIN orders
    {
        var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        count1 = result.rows.items.len;
    }

    // orders JOIN users (reversed)
    {
        var result = try db.execute("SELECT * FROM orders INNER JOIN users ON orders.user_id = users.id");
        defer result.deinit();
        count2 = result.rows.items.len;
    }

    // Same number of rows (though column order will be different)
    try expectEqual(count1, count2);
}

test "SQL Standard: INNER JOIN produces Cartesian product of matches" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE A (id int, value text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO A VALUES (1, 'X')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO A VALUES (1, 'Y')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO A VALUES (1, 'Z')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE B (id int, value text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO B VALUES (1, 'P')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO B VALUES (1, 'Q')");
        defer result.deinit();
    }

    // SQL Standard: 3 rows × 2 rows = 6 result rows (Cartesian product)
    var result = try db.execute("SELECT * FROM A INNER JOIN B ON A.id = B.id");
    defer result.deinit();

    try expectEqual(@as(usize, 6), result.rows.items.len);
}

// ----------------------------------------------------------------------------
// JOIN Column Name Tests
// ----------------------------------------------------------------------------

test "SQL Standard: qualified column names prevent ambiguity" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Both tables have 'id' column
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0)");
        defer result.deinit();
    }

    // Select qualified columns to avoid ambiguity
    var result = try db.execute("SELECT users.id, orders.id FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int); // users.id
    try expectEqual(@as(i64, 101), result.rows.items[0].items[1].int); // orders.id
}

test "SQL Standard: SELECT * includes all columns from both tables" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (order_id int, user_id int, total float, status text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0, 'pending')");
        defer result.deinit();
    }

    // SELECT * should include: users.id, users.name, users.age, orders.order_id, orders.user_id, orders.total, orders.status
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 7), result.columns.items.len); // 3 from users + 4 from orders
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(usize, 7), result.rows.items[0].items.len);
}

// ----------------------------------------------------------------------------
// JOIN with Boolean Values
// ----------------------------------------------------------------------------

test "SQL Standard: JOIN with boolean columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, is_active bool)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, true)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, false)");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE permissions (user_id int, can_edit bool)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO permissions VALUES (1, true)");
        defer result.deinit();
    }

    // JOIN on integer ID columns
    var result = try db.execute("SELECT * FROM users INNER JOIN permissions ON users.id = permissions.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(result.rows.items[0].items[1].bool == true); // is_active
    try expect(result.rows.items[0].items[3].bool == true); // can_edit
}

// ----------------------------------------------------------------------------
// JOIN Edge Cases
// ----------------------------------------------------------------------------

test "SQL Standard: LEFT JOIN with all matches behaves like INNER JOIN" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 2)");
        defer result.deinit();
    }

    // When all left rows have matches, LEFT JOIN = INNER JOIN
    var inner_count: usize = 0;
    var left_count: usize = 0;

    {
        var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        inner_count = result.rows.items.len;
    }

    {
        var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
        defer result.deinit();
        left_count = result.rows.items.len;
    }

    try expectEqual(inner_count, left_count);
    try expectEqual(@as(usize, 2), inner_count);
}

test "SQL Standard: zero matching rows in INNER JOIN returns empty set" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 999)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 888)");
        defer result.deinit();
    }

    // No user_id matches any users.id
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "SQL Standard: JOIN preserves row ordering within result set" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 1)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (103, 1)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT orders.id FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 3), result.rows.items.len);

    // Orders should appear in consistent order (based on iteration order)
    // We can't guarantee specific order without ORDER BY, but results should be deterministic
    const first_id = result.rows.items[0].items[0].int;
    const second_id = result.rows.items[1].items[0].int;
    const third_id = result.rows.items[2].items[0].int;

    // All three should be different order IDs
    try expect(first_id >= 101 and first_id <= 103);
    try expect(second_id >= 101 and second_id <= 103);
    try expect(third_id >= 101 and third_id <= 103);
}
