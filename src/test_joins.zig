const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Database = @import("database/core.zig").Database;

test "INNER JOIN: basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create users table
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
        var result = try db.execute("INSERT INTO users VALUES (3, 'Charlie')");
        defer result.deinit();
    }

    // Create orders table
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
        var result = try db.execute("INSERT INTO orders VALUES (3, 2, 50.0)");
        defer result.deinit();
    }

    // Test INNER JOIN with SELECT *
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    // Should have 3 matching rows (Alice has 2 orders, Bob has 1, Charlie has 0)
    try expectEqual(@as(usize, 3), result.rows.items.len);

    // Check columns include all from both tables
    try expect(result.columns.items.len == 5); // users.id, users.name, orders.id, orders.user_id, orders.total
}

test "INNER JOIN: with specific columns" {
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
        var result = try db.execute("INSERT INTO orders VALUES (2, 2, 50.0)");
        defer result.deinit();
    }

    // Test INNER JOIN with qualified column names
    var result = try db.execute("SELECT users.name, orders.total FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
    try expectEqual(@as(usize, 2), result.columns.items.len);

    // Check the data
    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            try expect(@abs(row.items[1].float - 100.0) < 0.01);
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            try expect(@abs(row.items[1].float - 50.0) < 0.01);
        }
    }
}

test "LEFT JOIN: includes unmatched rows" {
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
        var result = try db.execute("INSERT INTO users VALUES (3, 'Charlie')");
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

    // Test LEFT JOIN - should include all users even if they have no orders
    var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    // Should have 3 rows: Alice (with order), Bob (NULL), Charlie (NULL)
    try expectEqual(@as(usize, 3), result.rows.items.len);

    // Count NULL rows (users without orders)
    var null_count: usize = 0;
    for (result.rows.items) |row| {
        // Check if orders.id (third column) is NULL
        if (row.items[2] == .null_value) {
            null_count += 1;
        }
    }
    try expectEqual(@as(usize, 2), null_count); // Bob and Charlie should have NULLs
}

test "LEFT JOIN: multiple matches" {
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

    // Alice should appear 3 times (one for each order)
    var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 3), result.rows.items.len);

    // All rows should be for Alice
    for (result.rows.items) |row| {
        try expect(std.mem.eql(u8, row.items[1].text, "Alice"));
    }
}

test "RIGHT JOIN: includes unmatched rows from right table" {
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
        var result = try db.execute("INSERT INTO orders VALUES (2, 2, 50.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (3, 3, 75.0)");
        defer result.deinit();
    }

    // Test RIGHT JOIN - should include all orders even if user doesn't exist
    var result = try db.execute("SELECT * FROM users RIGHT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    // Should have 3 rows: one matched (Alice), two with NULL users
    try expectEqual(@as(usize, 3), result.rows.items.len);

    // Count NULL rows (orders without users)
    var null_count: usize = 0;
    for (result.rows.items) |row| {
        // Check if users.name (second column) is NULL
        if (row.items[1] == .null_value) {
            null_count += 1;
        }
    }
    try expectEqual(@as(usize, 2), null_count); // Orders 2 and 3 should have NULL users
}

test "JOIN: with unqualified column names" {
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
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0)");
        defer result.deinit();
    }

    // Test JOIN with unqualified column name (should work if column is unique across tables)
    var result = try db.execute("SELECT name, total FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
    try expect(@abs(result.rows.items[0].items[1].float - 100.0) < 0.01);
}

test "JOIN: using JOIN without INNER keyword" {
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

    // JOIN defaults to INNER JOIN
    var result = try db.execute("SELECT * FROM users JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
}

test "INNER JOIN: no matches returns empty result" {
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
        var result = try db.execute("INSERT INTO orders VALUES (1, 99, 100.0)");
        defer result.deinit();
    }

    // INNER JOIN with no matches
    var result = try db.execute("SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "LEFT JOIN: all unmatched returns all with NULLs" {
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

    // LEFT JOIN with no orders at all
    var result = try db.execute("SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);

    // All order columns should be NULL
    for (result.rows.items) |row| {
        try expect(row.items[2] == .null_value); // orders.id
        try expect(row.items[3] == .null_value); // orders.user_id
        try expect(row.items[4] == .null_value); // orders.total
    }
}

test "JOIN: with different data types" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE products (sku text, name text, price float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES ('ABC123', 'Widget', 19.99)");
        defer result.deinit();
    }

    {
        var result = try db.execute("CREATE TABLE inventory (sku text, quantity int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO inventory VALUES ('ABC123', 50)");
        defer result.deinit();
    }

    // JOIN on text column
    var result = try db.execute("SELECT products.name, inventory.quantity FROM products INNER JOIN inventory ON products.sku = inventory.sku");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Widget"));
    try expectEqual(@as(i64, 50), result.rows.items[0].items[1].int);
}
