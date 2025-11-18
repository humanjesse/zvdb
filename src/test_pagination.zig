const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Database = @import("database/core.zig").Database;

test "pagination: basic OFFSET" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE messages (id int, text text, timestamp int)");
        defer result.deinit();
    }

    // Insert test data
    var i: i64 = 1;
    while (i <= 10) : (i += 1) {
        const query = try std.fmt.allocPrint(
            std.testing.allocator,
            "INSERT INTO messages VALUES ({d}, 'Message {d}', {d})",
            .{ i, i, i * 100 }
        );
        defer std.testing.allocator.free(query);
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Test OFFSET without LIMIT - skip first 5 rows
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY id OFFSET 5");
        defer result.deinit();

        try expectEqual(@as(usize, 5), result.rows.items.len);
        try expectEqual(@as(i64, 6), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 10), result.rows.items[4].items[0].int);
    }

    // Test OFFSET 0 (should return all rows)
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY id OFFSET 0");
        defer result.deinit();

        try expectEqual(@as(usize, 10), result.rows.items.len);
    }

    // Test OFFSET beyond total rows (should return empty result)
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY id OFFSET 20");
        defer result.deinit();

        try expectEqual(@as(usize, 0), result.rows.items.len);
    }
}

test "pagination: LIMIT with OFFSET" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE items (id int, name text)");
        defer result.deinit();
    }

    // Insert 20 items
    var i: i64 = 1;
    while (i <= 20) : (i += 1) {
        const query = try std.fmt.allocPrint(
            std.testing.allocator,
            "INSERT INTO items VALUES ({d}, 'Item {d}')",
            .{ i, i }
        );
        defer std.testing.allocator.free(query);
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Page 1: First 5 items (LIMIT 5 OFFSET 0)
    {
        var result = try db.execute("SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 0");
        defer result.deinit();

        try expectEqual(@as(usize, 5), result.rows.items.len);
        try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 5), result.rows.items[4].items[0].int);
    }

    // Page 2: Items 6-10 (LIMIT 5 OFFSET 5)
    {
        var result = try db.execute("SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 5");
        defer result.deinit();

        try expectEqual(@as(usize, 5), result.rows.items.len);
        try expectEqual(@as(i64, 6), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 10), result.rows.items[4].items[0].int);
    }

    // Page 3: Items 11-15 (LIMIT 5 OFFSET 10)
    {
        var result = try db.execute("SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 10");
        defer result.deinit();

        try expectEqual(@as(usize, 5), result.rows.items.len);
        try expectEqual(@as(i64, 11), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 15), result.rows.items[4].items[0].int);
    }

    // Page 4: Items 16-20 (LIMIT 5 OFFSET 15)
    {
        var result = try db.execute("SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 15");
        defer result.deinit();

        try expectEqual(@as(usize, 5), result.rows.items.len);
        try expectEqual(@as(i64, 16), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 20), result.rows.items[4].items[0].int);
    }

    // Page 5: Empty (LIMIT 5 OFFSET 20)
    {
        var result = try db.execute("SELECT * FROM items ORDER BY id LIMIT 5 OFFSET 20");
        defer result.deinit();

        try expectEqual(@as(usize, 0), result.rows.items.len);
    }
}

test "pagination: OFFSET with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, age int, name text)");
        defer result.deinit();
    }

    // Insert test data
    {
        var result1 = try db.execute("INSERT INTO users VALUES (1, 25, 'Alice')");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO users VALUES (2, 30, 'Bob')");
        defer result2.deinit();
    }
    {
        var result3 = try db.execute("INSERT INTO users VALUES (3, 35, 'Charlie')");
        defer result3.deinit();
    }
    {
        var result4 = try db.execute("INSERT INTO users VALUES (4, 40, 'David')");
        defer result4.deinit();
    }
    {
        var result5 = try db.execute("INSERT INTO users VALUES (5, 45, 'Eve')");
        defer result5.deinit();
    }

    // Get users with age >= 30, skip first one
    var result = try db.execute("SELECT * FROM users WHERE age >= 30 ORDER BY id LIMIT 2 OFFSET 1");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[2].text, "Charlie")); // ID 3
    try expect(std.mem.eql(u8, result.rows.items[1].items[2].text, "David"));   // ID 4
}

test "pagination: OFFSET with ORDER BY DESC" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE scores (player text, score int)");
        defer result.deinit();
    }

    {
        var result1 = try db.execute("INSERT INTO scores VALUES ('Alice', 100)");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO scores VALUES ('Bob', 85)");
        defer result2.deinit();
    }
    {
        var result3 = try db.execute("INSERT INTO scores VALUES ('Charlie', 90)");
        defer result3.deinit();
    }
    {
        var result4 = try db.execute("INSERT INTO scores VALUES ('David', 95)");
        defer result4.deinit();
    }

    // Get top scores, skip the highest one
    var result = try db.execute("SELECT * FROM scores ORDER BY score DESC LIMIT 2 OFFSET 1");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
    try expectEqual(@as(i64, 95), result.rows.items[0].items[1].int); // David
    try expectEqual(@as(i64, 90), result.rows.items[1].items[1].int); // Charlie
}

test "pagination: OFFSET with GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE sales (department text, amount int)");
        defer result.deinit();
    }

    {
        var result1 = try db.execute("INSERT INTO sales VALUES ('Engineering', 100)");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO sales VALUES ('Engineering', 200)");
        defer result2.deinit();
    }
    {
        var result3 = try db.execute("INSERT INTO sales VALUES ('Sales', 150)");
        defer result3.deinit();
    }
    {
        var result4 = try db.execute("INSERT INTO sales VALUES ('Sales', 250)");
        defer result4.deinit();
    }
    {
        var result5 = try db.execute("INSERT INTO sales VALUES ('Marketing', 180)");
        defer result5.deinit();
    }

    // Group by department, skip first group
    var result = try db.execute("SELECT department, COUNT(*) FROM sales GROUP BY department ORDER BY department LIMIT 2 OFFSET 1");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
    // After sorting by department: Engineering, Marketing, Sales
    // OFFSET 1 skips Engineering, so we get Marketing and Sales
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Marketing"));
    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Sales"));
}

test "pagination: OFFSET with JOIN" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, amount int)");
        defer result.deinit();
    }

    {
        var result1 = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result2.deinit();
    }

    {
        var result3 = try db.execute("INSERT INTO orders VALUES (101, 1, 50)");
        defer result3.deinit();
    }
    {
        var result4 = try db.execute("INSERT INTO orders VALUES (102, 1, 75)");
        defer result4.deinit();
    }
    {
        var result5 = try db.execute("INSERT INTO orders VALUES (103, 2, 100)");
        defer result5.deinit();
    }

    // Join users and orders, skip first result
    var result = try db.execute("SELECT users.name, orders.amount FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.id LIMIT 2 OFFSET 1");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice")); // Order 102
    try expectEqual(@as(i64, 75), result.rows.items[0].items[1].int);
    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Bob"));   // Order 103
    try expectEqual(@as(i64, 100), result.rows.items[1].items[1].int);
}

test "pagination: chat history use case" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE messages (id int, user text, message text, timestamp int)");
        defer result.deinit();
    }

    // Insert 30 messages
    var i: i64 = 1;
    while (i <= 30) : (i += 1) {
        const query = try std.fmt.allocPrint(
            std.testing.allocator,
            "INSERT INTO messages VALUES ({d}, 'User{d}', 'Hello {d}', {d})",
            .{ i, i % 3, i, i * 1000 }
        );
        defer std.testing.allocator.free(query);
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Get messages 21-30 (most recent, page 1 with 10 per page)
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT 10 OFFSET 0");
        defer result.deinit();

        try expectEqual(@as(usize, 10), result.rows.items.len);
        try expectEqual(@as(i64, 30), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 21), result.rows.items[9].items[0].int);
    }

    // Get messages 11-20 (page 2)
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT 10 OFFSET 10");
        defer result.deinit();

        try expectEqual(@as(usize, 10), result.rows.items.len);
        try expectEqual(@as(i64, 20), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 11), result.rows.items[9].items[0].int);
    }

    // Get messages 1-10 (oldest, page 3)
    {
        var result = try db.execute("SELECT * FROM messages ORDER BY timestamp DESC LIMIT 10 OFFSET 20");
        defer result.deinit();

        try expectEqual(@as(usize, 10), result.rows.items.len);
        try expectEqual(@as(i64, 10), result.rows.items[0].items[0].int);
        try expectEqual(@as(i64, 1), result.rows.items[9].items[0].int);
    }
}

test "pagination: COUNT(*) without GROUP BY still works" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE items (id int, name text)");
        defer result.deinit();
    }

    {
        var result1 = try db.execute("INSERT INTO items VALUES (1, 'Item1')");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO items VALUES (2, 'Item2')");
        defer result2.deinit();
    }
    {
        var result3 = try db.execute("INSERT INTO items VALUES (3, 'Item3')");
        defer result3.deinit();
    }

    // Test COUNT(*) works
    var result = try db.execute("SELECT COUNT(*) FROM items");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 3), result.rows.items[0].items[0].int);
}

test "pagination: COUNT(*) with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, age int)");
        defer result.deinit();
    }

    {
        var result1 = try db.execute("INSERT INTO users VALUES (1, 25)");
        defer result1.deinit();
    }
    {
        var result2 = try db.execute("INSERT INTO users VALUES (2, 30)");
        defer result2.deinit();
    }
    {
        var result3 = try db.execute("INSERT INTO users VALUES (3, 35)");
        defer result3.deinit();
    }

    // Count users with age >= 30
    var result = try db.execute("SELECT COUNT(*) FROM users WHERE age >= 30");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 2), result.rows.items[0].items[0].int);
}
