const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Database = @import("database/core.zig").Database;

test "aggregate: COUNT(*) basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    var result = try db.execute("SELECT COUNT(*) FROM users");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 3), result.rows.items[0].items[0].int);
}

test "aggregate: COUNT(*) empty table" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE empty (id int)");

    var result = try db.execute("SELECT COUNT(*) FROM empty");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 0), result.rows.items[0].items[0].int);
}

test "aggregate: COUNT(column) with nulls" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', NULL)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
    _ = try db.execute("INSERT INTO users VALUES (4, 'David', NULL)");

    var result = try db.execute("SELECT COUNT(age) FROM users");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 2), result.rows.items[0].items[0].int); // Only non-null values counted
}

test "aggregate: SUM basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 100.5)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 200.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 150.5)");

    var result = try db.execute("SELECT SUM(amount) FROM sales");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(@abs(result.rows.items[0].items[0].float - 451.0) < 0.01);
}

test "aggregate: SUM with integers" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE scores (id int, points int)");
    _ = try db.execute("INSERT INTO scores VALUES (1, 10)");
    _ = try db.execute("INSERT INTO scores VALUES (2, 20)");
    _ = try db.execute("INSERT INTO scores VALUES (3, 30)");

    var result = try db.execute("SELECT SUM(points) FROM scores");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(@abs(result.rows.items[0].items[0].float - 60.0) < 0.01);
}

test "aggregate: AVG basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 100.0)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 200.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 150.0)");

    var result = try db.execute("SELECT AVG(amount) FROM sales");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(@abs(result.rows.items[0].items[0].float - 150.0) < 0.01);
}

test "aggregate: AVG empty table returns NULL" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE empty (id int, value float)");

    var result = try db.execute("SELECT AVG(value) FROM empty");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(result.rows.items[0].items[0] == .null_value);
}

test "aggregate: MIN and MAX with floats" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE prices (id int, price float)");
    _ = try db.execute("INSERT INTO prices VALUES (1, 9.99)");
    _ = try db.execute("INSERT INTO prices VALUES (2, 19.99)");
    _ = try db.execute("INSERT INTO prices VALUES (3, 4.99)");
    _ = try db.execute("INSERT INTO prices VALUES (4, 14.99)");

    var result = try db.execute("SELECT MIN(price), MAX(price) FROM prices");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(@abs(result.rows.items[0].items[0].float - 4.99) < 0.01);
    try expect(@abs(result.rows.items[0].items[1].float - 19.99) < 0.01);
}

test "aggregate: MIN and MAX with integers" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE scores (id int, points int)");
    _ = try db.execute("INSERT INTO scores VALUES (1, 85)");
    _ = try db.execute("INSERT INTO scores VALUES (2, 92)");
    _ = try db.execute("INSERT INTO scores VALUES (3, 78)");
    _ = try db.execute("INSERT INTO scores VALUES (4, 95)");

    var result = try db.execute("SELECT MIN(points), MAX(points) FROM scores");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 78), result.rows.items[0].items[0].int);
    try expectEqual(@as(i64, 95), result.rows.items[0].items[1].int);
}

test "aggregate: MIN and MAX with text" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE names (id int, name text)");
    _ = try db.execute("INSERT INTO names VALUES (1, 'Zebra')");
    _ = try db.execute("INSERT INTO names VALUES (2, 'Apple')");
    _ = try db.execute("INSERT INTO names VALUES (3, 'Mango')");

    var result = try db.execute("SELECT MIN(name), MAX(name) FROM names");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Apple"));
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Zebra"));
}

test "aggregate: COUNT(*) with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");
    _ = try db.execute("INSERT INTO users VALUES (4, 'David', 40)");

    var result = try db.execute("SELECT COUNT(*) FROM users WHERE age = 30");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int);
}

test "aggregate: SUM with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, amount float, category text)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 100.0, 'electronics')");
    _ = try db.execute("INSERT INTO sales VALUES (2, 200.0, 'electronics')");
    _ = try db.execute("INSERT INTO sales VALUES (3, 50.0, 'clothing')");

    var result = try db.execute("SELECT SUM(amount) FROM sales WHERE category = 'electronics'");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(@abs(result.rows.items[0].items[0].float - 300.0) < 0.01);
}

test "aggregate: multiple aggregates in one query" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE products (id int, price float, stock int)");
    _ = try db.execute("INSERT INTO products VALUES (1, 10.99, 5)");
    _ = try db.execute("INSERT INTO products VALUES (2, 25.50, 3)");
    _ = try db.execute("INSERT INTO products VALUES (3, 15.00, 8)");

    var result = try db.execute("SELECT COUNT(*), SUM(price), AVG(price), MIN(price), MAX(price) FROM products");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 3), result.rows.items[0].items[0].int); // COUNT(*)
    try expect(@abs(result.rows.items[0].items[1].float - 51.49) < 0.01); // SUM
    try expect(@abs(result.rows.items[0].items[2].float - 17.163) < 0.01); // AVG
    try expect(@abs(result.rows.items[0].items[3].float - 10.99) < 0.01); // MIN
    try expect(@abs(result.rows.items[0].items[4].float - 25.50) < 0.01); // MAX
}

test "aggregate: MIN/MAX return NULL for empty result" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE products (id int, price float)");

    var result = try db.execute("SELECT MIN(price), MAX(price) FROM products");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(result.rows.items[0].items[0] == .null_value);
    try expect(result.rows.items[0].items[1] == .null_value);
}

test "aggregate: COUNT with different columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, email text)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', NULL)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");

    var result = try db.execute("SELECT COUNT(*), COUNT(name), COUNT(email) FROM users");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expectEqual(@as(i64, 3), result.rows.items[0].items[0].int); // COUNT(*)
    try expectEqual(@as(i64, 3), result.rows.items[0].items[1].int); // COUNT(name)
    try expectEqual(@as(i64, 2), result.rows.items[0].items[2].int); // COUNT(email) - Bob has NULL
}

test "aggregate: result column names" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 100.0)");

    var result = try db.execute("SELECT COUNT(*), SUM(amount), AVG(amount), MIN(amount), MAX(amount) FROM sales");
    defer result.deinit();

    // Check column names
    try expect(std.mem.eql(u8, result.columns.items[0], "COUNT(*)"));
    try expect(std.mem.eql(u8, result.columns.items[1], "SUM(amount)"));
    try expect(std.mem.eql(u8, result.columns.items[2], "AVG(amount)"));
    try expect(std.mem.eql(u8, result.columns.items[3], "MIN(amount)"));
    try expect(std.mem.eql(u8, result.columns.items[4], "MAX(amount)"));
}
