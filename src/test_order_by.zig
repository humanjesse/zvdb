const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "ORDER BY: Basic ASC" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Charlie\", 35)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Alice\", 25)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Bob\", 30)");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM users ORDER BY age ASC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);
    // Should be sorted by age: Alice(25), Bob(30), Charlie(35)
    try testing.expect(result.rows.items[0].items[2].int == 25);
    try testing.expect(result.rows.items[1].items[2].int == 30);
    try testing.expect(result.rows.items[2].items[2].int == 35);
}

test "ORDER BY: Basic DESC" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Charlie\", 35)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Alice\", 25)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Bob\", 30)");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM users ORDER BY age DESC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);
    // Should be sorted by age DESC: Charlie(35), Bob(30), Alice(25)
    try testing.expect(result.rows.items[0].items[2].int == 35);
    try testing.expect(result.rows.items[1].items[2].int == 30);
    try testing.expect(result.rows.items[2].items[2].int == 25);
}

test "ORDER BY: Multiple columns" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Charlie\", 30)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Alice\", 30)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Bob\", 25)");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM users ORDER BY age DESC, name ASC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);

    // ORDER BY age DESC, name ASC means:
    // 1. First sort by age descending: 30-year-olds come before 25-year-old
    // 2. Within same age, sort by name ascending
    // Expected order: Alice(30), Charlie(30), Bob(25)

    // Row 0: Alice, age 30 (first 30-year-old alphabetically)
    try testing.expectEqual(@as(i64, 2), result.rows.items[0].items[0].int); // id
    try testing.expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Alice"));
    try testing.expectEqual(@as(i64, 30), result.rows.items[0].items[2].int);

    // Row 1: Charlie, age 30 (second 30-year-old alphabetically)
    try testing.expectEqual(@as(i64, 1), result.rows.items[1].items[0].int); // id
    try testing.expect(std.mem.eql(u8, result.rows.items[1].items[1].text, "Charlie"));
    try testing.expectEqual(@as(i64, 30), result.rows.items[1].items[2].int);

    // Row 2: Bob, age 25 (youngest, so comes last with DESC)
    try testing.expectEqual(@as(i64, 3), result.rows.items[2].items[0].int); // id
    try testing.expect(std.mem.eql(u8, result.rows.items[2].items[1].text, "Bob"));
    try testing.expectEqual(@as(i64, 25), result.rows.items[2].items[2].int);
}

test "ORDER BY: With LIMIT" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Charlie\", 35)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Alice\", 25)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Bob\", 30)");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Diana\", 28)");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users ORDER BY age DESC LIMIT 2");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
    // Should get the 2 oldest: Charlie(35), Bob(30)
    try testing.expect(result.rows.items[0].items[2].int == 35);
    try testing.expect(result.rows.items[1].items[2].int == 30);
}

test "ORDER BY: Text column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Charlie\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Alice\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Bob\")");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM users ORDER BY name ASC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);
    // Should be sorted alphabetically: Alice, Bob, Charlie
    try testing.expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Alice"));
    try testing.expect(std.mem.eql(u8, result.rows.items[1].items[1].text, "Bob"));
    try testing.expect(std.mem.eql(u8, result.rows.items[2].items[1].text, "Charlie"));
}

test "ORDER BY: GROUP BY with COUNT" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE sales (id int, department text, amount int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO sales VALUES (1, \"IT\", 100)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO sales VALUES (2, \"Sales\", 200)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO sales VALUES (3, \"IT\", 150)");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO sales VALUES (4, \"HR\", 120)");
    defer insert4.deinit();
    var insert5 = try db.execute("INSERT INTO sales VALUES (5, \"Sales\", 180)");
    defer insert5.deinit();

    var result = try db.execute("SELECT department, COUNT(*) FROM sales GROUP BY department ORDER BY COUNT(*) DESC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);
    // Should be sorted by count DESC: IT(2), Sales(2), HR(1)
    // The order of IT and Sales may vary since they have the same count
    try testing.expect(result.rows.items[2].items[1].int == 1); // HR has count 1
}

test "ORDER BY: JOIN with ORDER BY" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_users = try db.execute("CREATE TABLE users (id int, name text, dept_id int)");
    defer create_users.deinit();
    var create_depts = try db.execute("CREATE TABLE departments (id int, dept_name text)");
    defer create_depts.deinit();

    var insert_user1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 1)");
    defer insert_user1.deinit();
    var insert_user2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 2)");
    defer insert_user2.deinit();
    var insert_user3 = try db.execute("INSERT INTO users VALUES (3, \"Charlie\", 1)");
    defer insert_user3.deinit();

    var insert_dept1 = try db.execute("INSERT INTO departments VALUES (1, \"Engineering\")");
    defer insert_dept1.deinit();
    var insert_dept2 = try db.execute("INSERT INTO departments VALUES (2, \"Sales\")");
    defer insert_dept2.deinit();

    var result = try db.execute("SELECT users.name, departments.dept_name FROM users JOIN departments ON users.dept_id = departments.id ORDER BY users.name ASC");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3);
    // Should be sorted by name: Alice, Bob, Charlie
    try testing.expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
    try testing.expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Bob"));
    try testing.expect(std.mem.eql(u8, result.rows.items[2].items[0].text, "Charlie"));
}
