const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Database = @import("database/core.zig").Database;

test "GROUP BY: basic with COUNT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE employees (id int, name text, department text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (4, 'David', 'Sales')");
        defer result.deinit();
    }

    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    try expect(result.columns.items.len == 2);
    try expect(std.mem.eql(u8, result.columns.items[0], "department"));
    try expect(std.mem.eql(u8, result.columns.items[1], "COUNT(*)"));

    // Check that we have both departments
    var found_engineering = false;
    var found_sales = false;
    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Engineering")) {
            found_engineering = true;
            try expectEqual(@as(i64, 2), row.items[1].int);
        } else if (std.mem.eql(u8, row.items[0].text, "Sales")) {
            found_sales = true;
            try expectEqual(@as(i64, 2), row.items[1].int);
        }
    }
    try expect(found_engineering);
    try expect(found_sales);
}

test "GROUP BY: with SUM and AVG" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE sales (id int, product text, amount float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (1, 'Widget', 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (2, 'Gadget', 200.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (3, 'Widget', 150.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (4, 'Gadget', 250.0)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT product, SUM(amount), AVG(amount) FROM sales GROUP BY product");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    try expect(result.columns.items.len == 3);

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Widget")) {
            try expect(@abs(row.items[1].float - 250.0) < 0.01); // SUM
            try expect(@abs(row.items[2].float - 125.0) < 0.01); // AVG
        } else if (std.mem.eql(u8, row.items[0].text, "Gadget")) {
            try expect(@abs(row.items[1].float - 450.0) < 0.01); // SUM
            try expect(@abs(row.items[2].float - 225.0) < 0.01); // AVG
        }
    }
}

test "GROUP BY: with MIN and MAX" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE scores (id int, player text, score int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO scores VALUES (1, 'Alice', 85)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO scores VALUES (2, 'Bob', 92)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO scores VALUES (3, 'Alice', 78)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO scores VALUES (4, 'Bob', 88)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO scores VALUES (5, 'Alice', 95)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT player, MIN(score), MAX(score) FROM scores GROUP BY player");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            try expectEqual(@as(i64, 78), row.items[1].int); // MIN
            try expectEqual(@as(i64, 95), row.items[2].int); // MAX
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            try expectEqual(@as(i64, 88), row.items[1].int); // MIN
            try expectEqual(@as(i64, 92), row.items[2].int); // MAX
        }
    }
}

test "GROUP BY: with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE orders (id int, customer text, amount float, status text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (1, 'Alice', 100.0, 'completed')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (2, 'Bob', 200.0, 'completed')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (3, 'Alice', 150.0, 'completed')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (4, 'Bob', 175.0, 'pending')");
        defer result.deinit();
    }

    var result = try db.execute("SELECT customer, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            try expect(@abs(row.items[1].float - 250.0) < 0.01);
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            try expect(@abs(row.items[1].float - 200.0) < 0.01);
        }
    }
}

test "GROUP BY: with multiple columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE sales (id int, region text, product text, amount float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (1, 'East', 'Widget', 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (2, 'East', 'Gadget', 200.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (3, 'West', 'Widget', 150.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (4, 'East', 'Widget', 120.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO sales VALUES (5, 'West', 'Widget', 180.0)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT region, product, SUM(amount) FROM sales GROUP BY region, product");
    defer result.deinit();

    try expect(result.rows.items.len == 3);
    try expect(result.columns.items.len == 3);

    // Verify each group
    var found_east_widget = false;
    var found_east_gadget = false;
    var found_west_widget = false;

    for (result.rows.items) |row| {
        const region = row.items[0].text;
        const product = row.items[1].text;
        const sum = row.items[2].float;

        if (std.mem.eql(u8, region, "East") and std.mem.eql(u8, product, "Widget")) {
            found_east_widget = true;
            try expect(@abs(sum - 220.0) < 0.01); // 100 + 120
        } else if (std.mem.eql(u8, region, "East") and std.mem.eql(u8, product, "Gadget")) {
            found_east_gadget = true;
            try expect(@abs(sum - 200.0) < 0.01);
        } else if (std.mem.eql(u8, region, "West") and std.mem.eql(u8, product, "Widget")) {
            found_west_widget = true;
            try expect(@abs(sum - 330.0) < 0.01); // 150 + 180
        }
    }

    try expect(found_east_widget);
    try expect(found_east_gadget);
    try expect(found_west_widget);
}

test "GROUP BY: empty result" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE data (id int, category text, value int)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT category, COUNT(*) FROM data GROUP BY category");
    defer result.deinit();

    try expect(result.rows.items.len == 0);
}

test "GROUP BY: with COUNT on specific column" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE survey (id int, category text, response text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO survey VALUES (1, 'A', 'yes')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO survey VALUES (2, 'A', NULL)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO survey VALUES (3, 'B', 'yes')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO survey VALUES (4, 'A', 'no')");
        defer result.deinit();
    }

    var result = try db.execute("SELECT category, COUNT(*), COUNT(response) FROM survey GROUP BY category");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "A")) {
            try expectEqual(@as(i64, 3), row.items[1].int); // COUNT(*)
            try expectEqual(@as(i64, 2), row.items[2].int); // COUNT(response) - excludes NULL
        } else if (std.mem.eql(u8, row.items[0].text, "B")) {
            try expectEqual(@as(i64, 1), row.items[1].int); // COUNT(*)
            try expectEqual(@as(i64, 1), row.items[2].int); // COUNT(response)
        }
    }
}

test "GROUP BY: all aggregates together" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE metrics (id int, team text, value float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO metrics VALUES (1, 'Alpha', 10.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO metrics VALUES (2, 'Alpha', 20.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO metrics VALUES (3, 'Beta', 15.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO metrics VALUES (4, 'Alpha', 30.0)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT team, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM metrics GROUP BY team");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    try expect(result.columns.items.len == 6);

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alpha")) {
            try expectEqual(@as(i64, 3), row.items[1].int); // COUNT
            try expect(@abs(row.items[2].float - 60.0) < 0.01); // SUM
            try expect(@abs(row.items[3].float - 20.0) < 0.01); // AVG
            try expect(@abs(row.items[4].float - 10.0) < 0.01); // MIN
            try expect(@abs(row.items[5].float - 30.0) < 0.01); // MAX
        } else if (std.mem.eql(u8, row.items[0].text, "Beta")) {
            try expectEqual(@as(i64, 1), row.items[1].int); // COUNT
            try expect(@abs(row.items[2].float - 15.0) < 0.01); // SUM
            try expect(@abs(row.items[3].float - 15.0) < 0.01); // AVG
            try expect(@abs(row.items[4].float - 15.0) < 0.01); // MIN
            try expect(@abs(row.items[5].float - 15.0) < 0.01); // MAX
        }
    }
}

test "GROUP BY: single group with multiple rows" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE items (id int, type text, quantity int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO items VALUES (1, 'fruit', 5)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO items VALUES (2, 'fruit', 10)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO items VALUES (3, 'fruit', 7)");
        defer result.deinit();
    }

    var result = try db.execute("SELECT type, SUM(quantity) FROM items GROUP BY type");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "fruit"));
    try expect(@abs(result.rows.items[0].items[1].float - 22.0) < 0.01);
}
