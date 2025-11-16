const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "Query Optimizer: Equality with index - uses index scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table with index
    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_age ON users (age)");
    defer index_result.deinit();

    // Insert many rows to make index beneficial (> 100)
    var i: u32 = 1;
    while (i <= 200) : (i += 1) {
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO users VALUES ({d}, 'User{d}', {d})", .{ i, i, i % 50 });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Query with WHERE on indexed column
    var select_result = try db.execute("SELECT * FROM users WHERE age = 25");
    defer select_result.deinit();

    // Should return all users with age 25
    // With 200 users and ages mod 50, we expect 4 users with age 25
    try testing.expect(select_result.rows.items.len == 4);

    // Verify correctness
    for (select_result.rows.items) |row| {
        const age = row.items[2];
        try testing.expectEqual(ColumnValue{ .int = 25 }, age);
    }
}

test "Query Optimizer: Equality without index - uses table scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table WITHOUT index
    var create_result = try db.execute("CREATE TABLE products (id int, name text, price float)");
    defer create_result.deinit();

    // Insert rows
    var insert1 = try db.execute("INSERT INTO products VALUES (1, 'Widget', 19.99)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO products VALUES (2, 'Gadget', 29.99)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO products VALUES (3, 'Gizmo', 19.99)");
    defer insert3.deinit();

    // Query should use table scan (no index available)
    var select_result = try db.execute("SELECT * FROM products WHERE price = 19.99");
    defer select_result.deinit();

    // Should still return correct results via table scan
    try testing.expect(select_result.rows.items.len == 2);
}

test "Query Optimizer: Range query with index - uses index range scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table with index
    var create_result = try db.execute("CREATE TABLE employees (id int, name text, salary int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_salary ON employees (salary)");
    defer index_result.deinit();

    // Insert many employees with various salaries
    var i: u32 = 1;
    while (i <= 150) : (i += 1) {
        const salary = 30000 + (i * 500);
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO employees VALUES ({d}, 'Emp{d}', {d})", .{ i, i, salary });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Range query: salary > 50000 (should use index range scan)
    var select_result = try db.execute("SELECT * FROM employees WHERE salary > 50000");
    defer select_result.deinit();

    // Verify all returned salaries are > 50000
    for (select_result.rows.items) |row| {
        const salary = row.items[2];
        try testing.expect(salary.int > 50000);
    }
}

test "Query Optimizer: Range query BETWEEN - uses index range scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table with index
    var create_result = try db.execute("CREATE TABLE inventory (id int, item text, stock int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_stock ON inventory (stock)");
    defer index_result.deinit();

    // Insert items with various stock levels
    var i: u32 = 1;
    while (i <= 120) : (i += 1) {
        const stock = i * 10;
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO inventory VALUES ({d}, 'Item{d}', {d})", .{ i, i, stock });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // BETWEEN query: stock >= 500 AND stock <= 800
    var select_result = try db.execute("SELECT * FROM inventory WHERE stock >= 500 AND stock <= 800");
    defer select_result.deinit();

    // Verify all returned stock values are in range [500, 800]
    try testing.expect(select_result.rows.items.len > 0);
    for (select_result.rows.items) |row| {
        const stock = row.items[2];
        try testing.expect(stock.int >= 500);
        try testing.expect(stock.int <= 800);
    }
}

test "Query Optimizer: Small table - prefers table scan over index" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create small table with index
    var create_result = try db.execute("CREATE TABLE tiny (id int, value int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_value ON tiny (value)");
    defer index_result.deinit();

    // Insert only a few rows (< 100)
    var i: u32 = 1;
    while (i <= 10) : (i += 1) {
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO tiny VALUES ({d}, {d})", .{ i, i * 10 });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Query should use table scan (table too small for index to be beneficial)
    var select_result = try db.execute("SELECT * FROM tiny WHERE value = 50");
    defer select_result.deinit();

    // Should still return correct result
    try testing.expect(select_result.rows.items.len == 1);
    try testing.expectEqual(ColumnValue{ .int = 50 }, select_result.rows.items[0].items[1]);
}

test "Query Optimizer: Multiple queries on same index" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table with index
    var create_result = try db.execute("CREATE TABLE orders (id int, customer_id int, amount float)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_customer ON orders (customer_id)");
    defer index_result.deinit();

    // Insert many orders
    var i: u32 = 1;
    while (i <= 200) : (i += 1) {
        const customer_id = (i % 10) + 1; // 10 customers
        const amount = @as(f64, @floatFromInt(i)) * 12.5;
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO orders VALUES ({d}, {d}, {d})", .{ i, customer_id, amount });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // First query
    var select1 = try db.execute("SELECT * FROM orders WHERE customer_id = 5");
    defer select1.deinit();
    try testing.expect(select1.rows.items.len == 20); // 200 / 10 = 20

    // Second query on same index
    var select2 = try db.execute("SELECT * FROM orders WHERE customer_id = 3");
    defer select2.deinit();
    try testing.expect(select2.rows.items.len == 20);

    // Third query with different value
    var select3 = try db.execute("SELECT * FROM orders WHERE customer_id = 1");
    defer select3.deinit();
    try testing.expect(select3.rows.items.len == 20);
}

test "Query Optimizer: Range query > (greater than)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE scores (id int, score int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_score ON scores (score)");
    defer index_result.deinit();

    // Insert scores
    var i: u32 = 1;
    while (i <= 150) : (i += 1) {
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO scores VALUES ({d}, {d})", .{ i, i });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Query: score > 100
    var select_result = try db.execute("SELECT * FROM scores WHERE score > 100");
    defer select_result.deinit();

    // Should return scores 101-150 (50 rows)
    try testing.expect(select_result.rows.items.len == 50);
    for (select_result.rows.items) |row| {
        try testing.expect(row.items[1].int > 100);
    }
}

test "Query Optimizer: Range query < (less than)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE temps (id int, temperature int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_temp ON temps (temperature)");
    defer index_result.deinit();

    // Insert temperatures
    var i: u32 = 1;
    while (i <= 150) : (i += 1) {
        const temp = i - 50; // Range: -49 to 100
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO temps VALUES ({d}, {d})", .{ i, temp });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Query: temperature < 0
    var select_result = try db.execute("SELECT * FROM temps WHERE temperature < 0");
    defer select_result.deinit();

    // Should return temperatures -49 to -1 (49 rows)
    try testing.expect(select_result.rows.items.len == 49);
    for (select_result.rows.items) |row| {
        try testing.expect(row.items[1].int < 0);
    }
}

test "Query Optimizer: Range query >= (greater than or equal)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE ratings (id int, rating int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_rating ON ratings (rating)");
    defer index_result.deinit();

    // Insert ratings 1-10
    var i: u32 = 1;
    while (i <= 150) : (i += 1) {
        const rating = (i % 10) + 1;
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO ratings VALUES ({d}, {d})", .{ i, rating });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Query: rating >= 8
    var select_result = try db.execute("SELECT * FROM ratings WHERE rating >= 8");
    defer select_result.deinit();

    // Should return ratings 8, 9, 10
    try testing.expect(select_result.rows.items.len > 0);
    for (select_result.rows.items) |row| {
        try testing.expect(row.items[1].int >= 8);
        try testing.expect(row.items[1].int <= 10);
    }
}

test "Query Optimizer: Performance comparison - index vs table scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create two identical tables - one with index, one without
    var create1 = try db.execute("CREATE TABLE with_index (id int, value int)");
    defer create1.deinit();

    var create2 = try db.execute("CREATE TABLE without_index (id int, value int)");
    defer create2.deinit();

    var index_result = try db.execute("CREATE INDEX idx_value ON with_index (value)");
    defer index_result.deinit();

    // Insert same data into both tables
    var i: u32 = 1;
    while (i <= 500) : (i += 1) {
        const sql1 = try std.fmt.allocPrint(testing.allocator, "INSERT INTO with_index VALUES ({d}, {d})", .{ i, i });
        defer testing.allocator.free(sql1);
        var insert1 = try db.execute(sql1);
        defer insert1.deinit();

        const sql2 = try std.fmt.allocPrint(testing.allocator, "INSERT INTO without_index VALUES ({d}, {d})", .{ i, i });
        defer testing.allocator.free(sql2);
        var insert2 = try db.execute(sql2);
        defer insert2.deinit();
    }

    // Query both tables with same predicate
    var select1 = try db.execute("SELECT * FROM with_index WHERE value = 250");
    defer select1.deinit();

    var select2 = try db.execute("SELECT * FROM without_index WHERE value = 250");
    defer select2.deinit();

    // Both should return same result
    try testing.expect(select1.rows.items.len == 1);
    try testing.expect(select2.rows.items.len == 1);
    try testing.expectEqual(select1.rows.items[0].items[1], select2.rows.items[0].items[1]);
}

test "Query Optimizer: Complex WHERE with indexable condition" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE products (id int, category text, price int)");
    defer create_result.deinit();

    var index_result = try db.execute("CREATE INDEX idx_price ON products (price)");
    defer index_result.deinit();

    // Insert products
    var i: u32 = 1;
    while (i <= 150) : (i += 1) {
        const price = i * 10;
        const category = if (i % 2 == 0) "electronics" else "furniture";
        const sql = try std.fmt.allocPrint(testing.allocator, "INSERT INTO products VALUES ({d}, '{s}', {d})", .{ i, category, price });
        defer testing.allocator.free(sql);

        var insert_result = try db.execute(sql);
        defer insert_result.deinit();
    }

    // Complex WHERE: price > 500 AND category = 'electronics'
    // Optimizer should use index on price, then filter by category
    var select_result = try db.execute("SELECT * FROM products WHERE price > 500 AND category = 'electronics'");
    defer select_result.deinit();

    // Verify results
    for (select_result.rows.items) |row| {
        try testing.expect(row.items[2].int > 500);
        // Category check (assuming it's at index 1)
        const cat_val = row.items[1];
        try testing.expect(std.mem.eql(u8, cat_val.text, "electronics"));
    }
}
