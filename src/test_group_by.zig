const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const Database = @import("database/core.zig").Database;

// ============================================================================
// Test Fixtures - Reusable test data setup
// ============================================================================

/// Setup employees table with department grouping
fn setupEmployeesTable(db: *Database) !void {
    _ = try db.execute("CREATE TABLE employees (id int, name text, department text)");
    _ = try db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')");
    _ = try db.execute("INSERT INTO employees VALUES (2, 'Bob', 'Sales')");
    _ = try db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Engineering')");
    _ = try db.execute("INSERT INTO employees VALUES (4, 'David', 'Sales')");
}

/// Setup sales table with product grouping
fn setupSalesTable(db: *Database) !void {
    _ = try db.execute("CREATE TABLE sales (id int, product text, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 'Widget', 100.0)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 'Gadget', 200.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 'Widget', 150.0)");
    _ = try db.execute("INSERT INTO sales VALUES (4, 'Gadget', 250.0)");
}

/// Setup scores table with player grouping
fn setupScoresTable(db: *Database) !void {
    _ = try db.execute("CREATE TABLE scores (id int, player text, score int)");
    _ = try db.execute("INSERT INTO scores VALUES (1, 'Alice', 85)");
    _ = try db.execute("INSERT INTO scores VALUES (2, 'Bob', 92)");
    _ = try db.execute("INSERT INTO scores VALUES (3, 'Alice', 78)");
    _ = try db.execute("INSERT INTO scores VALUES (4, 'Bob', 88)");
    _ = try db.execute("INSERT INTO scores VALUES (5, 'Alice', 95)");
}

/// Setup orders table with customer grouping and status filtering
fn setupOrdersTable(db: *Database) !void {
    _ = try db.execute("CREATE TABLE orders (id int, customer text, amount float, status text)");
    _ = try db.execute("INSERT INTO orders VALUES (1, 'Alice', 100.0, 'completed')");
    _ = try db.execute("INSERT INTO orders VALUES (2, 'Bob', 200.0, 'completed')");
    _ = try db.execute("INSERT INTO orders VALUES (3, 'Alice', 150.0, 'completed')");
    _ = try db.execute("INSERT INTO orders VALUES (4, 'Bob', 175.0, 'pending')");
}

// ============================================================================
// Tests
// ============================================================================

test "GROUP BY: basic with COUNT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

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

    try setupSalesTable(&db);

    var result = try db.execute("SELECT product, SUM(amount), AVG(amount) FROM sales GROUP BY product");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    try expect(result.columns.items.len == 3);

    // Use found flags to avoid silent failures
    var found_widget = false;
    var found_gadget = false;

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Widget")) {
            found_widget = true;
            try expect(@abs(row.items[1].float - 250.0) < 0.01); // SUM: 100 + 150
            try expect(@abs(row.items[2].float - 125.0) < 0.01); // AVG: 250/2
        } else if (std.mem.eql(u8, row.items[0].text, "Gadget")) {
            found_gadget = true;
            try expect(@abs(row.items[1].float - 450.0) < 0.01); // SUM: 200 + 250
            try expect(@abs(row.items[2].float - 225.0) < 0.01); // AVG: 450/2
        }
    }

    try expect(found_widget);
    try expect(found_gadget);
}

test "GROUP BY: with MIN and MAX" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupScoresTable(&db);

    var result = try db.execute("SELECT player, MIN(score), MAX(score) FROM scores GROUP BY player");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    // Use found flags to avoid silent failures
    var found_alice = false;
    var found_bob = false;

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            found_alice = true;
            try expectEqual(@as(i64, 78), row.items[1].int); // MIN
            try expectEqual(@as(i64, 95), row.items[2].int); // MAX
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            found_bob = true;
            try expectEqual(@as(i64, 88), row.items[1].int); // MIN
            try expectEqual(@as(i64, 92), row.items[2].int); // MAX
        }
    }

    try expect(found_alice);
    try expect(found_bob);
}

test "GROUP BY: with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupOrdersTable(&db);

    var result = try db.execute("SELECT customer, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    // Use found flags to avoid silent failures
    var found_alice = false;
    var found_bob = false;

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            found_alice = true;
            try expect(@abs(row.items[1].float - 250.0) < 0.01); // 100 + 150 (only completed)
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            found_bob = true;
            try expect(@abs(row.items[1].float - 200.0) < 0.01); // 200 (only completed, 175 is pending)
        }
    }

    try expect(found_alice);
    try expect(found_bob);
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

    _ = try db.execute("CREATE TABLE metrics (id int, team text, value float)");
    _ = try db.execute("INSERT INTO metrics VALUES (1, 'Alpha', 10.0)");
    _ = try db.execute("INSERT INTO metrics VALUES (2, 'Alpha', 20.0)");
    _ = try db.execute("INSERT INTO metrics VALUES (3, 'Beta', 15.0)");
    _ = try db.execute("INSERT INTO metrics VALUES (4, 'Alpha', 30.0)");

    var result = try db.execute("SELECT team, COUNT(*), SUM(value), AVG(value), MIN(value), MAX(value) FROM metrics GROUP BY team");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    try expect(result.columns.items.len == 6);

    // Use found flags to avoid silent failures
    var found_alpha = false;
    var found_beta = false;

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alpha")) {
            found_alpha = true;
            try expectEqual(@as(i64, 3), row.items[1].int); // COUNT
            try expect(@abs(row.items[2].float - 60.0) < 0.01); // SUM: 10 + 20 + 30
            try expect(@abs(row.items[3].float - 20.0) < 0.01); // AVG: 60/3
            try expect(@abs(row.items[4].float - 10.0) < 0.01); // MIN
            try expect(@abs(row.items[5].float - 30.0) < 0.01); // MAX
        } else if (std.mem.eql(u8, row.items[0].text, "Beta")) {
            found_beta = true;
            try expectEqual(@as(i64, 1), row.items[1].int); // COUNT
            try expect(@abs(row.items[2].float - 15.0) < 0.01); // SUM
            try expect(@abs(row.items[3].float - 15.0) < 0.01); // AVG
            try expect(@abs(row.items[4].float - 15.0) < 0.01); // MIN
            try expect(@abs(row.items[5].float - 15.0) < 0.01); // MAX
        }
    }

    try expect(found_alpha);
    try expect(found_beta);
}

test "GROUP BY: single group with multiple rows" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE items (id int, type text, quantity int)");
    _ = try db.execute("INSERT INTO items VALUES (1, 'fruit', 5)");
    _ = try db.execute("INSERT INTO items VALUES (2, 'fruit', 10)");
    _ = try db.execute("INSERT INTO items VALUES (3, 'fruit', 7)");

    var result = try db.execute("SELECT type, SUM(quantity) FROM items GROUP BY type");
    defer result.deinit();

    try expect(result.rows.items.len == 1);

    // Verify the single group result
    const row = result.rows.items[0];
    try expect(std.mem.eql(u8, row.items[0].text, "fruit"));
    try expect(@abs(row.items[1].float - 22.0) < 0.01); // 5 + 10 + 7
}

// ============================================================================
// NULL Handling Tests (Phase 1.3)
// ============================================================================

test "GROUP BY: NULL values in GROUP BY column" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE employees (id int, name text, department text)");
    _ = try db.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering')");
    _ = try db.execute("INSERT INTO employees VALUES (2, 'Bob', NULL)");
    _ = try db.execute("INSERT INTO employees VALUES (3, 'Charlie', 'Sales')");
    _ = try db.execute("INSERT INTO employees VALUES (4, 'David', NULL)");
    _ = try db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering')");

    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department");
    defer result.deinit();

    // Should have 3 groups: Engineering, Sales, and NULL
    try expect(result.rows.items.len == 3);

    var found_engineering = false;
    var found_sales = false;
    var found_null = false;

    for (result.rows.items) |row| {
        if (row.items[0] == .text and std.mem.eql(u8, row.items[0].text, "Engineering")) {
            found_engineering = true;
            try expectEqual(@as(i64, 2), row.items[1].int); // Alice, Eve
        } else if (row.items[0] == .text and std.mem.eql(u8, row.items[0].text, "Sales")) {
            found_sales = true;
            try expectEqual(@as(i64, 1), row.items[1].int); // Charlie
        } else if (row.items[0] == .null_value) {
            found_null = true;
            try expectEqual(@as(i64, 2), row.items[1].int); // Bob, David
        }
    }

    try expect(found_engineering);
    try expect(found_sales);
    try expect(found_null);
}

test "GROUP BY: multiple NULLs group together" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, region text, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, NULL, 100.0)");
    _ = try db.execute("INSERT INTO sales VALUES (2, NULL, 200.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, NULL, 150.0)");
    _ = try db.execute("INSERT INTO sales VALUES (4, 'West', 300.0)");

    var result = try db.execute("SELECT region, SUM(amount), COUNT(*) FROM sales GROUP BY region");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    var found_null = false;
    var found_west = false;

    for (result.rows.items) |row| {
        if (row.items[0] == .null_value) {
            found_null = true;
            try expect(@abs(row.items[1].float - 450.0) < 0.01); // 100 + 200 + 150
            try expectEqual(@as(i64, 3), row.items[2].int);
        } else if (row.items[0] == .text and std.mem.eql(u8, row.items[0].text, "West")) {
            found_west = true;
            try expect(@abs(row.items[1].float - 300.0) < 0.01);
            try expectEqual(@as(i64, 1), row.items[2].int);
        }
    }

    try expect(found_null);
    try expect(found_west);
}

test "GROUP BY: NULL vs empty string distinction" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE data (id int, category text, value int)");
    _ = try db.execute("INSERT INTO data VALUES (1, NULL, 10)");
    _ = try db.execute("INSERT INTO data VALUES (2, '', 20)");
    _ = try db.execute("INSERT INTO data VALUES (3, 'A', 30)");

    var result = try db.execute("SELECT category, SUM(value) FROM data GROUP BY category");
    defer result.deinit();

    // NULL, empty string, and 'A' should be three distinct groups
    try expect(result.rows.items.len == 3);

    var found_null = false;
    var found_empty = false;
    var found_a = false;

    for (result.rows.items) |row| {
        if (row.items[0] == .null_value) {
            found_null = true;
            try expect(@abs(row.items[1].float - 10.0) < 0.01);
        } else if (row.items[0] == .text and row.items[0].text.len == 0) {
            found_empty = true;
            try expect(@abs(row.items[1].float - 20.0) < 0.01);
        } else if (row.items[0] == .text and std.mem.eql(u8, row.items[0].text, "A")) {
            found_a = true;
            try expect(@abs(row.items[1].float - 30.0) < 0.01);
        }
    }

    try expect(found_null);
    try expect(found_empty);
    try expect(found_a);
}

// ============================================================================
// Error Case Tests (Phase 1.4)
// ============================================================================

test "GROUP BY: error on non-aggregate column without GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // Selecting name without GROUP BY while using COUNT(*) should fail
    const result = db.execute("SELECT name, COUNT(*) FROM employees GROUP BY department");
    try expectError(error.ColumnNotInGroupBy, result);
}

test "GROUP BY: error on SELECT * with GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // SELECT * is not allowed with GROUP BY
    const result = db.execute("SELECT * FROM employees GROUP BY department");
    try expectError(error.CannotUseStarWithGroupBy, result);
}

test "GROUP BY: error on mixing aggregates without GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // Mixing aggregate (COUNT) with regular column (name) without GROUP BY should fail
    const result = db.execute("SELECT name, COUNT(*) FROM employees");
    try expectError(error.MixedAggregateAndRegular, result);
}

test "GROUP BY: valid non-aggregate column in GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // This should succeed: department is in GROUP BY, so it can be selected
    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
}

// ============================================================================
// ORDER BY Integration Tests (Phase 2.3)
// ============================================================================

test "GROUP BY: with ORDER BY aggregate DESC" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // Order by COUNT(*) descending - both departments have 2 employees, but let's add more data
    _ = try db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering')");
    _ = try db.execute("INSERT INTO employees VALUES (6, 'Frank', 'HR')");

    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY COUNT(*) DESC");
    defer result.deinit();

    try expect(result.rows.items.len == 3);

    // Engineering should be first (3 employees), then Sales (2), then HR (1)
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Engineering"));
    try expectEqual(@as(i64, 3), result.rows.items[0].items[1].int);

    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Sales"));
    try expectEqual(@as(i64, 2), result.rows.items[1].items[1].int);

    try expect(std.mem.eql(u8, result.rows.items[2].items[0].text, "HR"));
    try expectEqual(@as(i64, 1), result.rows.items[2].items[1].int);
}

test "GROUP BY: with ORDER BY grouped column ASC" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department ORDER BY department ASC");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    // Alphabetically: Engineering, Sales
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Engineering"));
    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Sales"));
}

test "GROUP BY: with ORDER BY SUM DESC" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupSalesTable(&db);

    var result = try db.execute("SELECT product, SUM(amount) FROM sales GROUP BY product ORDER BY SUM(amount) DESC");
    defer result.deinit();

    try expect(result.rows.items.len == 2);

    // Gadget has higher sum (450.0) than Widget (250.0)
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Gadget"));
    try expect(@abs(result.rows.items[0].items[1].float - 450.0) < 0.01);

    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Widget"));
    try expect(@abs(result.rows.items[1].items[1].float - 250.0) < 0.01);
}

test "GROUP BY: with ORDER BY and LIMIT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupSalesTable(&db);

    // Get top 1 product by total sales
    var result = try db.execute("SELECT product, SUM(amount) FROM sales GROUP BY product ORDER BY SUM(amount) DESC LIMIT 1");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Gadget"));
    try expect(@abs(result.rows.items[0].items[1].float - 450.0) < 0.01);
}

// ============================================================================
// HAVING Clause Tests (Phase 3.3)
// ============================================================================

test "GROUP BY: with HAVING COUNT filter" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);
    _ = try db.execute("INSERT INTO employees VALUES (5, 'Eve', 'Engineering')");
    _ = try db.execute("INSERT INTO employees VALUES (6, 'Frank', 'HR')");

    // Only departments with more than 1 employee
    var result = try db.execute("SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 1");
    defer result.deinit();

    // Should have 2 departments: Engineering (3) and Sales (2), but not HR (1)
    try expect(result.rows.items.len == 2);

    var found_engineering = false;
    var found_sales = false;

    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Engineering")) {
            found_engineering = true;
            try expectEqual(@as(i64, 3), row.items[1].int);
        } else if (std.mem.eql(u8, row.items[0].text, "Sales")) {
            found_sales = true;
            try expectEqual(@as(i64, 2), row.items[1].int);
        }
    }

    try expect(found_engineering);
    try expect(found_sales);
}

test "GROUP BY: with HAVING SUM filter" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupSalesTable(&db);

    // Only products with total sales > 300
    var result = try db.execute("SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 300.0");
    defer result.deinit();

    // Only Gadget (450.0) passes, Widget (250.0) doesn't
    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Gadget"));
    try expect(@abs(result.rows.items[0].items[1].float - 450.0) < 0.01);
}

test "GROUP BY: with HAVING AVG filter" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupScoresTable(&db);

    // Only players with average score > 85
    var result = try db.execute("SELECT player, AVG(score) FROM scores GROUP BY player HAVING AVG(score) > 85.0");
    defer result.deinit();

    // Bob's average: (92+88)/2 = 90 (passes)
    // Alice's average: (85+78+95)/3 = 86 (passes)
    try expect(result.rows.items.len == 2);
}

test "GROUP BY: with HAVING and WHERE" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupOrdersTable(&db);

    // WHERE filters rows first, then GROUP BY, then HAVING filters groups
    var result = try db.execute("SELECT customer, SUM(amount) FROM orders WHERE status = 'completed' GROUP BY customer HAVING SUM(amount) > 200.0");
    defer result.deinit();

    // Only Alice (100+150=250) passes, Bob (200) doesn't
    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
    try expect(@abs(result.rows.items[0].items[1].float - 250.0) < 0.01);
}

test "GROUP BY: with HAVING, ORDER BY, and LIMIT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupSalesTable(&db);
    _ = try db.execute("INSERT INTO sales VALUES (5, 'Doohickey', 50.0)");
    _ = try db.execute("INSERT INTO sales VALUES (6, 'Doohickey', 75.0)");

    // HAVING filters, then ORDER BY sorts, then LIMIT restricts
    var result = try db.execute("SELECT product, SUM(amount) FROM sales GROUP BY product HAVING SUM(amount) > 100.0 ORDER BY SUM(amount) DESC LIMIT 2");
    defer result.deinit();

    // Three products pass HAVING: Gadget (450), Widget (250), Doohickey (125)
    // Top 2 by sum: Gadget, Widget
    try expect(result.rows.items.len == 2);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Gadget"));
    try expect(std.mem.eql(u8, result.rows.items[1].items[0].text, "Widget"));
}

test "GROUP BY: error on HAVING without GROUP BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupEmployeesTable(&db);

    // HAVING requires GROUP BY
    const result = db.execute("SELECT name FROM employees HAVING id > 1");
    try expectError(error.HavingWithoutGroupBy, result);
}
