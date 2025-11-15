const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const Database = @import("database/core.zig").Database;

// ============================================================================
// Category 1: Basic 3-Table Joins
// ============================================================================

test "3-table INNER JOIN: basic e-commerce pattern" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create users table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text, email text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice', 'alice@example.com')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob', 'bob@example.com')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 'charlie@example.com')");
        defer result.deinit();
    }

    // Create orders table
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float, status text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 150.0, 'completed')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 1, 200.0, 'pending')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (103, 2, 75.0, 'completed')");
        defer result.deinit();
    }

    // Create products table (order line items)
    {
        var result = try db.execute("CREATE TABLE products (id int, order_id int, name text, price float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (1, 101, 'Widget', 50.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (2, 101, 'Gadget', 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (3, 102, 'Doohickey', 200.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (4, 103, 'Thingamajig', 75.0)");
        defer result.deinit();
    }

    // Test 3-table INNER JOIN
    var result = try db.execute(
        \\SELECT users.name, orders.id, products.name, products.price
        \\FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN products ON orders.id = products.order_id
    );
    defer result.deinit();

    // Should have 4 rows: Alice (2 products in order 101, 1 in order 102), Bob (1 product in order 103)
    try expectEqual(@as(usize, 4), result.rows.items.len);

    // Should have 4 columns
    try expectEqual(@as(usize, 4), result.columns.items.len);

    // Verify first row: Alice, order 101, Widget, 50.0
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
    try expectEqual(@as(i64, 101), result.rows.items[0].items[1].int);
    try expect(std.mem.eql(u8, result.rows.items[0].items[2].text, "Widget"));
    try expect(@abs(result.rows.items[0].items[3].float - 50.0) < 0.01);

    // Count rows per user
    var alice_count: usize = 0;
    var bob_count: usize = 0;
    for (result.rows.items) |row| {
        if (std.mem.eql(u8, row.items[0].text, "Alice")) {
            alice_count += 1;
        } else if (std.mem.eql(u8, row.items[0].text, "Bob")) {
            bob_count += 1;
        }
    }
    try expectEqual(@as(usize, 3), alice_count); // Alice has 3 products
    try expectEqual(@as(usize, 1), bob_count); // Bob has 1 product
}

test "3-table INNER JOIN: SELECT * with all columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create tables
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE products (id int, order_id int, name text)");
        defer result.deinit();
    }

    // Insert data
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (1, 101, 'Widget')");
        defer result.deinit();
    }

    // Test SELECT *
    var result = try db.execute(
        \\SELECT * FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN products ON orders.id = products.order_id
    );
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);

    // Should have 7 columns total (users: 2, orders: 3, products: 3)
    try expectEqual(@as(usize, 7), result.columns.items.len);

    // Verify qualified column names
    try expect(std.mem.eql(u8, result.columns.items[0], "users.id"));
    try expect(std.mem.eql(u8, result.columns.items[1], "users.name"));
    try expect(std.mem.eql(u8, result.columns.items[2], "orders.id"));
    try expect(std.mem.eql(u8, result.columns.items[3], "orders.user_id"));
    try expect(std.mem.eql(u8, result.columns.items[4], "orders.total"));
    try expect(std.mem.eql(u8, result.columns.items[5], "products.id"));
    try expect(std.mem.eql(u8, result.columns.items[6], "products.name"));
}

test "3-table LEFT JOIN: propagates NULLs through chain" {
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

    // Create orders table (Charlie has no orders)
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 2, 50.0)");
        defer result.deinit();
    }

    // Create shipments table (Bob's order not shipped)
    {
        var result = try db.execute("CREATE TABLE shipments (id int, order_id int, tracking text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO shipments VALUES (1, 101, 'TRACK123')");
        defer result.deinit();
    }

    // Test 3-table LEFT JOIN
    var result = try db.execute(
        \\SELECT users.name, orders.id, shipments.tracking
        \\FROM users
        \\LEFT JOIN orders ON users.id = orders.user_id
        \\LEFT JOIN shipments ON orders.id = shipments.order_id
    );
    defer result.deinit();

    // Should have 3 rows: Alice (matched), Bob (order but no shipment), Charlie (no order)
    try expectEqual(@as(usize, 3), result.rows.items.len);

    // Check each user's result
    var alice_found = false;
    var bob_found = false;
    var charlie_found = false;

    for (result.rows.items) |row| {
        const name = row.items[0].text;

        if (std.mem.eql(u8, name, "Alice")) {
            alice_found = true;
            // Alice should have order 101 and tracking TRACK123
            try expectEqual(@as(i64, 101), row.items[1].int);
            try expect(std.mem.eql(u8, row.items[2].text, "TRACK123"));
        } else if (std.mem.eql(u8, name, "Bob")) {
            bob_found = true;
            // Bob should have order 102 but NULL tracking
            try expectEqual(@as(i64, 102), row.items[1].int);
            try expect(row.items[2] == .null_value);
        } else if (std.mem.eql(u8, name, "Charlie")) {
            charlie_found = true;
            // Charlie should have NULL for both order and tracking
            try expect(row.items[1] == .null_value);
            try expect(row.items[2] == .null_value);
        }
    }

    try expect(alice_found);
    try expect(bob_found);
    try expect(charlie_found);
}

test "3-table RIGHT JOIN: unmatched rows from rightmost table" {
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

    // Create orders table
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (102, 999, 200.0)"); // Orphaned order
        defer result.deinit();
    }

    // Create shipments table
    {
        var result = try db.execute("CREATE TABLE shipments (id int, order_id int, tracking text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO shipments VALUES (1, 101, 'TRACK123')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO shipments VALUES (2, 888, 'TRACK456')"); // Orphaned shipment
        defer result.deinit();
    }

    // Test 3-table RIGHT JOIN chain
    var result = try db.execute(
        \\SELECT users.name, orders.id, shipments.tracking
        \\FROM users
        \\RIGHT JOIN orders ON users.id = orders.user_id
        \\RIGHT JOIN shipments ON orders.id = shipments.order_id
    );
    defer result.deinit();

    // Should include all shipments
    try expectEqual(@as(usize, 2), result.rows.items.len);

    // Verify rows
    var has_alice_shipment = false;
    var has_orphan_shipment = false;

    for (result.rows.items) |row| {
        const tracking = row.items[2].text;

        if (std.mem.eql(u8, tracking, "TRACK123")) {
            has_alice_shipment = true;
            // Should have Alice's name
            try expect(std.mem.eql(u8, row.items[0].text, "Alice"));
        } else if (std.mem.eql(u8, tracking, "TRACK456")) {
            has_orphan_shipment = true;
            // Should have NULL for user name (orphaned)
            try expect(row.items[0] == .null_value);
        }
    }

    try expect(has_alice_shipment);
    try expect(has_orphan_shipment);
}

// ============================================================================
// Category 2: Edge Cases
// ============================================================================

test "3-table JOIN: empty middle table breaks chain" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create tables
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE products (id int, order_id int, name text, price float)");
        defer result.deinit();
    }

    // Insert users
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
        defer result.deinit();
    }

    // Orders table is EMPTY (no inserts)

    // Insert products (orphaned, no matching orders)
    {
        var result = try db.execute("INSERT INTO products VALUES (1, 999, 'Widget', 50.0)");
        defer result.deinit();
    }

    // Test INNER JOIN with empty middle table
    {
        var result = try db.execute(
            \\SELECT users.name, orders.id, products.name
            \\FROM users
            \\INNER JOIN orders ON users.id = orders.user_id
            \\INNER JOIN products ON orders.id = products.order_id
        );
        defer result.deinit();

        // Should return 0 rows (broken chain)
        try expectEqual(@as(usize, 0), result.rows.items.len);
    }

    // Test LEFT JOIN with empty middle table
    {
        var result = try db.execute(
            \\SELECT users.name, orders.id, products.name
            \\FROM users
            \\LEFT JOIN orders ON users.id = orders.user_id
            \\LEFT JOIN products ON orders.id = products.order_id
        );
        defer result.deinit();

        // Should return 2 rows (all users with NULLs for orders and products)
        try expectEqual(@as(usize, 2), result.rows.items.len);

        // Both rows should have NULL for orders and products
        for (result.rows.items) |row| {
            try expect(row.items[1] == .null_value); // orders.id
            try expect(row.items[2] == .null_value); // products.name
        }
    }
}

test "3-table JOIN: NULL join keys don't match" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create tables
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE shipments (id int, order_id int, tracking text)");
        defer result.deinit();
    }

    // Insert users
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Insert order with NULL user_id
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, NULL, 100.0)");
        defer result.deinit();
    }

    // Insert shipment
    {
        var result = try db.execute("INSERT INTO shipments VALUES (1, 101, 'TRACK123')");
        defer result.deinit();
    }

    // Test INNER JOIN
    var result = try db.execute(
        \\SELECT users.name, orders.id, shipments.tracking
        \\FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN shipments ON orders.id = shipments.order_id
    );
    defer result.deinit();

    // Should return 0 rows (NULL keys don't match)
    try expectEqual(@as(usize, 0), result.rows.items.len);
}

// ============================================================================
// Category 3: 4-Table and 5-Table Joins (Scalability)
// ============================================================================

test "4-table INNER JOIN: scalability check" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create 4 tables in a chain
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE products (id int, order_id int, name text, price float)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE reviews (id int, product_id int, rating int, comment text)");
        defer result.deinit();
    }

    // Insert data through the chain
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1, 150.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (1, 101, 'Widget', 50.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (2, 101, 'Gadget', 100.0)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO reviews VALUES (1, 1, 5, 'Excellent')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO reviews VALUES (2, 2, 4, 'Good')");
        defer result.deinit();
    }

    // Execute 4-table join
    var result = try db.execute(
        \\SELECT users.name, orders.id, products.name, reviews.rating
        \\FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN products ON orders.id = products.order_id
        \\INNER JOIN reviews ON products.id = reviews.product_id
    );
    defer result.deinit();

    // Should have 2 rows (1 user → 1 order → 2 products → 2 reviews)
    try expectEqual(@as(usize, 2), result.rows.items.len);
    try expectEqual(@as(usize, 4), result.columns.items.len);

    // Both rows should be for Alice
    for (result.rows.items) |row| {
        try expect(std.mem.eql(u8, row.items[0].text, "Alice"));
        try expectEqual(@as(i64, 101), row.items[1].int);
    }

    // Check ratings are present
    var rating_sum: i64 = 0;
    for (result.rows.items) |row| {
        rating_sum += row.items[3].int;
    }
    try expectEqual(@as(i64, 9), rating_sum); // 5 + 4 = 9
}

test "5-table INNER JOIN: extended chain" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create 5 tables
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE products (id int, order_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE reviews (id int, product_id int)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE reviewers (id int, review_id int, name text)");
        defer result.deinit();
    }

    // Insert data
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO orders VALUES (101, 1)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO products VALUES (1, 101)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO reviews VALUES (1, 1)");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO reviewers VALUES (1, 1, 'Bob')");
        defer result.deinit();
    }

    // Execute 5-table join
    var result = try db.execute(
        \\SELECT users.name, reviewers.name
        \\FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN products ON orders.id = products.order_id
        \\INNER JOIN reviews ON products.id = reviews.product_id
        \\INNER JOIN reviewers ON reviews.id = reviewers.review_id
    );
    defer result.deinit();

    // Should have 1 row
    try expectEqual(@as(usize, 1), result.rows.items.len);

    // Verify data
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Bob"));
}

// ============================================================================
// Category 4: Mixed Join Types
// ============================================================================

test "3-table mixed JOIN: INNER + LEFT combination" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Create tables
    {
        var result = try db.execute("CREATE TABLE departments (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE employees (id int, dept_id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE projects (id int, employee_id int, name text)");
        defer result.deinit();
    }

    // Insert departments
    {
        var result = try db.execute("INSERT INTO departments VALUES (1, 'Engineering')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO departments VALUES (2, 'Sales')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO departments VALUES (3, 'Marketing')");
        defer result.deinit();
    }

    // Insert employees (Marketing has no employees)
    {
        var result = try db.execute("INSERT INTO employees VALUES (1, 1, 'Alice')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (2, 1, 'Bob')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO employees VALUES (3, 2, 'Charlie')");
        defer result.deinit();
    }

    // Insert projects (Charlie has no projects)
    {
        var result = try db.execute("INSERT INTO projects VALUES (1, 1, 'Project Alpha')");
        defer result.deinit();
    }
    {
        var result = try db.execute("INSERT INTO projects VALUES (2, 2, 'Project Beta')");
        defer result.deinit();
    }

    // Test LEFT then LEFT
    var result = try db.execute(
        \\SELECT departments.name, employees.name, projects.name
        \\FROM departments
        \\LEFT JOIN employees ON departments.id = employees.dept_id
        \\LEFT JOIN projects ON employees.id = projects.employee_id
    );
    defer result.deinit();

    // Should include all departments
    try expectEqual(@as(usize, 4), result.rows.items.len);
}
