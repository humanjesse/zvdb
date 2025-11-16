const std = @import("std");
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const Database = @import("database/core.zig").Database;
const sql = @import("sql.zig");
const ColumnValue = @import("table.zig").ColumnValue;

// ============================================================================
// Test Fixtures - Reusable test data setup
// ============================================================================

/// Helper to execute and cleanup query results that we don't need to inspect
fn executeAndCleanup(db: *Database, query: []const u8) !void {
    var result = try db.execute(query);
    result.deinit();
}

/// Setup basic users and orders tables for subquery tests
fn setupBasicTables(db: *Database) !void {
    // Users table
    try executeAndCleanup(db, "CREATE TABLE users (id int, name text, age int)");
    try executeAndCleanup(db, "INSERT INTO users VALUES (1, 'Alice', 25)");
    try executeAndCleanup(db, "INSERT INTO users VALUES (2, 'Bob', 30)");
    try executeAndCleanup(db, "INSERT INTO users VALUES (3, 'Charlie', 35)");

    // Orders table
    try executeAndCleanup(db, "CREATE TABLE orders (id int, user_id int, total float)");
    try executeAndCleanup(db, "INSERT INTO orders VALUES (1, 1, 100.0)");
    try executeAndCleanup(db, "INSERT INTO orders VALUES (2, 1, 200.0)");
    try executeAndCleanup(db, "INSERT INTO orders VALUES (3, 2, 50.0)");
}

/// Setup products table for scalar subquery tests
fn setupProductTables(db: *Database) !void {
    try executeAndCleanup(db, "CREATE TABLE products (id int, name text, price float, category text)");
    try executeAndCleanup(db, "INSERT INTO products VALUES (1, 'Widget', 10.0, 'tools')");
    try executeAndCleanup(db, "INSERT INTO products VALUES (2, 'Gadget', 20.0, 'electronics')");
    try executeAndCleanup(db, "INSERT INTO products VALUES (3, 'Doohickey', 30.0, 'tools')");
}

// ============================================================================
// Category 1: Parser Tests
// ============================================================================

test "parser: IN with subquery" {
    const allocator = std.testing.allocator;
    const query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    // Verify it's a SELECT command
    try expect(cmd == .select);

    // Verify WHERE expression exists
    const select_cmd = cmd.select;
    try expect(select_cmd.where_expr != null);

    // WHERE expr should be a binary expression with IN operator
    const where_expr = select_cmd.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .in_op);

    // Right side should be a subquery
    try expect(where_expr.binary.right == .subquery);
}

test "parser: NOT IN with subquery" {
    const allocator = std.testing.allocator;
    const query = "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)";

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    try expect(cmd == .select);
    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .not_in_op);
    try expect(where_expr.binary.right == .subquery);
}

test "parser: EXISTS with subquery" {
    const allocator = std.testing.allocator;
    const query = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 1)";

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    try expect(cmd == .select);
    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .exists_op);
    try expect(where_expr.binary.right == .subquery);
}

test "parser: NOT EXISTS with subquery" {
    const allocator = std.testing.allocator;
    const query = "SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE user_id = 1)";

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    try expect(cmd == .select);
    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .not_exists_op);
    try expect(where_expr.binary.right == .subquery);
}

test "parser: scalar subquery in comparison" {
    const allocator = std.testing.allocator;
    const query = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)";

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    try expect(cmd == .select);
    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .gt);
    try expect(where_expr.binary.right == .subquery);
}

// ============================================================================
// Category 2: IN Operator Tests
// ============================================================================

test "subquery: IN operator - basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users who have orders
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    // Should return users 1 and 2 (Alice and Bob have orders)
    try expectEqual(@as(usize, 2), result.rows.items.len);

    // Verify we got the right users
    var found_alice = false;
    var found_bob = false;
    for (result.rows.items) |row| {
        if (row.items.len > 1 and row.items[1] == .text) {
            if (std.mem.eql(u8, row.items[1].text, "Alice")) {
                found_alice = true;
            } else if (std.mem.eql(u8, row.items[1].text, "Bob")) {
                found_bob = true;
            }
        }
    }
    try expect(found_alice);
    try expect(found_bob);
}

test "subquery: IN operator - no matches" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Setup users who DON'T have orders
    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (4, 'David')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (5, 'Eve')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 2)");

    // Query: Should return 0 rows
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: IN operator - all match" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 2)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (3, 3)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (4, 4)");

    // Query: All users have orders
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
}

test "subquery: IN operator - with WHERE in subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users who have orders > 100
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)");
    defer result.deinit();

    // Only Alice (id=1) has an order with total=200 > 100
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int);
}

test "subquery: IN operator - empty subquery result" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");

    // Empty orders table
    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");

    // Query: Should return 0 rows (no orders exist)
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: IN operator - with text columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE customers (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO customers VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO customers VALUES (2, 'Bob')");
    try executeAndCleanup(&db, "INSERT INTO customers VALUES (3, 'Charlie')");

    try executeAndCleanup(&db, "CREATE TABLE vip_customers (customer_name text)");
    try executeAndCleanup(&db, "INSERT INTO vip_customers VALUES ('Alice')");
    try executeAndCleanup(&db, "INSERT INTO vip_customers VALUES ('Charlie')");

    // Query: Find customers who are VIPs
    var result = try db.execute("SELECT * FROM customers WHERE name IN (SELECT customer_name FROM vip_customers)");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
}

// ============================================================================
// Category 3: NOT IN Operator Tests
// ============================================================================

test "subquery: NOT IN operator - basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users who DON'T have orders
    var result = try db.execute("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)");
    defer result.deinit();

    // Only Charlie (id=3) has no orders
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 3), result.rows.items[0].items[0].int);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Charlie"));
}

test "subquery: NOT IN operator - all excluded" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 2)");

    // Query: All users have orders, so NOT IN returns nothing
    var result = try db.execute("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: NOT IN operator - none excluded" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");

    // Empty orders table
    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");

    // Query: No users have orders, so NOT IN returns all
    var result = try db.execute("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
}

test "subquery: NOT IN operator - with WHERE in subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users who DON'T have high-value orders
    var result = try db.execute("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders WHERE total > 100)");
    defer result.deinit();

    // Alice has order with total=200 > 100, so she's excluded
    // Bob and Charlie should be returned
    try expectEqual(@as(usize, 2), result.rows.items.len);
}

// ============================================================================
// Category 4: EXISTS Tests
// ============================================================================

test "subquery: EXISTS - basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users who have at least one order
    // Note: This is a simplified uncorrelated EXISTS for now
    // We'll test correlation when we support it
    var result = try db.execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 1)");
    defer result.deinit();

    // Since the EXISTS subquery returns rows (user 1 has orders),
    // all users should be returned (uncorrelated)
    try expectEqual(@as(usize, 3), result.rows.items.len);
}

test "subquery: EXISTS - none exist" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");

    // Empty orders table
    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");

    // Query: EXISTS with empty table
    var result = try db.execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders)");
    defer result.deinit();

    // No orders exist, so EXISTS returns false for all rows
    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: EXISTS - with WHERE condition" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Check if any high-value orders exist
    var result = try db.execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE total > 150)");
    defer result.deinit();

    // Order with total=200 exists, so all users returned (uncorrelated)
    try expectEqual(@as(usize, 3), result.rows.items.len);
}

test "subquery: NOT EXISTS - basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");

    // Empty orders table
    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");

    // Query: NOT EXISTS with empty table
    var result = try db.execute("SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders)");
    defer result.deinit();

    // No orders exist, so NOT EXISTS returns true
    try expectEqual(@as(usize, 1), result.rows.items.len);
}

// ============================================================================
// Category 5: Scalar Subquery Tests
// ============================================================================

test "subquery: scalar comparison - greater than AVG" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupProductTables(&db);

    // Query: Find products more expensive than average
    // Average = (10 + 20 + 30) / 3 = 20
    var result = try db.execute("SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)");
    defer result.deinit();

    // Only Doohickey (price=30) is > 20
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Doohickey"));
}

test "subquery: scalar comparison - equals MAX" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupProductTables(&db);

    // Query: Find most expensive product
    var result = try db.execute("SELECT * FROM products WHERE price = (SELECT MAX(price) FROM products)");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Doohickey"));
}

test "subquery: scalar comparison - less than MIN" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupProductTables(&db);

    // Query: Find products cheaper than minimum (impossible)
    var result = try db.execute("SELECT * FROM products WHERE price < (SELECT MIN(price) FROM products)");
    defer result.deinit();

    // Nothing is cheaper than the cheapest
    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: scalar subquery - empty result returns NULL" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE items (id int, price float)");
    try executeAndCleanup(&db, "INSERT INTO items VALUES (1, 15.0)");

    // Empty products table
    try executeAndCleanup(&db, "CREATE TABLE products (id int, price float)");

    // Query: Compare with AVG of empty table (returns NULL)
    var result = try db.execute("SELECT * FROM items WHERE price > (SELECT AVG(price) FROM products)");
    defer result.deinit();

    // Comparison with NULL returns no rows
    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: scalar comparison - with COUNT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: Find users where total order count > 2
    // This is a simple uncorrelated scalar subquery
    var result = try db.execute("SELECT * FROM users WHERE (SELECT COUNT(*) FROM orders) > 2");
    defer result.deinit();

    // COUNT(*) = 3, so 3 > 2 is true, all users returned
    try expectEqual(@as(usize, 3), result.rows.items.len);
}

// ============================================================================
// Category 6: Edge Case Tests
// ============================================================================

test "subquery: duplicate values in subquery result" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (3, 'Charlie')");

    // Orders with duplicate user_ids
    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (3, 2)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (4, 2)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (5, 3)");

    // Query: Duplicates shouldn't affect IN matching
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 3), result.rows.items.len);
}

test "subquery: subquery with LIMIT" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: IN with limited subquery results
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders LIMIT 2)");
    defer result.deinit();

    // LIMIT 2 on orders should return at most 2 user_ids
    try expect(result.rows.items.len <= 2);
}

test "subquery: subquery with ORDER BY" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupBasicTables(&db);

    // Query: ORDER BY in subquery shouldn't affect IN matching
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders ORDER BY total DESC)");
    defer result.deinit();

    // Should still match users 1 and 2
    try expectEqual(@as(usize, 2), result.rows.items.len);
}

test "subquery: empty outer table with subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Empty users table
    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");

    // Query: Empty outer table
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)");
    defer result.deinit();

    try expectEqual(@as(usize, 0), result.rows.items.len);
}

test "subquery: IN with multiple matching values" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (3, 'Charlie')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (4, 'David')");

    try executeAndCleanup(&db, "CREATE TABLE selected (id int)");
    try executeAndCleanup(&db, "INSERT INTO selected VALUES (2)");
    try executeAndCleanup(&db, "INSERT INTO selected VALUES (3)");
    try executeAndCleanup(&db, "INSERT INTO selected VALUES (4)");

    // Query: Multiple matches
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT id FROM selected)");
    defer result.deinit();

    try expectEqual(@as(usize, 3), result.rows.items.len);
}

test "subquery: scalar subquery with different data types" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE stats (value int)");
    try executeAndCleanup(&db, "INSERT INTO stats VALUES (5)");
    try executeAndCleanup(&db, "INSERT INTO stats VALUES (10)");
    try executeAndCleanup(&db, "INSERT INTO stats VALUES (15)");

    try executeAndCleanup(&db, "CREATE TABLE items (id int, quantity int)");
    try executeAndCleanup(&db, "INSERT INTO items VALUES (1, 8)");
    try executeAndCleanup(&db, "INSERT INTO items VALUES (2, 12)");

    // Query: Compare int column with AVG (returns float but should work)
    var result = try db.execute("SELECT * FROM items WHERE quantity > (SELECT AVG(value) FROM stats)");
    defer result.deinit();

    // AVG = 10, so item with quantity=12 should match
    try expectEqual(@as(usize, 1), result.rows.items.len);
}

// ============================================================================
// Category 7: Integration Tests
// ============================================================================

test "subquery: with aggregate functions in outer query" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE employees (id int, name text, dept text, salary int)");
    try executeAndCleanup(&db, "INSERT INTO employees VALUES (1, 'Alice', 'Engineering', 80000)");
    try executeAndCleanup(&db, "INSERT INTO employees VALUES (2, 'Bob', 'Engineering', 90000)");
    try executeAndCleanup(&db, "INSERT INTO employees VALUES (3, 'Charlie', 'Sales', 70000)");
    try executeAndCleanup(&db, "INSERT INTO employees VALUES (4, 'David', 'Sales', 75000)");

    // Query: Count employees above average salary, grouped by dept
    var result = try db.execute("SELECT dept, COUNT(*) FROM employees WHERE salary > (SELECT AVG(salary) FROM employees) GROUP BY dept");
    defer result.deinit();

    // AVG salary = 78750, so Alice (80k) and Bob (90k) are above average
    // Both are in Engineering dept
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Engineering"));
}

test "subquery: with JOIN in outer query" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (3, 'Charlie')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int, total float)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1, 100.0)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 2, 50.0)");

    try executeAndCleanup(&db, "CREATE TABLE vip_list (user_id int)");
    try executeAndCleanup(&db, "INSERT INTO vip_list VALUES (1)");

    // Query: JOIN with subquery filter
    var result = try db.execute("SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id IN (SELECT user_id FROM vip_list)");
    defer result.deinit();

    // Only Alice (id=1) is in VIP list and has an order
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Alice"));
}

test "subquery: multiple subqueries in WHERE" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text, age int)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice', 25)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob', 30)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (3, 'Charlie', 35)");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (2, 2)");

    try executeAndCleanup(&db, "CREATE TABLE age_stats (avg_age int)");
    try executeAndCleanup(&db, "INSERT INTO age_stats VALUES (28)");

    // Query: Multiple subqueries with AND
    var result = try db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM orders) AND age > (SELECT avg_age FROM age_stats)");
    defer result.deinit();

    // Alice (25) and Bob (30) have orders, but only Bob is > 28
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Bob"));
}

test "subquery: with GROUP BY and aggregates" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE sales (id int, product text, amount float, region text)");
    try executeAndCleanup(&db, "INSERT INTO sales VALUES (1, 'Widget', 100.0, 'North')");
    try executeAndCleanup(&db, "INSERT INTO sales VALUES (2, 'Widget', 150.0, 'South')");
    try executeAndCleanup(&db, "INSERT INTO sales VALUES (3, 'Gadget', 200.0, 'North')");
    try executeAndCleanup(&db, "INSERT INTO sales VALUES (4, 'Gadget', 180.0, 'South')");

    // Query: Group by region, filter by above-average amount
    var result = try db.execute("SELECT region, COUNT(*) FROM sales WHERE amount > (SELECT AVG(amount) FROM sales) GROUP BY region");
    defer result.deinit();

    // AVG = 157.5, so Widget-South (150) is excluded
    // Gadget-North (200) and Gadget-South (180) are included
    try expectEqual(@as(usize, 2), result.rows.items.len);
}

test "subquery: in UPDATE statement" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text, status text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice', 'regular')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (2, 'Bob', 'regular')");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (3, 'Charlie', 'regular')");

    try executeAndCleanup(&db, "CREATE TABLE high_value_orders (user_id int)");
    try executeAndCleanup(&db, "INSERT INTO high_value_orders VALUES (1)");
    try executeAndCleanup(&db, "INSERT INTO high_value_orders VALUES (2)");

    // Update users to premium if they have high value orders
    try executeAndCleanup(&db, "UPDATE users SET status = 'premium' WHERE id IN (SELECT user_id FROM high_value_orders)");

    // Verify the update
    var result = try db.execute("SELECT * FROM users WHERE status = 'premium'");
    defer result.deinit();

    try expectEqual(@as(usize, 2), result.rows.items.len);
}

// ============================================================================
// Category 8: Nested Subquery Tests
// ============================================================================

test "subquery: nested IN subqueries - 2 levels" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE a (a_id int)");
    try executeAndCleanup(&db, "INSERT INTO a VALUES (1)");
    try executeAndCleanup(&db, "INSERT INTO a VALUES (2)");
    try executeAndCleanup(&db, "INSERT INTO a VALUES (3)");

    try executeAndCleanup(&db, "CREATE TABLE b (b_id int)");
    try executeAndCleanup(&db, "INSERT INTO b VALUES (1)");
    try executeAndCleanup(&db, "INSERT INTO b VALUES (2)");

    try executeAndCleanup(&db, "CREATE TABLE c (c_id int)");
    try executeAndCleanup(&db, "INSERT INTO c VALUES (1)");

    // Query: Nested IN - a_id IN (b_id IN c_id)
    var result = try db.execute("SELECT * FROM a WHERE a_id IN (SELECT b_id FROM b WHERE b_id IN (SELECT c_id FROM c))");
    defer result.deinit();

    // Only a_id=1 matches (it's in b, and b_id=1 is in c)
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int);
}

test "subquery: nested IN subqueries - 3 levels" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE t1 (id int)");
    try executeAndCleanup(&db, "INSERT INTO t1 VALUES (1)");
    try executeAndCleanup(&db, "INSERT INTO t1 VALUES (2)");

    try executeAndCleanup(&db, "CREATE TABLE t2 (id int)");
    try executeAndCleanup(&db, "INSERT INTO t2 VALUES (1)");

    try executeAndCleanup(&db, "CREATE TABLE t3 (id int)");
    try executeAndCleanup(&db, "INSERT INTO t3 VALUES (1)");

    try executeAndCleanup(&db, "CREATE TABLE t4 (id int)");
    try executeAndCleanup(&db, "INSERT INTO t4 VALUES (1)");

    // Query: 3-level nested IN
    var result = try db.execute("SELECT * FROM t1 WHERE id IN (SELECT id FROM t2 WHERE id IN (SELECT id FROM t3 WHERE id IN (SELECT id FROM t4)))");
    defer result.deinit();

    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expectEqual(@as(i64, 1), result.rows.items[0].items[0].int);
}

test "subquery: scalar subquery containing subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE products (id int, name text, price float, category_id int)");
    try executeAndCleanup(&db, "INSERT INTO products VALUES (1, 'Widget', 10.0, 1)");
    try executeAndCleanup(&db, "INSERT INTO products VALUES (2, 'Gadget', 20.0, 2)");
    try executeAndCleanup(&db, "INSERT INTO products VALUES (3, 'Premium Widget', 100.0, 1)");

    try executeAndCleanup(&db, "CREATE TABLE premium_categories (id int)");
    try executeAndCleanup(&db, "INSERT INTO premium_categories VALUES (1)");

    // Query: Scalar subquery with nested IN
    var result = try db.execute("SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products WHERE category_id IN (SELECT id FROM premium_categories))");
    defer result.deinit();

    // AVG of premium category (1) products = (10 + 100) / 2 = 55
    // Only Premium Widget (100) is > 55
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Premium Widget"));
}

test "subquery: EXISTS with nested subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");

    try executeAndCleanup(&db, "CREATE TABLE shipped_orders (order_id int)");
    try executeAndCleanup(&db, "INSERT INTO shipped_orders VALUES (1)");

    // Query: EXISTS with nested IN
    var result = try db.execute("SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = 1 AND id IN (SELECT order_id FROM shipped_orders))");
    defer result.deinit();

    // User 1 has order 1, and order 1 is shipped
    try expectEqual(@as(usize, 1), result.rows.items.len);
}

// ============================================================================
// Category 9: Error Handling Tests
// ============================================================================

test "subquery: error - scalar subquery returns multiple rows" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try setupProductTables(&db);

    // Query: Scalar subquery that returns multiple values
    const result = db.execute("SELECT * FROM products WHERE price > (SELECT price FROM products)");

    // Should return an error because scalar subquery returns 3 rows
    try expectError(error.SubqueryReturnedMultipleRows, result);
}

test "subquery: error - table not found in subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");

    // Query: Subquery references non-existent table
    const result = db.execute("SELECT * FROM users WHERE id IN (SELECT user_id FROM nonexistent_table)");

    try expectError(error.TableNotFound, result);
}

test "subquery: error - column not found in subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");
    try executeAndCleanup(&db, "INSERT INTO users VALUES (1, 'Alice')");

    try executeAndCleanup(&db, "CREATE TABLE orders (id int, user_id int)");
    try executeAndCleanup(&db, "INSERT INTO orders VALUES (1, 1)");

    // Query: Subquery references non-existent column
    const result = db.execute("SELECT * FROM users WHERE id IN (SELECT nonexistent_column FROM orders)");

    try expectError(error.ColumnNotFound, result);
}

test "subquery: error - invalid syntax" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    try executeAndCleanup(&db, "CREATE TABLE users (id int, name text)");

    // Query: Missing closing parenthesis
    const result = db.execute("SELECT * FROM users WHERE id IN (SELECT id FROM users");

    // Should return a parse error
    try expect(std.meta.isError(result));
}
