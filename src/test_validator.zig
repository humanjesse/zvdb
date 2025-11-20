const std = @import("std");
const testing = std.testing;
const validator = @import("database/validator.zig");
const Table = @import("table.zig").Table;
const Database = @import("database/core.zig").Database;

test "validator: numeric literal detection" {
    try testing.expect(validator.isNumericLiteral("42"));
    try testing.expect(validator.isNumericLiteral("3.14"));
    try testing.expect(!validator.isNumericLiteral("name"));
    try testing.expect(!validator.isNumericLiteral("COUNT(*)"));
}

test "validator: validation context setup" {
    var ctx = validator.ValidationContext.init(testing.allocator, .select);
    defer ctx.deinit();

    try ctx.addAlias("u", "users");
    try testing.expectEqualStrings("users", ctx.resolveAlias("u").?);
}

test "validator: error message formatting" {
    const msg = try validator.formatErrorMessage(
        testing.allocator,
        validator.ValidationError.ColumnNotFound,
        "invalid_column",
    );
    defer testing.allocator.free(msg);

    try testing.expect(std.mem.indexOf(u8, msg, "invalid_column") != null);
}

test "validator: aggregate in WHERE clause detection" {
    // This would need a full expression tree - just testing the basic logic
    var ctx = validator.ValidationContext.init(testing.allocator, .where);
    defer ctx.deinit();

    // In WHERE scope, aggregates should not be allowed
    try testing.expectEqual(validator.ValidationScope.where, ctx.scope);
}

test "validator: validation with actual database" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create a table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert some data
    {
        var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Valid query - should work
    {
        var result = try db.execute("SELECT name FROM users WHERE id = 1");
        defer result.deinit();
        try testing.expectEqual(@as(usize, 1), result.rows.items.len);
    }
}

// ============================================================================
// Error Case Tests - ColumnNotFound
// ============================================================================

test "validator error: SELECT non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try to select a column that doesn't exist
    const result = db.execute("SELECT invalid_column FROM users");
    try testing.expectError(error.ColumnNotFound, result);
}

test "validator error: WHERE clause with non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // WHERE with non-existent column
    const result = db.execute("SELECT name FROM users WHERE invalid_col = 1");
    try testing.expectError(error.ColumnNotFound, result);
}

test "validator error: ORDER BY non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // ORDER BY with non-existent column
    const result = db.execute("SELECT name FROM users ORDER BY invalid_col");
    try testing.expectError(error.ColumnNotFound, result);
}

// ============================================================================
// Error Case Tests - TableNotFound
// ============================================================================

test "validator error: SELECT from non-existent table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Try to select from a table that doesn't exist
    const result = db.execute("SELECT * FROM nonexistent_table");
    try testing.expectError(error.TableNotFound, result);
}

test "validator error: JOIN with non-existent table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int)");
        defer result.deinit();
    }

    // JOIN with non-existent table
    const result = db.execute("SELECT * FROM users JOIN nonexistent ON users.id = nonexistent.id");
    try testing.expectError(error.TableNotFound, result);
}

// ============================================================================
// Error Case Tests - AmbiguousColumn
// ============================================================================

test "validator error: ambiguous column in JOIN without qualification" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }
    {
        var result = try db.execute("CREATE TABLE orders (id int, user_id int)");
        defer result.deinit();
    }

    // Both tables have 'id' column, selecting it without qualification should error
    const result = db.execute("SELECT id FROM users JOIN orders ON users.id = orders.user_id");
    // Note: This should error with AmbiguousColumn, but implementation may vary
    // If it doesn't error, that's a bug that should be fixed
    try testing.expectError(error.AmbiguousColumn, result);
}

// ============================================================================
// Error Case Tests - AggregateInWhere (if implemented)
// ============================================================================

test "validator error: aggregate function in WHERE clause" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE sales (id int, amount int)");
        defer result.deinit();
    }

    // Aggregate functions are not allowed in WHERE clause
    const result = db.execute("SELECT id FROM sales WHERE COUNT(*) > 5");
    // Note: This should error with AggregateInWhere or similar
    // If this test fails, it means the validation is not catching this error
    try testing.expectError(error.AggregateInWhere, result);
}

test "validator error: SUM in WHERE clause" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    {
        var result = try db.execute("CREATE TABLE products (id int, price int)");
        defer result.deinit();
    }

    // SUM is not allowed in WHERE clause (use HAVING instead)
    const result = db.execute("SELECT id FROM products WHERE SUM(price) > 100");
    try testing.expectError(error.AggregateInWhere, result);
}
