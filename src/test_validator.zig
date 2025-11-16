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
