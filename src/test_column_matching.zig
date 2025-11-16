const std = @import("std");
const testing = std.testing;
const column_matching = @import("database/column_matching.zig");
const ColumnValue = @import("table.zig").ColumnValue;
const StringHashMap = std.StringHashMap;

// Import the functions we're testing
const extractColumnPart = column_matching.extractColumnPart;
const matchColumnName = column_matching.matchColumnName;
const resolveColumnValue = column_matching.resolveColumnValue;
const findColumnIndex = column_matching.findColumnIndex;

test "extractColumnPart: basic extraction" {
    try testing.expectEqualStrings("name", extractColumnPart("users.name"));
    try testing.expectEqualStrings("id", extractColumnPart("u.id"));
    try testing.expectEqualStrings("price", extractColumnPart("products.price"));
}

test "extractColumnPart: unqualified names" {
    try testing.expectEqualStrings("name", extractColumnPart("name"));
    try testing.expectEqualStrings("id", extractColumnPart("id"));
}

test "matchColumnName: exact matches" {
    try testing.expect(matchColumnName("name", "name"));
    try testing.expect(matchColumnName("users.id", "users.id"));
    try testing.expect(matchColumnName("u.name", "u.name"));
}

test "matchColumnName: qualified vs unqualified" {
    try testing.expect(matchColumnName("users.name", "name"));
    try testing.expect(matchColumnName("name", "users.name"));
    try testing.expect(matchColumnName("u.id", "id"));
}

test "matchColumnName: different columns" {
    try testing.expect(!matchColumnName("name", "id"));
    try testing.expect(!matchColumnName("users.name", "users.id"));
    // Two qualified columns with different prefixes don't match via matchColumnName
    // Use resolveColumnValue for complex resolution
    try testing.expect(!matchColumnName("users.id", "orders.id"));
    try testing.expect(!matchColumnName("users.name", "u.name"));
}

test "resolveColumnValue: exact match" {
    var map = StringHashMap(ColumnValue).init(testing.allocator);
    defer map.deinit();

    try map.put("name", ColumnValue{ .text = "Alice" });
    try map.put("id", ColumnValue{ .int = 42 });

    const result1 = resolveColumnValue("name", map);
    try testing.expect(result1 != null);
    try testing.expectEqualStrings("Alice", result1.?.text);

    const result2 = resolveColumnValue("id", map);
    try testing.expect(result2 != null);
    try testing.expectEqual(@as(i64, 42), result2.?.int);
}

test "resolveColumnValue: qualified to unqualified" {
    var map = StringHashMap(ColumnValue).init(testing.allocator);
    defer map.deinit();

    // Map has unqualified names
    try map.put("name", ColumnValue{ .text = "Bob" });
    try map.put("id", ColumnValue{ .int = 123 });

    // Looking for qualified names should find unqualified ones
    const result1 = resolveColumnValue("users.name", map);
    try testing.expect(result1 != null);
    try testing.expectEqualStrings("Bob", result1.?.text);

    const result2 = resolveColumnValue("u.id", map);
    try testing.expect(result2 != null);
    try testing.expectEqual(@as(i64, 123), result2.?.int);
}

test "resolveColumnValue: alias mismatch" {
    var map = StringHashMap(ColumnValue).init(testing.allocator);
    defer map.deinit();

    // Map has qualified names with actual table name
    try map.put("users.name", ColumnValue{ .text = "Charlie" });
    try map.put("users.id", ColumnValue{ .int = 456 });

    // Looking for aliased names should find actual table names
    const result1 = resolveColumnValue("u.name", map);
    try testing.expect(result1 != null);
    try testing.expectEqualStrings("Charlie", result1.?.text);

    const result2 = resolveColumnValue("u.id", map);
    try testing.expect(result2 != null);
    try testing.expectEqual(@as(i64, 456), result2.?.int);
}

test "resolveColumnValue: not found" {
    var map = StringHashMap(ColumnValue).init(testing.allocator);
    defer map.deinit();

    try map.put("name", ColumnValue{ .text = "Test" });

    const result = resolveColumnValue("invalid_column", map);
    try testing.expect(result == null);
}

test "resolveColumnValue: precedence - unqualified wins" {
    var map = StringHashMap(ColumnValue).init(testing.allocator);
    defer map.deinit();

    // Map has both qualified and unqualified versions
    // This tests that unqualified has precedence (Phase 2 before Phase 3)
    try map.put("name", ColumnValue{ .text = "Unqualified" });
    try map.put("users.name", ColumnValue{ .text = "Qualified" });

    // Looking for "u.name" should find "name" (unqualified) first
    const result = resolveColumnValue("u.name", map);
    try testing.expect(result != null);
    try testing.expectEqualStrings("Unqualified", result.?.text);
}

test "findColumnIndex: exact match" {
    const columns = [_][]const u8{ "id", "name", "email" };

    try testing.expectEqual(@as(?usize, 0), findColumnIndex("id", &columns));
    try testing.expectEqual(@as(?usize, 1), findColumnIndex("name", &columns));
    try testing.expectEqual(@as(?usize, 2), findColumnIndex("email", &columns));
}

test "findColumnIndex: qualified vs unqualified" {
    const columns = [_][]const u8{ "users.id", "users.name", "users.email" };

    try testing.expectEqual(@as(?usize, 0), findColumnIndex("id", &columns));
    try testing.expectEqual(@as(?usize, 1), findColumnIndex("name", &columns));
    try testing.expectEqual(@as(?usize, 2), findColumnIndex("email", &columns));
}

test "findColumnIndex: alias mismatch" {
    const columns = [_][]const u8{ "users.id", "users.name" };

    try testing.expectEqual(@as(?usize, 0), findColumnIndex("u.id", &columns));
    try testing.expectEqual(@as(?usize, 1), findColumnIndex("u.name", &columns));
}

test "findColumnIndex: not found" {
    const columns = [_][]const u8{ "id", "name" };

    try testing.expectEqual(@as(?usize, null), findColumnIndex("invalid", &columns));
}
