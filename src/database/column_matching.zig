// column_matching.zig
// ============================================================================
// Column Name Matching Utilities
// ============================================================================
//
// Simple helper functions for column name resolution in hash maps.
// Extracts duplicate logic from executor.zig for DRY principles.
//
// For more sophisticated multi-table validation, see column_resolver.zig
// ============================================================================

const std = @import("std");

/// Extract the unqualified column part from a potentially qualified column name
/// Examples:
///   "users.name" → "name"
///   "u.id" → "id"
///   "name" → "name" (returns the full name if no dot found)
pub fn extractColumnPart(column_name: []const u8) []const u8 {
    if (std.mem.indexOf(u8, column_name, ".")) |dot_idx| {
        return column_name[dot_idx + 1 ..];
    }
    return column_name;
}

/// Check if two column names match, handling qualified vs unqualified names
///
/// Matching Strategy (multi-phase resolution):
/// 1. Exact match: "name" == "name" ✓
/// 2. One qualified, one not, same column part: "users.name" == "name" ✓
/// 3. Both qualified, DIFFERENT tables: "users.id" != "orders.id" ✗
pub fn matchColumnName(col1: []const u8, col2: []const u8) bool {
    // Phase 1: Try exact match first (fast path)
    if (std.mem.eql(u8, col1, col2)) {
        return true;
    }

    // Check if both are qualified (have dots)
    const col1_has_dot = std.mem.indexOf(u8, col1, ".") != null;
    const col2_has_dot = std.mem.indexOf(u8, col2, ".") != null;

    // If both are qualified and not exactly equal, they don't match
    // (e.g., "users.id" vs "orders.id" should not match)
    if (col1_has_dot and col2_has_dot) {
        return false;
    }

    // Phase 2: One is qualified, one is not - match by column part
    const col1_part = extractColumnPart(col1);
    const col2_part = extractColumnPart(col2);

    return std.mem.eql(u8, col1_part, col2_part);
}

/// Resolve a column name from a StringHashMap of column values
///
/// Resolution Strategy (prioritized fallbacks):
/// 1. Exact match: Try to find the column name as-is
/// 2. Unqualified lookup: If column is qualified (has dot), try just the column part
/// 3. Qualified scan: If still not found, scan all entries for matching column part
/// 4. Return null if not found
///
/// This handles cases like:
/// - Looking for "u.name" when map has "users.name"
/// - Looking for "id" when map has "users.id" and "orders.id" (returns first match)
/// - Looking for "users.name" when map has "name" (unqualified)
pub fn resolveColumnValue(
    column_name: []const u8,
    column_map: anytype,
) @TypeOf(column_map.get("")) {
    // Phase 1: Try exact match (most common case, fastest)
    if (column_map.get(column_name)) |value| {
        return value;
    }

    // Check if the column name is qualified (contains a dot)
    if (std.mem.indexOf(u8, column_name, ".")) |dot_idx| {
        const col_part = column_name[dot_idx + 1 ..];

        // Phase 2: Try unqualified column name directly
        // This is important because evaluateWhereOnJoinedRow adds unqualified names
        // with the FIRST occurrence's value, which gives us the correct precedence
        if (column_map.get(col_part)) |value| {
            return value;
        }

        // Phase 3: Scan all entries for qualified columns with matching column part
        // This handles edge cases where unqualified name wasn't added due to conflicts
        var it = column_map.iterator();
        while (it.next()) |entry| {
            if (std.mem.indexOf(u8, entry.key_ptr.*, ".")) |entry_dot_idx| {
                const entry_col_part = entry.key_ptr.*[entry_dot_idx + 1 ..];
                if (std.mem.eql(u8, entry_col_part, col_part)) {
                    return entry.value_ptr.*;
                }
            }
        }
    }

    // Not found in any phase
    return null;
}

/// Find the index of a column in an array of column names
///
/// This is similar to resolveColumnValue but works with arrays instead of maps,
/// and returns the index rather than the value.
pub fn findColumnIndex(
    column_name: []const u8,
    available_columns: []const []const u8,
) ?usize {
    // Phase 1: Try exact match
    for (available_columns, 0..) |avail_col, idx| {
        if (std.mem.eql(u8, avail_col, column_name)) {
            return idx;
        }
    }

    // Phase 2: Try matching by column part
    // This handles both cases:
    // - Looking for qualified "users.id" in unqualified ["id", "name"]
    // - Looking for unqualified "id" in qualified ["users.id", "users.name"]
    const col_part = extractColumnPart(column_name);

    for (available_columns, 0..) |avail_col, idx| {
        const avail_part = extractColumnPart(avail_col);
        if (std.mem.eql(u8, avail_part, col_part)) {
            return idx;
        }
    }

    return null;
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;
const ColumnValue = @import("../table.zig").ColumnValue;
const StringHashMap = std.StringHashMap;

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
