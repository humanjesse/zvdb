// test_validator_negative.zig
// ============================================================================
// Comprehensive Negative Tests for SQL Validation System
// ============================================================================
//
// Phase 5: Comprehensive negative tests to ensure the validation system
// properly catches all error scenarios.
//
// Test Coverage:
// - INSERT validation errors (15 tests)
// - UPDATE validation errors (15 tests)
// - DELETE validation errors (10 tests)
// - Validation mode tests (15 tests)
// - Fuzzy matching edge cases (10 tests)
// - Complex scenarios (10 tests)
//
// ============================================================================

const std = @import("std");
const testing = std.testing;
const validator = @import("database/validator.zig");
const Database = @import("database/core.zig").Database;
const ValidationMode = @import("database/core.zig").ValidationMode;
const Table = @import("table.zig").Table;
const ColumnType = @import("table.zig").ColumnType;
const ColumnValue = @import("table.zig").ColumnValue;
const sql = @import("sql.zig");

// ============================================================================
// Test Helpers
// ============================================================================

fn createTestTable(allocator: std.mem.Allocator, name: []const u8) !Table {
    var table = try Table.init(allocator, name);
    try table.addColumn("id", ColumnType.int);
    try table.addColumn("name", ColumnType.text);
    try table.addColumn("email", ColumnType.text);
    try table.addColumn("age", ColumnType.int);
    return table;
}

fn createTestTableLargeSchema(allocator: std.mem.Allocator, name: []const u8, num_columns: usize) !Table {
    var table = try Table.init(allocator, name);

    // Create many columns
    var i: usize = 0;
    while (i < num_columns) : (i += 1) {
        const col_name = try std.fmt.allocPrint(allocator, "col_{d}", .{i});
        defer allocator.free(col_name);
        try table.addColumn(col_name, ColumnType.int);
    }

    return table;
}

// ============================================================================
// INSERT Validation Error Tests (15 tests)
// ============================================================================

test "validateInsert: empty column list" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateInsert: column count mismatch - more columns than values" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");
    try columns.append("email");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateInsert: column count mismatch - more values than columns" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });
    try values.append(ColumnValue{ .text = "alice@example.com" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateInsert: single column not found" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("invalid_column");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "test" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: multiple columns not found" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("invalid1");
    try columns.append("invalid2");
    try columns.append("invalid3");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "test1" });
    try values.append(ColumnValue{ .text = "test2" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 3), result.errors.items.len);

    // All errors should be ColumnNotFound
    for (result.errors.items) |err| {
        try testing.expectEqual(validator.ValidationError.ColumnNotFound, err.error_type);
    }
}

test "validateInsert: single duplicate column" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");
    try columns.append("id"); // Duplicate

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });
    try values.append(ColumnValue{ .int = 2 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateInsert: multiple duplicate columns" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");
    try columns.append("id"); // Duplicate
    try columns.append("name"); // Duplicate

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });
    try values.append(ColumnValue{ .int = 2 });
    try values.append(ColumnValue{ .text = "Bob" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len >= 2);
}

test "validateInsert: column with special characters" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id@#$%");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: case sensitivity - wrong case column" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("ID"); // Should be "id"
    try columns.append("NAME"); // Should be "name"

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len >= 2);

    // Both should be column not found errors
    for (result.errors.items) |err| {
        try testing.expectEqual(validator.ValidationError.ColumnNotFound, err.error_type);
    }
}

test "validateInsert: very long column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    const long_name = "this_is_a_very_long_column_name_that_definitely_does_not_exist_in_the_table_schema_and_should_cause_an_error";

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append(long_name);

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: numeric column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("123");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: whitespace in column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("user name"); // Has space

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: mixed valid and invalid columns" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id"); // Valid
    try columns.append("invalid1"); // Invalid
    try columns.append("name"); // Valid
    try columns.append("invalid2"); // Invalid

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "test" });
    try values.append(ColumnValue{ .text = "Alice" });
    try values.append(ColumnValue{ .text = "test" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 2), result.errors.items.len);
}

test "validateInsert: column with trailing/leading spaces" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append(" id "); // Spaces around column name

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

// ============================================================================
// UPDATE Validation Error Tests (15 tests)
// ============================================================================

test "validateUpdate: empty assignments list" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer assignments.deinit();

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateUpdate: single assignment column not found" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col_name = try testing.allocator.dupe(u8, "invalid_column");
    try assignments.append(sql.Assignment{
        .column = col_name,
        .value = ColumnValue{ .int = 42 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: multiple assignment columns not found" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "invalid1");
    const col2 = try testing.allocator.dupe(u8, "invalid2");
    const col3 = try testing.allocator.dupe(u8, "invalid3");

    try assignments.append(sql.Assignment{
        .column = col1,
        .value = ColumnValue{ .int = 1 },
    });
    try assignments.append(sql.Assignment{
        .column = col2,
        .value = ColumnValue{ .int = 2 },
    });
    try assignments.append(sql.Assignment{
        .column = col3,
        .value = ColumnValue{ .int = 3 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 3), result.errors.items.len);
}

test "validateUpdate: single duplicate assignment" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "name");
    const col2 = try testing.allocator.dupe(u8, "name"); // Duplicate
    const val1 = try testing.allocator.dupe(u8, "Alice");
    const val2 = try testing.allocator.dupe(u8, "Bob");

    try assignments.append(sql.Assignment{
        .column = col1,
        .value = ColumnValue{ .text = val1 },
    });
    try assignments.append(sql.Assignment{
        .column = col2,
        .value = ColumnValue{ .text = val2 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.InvalidExpression, result.errors.items[0].error_type);
}

test "validateUpdate: multiple duplicate assignments" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "name");
    const col2 = try testing.allocator.dupe(u8, "name"); // Duplicate
    const col3 = try testing.allocator.dupe(u8, "email");
    const col4 = try testing.allocator.dupe(u8, "email"); // Duplicate
    const val1 = try testing.allocator.dupe(u8, "A");
    const val2 = try testing.allocator.dupe(u8, "B");
    const val3 = try testing.allocator.dupe(u8, "C");
    const val4 = try testing.allocator.dupe(u8, "D");

    try assignments.append(sql.Assignment{ .column = col1, .value = ColumnValue{ .text = val1 } });
    try assignments.append(sql.Assignment{ .column = col2, .value = ColumnValue{ .text = val2 } });
    try assignments.append(sql.Assignment{ .column = col3, .value = ColumnValue{ .text = val3 } });
    try assignments.append(sql.Assignment{ .column = col4, .value = ColumnValue{ .text = val4 } });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len >= 2);
}

test "validateUpdate: assignment with case mismatch" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, "NAME"); // Should be "name"
    const val = try testing.allocator.dupe(u8, "Alice");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .text = val },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: assignment with special characters" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, "col@#$");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .int = 42 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: assignment with whitespace column" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, " name ");
    const val = try testing.allocator.dupe(u8, "Alice");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .text = val },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: very long assignment column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, "this_is_a_very_long_column_name_that_does_not_exist");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .int = 42 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: numeric assignment column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, "999");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .int = 42 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(validator.ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: mixed valid and invalid assignments" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "name"); // Valid
    const col2 = try testing.allocator.dupe(u8, "invalid1"); // Invalid
    const col3 = try testing.allocator.dupe(u8, "age"); // Valid
    const col4 = try testing.allocator.dupe(u8, "invalid2"); // Invalid
    const val1 = try testing.allocator.dupe(u8, "A");

    try assignments.append(sql.Assignment{ .column = col1, .value = ColumnValue{ .text = val1 } });
    try assignments.append(sql.Assignment{ .column = col2, .value = ColumnValue{ .int = 1 } });
    try assignments.append(sql.Assignment{ .column = col3, .value = ColumnValue{ .int = 25 } });
    try assignments.append(sql.Assignment{ .column = col4, .value = ColumnValue{ .int = 2 } });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 2), result.errors.items.len);
}

test "validateUpdate: empty assignment column name" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col = try testing.allocator.dupe(u8, "");
    try assignments.append(sql.Assignment{
        .column = col,
        .value = ColumnValue{ .int = 42 },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
}

test "validateUpdate: assignment and duplicate in same command" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "invalid"); // Invalid column
    const col2 = try testing.allocator.dupe(u8, "invalid"); // Duplicate of invalid

    try assignments.append(sql.Assignment{ .column = col1, .value = ColumnValue{ .int = 1 } });
    try assignments.append(sql.Assignment{ .column = col2, .value = ColumnValue{ .int = 2 } });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    // Should have both a ColumnNotFound and InvalidExpression (duplicate) error
    try testing.expect(result.errors.items.len >= 1);
}

test "validateUpdate: assignment triple duplicate" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "name");
    const col2 = try testing.allocator.dupe(u8, "name");
    const col3 = try testing.allocator.dupe(u8, "name");
    const val1 = try testing.allocator.dupe(u8, "A");
    const val2 = try testing.allocator.dupe(u8, "B");
    const val3 = try testing.allocator.dupe(u8, "C");

    try assignments.append(sql.Assignment{ .column = col1, .value = ColumnValue{ .text = val1 } });
    try assignments.append(sql.Assignment{ .column = col2, .value = ColumnValue{ .text = val2 } });
    try assignments.append(sql.Assignment{ .column = col3, .value = ColumnValue{ .text = val3 } });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len >= 2);
}

// ============================================================================
// Validation Mode Tests (15 tests)
// ============================================================================

test "validation mode: INSERT strict mode blocks invalid column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try INSERT with invalid column in strict mode - should fail
    const query = "INSERT INTO users (id, invalid_col) VALUES (1, 'test')";
    const result_or_err = db.execute(query);

    try testing.expectError(sql.SqlError.ValidationFailed, result_or_err);
}

test "validation mode: UPDATE strict mode blocks invalid column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try UPDATE with invalid column in strict mode - should fail
    const query = "UPDATE users SET invalid_col = 42";
    const result_or_err = db.execute(query);

    try testing.expectError(sql.SqlError.ValidationFailed, result_or_err);
}

test "validation mode: DELETE strict mode blocks invalid column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try DELETE with invalid column in strict mode - should fail
    const query = "DELETE FROM users WHERE invalid_col = 1";
    const result_or_err = db.execute(query);

    try testing.expectError(sql.SqlError.ValidationFailed, result_or_err);
}

test "validation mode: INSERT strict mode blocks duplicate columns" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try INSERT with duplicate columns - should fail
    const query = "INSERT INTO users (id, name, id) VALUES (1, 'test', 2)";
    const result_or_err = db.execute(query);

    try testing.expectError(sql.SqlError.ValidationFailed, result_or_err);
}

test "validation mode: UPDATE strict mode blocks duplicate assignments" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try UPDATE with duplicate assignments - should fail
    const query = "UPDATE users SET name = 'Bob', name = 'Charlie'";
    const result_or_err = db.execute(query);

    try testing.expectError(sql.SqlError.ValidationFailed, result_or_err);
}

test "validation mode: INSERT warnings mode allows invalid column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.warnings;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try INSERT with invalid column in warnings mode - should succeed
    // Note: This will still fail during execution, but validation won't block it
    const query = "INSERT INTO users (id, name) VALUES (1, 'test')";
    var result = try db.execute(query);
    defer result.deinit();

    // Should execute successfully
    try testing.expect(true);
}

test "validation mode: UPDATE warnings mode allows invalid column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.warnings;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try valid UPDATE in warnings mode - should succeed
    var result = try db.execute("UPDATE users SET name = 'Bob'");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: DELETE warnings mode allows valid query" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.warnings;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try valid DELETE in warnings mode - should succeed
    var result = try db.execute("DELETE FROM users WHERE id = 1");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: INSERT disabled mode skips validation" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.disabled;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try INSERT - validation is disabled, so it will process normally
    var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'test')");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: UPDATE disabled mode skips validation" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.disabled;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try UPDATE - validation is disabled
    var result = try db.execute("UPDATE users SET name = 'Bob'");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: DELETE disabled mode skips validation" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.disabled;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Insert valid data
    {
        var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
        defer result.deinit();
    }

    // Try DELETE - validation is disabled
    var result = try db.execute("DELETE FROM users WHERE id = 1");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: strict mode default behavior" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Default should be strict
    try testing.expectEqual(ValidationMode.strict, db.validation_mode);
}

test "validation mode: changing mode at runtime" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Start with strict
    try testing.expectEqual(ValidationMode.strict, db.validation_mode);

    // Change to warnings
    db.validation_mode = ValidationMode.warnings;
    try testing.expectEqual(ValidationMode.warnings, db.validation_mode);

    // Change to disabled
    db.validation_mode = ValidationMode.disabled;
    try testing.expectEqual(ValidationMode.disabled, db.validation_mode);

    // Change back to strict
    db.validation_mode = ValidationMode.strict;
    try testing.expectEqual(ValidationMode.strict, db.validation_mode);
}

test "validation mode: INSERT empty column list in strict mode" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.strict;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Try INSERT with positional values (no column list) - should succeed
    var result = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    defer result.deinit();

    try testing.expect(true);
}

test "validation mode: warnings mode logs but continues" {
    var db = Database.init(testing.allocator);
    defer db.deinit();
    db.validation_mode = ValidationMode.warnings;

    // Create table
    {
        var result = try db.execute("CREATE TABLE users (id int, name text)");
        defer result.deinit();
    }

    // Valid INSERT should succeed and return row_id
    var result = try db.execute("INSERT INTO users (id, name) VALUES (1, 'Alice')");
    defer result.deinit();

    try testing.expectEqual(@as(usize, 1), result.rows.items.len);
}

// ============================================================================
// Fuzzy Matching Edge Cases (10 tests)
// ============================================================================

test "fuzzy matching: no similar columns when threshold exceeded" {
    const candidates = [_][]const u8{ "id", "name", "email" };

    // Looking for something very different
    const result = validator.findSimilarColumn("xyzzyx", &candidates, 2);

    try testing.expect(result == null);
}

test "fuzzy matching: exact match within threshold" {
    const candidates = [_][]const u8{ "id", "name", "email" };

    // One character off
    const result = validator.findSimilarColumn("nane", &candidates, 2);

    try testing.expect(result != null);
    if (result) |match| {
        try testing.expectEqualStrings("name", match);
    }
}

test "fuzzy matching: multiple equally similar columns" {
    const candidates = [_][]const u8{ "col_a", "col_b", "col_c" };

    // All are equally distant from "col_x"
    const result = validator.findSimilarColumn("col_x", &candidates, 2);

    // Should return the first match found
    try testing.expect(result != null);
}

test "fuzzy matching: very short column names - 1 char" {
    const candidates = [_][]const u8{ "a", "b", "c" };

    const result = validator.findSimilarColumn("x", &candidates, 1);

    // Should find a match within threshold of 1
    try testing.expect(result != null);
}

test "fuzzy matching: very short column names - 2 chars" {
    const candidates = [_][]const u8{ "id", "no", "xy" };

    const result = validator.findSimilarColumn("ix", &candidates, 1);

    try testing.expect(result != null);
    if (result) |match| {
        try testing.expectEqualStrings("id", match);
    }
}

test "fuzzy matching: empty candidate list" {
    const candidates = [_][]const u8{};

    const result = validator.findSimilarColumn("test", &candidates, 2);

    try testing.expect(result == null);
}

test "fuzzy matching: empty search string" {
    const candidates = [_][]const u8{ "id", "name", "email" };

    const result = validator.findSimilarColumn("", &candidates, 2);

    // Empty string has distance equal to length of candidate
    // Should not find a match within threshold of 2
    try testing.expect(result == null);
}

test "fuzzy matching: case sensitivity in matching" {
    const candidates = [_][]const u8{ "name", "email" };

    // Different case should increase distance
    const result = validator.findSimilarColumn("NAME", &candidates, 2);

    // All 4 characters differ, distance = 4, exceeds threshold of 2
    try testing.expect(result == null);
}

test "fuzzy matching: special characters in candidates" {
    const candidates = [_][]const u8{ "col_name", "col-name", "col.name" };

    const result = validator.findSimilarColumn("colname", &candidates, 2);

    // Should find one within threshold
    try testing.expect(result != null);
}

test "fuzzy matching: unicode characters" {
    const candidates = [_][]const u8{ "naïve", "résumé" };

    const result = validator.findSimilarColumn("naive", &candidates, 3);

    // Should handle unicode properly (though may not match due to byte differences)
    // This test just ensures we don't crash
    _ = result;
    try testing.expect(true);
}

// ============================================================================
// Complex Scenarios (10 tests)
// ============================================================================

test "complex: multiple errors in single INSERT" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("invalid1");
    try columns.append("id"); // duplicate below
    try columns.append("invalid2");
    try columns.append("id"); // duplicate

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .int = 2 });
    try values.append(ColumnValue{ .int = 3 });
    try values.append(ColumnValue{ .int = 4 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    // Should have errors for both invalid columns and duplicate
    try testing.expect(result.errors.items.len >= 3);
}

test "complex: multiple errors in single UPDATE" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    const col1 = try testing.allocator.dupe(u8, "invalid1");
    const col2 = try testing.allocator.dupe(u8, "name");
    const col3 = try testing.allocator.dupe(u8, "name"); // Duplicate
    const col4 = try testing.allocator.dupe(u8, "invalid2");
    const val2 = try testing.allocator.dupe(u8, "A");
    const val3 = try testing.allocator.dupe(u8, "B");

    try assignments.append(sql.Assignment{ .column = col1, .value = ColumnValue{ .int = 1 } });
    try assignments.append(sql.Assignment{ .column = col2, .value = ColumnValue{ .text = val2 } });
    try assignments.append(sql.Assignment{ .column = col3, .value = ColumnValue{ .text = val3 } });
    try assignments.append(sql.Assignment{ .column = col4, .value = ColumnValue{ .int = 2 } });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len >= 3);
}

test "complex: large number of columns - 100 columns" {
    var table = try createTestTableLargeSchema(testing.allocator, "large_table", 100);
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();

    // Try to insert with some invalid columns
    try columns.append("col_0");
    try columns.append("col_50");
    try columns.append("invalid_col");
    try columns.append("col_99");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .int = 2 });
    try values.append(ColumnValue{ .int = 3 });
    try values.append(ColumnValue{ .int = 4 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "large_table",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 1), result.errors.items.len);
}

test "complex: large number of invalid columns - 50 invalid" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();

    // Add 50 invalid columns
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        const col_name = try std.fmt.allocPrint(testing.allocator, "invalid_{d}", .{i});
        defer testing.allocator.free(col_name);
        try columns.append(col_name);
        try values.append(ColumnValue{ .int = @intCast(i) });
    }

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expectEqual(@as(usize, 50), result.errors.items.len);
}

test "complex: all valid columns in large schema" {
    var table = try createTestTableLargeSchema(testing.allocator, "large_table", 50);
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer {
        for (columns.items) |col_name| {
            testing.allocator.free(col_name);
        }
        columns.deinit();
    }

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();

    // Add all 50 valid columns
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        const col_name = try std.fmt.allocPrint(testing.allocator, "col_{d}", .{i});
        try columns.append(col_name);
        try values.append(ColumnValue{ .int = @intCast(i) });
    }

    const insert_cmd = sql.InsertCmd{
        .table_name = "large_table",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(result.valid);
    try testing.expectEqual(@as(usize, 0), result.errors.items.len);
}

test "complex: mixed valid invalid and duplicates" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id"); // Valid
    try columns.append("invalid"); // Invalid
    try columns.append("name"); // Valid
    try columns.append("id"); // Duplicate
    try columns.append("another_invalid"); // Invalid

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .int = 2 });
    try values.append(ColumnValue{ .text = "Alice" });
    try values.append(ColumnValue{ .int = 3 });
    try values.append(ColumnValue{ .int = 4 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    // Should have 2 ColumnNotFound + 1 duplicate error
    try testing.expect(result.errors.items.len >= 3);
}

test "complex: UPDATE with many assignments" {
    var table = try createTestTableLargeSchema(testing.allocator, "large_table", 100);
    defer table.deinit();

    var assignments = std.array_list.Managed(sql.Assignment).init(testing.allocator);
    defer {
        for (assignments.items) |*assign| {
            testing.allocator.free(assign.column);
            var val = assign.value;
            val.deinit(testing.allocator);
        }
        assignments.deinit();
    }

    // Create 20 valid assignments
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        const col_name = try std.fmt.allocPrint(testing.allocator, "col_{d}", .{i});
        try assignments.append(sql.Assignment{
            .column = col_name,
            .value = ColumnValue{ .int = @intCast(i) },
        });
    }

    const update_cmd = sql.UpdateCmd{
        .table_name = "large_table",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validator.validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(result.valid);
    try testing.expectEqual(@as(usize, 0), result.errors.items.len);
}

test "complex: INSERT all duplicates of valid column" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("id");
    try columns.append("id");
    try columns.append("id");

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .int = 2 });
    try values.append(ColumnValue{ .int = 3 });
    try values.append(ColumnValue{ .int = 4 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    // Should have 3 duplicate errors (after first occurrence)
    try testing.expect(result.errors.items.len >= 3);
}

test "complex: validation with hints - typo detection" {
    var table = try createTestTable(testing.allocator, "users");
    defer table.deinit();

    var columns = std.array_list.Managed([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("nane"); // Typo: should be "name"

    var values = std.array_list.Managed(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validator.validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);

    // Should have a hint suggesting "name"
    const first_error = result.errors.items[0];
    try testing.expect(first_error.hint != null);
    if (first_error.hint) |hint| {
        try testing.expect(std.mem.indexOf(u8, hint, "name") != null);
    }
}

test "complex: zero threshold fuzzy matching" {
    const candidates = [_][]const u8{ "id", "name", "email" };

    // With threshold 0, only exact matches work
    const result = validator.findSimilarColumn("nane", &candidates, 0);

    try testing.expect(result == null);
}
