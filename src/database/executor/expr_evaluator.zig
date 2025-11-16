// ============================================================================
// Expression and Subquery Evaluation
// Handles WHERE clause evaluation, subqueries, and expression resolution
// ============================================================================

const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const sql = @import("../../sql.zig");
const column_matching = @import("../column_matching.zig");
const StringHashMap = std.StringHashMap;
const Allocator = std.mem.Allocator;

// ============================================================================
// Subquery Execution Support
// ============================================================================

// Forward declaration - executeSelect is defined in the main executor
// We use a function pointer approach to avoid circular dependencies
const ExecuteSelectFn = *const fn (db: *Database, cmd: sql.SelectCmd) anyerror!QueryResult;

/// Execute a subquery and return the result
/// Note: This calls back to the main executor's executeSelect function
fn executeSubquery(
    db: *Database,
    subquery: *const sql.SelectCmd,
    executeSelectFn: ExecuteSelectFn,
) !QueryResult {
    // Execute the nested SELECT statement
    return executeSelectFn(db, subquery.*);
}

/// Evaluate IN operator with subquery
/// Returns true if left_val is in the subquery result set
fn evaluateInSubquery(
    db: *Database,
    left_val: ColumnValue,
    subquery: *const sql.SelectCmd,
    negate: bool,
    executeSelectFn: ExecuteSelectFn,
) !bool {
    // Execute subquery
    var result = try executeSubquery(db, subquery, executeSelectFn);
    defer result.deinit();

    // Subquery for IN must return a single column
    if (result.columns.items.len != 1) {
        return error.InvalidSubquery;
    }

    // Check if left_val is in the result set
    for (result.rows.items) |row| {
        if (row.items.len > 0) {
            const val = row.items[0];
            if (valuesEqual(left_val, val)) {
                return !negate; // Found match
            }
        }
    }

    return negate; // No match found
}

/// Evaluate EXISTS operator
/// Returns true if subquery returns at least one row
fn evaluateExistsSubquery(
    db: *Database,
    subquery: *const sql.SelectCmd,
    negate: bool,
    executeSelectFn: ExecuteSelectFn,
) !bool {
    // Execute subquery
    var result = try executeSubquery(db, subquery, executeSelectFn);
    defer result.deinit();

    // EXISTS returns true if result has at least one row
    const has_rows = result.rows.items.len > 0;
    return if (negate) !has_rows else has_rows;
}

/// Evaluate scalar subquery (returns single value)
/// Used for comparisons like: WHERE price > (SELECT AVG(price) ...)
fn evaluateScalarSubquery(
    db: *Database,
    subquery: *const sql.SelectCmd,
    allocator: Allocator,
    executeSelectFn: ExecuteSelectFn,
) !ColumnValue {
    var result = try executeSubquery(db, subquery, executeSelectFn);
    defer result.deinit();

    // Scalar subquery must return exactly 1 column
    if (result.columns.items.len != 1) {
        return error.InvalidSubquery;
    }

    // Scalar subquery should return 0 or 1 rows
    if (result.rows.items.len == 0) {
        // SQL standard: return NULL if no rows
        return ColumnValue.null_value;
    }

    if (result.rows.items.len > 1) {
        // SQL standard: error if more than 1 row
        return error.SubqueryReturnedMultipleRows;
    }

    // Return the single value (clone it since result will be deinit'd)
    return result.rows.items[0].items[0].clone(allocator);
}

// ============================================================================
// Expression Evaluation
// ============================================================================

/// Helper for comparing values with binary operator
fn compareValuesWithOp(left: ColumnValue, right: ColumnValue, op: sql.BinaryOp) bool {
    // Handle NULL comparisons
    if (left == .null_value or right == .null_value) {
        return false; // NULL comparisons return false
    }

    switch (op) {
        .eq => {
            return valuesEqual(left, right);
        },
        .neq => {
            return !valuesEqual(left, right);
        },
        .lt => {
            return switch (left) {
                .int => |li| switch (right) {
                    .int => |ri| li < ri,
                    .float => |rf| @as(f64, @floatFromInt(li)) < rf,
                    else => false,
                },
                .float => |lf| switch (right) {
                    .int => |ri| lf < @as(f64, @floatFromInt(ri)),
                    .float => |rf| lf < rf,
                    else => false,
                },
                .text => |lt| switch (right) {
                    .text => |rt| std.mem.order(u8, lt, rt) == .lt,
                    else => false,
                },
                else => false,
            };
        },
        .gt => {
            return switch (left) {
                .int => |li| switch (right) {
                    .int => |ri| li > ri,
                    .float => |rf| @as(f64, @floatFromInt(li)) > rf,
                    else => false,
                },
                .float => |lf| switch (right) {
                    .int => |ri| lf > @as(f64, @floatFromInt(ri)),
                    .float => |rf| lf > rf,
                    else => false,
                },
                .text => |lt| switch (right) {
                    .text => |rt| std.mem.order(u8, lt, rt) == .gt,
                    else => false,
                },
                else => false,
            };
        },
        .lte => {
            return compareValuesWithOp(left, right, .lt) or compareValuesWithOp(left, right, .eq);
        },
        .gte => {
            return compareValuesWithOp(left, right, .gt) or compareValuesWithOp(left, right, .eq);
        },
        else => return false,
    }
}

/// Helper to extract value from expression (needed for subquery evaluation)
fn getExprValueFromExpr(
    expr: sql.Expr,
    row_values: anytype,
    db: *Database,
    executeSelectFn: ExecuteSelectFn,
) !ColumnValue {
    switch (expr) {
        .literal => |val| return val,
        .column => |col| {
            // Use column_matching helper for resolution
            // This handles exact matches, qualified/unqualified resolution, and alias mismatches
            if (column_matching.resolveColumnValue(col, row_values)) |value| {
                return value;
            }
            return ColumnValue.null_value;
        },
        .aggregate => |agg| {
            // Build the aggregate column name to match what's stored in grouped results
            var buf: [256]u8 = undefined;
            const col_name = switch (agg.func) {
                .count => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "COUNT({s})", .{col}) catch "COUNT(*)"
                else
                    "COUNT(*)",
                .sum => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "SUM({s})", .{col}) catch ""
                else
                    "",
                .avg => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "AVG({s})", .{col}) catch ""
                else
                    "",
                .min => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "MIN({s})", .{col}) catch ""
                else
                    "",
                .max => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "MAX({s})", .{col}) catch ""
                else
                    "",
            };

            // Look up the aggregate column in row_values
            var it = row_values.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, col_name)) {
                    return entry.value_ptr.*;
                }
            }
            return ColumnValue.null_value;
        },
        .subquery => |sq| {
            // Evaluate as scalar subquery
            return evaluateScalarSubquery(db, sq, db.allocator, executeSelectFn);
        },
        .binary, .unary => {
            // Evaluate as boolean and convert
            const result = try evaluateExprWithSubqueriesInternal(db, expr, row_values, executeSelectFn);
            return ColumnValue{ .bool = result };
        },
    }
}

/// Internal evaluator that takes executeSelectFn parameter
fn evaluateExprWithSubqueriesInternal(
    db: *Database,
    expr: sql.Expr,
    row_values: anytype,
    executeSelectFn: ExecuteSelectFn,
) anyerror!bool {
    // For non-binary expressions, delegate to sql.evaluateExpr
    if (expr != .binary) {
        return sql.evaluateExpr(expr, row_values, @ptrCast(db));
    }

    const bin = expr.binary;

    // Handle subquery operators
    switch (bin.op) {
        .in_op => {
            // IN operator: left_val IN (subquery)
            if (bin.right != .subquery) {
                // Not a subquery - fall back to standard evaluation
                return sql.evaluateExpr(expr, row_values, @ptrCast(db));
            }

            // Get left value
            const left_val = try getExprValueFromExpr(bin.left, row_values, db, executeSelectFn);
            defer {
                // Clean up if left_val was a cloned value (e.g., from a subquery)
                if (bin.left == .subquery) {
                    var val = left_val;
                    val.deinit(db.allocator);
                }
            }

            // Evaluate IN subquery
            return try evaluateInSubquery(
                db,
                left_val,
                bin.right.subquery,
                false, // not negated
                executeSelectFn,
            );
        },

        .not_in_op => {
            // NOT IN operator
            if (bin.right != .subquery) {
                return sql.evaluateExpr(expr, row_values, @ptrCast(db));
            }

            const left_val = try getExprValueFromExpr(bin.left, row_values, db, executeSelectFn);
            defer {
                // Clean up if left_val was a cloned value (e.g., from a subquery)
                if (bin.left == .subquery) {
                    var val = left_val;
                    val.deinit(db.allocator);
                }
            }

            return try evaluateInSubquery(
                db,
                left_val,
                bin.right.subquery,
                true, // negated
                executeSelectFn,
            );
        },

        .exists_op => {
            // EXISTS operator
            if (bin.right != .subquery) {
                return sql.evaluateExpr(expr, row_values, @ptrCast(db));
            }

            return evaluateExistsSubquery(
                db,
                bin.right.subquery,
                false, // not negated
                executeSelectFn,
            ) catch false;
        },

        .not_exists_op => {
            // NOT EXISTS operator
            if (bin.right != .subquery) {
                return sql.evaluateExpr(expr, row_values, @ptrCast(db));
            }

            return evaluateExistsSubquery(
                db,
                bin.right.subquery,
                true, // negated
                executeSelectFn,
            ) catch false;
        },

        // For comparison operators, check if either side is a scalar subquery
        .eq, .neq, .lt, .gt, .lte, .gte => {
            // Check if left side is a subquery
            if (bin.left == .subquery) {
                const left_val = try evaluateScalarSubquery(
                    db,
                    bin.left.subquery,
                    db.allocator,
                    executeSelectFn,
                );
                defer {
                    var val = left_val;
                    val.deinit(db.allocator);
                }

                const right_val = try getExprValueFromExpr(bin.right, row_values, db, executeSelectFn);
                defer {
                    // Clean up if right_val was a cloned value (e.g., from a subquery)
                    if (bin.right == .subquery) {
                        var val = right_val;
                        val.deinit(db.allocator);
                    }
                }

                return compareValuesWithOp(left_val, right_val, bin.op);
            }

            // Check if right side is a subquery
            if (bin.right == .subquery) {
                // Scalar subquery comparison
                const left_val = try getExprValueFromExpr(bin.left, row_values, db, executeSelectFn);
                defer {
                    // Clean up if left_val was a cloned value (e.g., from a subquery)
                    if (bin.left == .subquery) {
                        var val = left_val;
                        val.deinit(db.allocator);
                    }
                }

                const right_val = try evaluateScalarSubquery(
                    db,
                    bin.right.subquery,
                    db.allocator,
                    executeSelectFn,
                );
                defer {
                    var val = right_val;
                    val.deinit(db.allocator);
                }

                return compareValuesWithOp(left_val, right_val, bin.op);
            } else {
                // Regular comparison - delegate to sql.evaluateExpr
                return sql.evaluateExpr(expr, row_values, @ptrCast(db));
            }
        },

        // For AND/OR, need to recursively evaluate with subquery support
        .and_op => {
            const left_result = try evaluateExprWithSubqueriesInternal(db, bin.left, row_values, executeSelectFn);
            const right_result = try evaluateExprWithSubqueriesInternal(db, bin.right, row_values, executeSelectFn);
            return left_result and right_result;
        },

        .or_op => {
            const left_result = try evaluateExprWithSubqueriesInternal(db, bin.left, row_values, executeSelectFn);
            const right_result = try evaluateExprWithSubqueriesInternal(db, bin.right, row_values, executeSelectFn);
            return left_result or right_result;
        },
    }
}

/// Enhanced expression evaluator that handles subqueries
/// This wraps sql.evaluateExpr and adds subquery execution support
///
/// PUBLIC API - This is called from main executor and must be exported
pub fn evaluateExprWithSubqueries(
    db: *Database,
    expr: sql.Expr,
    row_values: anytype,
    executeSelectFn: ExecuteSelectFn,
) anyerror!bool {
    return evaluateExprWithSubqueriesInternal(db, expr, row_values, executeSelectFn);
}
