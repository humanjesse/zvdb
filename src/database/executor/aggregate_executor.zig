// ============================================================================
// Aggregate Functions and GROUP BY Support
// ============================================================================

const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const Row = @import("../../table.zig").Row;
const sql = @import("../../sql.zig");
const AggregateFunc = sql.AggregateFunc;
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const Allocator = std.mem.Allocator;
const Snapshot = @import("../../transaction.zig").Snapshot;
const CommitLog = @import("../../transaction.zig").CommitLog;

// Forward declaration for expression evaluation (still in main executor)
// This will be moved to where_evaluator once that's extracted
const evaluateExprWithSubqueries = @import("../executor.zig").evaluateExprWithSubqueries;

const sort_executor = @import("sort_executor.zig");

// ============================================================================
// Aggregate State Management
// ============================================================================

/// Aggregate accumulator for COUNT, SUM, AVG, MIN, MAX
const AggregateState = struct {
    func: AggregateFunc,
    column: ?[]const u8,

    // Accumulator state
    count: u64,
    sum: f64,
    min: ?ColumnValue,
    max: ?ColumnValue,

    allocator: Allocator,

    pub fn init(allocator: Allocator, func: AggregateFunc, column: ?[]const u8) AggregateState {
        return .{
            .func = func,
            .column = column,
            .count = 0,
            .sum = 0.0,
            .min = null,
            .max = null,
            .allocator = allocator,
        };
    }

    /// Process a single row
    pub fn accumulate(self: *AggregateState, row: *const Row) !void {
        switch (self.func) {
            .count => {
                // COUNT(*) counts all rows
                if (self.column == null) {
                    self.count += 1;
                } else {
                    // COUNT(column) counts non-null values
                    if (row.get(self.column.?)) |val| {
                        if (val != .null_value) {
                            self.count += 1;
                        }
                    }
                }
            },
            .sum, .avg => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        const num = switch (val) {
                            .int => |i| @as(f64, @floatFromInt(i)),
                            .float => |f| f,
                            .null_value => return, // Skip nulls
                            else => return error.TypeMismatch,
                        };
                        self.sum += num;
                        self.count += 1;
                    }
                }
            },
            .min => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        if (val == .null_value) return; // Skip nulls

                        if (self.min == null) {
                            self.min = try val.clone(self.allocator);
                        } else {
                            // Compare and update if smaller
                            if (compareForMinMax(val, self.min.?) == .lt) {
                                var old = self.min.?;
                                old.deinit(self.allocator);
                                self.min = try val.clone(self.allocator);
                            }
                        }
                    }
                }
            },
            .max => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        if (val == .null_value) return; // Skip nulls

                        if (self.max == null) {
                            self.max = try val.clone(self.allocator);
                        } else {
                            // Compare and update if larger
                            if (compareForMinMax(val, self.max.?) == .gt) {
                                var old = self.max.?;
                                old.deinit(self.allocator);
                                self.max = try val.clone(self.allocator);
                            }
                        }
                    }
                }
            },
        }
    }

    /// Get final result
    pub fn finalize(self: *AggregateState) !ColumnValue {
        switch (self.func) {
            .count => return ColumnValue{ .int = @intCast(self.count) },
            .sum => return ColumnValue{ .float = self.sum },
            .avg => {
                if (self.count == 0) return ColumnValue.null_value;
                return ColumnValue{ .float = self.sum / @as(f64, @floatFromInt(self.count)) };
            },
            .min => {
                if (self.min) |m| {
                    return try m.clone(self.allocator);
                } else {
                    return ColumnValue.null_value;
                }
            },
            .max => {
                if (self.max) |m| {
                    return try m.clone(self.allocator);
                } else {
                    return ColumnValue.null_value;
                }
            },
        }
    }

    pub fn deinit(self: *AggregateState) void {
        if (self.min) |*m| {
            var val = m.*;
            val.deinit(self.allocator);
        }
        if (self.max) |*m| {
            var val = m.*;
            val.deinit(self.allocator);
        }
    }
};

/// Compare two ColumnValues for MIN/MAX operations
fn compareForMinMax(a: ColumnValue, b: ColumnValue) std.math.Order {
    // Similar to compareValues but returns Order
    switch (a) {
        .int => |ai| {
            const bi = switch (b) {
                .int => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return .eq,
            };
            if (ai < bi) return .lt;
            if (ai > bi) return .gt;
            return .eq;
        },
        .float => |af| {
            const bf = switch (b) {
                .float => |f| f,
                .int => |i| @as(f64, @floatFromInt(i)),
                else => return .eq,
            };
            if (af < bf) return .lt;
            if (af > bf) return .gt;
            return .eq;
        },
        .text => |at| {
            if (b != .text) return .eq;
            return std.mem.order(u8, at, b.text);
        },
        else => return .eq,
    }
}

// ============================================================================
// GROUP BY Support
// ============================================================================

/// Create a hash key from group column values
fn makeGroupKey(allocator: Allocator, row: *const Row, group_columns: []const []const u8) ![]u8 {
    var key_parts = ArrayList([]const u8).init(allocator);
    defer {
        for (key_parts.items) |part| {
            allocator.free(part);
        }
        key_parts.deinit();
    }

    for (group_columns) |col| {
        const val = row.get(col) orelse {
            try key_parts.append(try allocator.dupe(u8, "NULL"));
            continue;
        };

        const val_str = switch (val) {
            .int => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .text => |t| try allocator.dupe(u8, t),
            .bool => |b| if (b) try allocator.dupe(u8, "true") else try allocator.dupe(u8, "false"),
            .null_value => try allocator.dupe(u8, "NULL"),
            else => try allocator.dupe(u8, ""),
        };
        try key_parts.append(val_str);
    }

    // Join with separator
    return std.mem.join(allocator, "|", key_parts.items);
}

/// Apply HAVING clause to filter GROUP BY results
pub fn applyHavingFilter(
    result: *QueryResult,
    having_expr: sql.Expr,
    db: *Database,
) !void {
    var filtered_rows = ArrayList(ArrayList(ColumnValue)).init(result.allocator);
    errdefer {
        for (filtered_rows.items) |*row| {
            for (row.items) |*val| {
                var v = val.*;
                v.deinit(result.allocator);
            }
            row.deinit();
        }
        filtered_rows.deinit();
    }

    // For each grouped row, check if it passes the HAVING condition
    for (result.rows.items) |row| {
        // Build a map of column name → value for this group
        var row_values = StringHashMap(ColumnValue).init(result.allocator);
        defer row_values.deinit();

        for (result.columns.items, 0..) |col_name, idx| {
            if (idx < row.items.len) {
                try row_values.put(col_name, row.items[idx]);
            }
        }

        // Evaluate HAVING expression for this grouped row
        // Use the same evaluateExprWithSubqueries function that WHERE uses
        const passes = try evaluateExprWithSubqueries(db, having_expr, row_values);

        if (passes) {
            // Keep this row - clone it to filtered_rows
            var cloned_row = ArrayList(ColumnValue).init(result.allocator);
            errdefer cloned_row.deinit();

            for (row.items) |val| {
                const cloned_val = try val.clone(result.allocator);
                try cloned_row.append(cloned_val);
            }

            try filtered_rows.append(cloned_row);
        }
    }

    // Free original rows
    for (result.rows.items) |*row| {
        for (row.items) |*val| {
            var v = val.*;
            v.deinit(result.allocator);
        }
        row.deinit();
    }
    result.rows.deinit();

    // Replace with filtered rows
    result.rows = filtered_rows;
}

// ============================================================================
// Aggregate Query Execution
// ============================================================================

/// Execute SELECT with aggregates but no GROUP BY
pub fn executeAggregateSelect(db: *Database, table: *Table, cmd: sql.SelectCmd) !QueryResult {
    var result = QueryResult.init(db.allocator);
    errdefer result.deinit();

    // Initialize aggregate states
    var agg_states = ArrayList(AggregateState).init(db.allocator);
    defer {
        for (agg_states.items) |*state| {
            state.deinit();
        }
        agg_states.deinit();
    }

    // Setup result columns and aggregate states
    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => |agg| {
                // Add column name to result
                const col_name = switch (agg.func) {
                    .count => if (agg.column) |c|
                        try std.fmt.allocPrint(db.allocator, "COUNT({s})", .{c})
                    else
                        try db.allocator.dupe(u8, "COUNT(*)"),
                    .sum => try std.fmt.allocPrint(db.allocator, "SUM({s})", .{agg.column.?}),
                    .avg => try std.fmt.allocPrint(db.allocator, "AVG({s})", .{agg.column.?}),
                    .min => try std.fmt.allocPrint(db.allocator, "MIN({s})", .{agg.column.?}),
                    .max => try std.fmt.allocPrint(db.allocator, "MAX({s})", .{agg.column.?}),
                };
                defer db.allocator.free(col_name);
                try result.addColumn(col_name);

                // Validation temporarily disabled
                // TODO: Re-enable with proper edge case handling

                // Initialize aggregate state
                try agg_states.append(AggregateState.init(db.allocator, agg.func, agg.column));
            },
            else => unreachable, // Already validated
        }
    }

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    // Scan all rows and accumulate
    const row_ids = try table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id, snapshot, clog) orelse continue;

        // Apply WHERE filter
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                const row_val = row.get(where_col) orelse continue;
                if (!valuesEqual(row_val, where_val)) continue;
            }
        } else if (cmd.where_expr) |expr| {
            // Evaluate complex WHERE expression with subquery support
            const matches = try evaluateExprWithSubqueries(db, expr, row.values);
            if (!matches) continue;
        }

        // Accumulate in all aggregate states
        for (agg_states.items) |*state| {
            try state.accumulate(row);
        }
    }

    // Finalize and create result row
    var result_row = ArrayList(ColumnValue).init(db.allocator);
    for (agg_states.items) |*state| {
        const final_value = try state.finalize();
        try result_row.append(final_value);
    }
    try result.addRow(result_row);

    return result;
}

/// Execute SELECT with GROUP BY clause
pub fn executeGroupBySelect(db: *Database, table: *Table, cmd: sql.SelectCmd) !QueryResult {
    var result = QueryResult.init(db.allocator);
    errdefer result.deinit();

    // Group data structure: group_key -> (group_values, aggregate_states)
    const GroupData = struct {
        group_values: ArrayList(ColumnValue),
        agg_states: ArrayList(AggregateState),

        fn deinit(self: *@This(), allocator: Allocator) void {
            for (self.group_values.items) |*val| {
                var v = val.*;
                v.deinit(allocator);
            }
            self.group_values.deinit();

            for (self.agg_states.items) |*state| {
                state.deinit();
            }
            self.agg_states.deinit();
        }
    };

    var groups = StringHashMap(GroupData).init(db.allocator);
    defer {
        var it = groups.iterator();
        while (it.next()) |entry| {
            db.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(db.allocator);
        }
        groups.deinit();
    }

    // Setup result columns
    // 1. Group columns - validation temporarily disabled
    // TODO: Re-enable with proper edge case handling
    for (cmd.group_by.items) |col| {
        try result.addColumn(col);
    }

    // 2. Aggregate columns and validate regular columns
    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => |agg| {
                const col_name = switch (agg.func) {
                    .count => if (agg.column) |c|
                        try std.fmt.allocPrint(db.allocator, "COUNT({s})", .{c})
                    else
                        try db.allocator.dupe(u8, "COUNT(*)"),
                    .sum => try std.fmt.allocPrint(db.allocator, "SUM({s})", .{agg.column.?}),
                    .avg => try std.fmt.allocPrint(db.allocator, "AVG({s})", .{agg.column.?}),
                    .min => try std.fmt.allocPrint(db.allocator, "MIN({s})", .{agg.column.?}),
                    .max => try std.fmt.allocPrint(db.allocator, "MAX({s})", .{agg.column.?}),
                };
                defer db.allocator.free(col_name);
                try result.addColumn(col_name);

                // Validation temporarily disabled
                // TODO: Re-enable with proper edge case handling
            },
            .regular => |col_name| {
                // Regular column must be in GROUP BY
                var found = false;
                for (cmd.group_by.items) |group_col| {
                    if (std.mem.eql(u8, col_name, group_col)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return error.ColumnNotInGroupBy;
                }
            },
            .star => return error.CannotUseStarWithGroupBy,
        }
    }

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    // Scan rows and build groups
    const row_ids = try table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id, snapshot, clog) orelse continue;

        // Apply WHERE filter
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                const row_val = row.get(where_col) orelse continue;
                if (!valuesEqual(row_val, where_val)) continue;
            }
        } else if (cmd.where_expr) |expr| {
            // Evaluate complex WHERE expression with subquery support
            const matches = try evaluateExprWithSubqueries(db, expr, row.values);
            if (!matches) continue;
        }

        // Create group key
        const group_key = try makeGroupKey(db.allocator, row, cmd.group_by.items);

        // Get or create group
        const gop = try groups.getOrPut(group_key);
        if (!gop.found_existing) {
            // New group - initialize
            var group_values = ArrayList(ColumnValue).init(db.allocator);
            for (cmd.group_by.items) |col| {
                const val = row.get(col) orelse ColumnValue.null_value;
                try group_values.append(try val.clone(db.allocator));
            }

            var agg_states = ArrayList(AggregateState).init(db.allocator);
            for (cmd.columns.items) |col| {
                if (col == .aggregate) {
                    try agg_states.append(AggregateState.init(
                        db.allocator,
                        col.aggregate.func,
                        col.aggregate.column,
                    ));
                }
            }

            gop.value_ptr.* = GroupData{
                .group_values = group_values,
                .agg_states = agg_states,
            };
        } else {
            // Group exists - free the duplicate key
            db.allocator.free(group_key);
        }

        // Accumulate in aggregate states
        for (gop.value_ptr.agg_states.items) |*state| {
            try state.accumulate(row);
        }
    }

    // Finalize all groups and create result rows
    var group_it = groups.iterator();
    while (group_it.next()) |entry| {
        var result_row = ArrayList(ColumnValue).init(db.allocator);

        // Add group column values
        for (entry.value_ptr.group_values.items) |val| {
            try result_row.append(try val.clone(db.allocator));
        }

        // Add aggregate results
        for (entry.value_ptr.agg_states.items) |*state| {
            const final_value = try state.finalize();
            try result_row.append(final_value);
        }

        try result.addRow(result_row);
    }

    // Apply HAVING clause if present
    if (cmd.having_expr) |having_expr| {
        try applyHavingFilter(&result, having_expr, db);
    }

    // Apply ORDER BY if present
    if (cmd.order_by) |order_by| {
        try sort_executor.applyOrderBy(&result, order_by);
    }

    // Apply OFFSET and LIMIT if present
    const offset = cmd.offset orelse 0;
    const total_rows = result.rows.items.len;

    if (offset > 0 or cmd.limit != null) {
        // If offset is beyond total rows, free all rows and return empty result
        if (offset >= total_rows) {
            for (result.rows.items) |*row| {
                for (row.items) |*val| {
                    var v = val.*;
                    v.deinit(result.allocator);
                }
                row.deinit();
            }
            result.rows.items.len = 0;
        } else {
            // Calculate how many rows to keep after offset
            const rows_after_offset = total_rows - offset;
            const final_row_count = if (cmd.limit) |limit|
                @min(limit, rows_after_offset)
            else
                rows_after_offset;

            // Create new ArrayList with only the rows we want to keep
            var new_rows = ArrayList(ArrayList(ColumnValue)).init(result.allocator);
            errdefer {
                for (new_rows.items) |*row| {
                    row.deinit();
                }
                new_rows.deinit();
            }

            // Copy rows we want to keep (shallow copy of ArrayList structs)
            var i: usize = 0;
            while (i < final_row_count) : (i += 1) {
                try new_rows.append(result.rows.items[offset + i]);
            }

            // Free rows we're not keeping
            for (result.rows.items[0..offset]) |*row| {
                for (row.items) |*val| {
                    var v = val.*;
                    v.deinit(result.allocator);
                }
                row.deinit();
            }
            for (result.rows.items[offset + final_row_count .. total_rows]) |*row| {
                for (row.items) |*val| {
                    var v = val.*;
                    v.deinit(result.allocator);
                }
                row.deinit();
            }

            // Replace rows (don't deinit row data, just the backing array)
            result.rows.deinit();
            result.rows = new_rows;
        }
    }

    return result;
}
