const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const hash_join = @import("../hash_join.zig");
const column_resolver = @import("../column_resolver.zig");
const ColumnResolver = column_resolver.ColumnResolver;
const column_matching = @import("../column_matching.zig");
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const ColumnType = @import("../../table.zig").ColumnType;
const Row = @import("../../table.zig").Row;
const sql = @import("../../sql.zig");
const AggregateFunc = sql.AggregateFunc;
const OrderByClause = sql.OrderByClause;
const OrderDirection = sql.OrderDirection;
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const Transaction = @import("../../transaction.zig");
const Snapshot = Transaction.Snapshot;
const CommitLog = Transaction.CommitLog;
const Allocator = std.mem.Allocator;

// Import evaluateExprWithSubqueries from main executor
const executor = @import("../executor.zig");
const evaluateExprWithSubqueries = executor.evaluateExprWithSubqueries;

// Import applyOrderBy from sort executor
const sort_executor = @import("sort_executor.zig");
const applyOrderBy = sort_executor.applyOrderBy;

// ============================================================================
// JOIN Support
// ============================================================================

/// Split a qualified column name (table.column) into table and column parts
fn splitQualifiedColumn(col_name: []const u8) struct { table: ?[]const u8, column: []const u8 } {
    if (std.mem.indexOf(u8, col_name, ".")) |dot_idx| {
        return .{
            .table = col_name[0..dot_idx],
            .column = col_name[dot_idx + 1 ..],
        };
    }
    return .{ .table = null, .column = col_name };
}

// ============================================================================
// WHERE Clause Evaluation for JOINs
// ============================================================================

/// Evaluate WHERE clause on a joined row
/// Works with QueryResult rows where columns are in a specific order
///
/// Design: We build a StringHashMap from the row values using column names from the result,
/// then leverage the existing sql.evaluateExpr infrastructure for complex WHERE expressions
fn evaluateWhereOnJoinedRow(
    db: *Database,
    row_values: []const ColumnValue,
    column_names: []const []const u8,
    cmd: sql.SelectCmd,
) !bool {
    const allocator = db.allocator;
    // Simple WHERE: column = value (fast path)
    if (cmd.where_column != null and cmd.where_value != null and cmd.where_expr == null) {
        const where_col = cmd.where_column.?;
        const where_val = cmd.where_value.?;

        // Find the column in the result schema
        for (column_names, 0..) |col_name, idx| {
            // Match both qualified (table.column) and unqualified (column) names
            const matches = std.mem.eql(u8, col_name, where_col) or blk: {
                // Try matching unqualified part of qualified name
                if (std.mem.indexOf(u8, col_name, ".")) |dot_idx| {
                    const unqualified = col_name[dot_idx + 1 ..];
                    break :blk std.mem.eql(u8, unqualified, where_col);
                }
                break :blk false;
            };

            if (matches) {
                const row_val = if (idx < row_values.len) row_values[idx] else ColumnValue.null_value;
                return valuesEqual(row_val, where_val);
            }
        }

        // Column not found - row doesn't pass filter
        return false;
    }

    // Complex WHERE: use expression evaluator
    if (cmd.where_expr) |expr| {
        // Build a StringHashMap for the expression evaluator
        var row_map = StringHashMap(ColumnValue).init(allocator);
        defer row_map.deinit();

        // Populate map with all column values
        for (column_names, 0..) |col_name, idx| {
            if (idx < row_values.len) {
                try row_map.put(col_name, row_values[idx]);

                // Also add unqualified version if it's a qualified name
                if (std.mem.indexOf(u8, col_name, ".")) |dot_idx| {
                    const unqualified = col_name[dot_idx + 1 ..];
                    // Only add if not already present (avoid ambiguous column conflicts)
                    if (!row_map.contains(unqualified)) {
                        try row_map.put(unqualified, row_values[idx]);
                    }
                }
            }
        }

        // Evaluate the expression with subquery support
        return evaluateExprWithSubqueries(db, expr, row_map);
    }

    // No WHERE clause - row passes
    return true;
}

/// Project a QueryResult to selected columns
/// Used after WHERE filtering when we joined with all columns
fn projectToSelectedColumns(
    allocator: Allocator,
    result: *QueryResult,
    selected_columns: []const sql.SelectColumn,
) !QueryResult {
    var projected = QueryResult.init(allocator);
    errdefer projected.deinit();

    // Add selected column names to result schema
    for (selected_columns) |col_spec| {
        switch (col_spec) {
            .regular => |col_name| try projected.addColumn(col_name),
            .star => {
                // Copy all columns
                for (result.columns.items) |col| {
                    try projected.addColumn(col);
                }
            },
            .aggregate => return error.AggregateNotSupportedInJoin,
        }
    }

    // Project each row
    for (result.rows.items) |row| {
        var projected_row = ArrayList(ColumnValue).init(allocator);

        for (selected_columns) |col_spec| {
            if (col_spec == .regular) {
                const col_name = col_spec.regular;

                // Use column_matching helper to find the column index
                // This handles exact matches, qualified/unqualified resolution, and alias mismatches
                if (column_matching.findColumnIndex(col_name, result.columns.items)) |idx| {
                    if (idx < row.items.len) {
                        try projected_row.append(try row.items[idx].clone(allocator));
                    } else {
                        // Index out of bounds - add NULL
                        try projected_row.append(ColumnValue.null_value);
                    }
                } else {
                    // Column not found - add NULL
                    try projected_row.append(ColumnValue.null_value);
                }
            } else if (col_spec == .star) {
                // Copy all values
                for (row.items) |val| {
                    try projected_row.append(try val.clone(allocator));
                }
            }
        }

        try projected.addRow(projected_row);
    }

    return projected;
}

/// Apply WHERE filter to a QueryResult (for 2-table joins)
/// Returns a new QueryResult with only rows that pass the WHERE clause
fn applyWhereToQueryResult(
    db: *Database,
    result: *QueryResult,
    cmd: sql.SelectCmd,
) !QueryResult {
    const allocator = db.allocator;
    // If no WHERE clause, return original result
    if (cmd.where_column == null and cmd.where_expr == null) {
        // Return a copy of the result
        var filtered = QueryResult.init(allocator);
        for (result.columns.items) |col| {
            try filtered.addColumn(col);
        }
        for (result.rows.items) |row| {
            var filtered_row = ArrayList(ColumnValue).init(allocator);
            for (row.items) |val| {
                try filtered_row.append(try val.clone(allocator));
            }
            try filtered.addRow(filtered_row);
        }
        return filtered;
    }

    // Create filtered result with same schema
    var filtered = QueryResult.init(allocator);
    errdefer filtered.deinit();

    for (result.columns.items) |col| {
        try filtered.addColumn(col);
    }

    // Filter rows
    for (result.rows.items) |row| {
        const matches = try evaluateWhereOnJoinedRow(
            db,
            row.items,
            result.columns.items,
            cmd,
        );

        if (matches) {
            var filtered_row = ArrayList(ColumnValue).init(allocator);
            for (row.items) |val| {
                try filtered_row.append(try val.clone(allocator));
            }
            try filtered.addRow(filtered_row);
        }
    }

    return filtered;
}

// ============================================================================
// Row Emission for JOINs
// ============================================================================

/// Emit a joined row for nested loop joins
/// Handles both SELECT * and specific column selection
/// Accepts optional base_row and join_row (null for unmatched rows in outer joins)
fn emitNestedLoopJoinRow(
    result: *QueryResult,
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    _: []const u8, // join_table_name (unused but kept for signature consistency)
    base_row: ?*const Row,
    join_row: ?*const Row,
    select_all: bool,
    columns: []const sql.SelectColumn,
) !void {
    var result_row = ArrayList(ColumnValue).init(allocator);
    errdefer {
        for (result_row.items) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        result_row.deinit();
    }

    if (select_all) {
        // Add all columns from base table
        for (base_table.columns.items) |col| {
            const val = if (base_row) |br|
                br.get(col.name) orelse ColumnValue.null_value
            else
                ColumnValue.null_value;
            try result_row.append(try val.clone(allocator));
        }
        // Add all columns from join table
        for (join_table.columns.items) |col| {
            const val = if (join_row) |jr|
                jr.get(col.name) orelse ColumnValue.null_value
            else
                ColumnValue.null_value;
            try result_row.append(try val.clone(allocator));
        }
    } else {
        // Add only selected columns
        for (columns) |col_spec| {
            if (col_spec == .regular) {
                const col_name = col_spec.regular;
                const parts = splitQualifiedColumn(col_name);

                const val = if (parts.table) |tbl| blk: {
                    if (std.mem.eql(u8, tbl, base_table_name)) {
                        if (base_row) |br| {
                            break :blk br.get(parts.column) orelse ColumnValue.null_value;
                        } else {
                            break :blk ColumnValue.null_value;
                        }
                    } else {
                        if (join_row) |jr| {
                            break :blk jr.get(parts.column) orelse ColumnValue.null_value;
                        } else {
                            break :blk ColumnValue.null_value;
                        }
                    }
                } else blk: {
                    // Try both tables (unqualified column name)
                    if (base_row) |br| {
                        if (br.get(col_name)) |v| break :blk v;
                    }
                    if (join_row) |jr| {
                        if (jr.get(col_name)) |v| break :blk v;
                    }
                    break :blk ColumnValue.null_value;
                };

                try result_row.append(try val.clone(allocator));
            }
        }
    }

    try result.addRow(result_row);
}

/// Estimate the number of rows in a table
fn estimateTableSize(table: *Table, allocator: Allocator, snapshot: ?*const Snapshot, clog: ?*CommitLog) !usize {
    // Simple estimation: count all rows
    // In the future, we could track this in table metadata for O(1) access
    const row_ids = try table.getAllRows(allocator, snapshot, clog);
    defer allocator.free(row_ids);
    return row_ids.len;
}

/// Cost-based optimizer: decide whether to use hash join or nested loop
fn shouldUseHashJoin(base_table_size: usize, join_table_size: usize) bool {
    const total_size = base_table_size + join_table_size;

    // Threshold: below this size, nested loop is comparable or faster
    // (Hash join has overhead from hash table construction)
    const MIN_SIZE_FOR_HASH = 100;

    if (total_size < MIN_SIZE_FOR_HASH) {
        return false; // Use nested loop for small tables
    }

    // For larger tables, hash join is almost always faster for equi-joins
    // Nested loop cost: O(n * m) comparisons
    // Hash join cost: O(n + m) operations with some constant overhead

    const nested_cost = base_table_size * join_table_size;
    const hash_cost = total_size * 5; // Rough estimate with overhead factor

    return hash_cost < nested_cost;
}

// ============================================================================
// Multi-Table Join Support (N-way joins)
// ============================================================================

/// Column information in an intermediate result
const IntermediateColumnInfo = struct {
    /// Qualified name: "table.column"
    qualified_name: []const u8,
    /// Original table name
    table_name: []const u8,
    /// Column name without table qualifier
    column_name: []const u8,
    /// Index in the row array
    index: usize,

    allocator: Allocator,

    pub fn deinit(self: *IntermediateColumnInfo) void {
        self.allocator.free(self.qualified_name);
        self.allocator.free(self.table_name);
        self.allocator.free(self.column_name);
    }
};

/// Intermediate result from a join operation
/// Used in the pipeline when joining 3+ tables
const IntermediateResult = struct {
    /// Schema: column information with table qualifications
    schema: ArrayList(IntermediateColumnInfo),
    /// Rows: each row is a flat array of values matching schema order
    rows: ArrayList(ArrayList(ColumnValue)),
    /// Allocator for memory management
    allocator: Allocator,

    pub fn init(allocator: Allocator) IntermediateResult {
        return .{
            .schema = ArrayList(IntermediateColumnInfo).init(allocator),
            .rows = ArrayList(ArrayList(ColumnValue)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IntermediateResult) void {
        // Free schema
        for (self.schema.items) |*col_info| {
            col_info.deinit();
        }
        self.schema.deinit();

        // Free rows
        for (self.rows.items) |*row| {
            for (row.items) |*val| {
                var v = val.*;
                v.deinit(self.allocator);
            }
            row.deinit();
        }
        self.rows.deinit();
    }

    /// Add a column to the schema
    pub fn addColumn(
        self: *IntermediateResult,
        table_name: []const u8,
        column_name: []const u8,
    ) !void {
        const qualified = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}",
            .{ table_name, column_name },
        );
        errdefer self.allocator.free(qualified);

        const table_owned = try self.allocator.dupe(u8, table_name);
        errdefer self.allocator.free(table_owned);

        const column_owned = try self.allocator.dupe(u8, column_name);
        errdefer self.allocator.free(column_owned);

        try self.schema.append(.{
            .qualified_name = qualified,
            .table_name = table_owned,
            .column_name = column_owned,
            .index = self.schema.items.len,
            .allocator = self.allocator,
        });
    }

    /// Add a row to the result
    pub fn addRow(self: *IntermediateResult, row: ArrayList(ColumnValue)) !void {
        try self.rows.append(row);
    }

    /// Find column index by name (supports both qualified and unqualified)
    pub fn findColumn(self: *const IntermediateResult, col_name: []const u8) ?usize {
        // Try exact match on qualified name first
        for (self.schema.items) |col_info| {
            if (std.mem.eql(u8, col_info.qualified_name, col_name)) {
                return col_info.index;
            }
        }

        // Use column_matching helper for fuzzy resolution
        // Try matching against unqualified column names with fallback logic
        for (self.schema.items) |col_info| {
            if (column_matching.matchColumnName(col_info.column_name, col_name)) {
                return col_info.index;
            }
        }

        // Try matching against qualified names with fallback logic
        for (self.schema.items) |col_info| {
            if (column_matching.matchColumnName(col_info.qualified_name, col_name)) {
                return col_info.index;
            }
        }

        return null;
    }

    /// Convert to QueryResult for returning to user
    pub fn toQueryResult(self: *IntermediateResult, select_all: bool, selected_columns: []const sql.SelectColumn) !QueryResult {
        var result = QueryResult.init(self.allocator);
        errdefer result.deinit();

        if (select_all) {
            // Add all columns
            for (self.schema.items) |col_info| {
                try result.addColumn(col_info.qualified_name);
            }

            // Add all rows
            for (self.rows.items) |row| {
                var result_row = ArrayList(ColumnValue).init(self.allocator);
                for (row.items) |val| {
                    try result_row.append(try val.clone(self.allocator));
                }
                try result.addRow(result_row);
            }
        } else {
            // Add only selected columns
            for (selected_columns) |col_spec| {
                switch (col_spec) {
                    .regular => |col_name| try result.addColumn(col_name),
                    .aggregate => return error.AggregateNotSupportedInJoin,
                    .star => try result.addColumn("*"),
                }
            }

            // Project rows
            for (self.rows.items) |row| {
                var result_row = ArrayList(ColumnValue).init(self.allocator);

                for (selected_columns) |col_spec| {
                    if (col_spec == .regular) {
                        const col_name = col_spec.regular;
                        const idx = self.findColumn(col_name) orelse {
                            return error.ColumnNotFound;
                        };
                        const val = if (idx < row.items.len) row.items[idx] else ColumnValue.null_value;
                        try result_row.append(try val.clone(self.allocator));
                    }
                }

                try result.addRow(result_row);
            }
        }

        return result;
    }
};

// ============================================================================
// Main JOIN Execution
// ============================================================================

/// Execute a SELECT query with JOINs
/// Now supports N-table joins using a pipelined approach
pub fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // Get base table
    const base_table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Validate we have at least one join
    if (cmd.joins.items.len == 0) return error.NoJoins;

    // For single join, use the optimized 2-table path
    var result = if (cmd.joins.items.len == 1)
        try executeTwoTableJoin(db, cmd, base_table)
    else
        // For multiple joins (3+ tables), use the pipeline approach
        try executeMultiTableJoin(db, cmd, base_table);

    // Apply ORDER BY if present
    if (cmd.order_by) |order_by| {
        try applyOrderBy(&result, order_by);
    }

    // Apply LIMIT if present
    if (cmd.limit) |limit| {
        if (result.rows.items.len > limit) {
            // Free excess rows
            for (result.rows.items[limit..]) |*row| {
                for (row.items) |*val| {
                    var v = val.*;
                    v.deinit(result.allocator);
                }
                row.deinit();
            }
            // Truncate
            result.rows.items.len = limit;
        }
    }

    return result;
}

// ============================================================================
// Two-Table JOIN Implementation
// ============================================================================

/// Optimized path for 2-table joins (base table + 1 join)
fn executeTwoTableJoin(db: *Database, cmd: sql.SelectCmd, base_table: *Table) !QueryResult {
    var result = QueryResult.init(db.allocator);

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    const join = cmd.joins.items[0];
    const join_table = db.tables.get(join.table_name) orelse return sql.SqlError.TableNotFound;

    // Parse join columns
    const left_parts = splitQualifiedColumn(join.left_column);
    const right_parts = splitQualifiedColumn(join.right_column);

    // Cost-based optimizer: decide whether to use hash join or nested loop
    const base_size = try estimateTableSize(base_table, db.allocator, snapshot, clog);
    const join_size = try estimateTableSize(join_table, db.allocator, snapshot, clog);

    if (shouldUseHashJoin(base_size, join_size)) {
        // Use hash join for better performance on large tables
        const select_all = cmd.columns.items.len == 1 and cmd.columns.items[0] == .star;
        const has_where = cmd.where_column != null or cmd.where_expr != null;

        // If we have a WHERE clause and not SELECT *, we need to join with all columns first,
        // then filter, then project to the selected columns
        const join_with_all_columns = has_where and !select_all;

        var join_result = try hash_join.executeHashJoin(
            db.allocator,
            base_table,
            join_table,
            cmd.table_name,
            join.table_name,
            join.join_type,
            left_parts.column,
            right_parts.column,
            join_with_all_columns or select_all, // Include all columns if WHERE needs them
            if (join_with_all_columns) &[_]sql.SelectColumn{} else cmd.columns.items,
            snapshot,
            clog,
        );

        // Apply WHERE clause filter if present
        if (has_where) {
            var filtered = try applyWhereToQueryResult(db, &join_result, cmd);
            join_result.deinit();

            // If we joined with all columns but need specific columns, project now
            if (join_with_all_columns) {
                defer filtered.deinit();
                return projectToSelectedColumns(db.allocator, &filtered, cmd.columns.items);
            }

            return filtered;
        }

        return join_result;
    }

    // Fall back to nested loop join for small tables or when hash join is not beneficial
    // Setup result columns
    const select_all = cmd.columns.items.len == 1 and cmd.columns.items[0] == .star;
    const has_where = cmd.where_column != null or cmd.where_expr != null;

    // If we have WHERE and not SELECT *, include all columns for filtering, then project later
    const include_all_columns = select_all or (has_where and !select_all);

    if (include_all_columns) {
        // Include all columns from both tables with qualified names
        for (base_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                db.allocator,
                "{s}.{s}",
                .{ cmd.table_name, col.name },
            );
            defer db.allocator.free(qualified);
            try result.addColumn(qualified);
        }
        for (join_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                db.allocator,
                "{s}.{s}",
                .{ join.table_name, col.name },
            );
            defer db.allocator.free(qualified);
            try result.addColumn(qualified);
        }
    } else {
        // Add specified columns
        for (cmd.columns.items) |col_spec| {
            switch (col_spec) {
                .regular => |col_name| try result.addColumn(col_name),
                .aggregate => return error.AggregateNotSupportedInJoin,
                .star => try result.addColumn("*"),
            }
        }
    }

    // Get all rows from both tables (Phase 3: Use MVCC snapshot)
    const base_row_ids = try base_table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(base_row_ids);

    const join_row_ids = try join_table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(join_row_ids);

    // Perform nested loop join
    switch (join.join_type) {
        .inner => {
            // INNER JOIN: Only include matching rows
            for (base_row_ids) |base_id| {
                const base_row = base_table.get(base_id, snapshot, clog) orelse continue;
                const left_val = base_row.get(left_parts.column) orelse continue;

                for (join_row_ids) |join_id| {
                    const join_row = join_table.get(join_id, snapshot, clog) orelse continue;
                    const right_val = join_row.get(right_parts.column) orelse continue;

                    // Check join condition
                    if (valuesEqual(left_val, right_val)) {
                        // Match! Emit joined row
                        try emitNestedLoopJoinRow(
                            &result,
                            db.allocator,
                            base_table,
                            join_table,
                            cmd.table_name,
                            join.table_name,
                            base_row,
                            join_row,
                            include_all_columns,
                            cmd.columns.items,
                        );
                    }
                }
            }
        },
        .left => {
            // LEFT JOIN: Include all rows from base table, with NULLs for unmatched join table rows
            for (base_row_ids) |base_id| {
                const base_row = base_table.get(base_id, snapshot, clog) orelse continue;
                const left_val = base_row.get(left_parts.column) orelse {
                    // Base row has NULL in join column - still include with NULLs for join table
                    try emitNestedLoopJoinRow(
                        &result,
                        db.allocator,
                        base_table,
                        join_table,
                        cmd.table_name,
                        join.table_name,
                        base_row,
                        null, // No join row (emit NULLs)
                        include_all_columns,
                        cmd.columns.items,
                    );
                    continue;
                };

                var matched = false;

                for (join_row_ids) |join_id| {
                    const join_row = join_table.get(join_id, snapshot, clog) orelse continue;
                    const right_val = join_row.get(right_parts.column) orelse continue;

                    if (valuesEqual(left_val, right_val)) {
                        matched = true;
                        // Match! Emit joined row
                        try emitNestedLoopJoinRow(
                            &result,
                            db.allocator,
                            base_table,
                            join_table,
                            cmd.table_name,
                            join.table_name,
                            base_row,
                            join_row,
                            include_all_columns,
                            cmd.columns.items,
                        );
                    }
                }

                // LEFT JOIN: If no match, still include base row with NULLs for join table
                if (!matched) {
                    try emitNestedLoopJoinRow(
                        &result,
                        db.allocator,
                        base_table,
                        join_table,
                        cmd.table_name,
                        join.table_name,
                        base_row,
                        null, // No join row (emit NULLs)
                        include_all_columns,
                        cmd.columns.items,
                    );
                }
            }
        },
        .right => {
            // RIGHT JOIN: Include all rows from join table, with NULLs for unmatched base table rows
            // This is similar to LEFT JOIN but with tables swapped
            for (join_row_ids) |join_id| {
                const join_row = join_table.get(join_id, snapshot, clog) orelse continue;
                const right_val = join_row.get(right_parts.column) orelse {
                    // Join row has NULL in join column - still include with NULLs for base table
                    try emitNestedLoopJoinRow(
                        &result,
                        db.allocator,
                        base_table,
                        join_table,
                        cmd.table_name,
                        join.table_name,
                        null, // No base row (emit NULLs)
                        join_row,
                        include_all_columns,
                        cmd.columns.items,
                    );
                    continue;
                };

                var matched = false;

                for (base_row_ids) |base_id| {
                    const base_row = base_table.get(base_id, snapshot, clog) orelse continue;
                    const left_val = base_row.get(left_parts.column) orelse continue;

                    if (valuesEqual(left_val, right_val)) {
                        matched = true;
                        // Match! Emit joined row
                        try emitNestedLoopJoinRow(
                            &result,
                            db.allocator,
                            base_table,
                            join_table,
                            cmd.table_name,
                            join.table_name,
                            base_row,
                            join_row,
                            include_all_columns,
                            cmd.columns.items,
                        );
                    }
                }

                // RIGHT JOIN: If no match, still include join row with NULLs for base table
                if (!matched) {
                    try emitNestedLoopJoinRow(
                        &result,
                        db.allocator,
                        base_table,
                        join_table,
                        cmd.table_name,
                        join.table_name,
                        null, // No base row (emit NULLs)
                        join_row,
                        include_all_columns,
                        cmd.columns.items,
                    );
                }
            }
        },
    }

    // Apply WHERE clause filter if present
    if (has_where) {
        var filtered = try applyWhereToQueryResult(db, &result, cmd);
        result.deinit();

        // If we included all columns but need specific columns, project now
        if (include_all_columns and !select_all) {
            defer filtered.deinit();
            return projectToSelectedColumns(db.allocator, &filtered, cmd.columns.items);
        }

        return filtered;
    }

    return result;
}

// ============================================================================
// Multi-Table JOIN Implementation
// ============================================================================

/// Pipeline approach for multi-table joins (3+ tables)
fn executeMultiTableJoin(db: *Database, cmd: sql.SelectCmd, base_table: *Table) !QueryResult {
    const select_all = cmd.columns.items.len == 1 and cmd.columns.items[0] == .star;

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    // Start with base table and build intermediate result
    var current_intermediate = IntermediateResult.init(db.allocator);
    errdefer current_intermediate.deinit();

    // Build schema from base table
    for (base_table.columns.items) |col| {
        try current_intermediate.addColumn(cmd.table_name, col.name);
    }

    // Get all rows from base table and add to intermediate
    const base_row_ids = try base_table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(base_row_ids);

    for (base_row_ids) |row_id| {
        const base_row = base_table.get(row_id, snapshot, clog) orelse continue;
        var row_values = ArrayList(ColumnValue).init(db.allocator);

        for (base_table.columns.items) |col| {
            const val = base_row.get(col.name) orelse ColumnValue.null_value;
            try row_values.append(try val.clone(db.allocator));
        }

        try current_intermediate.addRow(row_values);
    }

    // Process each join sequentially
    for (cmd.joins.items) |join_clause| {
        const join_table = db.tables.get(join_clause.table_name) orelse return sql.SqlError.TableNotFound;

        // Execute this join stage
        const next_intermediate = try executeJoinStage(
            db.allocator,
            &current_intermediate,
            join_table,
            join_clause,
            snapshot,
            clog,
        );

        // Clean up previous intermediate and use the new one
        current_intermediate.deinit();
        current_intermediate = next_intermediate;
    }

    // Apply WHERE clause filter if present
    if (cmd.where_column != null or cmd.where_expr != null) {
        const filtered = try applyWhereFilter(db, &current_intermediate, cmd);
        current_intermediate.deinit();
        current_intermediate = filtered;
    }

    // Convert final intermediate result to QueryResult
    defer current_intermediate.deinit();
    return try current_intermediate.toQueryResult(select_all, cmd.columns.items);
}

// ============================================================================
// Join Pipeline Stage Execution
// ============================================================================

/// Execute a single join stage in the pipeline
fn executeJoinStage(
    allocator: Allocator,
    left_intermediate: *IntermediateResult,
    right_table: *Table,
    join_clause: sql.JoinClause,
    snapshot: ?*const Snapshot,
    clog: ?*CommitLog,
) !IntermediateResult {
    var result = IntermediateResult.init(allocator);
    errdefer result.deinit();

    // Build schema: left columns + right columns
    for (left_intermediate.schema.items) |col_info| {
        try result.addColumn(col_info.table_name, col_info.column_name);
    }
    for (right_table.columns.items) |col| {
        try result.addColumn(join_clause.table_name, col.name);
    }

    // Parse right join column name (remove table prefix if present)
    const right_parts = splitQualifiedColumn(join_clause.right_column);

    // Get all rows from right table (Phase 3: Use MVCC snapshot)
    const right_row_ids = try right_table.getAllRows(allocator, snapshot, clog);
    defer allocator.free(right_row_ids);

    // Execute join based on type
    switch (join_clause.join_type) {
        .inner => {
            // INNER JOIN: only matching rows
            for (left_intermediate.rows.items) |left_row| {
                // Get left join key - use full qualified name if available
                const left_col_idx = left_intermediate.findColumn(join_clause.left_column) orelse continue;
                if (left_col_idx >= left_row.items.len) continue;
                const left_val = left_row.items[left_col_idx];

                if (left_val == .null_value) continue; // NULL doesn't match

                // Find matching right rows
                for (right_row_ids) |right_id| {
                    const right_row = right_table.get(right_id, snapshot, clog) orelse continue;
                    const right_val = right_row.get(right_parts.column) orelse continue;

                    if (valuesEqual(left_val, right_val)) {
                        // Match! Create combined row
                        var combined = ArrayList(ColumnValue).init(allocator);

                        // Add left values
                        for (left_row.items) |val| {
                            try combined.append(try val.clone(allocator));
                        }

                        // Add right values
                        for (right_table.columns.items) |col| {
                            const val = right_row.get(col.name) orelse ColumnValue.null_value;
                            try combined.append(try val.clone(allocator));
                        }

                        try result.addRow(combined);
                    }
                }
            }
        },
        .left => {
            // LEFT JOIN: all left rows, NULLs for unmatched
            for (left_intermediate.rows.items) |left_row| {
                const left_col_idx = left_intermediate.findColumn(join_clause.left_column);
                var matched = false;

                if (left_col_idx) |idx| {
                    if (idx < left_row.items.len) {
                        const left_val = left_row.items[idx];

                        if (left_val != .null_value) {
                            // Try to find matches
                            for (right_row_ids) |right_id| {
                                const right_row = right_table.get(right_id, snapshot, clog) orelse continue;
                                const right_val = right_row.get(right_parts.column) orelse continue;

                                if (valuesEqual(left_val, right_val)) {
                                    matched = true;

                                    var combined = ArrayList(ColumnValue).init(allocator);
                                    for (left_row.items) |val| {
                                        try combined.append(try val.clone(allocator));
                                    }
                                    for (right_table.columns.items) |col| {
                                        const val = right_row.get(col.name) orelse ColumnValue.null_value;
                                        try combined.append(try val.clone(allocator));
                                    }
                                    try result.addRow(combined);
                                }
                            }
                        }
                    }
                }

                // No match: emit with NULLs
                if (!matched) {
                    var combined = ArrayList(ColumnValue).init(allocator);
                    for (left_row.items) |val| {
                        try combined.append(try val.clone(allocator));
                    }
                    for (right_table.columns.items) |_| {
                        try combined.append(ColumnValue.null_value);
                    }
                    try result.addRow(combined);
                }
            }
        },
        .right => {
            // RIGHT JOIN: all right rows, NULLs for unmatched left
            // Track which right rows were matched
            var matched_right = std.AutoHashMap(u64, bool).init(allocator);
            defer matched_right.deinit();

            // First pass: emit all matches
            for (right_row_ids) |right_id| {
                const right_row = right_table.get(right_id, snapshot, clog) orelse continue;
                const right_val = right_row.get(right_parts.column);
                var this_right_matched = false;

                if (right_val) |rv| {
                    if (rv != .null_value) {
                        for (left_intermediate.rows.items) |left_row| {
                            const left_col_idx = left_intermediate.findColumn(join_clause.left_column) orelse continue;
                            if (left_col_idx >= left_row.items.len) continue;
                            const left_val = left_row.items[left_col_idx];

                            if (valuesEqual(left_val, rv)) {
                                this_right_matched = true;

                                var combined = ArrayList(ColumnValue).init(allocator);
                                for (left_row.items) |val| {
                                    try combined.append(try val.clone(allocator));
                                }
                                for (right_table.columns.items) |col| {
                                    const val = right_row.get(col.name) orelse ColumnValue.null_value;
                                    try combined.append(try val.clone(allocator));
                                }
                                try result.addRow(combined);
                            }
                        }
                    }
                }

                if (this_right_matched) {
                    try matched_right.put(right_id, true);
                }
            }

            // Second pass: emit unmatched right rows with NULLs for left
            for (right_row_ids) |right_id| {
                if (matched_right.contains(right_id)) continue;

                const right_row = right_table.get(right_id, snapshot, clog) orelse continue;
                var combined = ArrayList(ColumnValue).init(allocator);

                // NULLs for all left columns
                for (left_intermediate.schema.items) |_| {
                    try combined.append(ColumnValue.null_value);
                }

                // Values from right table
                for (right_table.columns.items) |col| {
                    const val = right_row.get(col.name) orelse ColumnValue.null_value;
                    try combined.append(try val.clone(allocator));
                }

                try result.addRow(combined);
            }
        },
    }

    return result;
}

// ============================================================================
// WHERE Clause Filtering for Multi-Table JOINs
// ============================================================================

/// Apply WHERE clause filter to intermediate result (post-join filtering)
fn applyWhereFilter(
    db: *Database,
    intermediate: *IntermediateResult,
    cmd: sql.SelectCmd,
) !IntermediateResult {
    const allocator = db.allocator;
    var filtered = IntermediateResult.init(allocator);
    errdefer filtered.deinit();

    // Copy schema from input
    for (intermediate.schema.items) |col_info| {
        try filtered.addColumn(col_info.table_name, col_info.column_name);
    }

    // Build column names array for evaluateWhereOnJoinedRow
    var column_names = ArrayList([]const u8).init(allocator);
    defer column_names.deinit();

    for (intermediate.schema.items) |col_info| {
        try column_names.append(col_info.qualified_name);
    }

    // Filter rows using our shared helper
    for (intermediate.rows.items) |row| {
        const matches = try evaluateWhereOnJoinedRow(
            db,
            row.items,
            column_names.items,
            cmd,
        );

        if (matches) {
            var filtered_row = ArrayList(ColumnValue).init(allocator);
            for (row.items) |val| {
                try filtered_row.append(try val.clone(allocator));
            }
            try filtered.addRow(filtered_row);
        }
    }

    return filtered;
}
