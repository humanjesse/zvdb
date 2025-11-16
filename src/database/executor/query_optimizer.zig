const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const sql = @import("../../sql.zig");
const IndexManager = @import("../../index_manager.zig").IndexManager;
const IndexInfo = @import("../../index_manager.zig").IndexInfo;

/// Type of index scan operation
pub const IndexScanType = enum {
    /// Exact match: WHERE col = value
    exact_match,
    /// Range scan: WHERE col > value, WHERE col < value, etc.
    range_scan,
    /// Multiple index scans combined (for OR conditions)
    multi_index,
};

/// Represents an index scan operation
pub const IndexScan = struct {
    index: *IndexInfo,
    scan_type: IndexScanType,

    // For exact match
    exact_value: ?ColumnValue,

    // For range scans
    min_value: ?ColumnValue,
    max_value: ?ColumnValue,
    min_inclusive: bool,
    max_inclusive: bool,

    // Estimated selectivity (fraction of rows that match: 0.0 to 1.0)
    selectivity: f64,
};

/// Execution strategy for a query
pub const ExecutionStrategy = union(enum) {
    /// Full table scan - no index used
    table_scan: struct {
        estimated_rows: usize,
    },

    /// Single index scan
    index_scan: IndexScan,

    /// Multiple index scans (for OR conditions)
    multi_index_scan: struct {
        scans: []IndexScan,
    },
};

/// Cost estimate for a query plan
pub const CostEstimate = struct {
    /// Estimated number of rows that will be returned
    estimated_rows: usize,

    /// Estimated cost (arbitrary units - lower is better)
    /// For comparison: table_scan_cost ≈ num_rows, index_scan_cost ≈ log(num_rows) * selectivity
    cost: f64,
};

/// Complete query execution plan
pub const QueryPlan = struct {
    strategy: ExecutionStrategy,
    cost: CostEstimate,
};

/// Query optimizer - chooses the best execution strategy
pub const QueryOptimizer = struct {
    allocator: std.mem.Allocator,
    db: *Database,
    table: *Table,

    pub fn init(allocator: std.mem.Allocator, db: *Database, table: *Table) QueryOptimizer {
        return .{
            .allocator = allocator,
            .db = db,
            .table = table,
        };
    }

    /// Main optimization entry point - generates the best query plan
    pub fn optimize(self: *QueryOptimizer, cmd: sql.SelectCmd) !QueryPlan {
        // Get table statistics
        const snapshot = self.db.getCurrentSnapshot();
        const clog = self.db.getClog();
        const total_rows = blk: {
            const rows = try self.table.getAllRows(self.allocator, snapshot, clog);
            defer self.allocator.free(rows);
            break :blk rows.len;
        };

        // If table is tiny (< 100 rows), just use table scan
        if (total_rows < 100) {
            return QueryPlan{
                .strategy = .{ .table_scan = .{ .estimated_rows = total_rows } },
                .cost = .{ .estimated_rows = total_rows, .cost = @floatFromInt(total_rows) },
            };
        }

        // Try to find an index scan plan
        const index_plan = try self.findIndexPlan(cmd, total_rows);

        // Cost of full table scan
        const table_scan_cost = @as(f64, @floatFromInt(total_rows));

        // Compare costs and choose the best plan
        if (index_plan) |plan| {
            if (plan.cost.cost < table_scan_cost * 0.8) { // Use index if it's at least 20% better
                return plan;
            }
        }

        // Fallback to table scan
        return QueryPlan{
            .strategy = .{ .table_scan = .{ .estimated_rows = total_rows } },
            .cost = .{ .estimated_rows = total_rows, .cost = table_scan_cost },
        };
    }

    /// Try to find a plan using indexes
    fn findIndexPlan(self: *QueryOptimizer, cmd: sql.SelectCmd, total_rows: usize) !?QueryPlan {
        // Check for simple WHERE column = value
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                return try self.optimizeSimpleWhere(cmd.table_name, where_col, where_val, total_rows);
            }
        }

        // Check for complex WHERE expression
        if (cmd.where_expr) |expr| {
            return try self.optimizeComplexWhere(cmd.table_name, expr, total_rows);
        }

        return null;
    }

    /// Optimize a simple WHERE column = value
    fn optimizeSimpleWhere(
        self: *QueryOptimizer,
        table_name: []const u8,
        column_name: []const u8,
        value: ColumnValue,
        total_rows: usize,
    ) !?QueryPlan {
        // Look for an index on this column
        const index = self.db.index_manager.findIndexForColumn(table_name, column_name) orelse return null;

        // Estimate selectivity (for now, assume 1% for equality - in reality, use statistics)
        const selectivity = self.estimateEqualitySelectivity(value, total_rows);
        const estimated_rows = @as(usize, @intFromFloat(@as(f64, @floatFromInt(total_rows)) * selectivity));

        // Cost of index scan: log(N) to find + selectivity * N to read matching rows
        const log_cost = @log(@as(f64, @floatFromInt(total_rows))) / @log(2.0);
        const index_cost = log_cost + (@as(f64, @floatFromInt(total_rows)) * selectivity);

        return QueryPlan{
            .strategy = .{
                .index_scan = .{
                    .index = index,
                    .scan_type = .exact_match,
                    .exact_value = value,
                    .min_value = null,
                    .max_value = null,
                    .min_inclusive = false,
                    .max_inclusive = false,
                    .selectivity = selectivity,
                },
            },
            .cost = .{
                .estimated_rows = estimated_rows,
                .cost = index_cost,
            },
        };
    }

    /// Optimize complex WHERE expressions (AND, OR, comparison operators)
    fn optimizeComplexWhere(
        self: *QueryOptimizer,
        table_name: []const u8,
        expr: sql.WhereExpr,
        total_rows: usize,
    ) !?QueryPlan {
        switch (expr) {
            .binary_op => |bin_op| {
                // Handle range queries: col > value, col < value, col >= value, col <= value
                if (bin_op.left == .column and bin_op.right == .literal) {
                    const column = bin_op.left.column;
                    const value = bin_op.right.literal;

                    switch (bin_op.op) {
                        .eq => {
                            // Simple equality - delegate to optimizeSimpleWhere
                            return try self.optimizeSimpleWhere(table_name, column, value, total_rows);
                        },
                        .lt, .lte, .gt, .gte => {
                            // Range query - try to use index
                            return try self.optimizeRangeQuery(table_name, column, bin_op.op, value, total_rows);
                        },
                        else => {},
                    }
                }

                // Handle BETWEEN: (col >= min) AND (col <= max)
                if (bin_op.op == .@"and") {
                    return try self.optimizeBetween(table_name, bin_op, total_rows);
                }

                // Handle OR: (col = val1) OR (col = val2)
                if (bin_op.op == .@"or") {
                    return try self.optimizeOr(table_name, bin_op, total_rows);
                }
            },
            else => {},
        }

        return null;
    }

    /// Optimize range queries: col > value, col < value, etc.
    fn optimizeRangeQuery(
        self: *QueryOptimizer,
        table_name: []const u8,
        column_name: []const u8,
        op: sql.BinaryOp,
        value: ColumnValue,
        total_rows: usize,
    ) !?QueryPlan {
        // Look for an index on this column
        const index = self.db.index_manager.findIndexForColumn(table_name, column_name) orelse return null;

        // Estimate selectivity for range queries
        const selectivity = self.estimateRangeSelectivity(op, total_rows);
        const estimated_rows = @as(usize, @intFromFloat(@as(f64, @floatFromInt(total_rows)) * selectivity));

        // Determine min/max values and inclusivity
        var min_value: ?ColumnValue = null;
        var max_value: ?ColumnValue = null;
        var min_inclusive = false;
        var max_inclusive = false;

        switch (op) {
            .gt => {
                min_value = value;
                min_inclusive = false;
            },
            .gte => {
                min_value = value;
                min_inclusive = true;
            },
            .lt => {
                max_value = value;
                max_inclusive = false;
            },
            .lte => {
                max_value = value;
                max_inclusive = true;
            },
            else => unreachable,
        }

        // Cost of range scan: log(N) + selectivity * N
        const log_cost = @log(@as(f64, @floatFromInt(total_rows))) / @log(2.0);
        const index_cost = log_cost + (@as(f64, @floatFromInt(total_rows)) * selectivity);

        return QueryPlan{
            .strategy = .{
                .index_scan = .{
                    .index = index,
                    .scan_type = .range_scan,
                    .exact_value = null,
                    .min_value = min_value,
                    .max_value = max_value,
                    .min_inclusive = min_inclusive,
                    .max_inclusive = max_inclusive,
                    .selectivity = selectivity,
                },
            },
            .cost = .{
                .estimated_rows = estimated_rows,
                .cost = index_cost,
            },
        };
    }

    /// Optimize BETWEEN queries: col >= min AND col <= max
    fn optimizeBetween(
        self: *QueryOptimizer,
        table_name: []const u8,
        and_expr: sql.BinaryOpExpr,
        total_rows: usize,
    ) !?QueryPlan {
        // Try to detect pattern: (col >= min) AND (col <= max)
        if (and_expr.left.* == .binary_op and and_expr.right.* == .binary_op) {
            const left = and_expr.left.binary_op;
            const right = and_expr.right.binary_op;

            // Check if both sides reference the same column
            if (left.left == .column and right.left == .column and
                left.right == .literal and right.right == .literal)
            {
                const col1 = left.left.column;
                const col2 = right.left.column;

                if (std.mem.eql(u8, col1, col2)) {
                    // Same column - check if it's a BETWEEN pattern
                    const is_between = (left.op == .gte or left.op == .gt) and
                        (right.op == .lte or right.op == .lt);

                    if (is_between) {
                        // Look for an index
                        const index = self.db.index_manager.findIndexForColumn(table_name, col1) orelse return null;

                        // Estimate selectivity (BETWEEN is typically more selective)
                        const selectivity = 0.1; // Assume 10% for BETWEEN
                        const estimated_rows = @as(usize, @intFromFloat(@as(f64, @floatFromInt(total_rows)) * selectivity));

                        const log_cost = @log(@as(f64, @floatFromInt(total_rows))) / @log(2.0);
                        const index_cost = log_cost + (@as(f64, @floatFromInt(total_rows)) * selectivity);

                        return QueryPlan{
                            .strategy = .{
                                .index_scan = .{
                                    .index = index,
                                    .scan_type = .range_scan,
                                    .exact_value = null,
                                    .min_value = left.right.literal,
                                    .max_value = right.right.literal,
                                    .min_inclusive = left.op == .gte,
                                    .max_inclusive = right.op == .lte,
                                    .selectivity = selectivity,
                                },
                            },
                            .cost = .{
                                .estimated_rows = estimated_rows,
                                .cost = index_cost,
                            },
                        };
                    }
                }
            }
        }

        return null;
    }

    /// Optimize OR queries (could use multiple indexes)
    fn optimizeOr(
        self: *QueryOptimizer,
        table_name: []const u8,
        or_expr: sql.BinaryOpExpr,
        total_rows: usize,
    ) !?QueryPlan {
        _ = self;
        _ = table_name;
        _ = or_expr;
        _ = total_rows;

        // For now, don't optimize OR queries
        // In the future, we could union multiple index scans
        return null;
    }

    /// Estimate selectivity for equality predicates
    fn estimateEqualitySelectivity(self: *QueryOptimizer, value: ColumnValue, total_rows: usize) f64 {
        _ = self;
        _ = value;
        _ = total_rows;

        // In a real database, we would:
        // 1. Maintain histograms or samples of column values
        // 2. Track number of distinct values (cardinality)
        // 3. Use formula: selectivity = 1 / num_distinct_values
        //
        // For now, use heuristics:
        // - Assume primary keys are highly selective (1/N)
        // - Assume other columns are moderately selective (1%)

        // Simple heuristic: assume 1% selectivity for equality
        return 0.01;
    }

    /// Estimate selectivity for range predicates
    fn estimateRangeSelectivity(self: *QueryOptimizer, op: sql.BinaryOp, total_rows: usize) f64 {
        _ = self;
        _ = total_rows;

        // In a real database, we would:
        // 1. Use histograms to estimate the fraction of values in a range
        // 2. Track min/max values for each column
        // 3. Use formula: selectivity = (value - min) / (max - min)
        //
        // For now, use simple heuristics:
        return switch (op) {
            .lt, .lte, .gt, .gte => 0.33, // Assume 33% for single-ended ranges
            else => 0.5,
        };
    }
};

/// Execute a query using the optimized plan
pub fn executeWithPlan(
    db: *Database,
    table: *Table,
    plan: QueryPlan,
    allocator: std.mem.Allocator,
) ![]u64 {
    switch (plan.strategy) {
        .table_scan => {
            // Full table scan
            const snapshot = db.getCurrentSnapshot();
            const clog = db.getClog();
            return try table.getAllRows(allocator, snapshot, clog);
        },
        .index_scan => |scan| {
            return try executeIndexScan(scan, allocator);
        },
        .multi_index_scan => |multi| {
            // Union results from multiple index scans
            var all_results = std.ArrayList(u64).init(allocator);
            errdefer all_results.deinit();

            for (multi.scans) |scan| {
                const scan_results = try executeIndexScan(scan, allocator);
                defer allocator.free(scan_results);

                try all_results.appendSlice(scan_results);
            }

            // Remove duplicates and sort
            const results = try all_results.toOwnedSlice();
            std.mem.sort(u64, results, {}, std.sort.asc(u64));

            return results;
        },
    }
}

/// Execute a single index scan
fn executeIndexScan(scan: IndexScan, allocator: std.mem.Allocator) ![]u64 {
    switch (scan.scan_type) {
        .exact_match => {
            // Use B-tree search for exact match
            return try scan.index.btree.search(scan.exact_value.?);
        },
        .range_scan => {
            // Use B-tree findRange for range queries
            const min = scan.min_value orelse ColumnValue{ .int = std.math.minInt(i64) };
            const max = scan.max_value orelse ColumnValue{ .int = std.math.maxInt(i64) };

            return try scan.index.btree.findRange(min, max, scan.min_inclusive, scan.max_inclusive);
        },
        .multi_index => {
            // Not implemented yet
            return try allocator.alloc(u64, 0);
        },
    }
}
