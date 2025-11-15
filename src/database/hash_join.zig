const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const Table = @import("../table.zig").Table;
const ColumnValue = @import("../table.zig").ColumnValue;
const Row = @import("../table.zig").Row;
const sql = @import("../sql.zig");
const JoinType = sql.JoinType;
const SelectColumn = sql.SelectColumn;
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const AutoHashMap = std.AutoHashMap;

// ============================================================================
// Hash Function for Join Keys
// ============================================================================

/// Hash a ColumnValue for use in hash joins
/// This must be deterministic: same value always produces same hash
pub fn hashColumnValue(value: ColumnValue) u64 {
    var hasher = std.hash.Wyhash.init(0);

    switch (value) {
        .null_value => {
            // Special hash for NULL (though we skip NULLs in practice)
            hasher.update(&[_]u8{0xFF});
        },
        .int => |i| {
            const bytes = std.mem.asBytes(&i);
            hasher.update(bytes);
        },
        .float => |f| {
            // Use bit representation to handle float equality correctly
            const bits: u64 = @bitCast(f);
            const bytes = std.mem.asBytes(&bits);
            hasher.update(bytes);
        },
        .text => |s| {
            hasher.update(s);
        },
        .bool => |b| {
            hasher.update(&[_]u8{if (b) 1 else 0});
        },
        .embedding => {
            // Embeddings shouldn't be join keys in practice
            // Hash first element or return special value
            hasher.update(&[_]u8{0xEE});
        },
    }

    return hasher.final();
}

// ============================================================================
// Join Hash Table
// ============================================================================

/// Hash table for join operations
/// Maps hash(join_key) -> list of row IDs with that key
pub const JoinHashTable = struct {
    /// Hash buckets: hash value -> list of row IDs
    buckets: AutoHashMap(u64, ArrayList(u64)),

    /// Track all inserted row IDs (for LEFT/RIGHT join tracking)
    all_row_ids: ArrayList(u64),

    /// Track which rows were matched during probe (for LEFT/RIGHT joins)
    matched_rows: AutoHashMap(u64, bool),

    allocator: Allocator,

    pub fn init(allocator: Allocator) JoinHashTable {
        return .{
            .buckets = AutoHashMap(u64, ArrayList(u64)).init(allocator),
            .all_row_ids = ArrayList(u64).init(allocator),
            .matched_rows = AutoHashMap(u64, bool).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *JoinHashTable) void {
        // Free all bucket lists
        var it = self.buckets.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.deinit();
        }
        self.buckets.deinit();

        self.all_row_ids.deinit();
        self.matched_rows.deinit();
    }

    /// Insert a row into the hash table
    pub fn insert(self: *JoinHashTable, hash: u64, row_id: u64) !void {
        // Get or create bucket for this hash
        const gop = try self.buckets.getOrPut(hash);
        if (!gop.found_existing) {
            gop.value_ptr.* = ArrayList(u64).init(self.allocator);
        }

        // Add row ID to bucket
        try gop.value_ptr.append(row_id);
    }

    /// Probe the hash table for matching rows
    /// Returns null if no matches found
    pub fn probe(self: *const JoinHashTable, hash: u64) ?[]const u64 {
        if (self.buckets.get(hash)) |row_ids| {
            return row_ids.items;
        }
        return null;
    }

    /// Mark a row as matched (for LEFT/RIGHT join tracking)
    pub fn markMatched(self: *JoinHashTable, row_id: u64) !void {
        try self.matched_rows.put(row_id, true);
    }

    /// Check if a row was matched
    pub fn wasMatched(self: *const JoinHashTable, row_id: u64) bool {
        return self.matched_rows.contains(row_id);
    }

    /// Get all row IDs that were inserted
    pub fn getAllRowIds(self: *const JoinHashTable) []const u64 {
        return self.all_row_ids.items;
    }
};

// ============================================================================
// Hash Join Build Phase
// ============================================================================

/// Build a hash table from a table's join column
pub fn buildHashTable(
    allocator: Allocator,
    table: *Table,
    join_column: []const u8,
) !JoinHashTable {
    var hash_table = JoinHashTable.init(allocator);
    errdefer hash_table.deinit();

    // Get all rows from the build table
    const row_ids = try table.getAllRows(allocator);
    defer allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id) orelse continue;
        const key_value = row.get(join_column) orelse continue;

        // Skip NULL keys (NULL doesn't match NULL in SQL)
        if (key_value == .null_value) continue;

        // Hash the join key
        const hash = hashColumnValue(key_value);

        // Add to hash table
        try hash_table.insert(hash, row_id);
        try hash_table.all_row_ids.append(row_id);
    }

    return hash_table;
}

// ============================================================================
// Helper: Split qualified column names
// ============================================================================

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
// Helper: Emit a joined row
// ============================================================================

fn emitJoinedRow(
    result: *QueryResult,
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    _: []const u8,
    base_row: ?*const Row,
    join_row: ?*const Row,
    select_all: bool,
    columns: []const SelectColumn,
) !void {
    var result_row = ArrayList(ColumnValue).init(allocator);

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

// ============================================================================
// Hash Join Execution
// ============================================================================

/// Execute a hash join between two tables
pub fn executeHashJoin(
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    join_table_name: []const u8,
    join_type: JoinType,
    left_column: []const u8,
    right_column: []const u8,
    select_all: bool,
    columns: []const SelectColumn,
) !QueryResult {
    var result = QueryResult.init(allocator);

    // Setup result columns
    if (select_all) {
        // SELECT * includes all columns from both tables with qualified names
        for (base_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                allocator,
                "{s}.{s}",
                .{ base_table_name, col.name },
            );
            defer allocator.free(qualified);
            try result.addColumn(qualified);
        }
        for (join_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                allocator,
                "{s}.{s}",
                .{ join_table_name, col.name },
            );
            defer allocator.free(qualified);
            try result.addColumn(qualified);
        }
    } else {
        // Add specified columns
        for (columns) |col_spec| {
            switch (col_spec) {
                .regular => |col_name| try result.addColumn(col_name),
                .aggregate => return error.AggregateNotSupportedInJoin,
                .star => try result.addColumn("*"),
            }
        }
    }

    switch (join_type) {
        .inner => {
            // INNER JOIN using hash join
            try executeInnerHashJoin(
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                left_column,
                right_column,
                select_all,
                columns,
                &result,
            );
        },
        .left => {
            // LEFT JOIN using hash join
            try executeLeftHashJoin(
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                left_column,
                right_column,
                select_all,
                columns,
                &result,
            );
        },
        .right => {
            // RIGHT JOIN using hash join
            try executeRightHashJoin(
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                left_column,
                right_column,
                select_all,
                columns,
                &result,
            );
        },
    }

    return result;
}

// ============================================================================
// INNER JOIN Implementation
// ============================================================================

fn executeInnerHashJoin(
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    join_table_name: []const u8,
    left_column: []const u8,
    right_column: []const u8,
    select_all: bool,
    columns: []const SelectColumn,
    result: *QueryResult,
) !void {
    // Build phase: hash the join table (build table)
    var build_hash = try buildHashTable(allocator, join_table, right_column);
    defer build_hash.deinit();

    // Probe phase: scan the base table (probe table)
    const base_row_ids = try base_table.getAllRows(allocator);
    defer allocator.free(base_row_ids);

    for (base_row_ids) |base_id| {
        const base_row = base_table.get(base_id) orelse continue;
        const probe_key = base_row.get(left_column) orelse continue;

        // Skip NULL keys (NULL doesn't match NULL in SQL)
        if (probe_key == .null_value) continue;

        // Hash the probe key and lookup in hash table
        const probe_hash = hashColumnValue(probe_key);

        if (build_hash.probe(probe_hash)) |matching_ids| {
            // Found potential matches - verify with actual equality check
            for (matching_ids) |join_id| {
                const join_row = join_table.get(join_id) orelse continue;
                const right_val = join_row.get(right_column) orelse continue;

                // Double-check equality to handle hash collisions
                if (valuesEqual(probe_key, right_val)) {
                    // Match! Emit joined row
                    try emitJoinedRow(
                        result,
                        allocator,
                        base_table,
                        join_table,
                        base_table_name,
                        join_table_name,
                        base_row,
                        join_row,
                        select_all,
                        columns,
                    );
                }
            }
        }
    }
}

// ============================================================================
// LEFT JOIN Implementation
// ============================================================================

fn executeLeftHashJoin(
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    join_table_name: []const u8,
    left_column: []const u8,
    right_column: []const u8,
    select_all: bool,
    columns: []const SelectColumn,
    result: *QueryResult,
) !void {
    // Build phase: hash the join table
    var build_hash = try buildHashTable(allocator, join_table, right_column);
    defer build_hash.deinit();

    // Track which base rows were matched
    var matched_base_rows = AutoHashMap(u64, bool).init(allocator);
    defer matched_base_rows.deinit();

    // Probe phase: scan the base table
    const base_row_ids = try base_table.getAllRows(allocator);
    defer allocator.free(base_row_ids);

    for (base_row_ids) |base_id| {
        const base_row = base_table.get(base_id) orelse continue;
        const probe_key = base_row.get(left_column) orelse {
            // Base row has NULL in join column - emit with NULLs for join table
            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                base_row,
                null,
                select_all,
                columns,
            );
            continue;
        };

        if (probe_key == .null_value) {
            // NULL key - emit with NULLs for join table
            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                base_row,
                null,
                select_all,
                columns,
            );
            continue;
        }

        // Hash and probe
        const probe_hash = hashColumnValue(probe_key);
        var found_match = false;

        if (build_hash.probe(probe_hash)) |matching_ids| {
            for (matching_ids) |join_id| {
                const join_row = join_table.get(join_id) orelse continue;
                const right_val = join_row.get(right_column) orelse continue;

                if (valuesEqual(probe_key, right_val)) {
                    // Match! Emit joined row
                    try emitJoinedRow(
                        result,
                        allocator,
                        base_table,
                        join_table,
                        base_table_name,
                        join_table_name,
                        base_row,
                        join_row,
                        select_all,
                        columns,
                    );
                    found_match = true;
                }
            }
        }

        // Mark as matched if we found at least one match
        if (found_match) {
            try matched_base_rows.put(base_id, true);
        }
    }

    // Emit unmatched base rows with NULLs for join table
    for (base_row_ids) |base_id| {
        if (!matched_base_rows.contains(base_id)) {
            const base_row = base_table.get(base_id) orelse continue;

            // Check if this row has a NULL key (already emitted above)
            const key_val = base_row.get(left_column);
            if (key_val == null or key_val.? == .null_value) continue;

            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                base_row,
                null,
                select_all,
                columns,
            );
        }
    }
}

// ============================================================================
// RIGHT JOIN Implementation
// ============================================================================

fn executeRightHashJoin(
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    base_table_name: []const u8,
    join_table_name: []const u8,
    left_column: []const u8,
    right_column: []const u8,
    select_all: bool,
    columns: []const SelectColumn,
    result: *QueryResult,
) !void {
    // Build phase: hash the base table (reversed)
    var build_hash = try buildHashTable(allocator, base_table, left_column);
    defer build_hash.deinit();

    // Track which join rows were matched
    var matched_join_rows = AutoHashMap(u64, bool).init(allocator);
    defer matched_join_rows.deinit();

    // Probe phase: scan the join table (reversed)
    const join_row_ids = try join_table.getAllRows(allocator);
    defer allocator.free(join_row_ids);

    for (join_row_ids) |join_id| {
        const join_row = join_table.get(join_id) orelse continue;
        const probe_key = join_row.get(right_column) orelse {
            // Join row has NULL in join column - emit with NULLs for base table
            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                null,
                join_row,
                select_all,
                columns,
            );
            continue;
        };

        if (probe_key == .null_value) {
            // NULL key - emit with NULLs for base table
            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                null,
                join_row,
                select_all,
                columns,
            );
            continue;
        }

        // Hash and probe
        const probe_hash = hashColumnValue(probe_key);
        var found_match = false;

        if (build_hash.probe(probe_hash)) |matching_ids| {
            for (matching_ids) |base_id| {
                const base_row = base_table.get(base_id) orelse continue;
                const left_val = base_row.get(left_column) orelse continue;

                if (valuesEqual(left_val, probe_key)) {
                    // Match! Emit joined row
                    try emitJoinedRow(
                        result,
                        allocator,
                        base_table,
                        join_table,
                        base_table_name,
                        join_table_name,
                        base_row,
                        join_row,
                        select_all,
                        columns,
                    );
                    found_match = true;
                }
            }
        }

        // Mark as matched if we found at least one match
        if (found_match) {
            try matched_join_rows.put(join_id, true);
        }
    }

    // Emit unmatched join rows with NULLs for base table
    for (join_row_ids) |join_id| {
        if (!matched_join_rows.contains(join_id)) {
            const join_row = join_table.get(join_id) orelse continue;

            // Check if this row has a NULL key (already emitted above)
            const key_val = join_row.get(right_column);
            if (key_val == null or key_val.? == .null_value) continue;

            try emitJoinedRow(
                result,
                allocator,
                base_table,
                join_table,
                base_table_name,
                join_table_name,
                null,
                join_row,
                select_all,
                columns,
            );
        }
    }
}
