// ============================================================================
// ORDER BY Support - Sort query results
// ============================================================================

const std = @import("std");
const core = @import("../core.zig");
const QueryResult = core.QueryResult;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const sql = @import("../../sql.zig");
const OrderByClause = sql.OrderByClause;
const ArrayList = std.array_list.Managed;

/// Compare two ColumnValues for sorting
fn compareColumnValues(a: ColumnValue, b: ColumnValue) std.math.Order {
    // NULL handling: NULL < any value
    if (a == .null_value and b == .null_value) return .eq;
    if (a == .null_value) return .lt;
    if (b == .null_value) return .gt;

    // Type-based comparison
    switch (a) {
        .int => |a_int| {
            if (b != .int) return .eq; // Type mismatch
            const b_int = b.int;
            if (a_int < b_int) return .lt;
            if (a_int > b_int) return .gt;
            return .eq;
        },
        .float => |a_float| {
            if (b != .float) return .eq; // Type mismatch
            const b_float = b.float;
            if (a_float < b_float) return .lt;
            if (a_float > b_float) return .gt;
            return .eq;
        },
        .text => |a_text| {
            if (b != .text) return .eq; // Type mismatch
            const cmp = std.mem.order(u8, a_text, b.text);
            return cmp;
        },
        .bool => |a_bool| {
            if (b != .bool) return .eq; // Type mismatch
            const b_bool = b.bool;
            if (a_bool == b_bool) return .eq;
            // false < true
            if (!a_bool and b_bool) return .lt;
            return .gt;
        },
        .null_value => return .eq, // Already handled above
        .embedding => return .eq, // Embeddings not comparable
    }
}

/// Apply ORDER BY clause to sort query results
pub fn applyOrderBy(result: *QueryResult, order_by: OrderByClause) !void {
    if (order_by.items.items.len == 0) return;

    // Create a context for sorting that includes both result and order_by
    const SortContext = struct {
        result: *QueryResult,
        order_by: OrderByClause,

        fn lessThan(ctx: @This(), a_idx: usize, b_idx: usize) bool {
            const a_row = ctx.result.rows.items[a_idx];
            const b_row = ctx.result.rows.items[b_idx];

            // Compare each ORDER BY column in sequence
            for (ctx.order_by.items.items) |order_item| {
                // Find column index
                var col_idx: ?usize = null;
                for (ctx.result.columns.items, 0..) |col_name, idx| {
                    if (std.mem.eql(u8, col_name, order_item.column)) {
                        col_idx = idx;
                        break;
                    }
                }

                if (col_idx == null) continue; // Column not found, skip

                const a_val = a_row.items[col_idx.?];
                const b_val = b_row.items[col_idx.?];

                // Compare values
                const cmp = compareColumnValues(a_val, b_val);

                if (cmp == .eq) {
                    // Values equal, continue to next ORDER BY column
                    continue;
                }

                // Apply direction
                if (order_item.direction == .asc) {
                    return cmp == .lt;
                } else { // desc
                    return cmp == .gt;
                }
            }

            // All ORDER BY columns are equal
            return false;
        }
    };

    // Sort using indices
    const indices = try result.allocator.alloc(usize, result.rows.items.len);
    defer result.allocator.free(indices);

    for (indices, 0..) |*idx, i| {
        idx.* = i;
    }

    const context = SortContext{
        .result = result,
        .order_by = order_by,
    };

    std.sort.pdq(usize, indices, context, SortContext.lessThan);

    // Rearrange rows based on sorted indices
    var sorted_rows = ArrayList(ArrayList(ColumnValue)).init(result.allocator);
    errdefer {
        for (sorted_rows.items) |*row| {
            row.deinit();
        }
        sorted_rows.deinit();
    }

    for (indices) |idx| {
        try sorted_rows.append(result.rows.items[idx]);
    }

    // Replace original rows with sorted rows
    // Don't deinit the row data since we moved it to sorted_rows
    result.rows.deinit();
    result.rows = sorted_rows;
}
