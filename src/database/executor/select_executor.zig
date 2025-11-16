const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const sql = @import("../../sql.zig");
const ArrayList = std.array_list.Managed;

// Import other executor modules
const join_executor = @import("join_executor.zig");
const aggregate_executor = @import("aggregate_executor.zig");
const sort_executor = @import("sort_executor.zig");
const expr_evaluator = @import("expr_evaluator.zig");

/// Main SELECT execution function
/// Routes to specialized executors based on query type:
/// - Has JOINs → join_executor.executeJoinSelect
/// - Has GROUP BY → aggregate_executor.executeGroupBySelect
/// - Has aggregates without GROUP BY → aggregate_executor.executeAggregateSelect
/// - Simple SELECT → handle directly with WHERE, ORDER BY, LIMIT, etc.
pub fn executeSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // Route to JOIN handler if needed
    if (cmd.joins.items.len > 0) {
        return join_executor.executeJoinSelect(db, cmd);
    }

    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Validate column names before execution
    const validator = @import("../validator.zig");
    try validator.validateSelectColumns(db.allocator, &cmd, table, null);

    // Check if this is an aggregate query
    var has_aggregates = false;
    var has_regular_columns = false;
    var select_all = false;

    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => has_aggregates = true,
            .regular => has_regular_columns = true,
            .star => {
                select_all = true;
                has_regular_columns = true;
            },
        }
    }

    // Route to GROUP BY handler if needed
    if (cmd.group_by.items.len > 0) {
        return aggregate_executor.executeGroupBySelect(db, table, cmd);
    }

    // Error: Cannot mix aggregates with regular columns without GROUP BY
    if (has_aggregates and has_regular_columns) {
        return error.MixedAggregateAndRegular;
    }

    // Route to aggregate handler if needed (without GROUP BY)
    if (has_aggregates) {
        return aggregate_executor.executeAggregateSelect(db, table, cmd);
    }

    // Regular SELECT (non-aggregate)
    var result = QueryResult.init(db.allocator);
    errdefer result.deinit();

    // Determine which columns to select
    select_all = select_all or cmd.columns.items.len == 0;
    if (select_all) {
        // Check if table has an "id" column in its schema
        var has_id_column = false;
        for (table.columns.items) |col| {
            if (std.mem.eql(u8, col.name, "id")) {
                has_id_column = true;
                break;
            }
        }

        // Only add row_id as "id" if table doesn't have its own "id" column
        if (!has_id_column) {
            try result.addColumn("id");
        }

        for (table.columns.items) |col| {
            try result.addColumn(col.name);
        }
    } else {
        for (cmd.columns.items) |col| {
            switch (col) {
                .regular => |col_name| {
                    // Note: Column validation is intentionally NOT performed here.
                    // Per PostgreSQL/SQLite semantics, validation happens during semantic analysis,
                    // not schema creation. This allows:
                    // - Literals: SELECT 1, SELECT 'text'
                    // - Aggregates: SELECT COUNT(*), SELECT AVG(price)
                    // - Expressions: SELECT price * 1.1
                    // - Subqueries: SELECT (SELECT MAX(id) FROM other_table)
                    //
                    // For deferred validation, see database/validator.zig
                    try result.addColumn(col_name);
                },
                .star => unreachable, // Already handled above
                .aggregate => unreachable, // Already handled above
            }
        }
    }

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    // Get rows to process
    var row_ids: []u64 = undefined;
    const should_free_ids = true;

    // Phase 1: Query Optimizer - Check if we can use an index
    const use_index = if (cmd.where_column) |where_col| blk: {
        if (cmd.where_value) |where_val| {
            // Look for an index on this column
            if (db.index_manager.findIndexForColumn(cmd.table_name, where_col)) |index_info| {
                // Found an index! Use it instead of table scan
                row_ids = try index_info.btree.search(where_val);
                break :blk true;
            }
        }
        break :blk false;
    } else false;

    // Handle ORDER BY SIMILARITY TO "text"
    if (!use_index) {
        if (cmd.order_by_similarity) |similarity_text| {
            // Find embedding column in table to get its dimension
            var embedding_dim: ?usize = null;
            for (table.columns.items) |col| {
                if (col.col_type == .embedding) {
                    embedding_dim = col.embedding_dim;
                    break;
                }
            }

            if (embedding_dim == null) return sql.SqlError.InvalidSyntax;
            const dim = embedding_dim.?;

            // For semantic search, we need to generate an embedding from the text
            const query_embedding = try db.allocator.alloc(f32, dim);
            defer db.allocator.free(query_embedding);

            // Simple hash-based embedding (in real use, you'd use an actual embedding model)
            const hash = std.hash.Wyhash.hash(0, similarity_text);
            for (query_embedding, 0..) |*val, i| {
                const seed = hash +% i;
                val.* = @as(f32, @floatFromInt(seed & 0xFF)) / 255.0;
            }

            // Get or create HNSW index for this dimension
            const hnsw = try db.getOrCreateHnswForDim(dim);
            const search_results = try hnsw.search(query_embedding, cmd.limit orelse 10);
            defer db.allocator.free(search_results);

            row_ids = try db.allocator.alloc(u64, search_results.len);
            for (search_results, 0..) |res, i| {
                row_ids[i] = res.external_id;
            }
        } else if (cmd.order_by_vibes) {
            // Fun parody feature: random order!
            row_ids = try table.getAllRows(db.allocator, snapshot, clog);
            var prng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
            const random = prng.random();

            // Shuffle
            for (row_ids, 0..) |_, i| {
                const j = random.intRangeLessThan(usize, 0, row_ids.len);
                const temp = row_ids[i];
                row_ids[i] = row_ids[j];
                row_ids[j] = temp;
            }
        } else {
            // Fallback to full table scan (no index available)
            row_ids = try table.getAllRows(db.allocator, snapshot, clog);
        }
    }

    defer if (should_free_ids) db.allocator.free(row_ids);

    // Determine if we need to process all rows (for ORDER BY) or can apply LIMIT early
    // Don't apply LIMIT early if we have ORDER BY (unless it's similarity/vibes which already sorted)
    const has_generic_order_by = cmd.order_by != null and cmd.order_by_similarity == null and !cmd.order_by_vibes;
    const max_rows = if (!has_generic_order_by and cmd.limit != null)
        @min(cmd.limit.?, row_ids.len)
    else
        row_ids.len;

    // Process each row
    var count: usize = 0;
    for (row_ids) |row_id| {
        if (count >= max_rows) break;

        const row = table.get(row_id, snapshot, clog) orelse continue;

        // Apply WHERE filter (skip if we already used an index to filter)
        if (!use_index) {
            // Try simple WHERE first (for backward compatibility and optimization)
            if (cmd.where_column) |where_col| {
                if (cmd.where_value) |where_val| {
                    const row_val = row.get(where_col) orelse continue;
                    if (!valuesEqual(row_val, where_val)) continue;
                }
            } else if (cmd.where_expr) |expr| {
                // Evaluate complex WHERE expression with subquery support
                // Pass executeSelect as function pointer to handle circular dependency
                const matches = try expr_evaluator.evaluateExprWithSubqueries(db, expr, row.values, executeSelect);
                if (!matches) continue;
            }
        }

        // Apply SIMILAR TO filter (semantic search on text columns)
        if (cmd.similar_to_column) |_| {
            // In a real implementation, this would do semantic similarity
            // Skip for simplicity in this demo
        }

        // Add row to results
        var result_row = ArrayList(ColumnValue).init(db.allocator);

        if (select_all) {
            // Check if table has an "id" column in its schema
            var has_id_column = false;
            for (table.columns.items) |col| {
                if (std.mem.eql(u8, col.name, "id")) {
                    has_id_column = true;
                    break;
                }
            }

            // Only append row_id if table doesn't have its own "id" column
            if (!has_id_column) {
                try result_row.append(ColumnValue{ .int = @intCast(row_id) });
            }

            for (table.columns.items) |col| {
                if (row.get(col.name)) |val| {
                    try result_row.append(try val.clone(db.allocator));
                } else {
                    try result_row.append(ColumnValue.null_value);
                }
            }
        } else {
            for (cmd.columns.items) |col| {
                switch (col) {
                    .regular => |col_name| {
                        if (row.get(col_name)) |val| {
                            try result_row.append(try val.clone(db.allocator));
                        } else {
                            try result_row.append(ColumnValue.null_value);
                        }
                    },
                    .star => unreachable, // Already handled above
                    .aggregate => unreachable, // Already handled above
                }
            }
        }

        try result.addRow(result_row);
        count += 1;
    }

    // Apply ORDER BY if present (and not already sorted by similarity/vibes)
    if (cmd.order_by) |order_by| {
        if (cmd.order_by_similarity == null and !cmd.order_by_vibes) {
            try sort_executor.applyOrderBy(&result, order_by);
        }
    }

    // Apply LIMIT after ORDER BY (if we didn't apply it earlier)
    if (has_generic_order_by and cmd.limit != null) {
        const limit = cmd.limit.?;
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
