const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const valuesEqual = core.valuesEqual;
const recovery = @import("recovery.zig");
const Table = @import("../table.zig").Table;
const ColumnValue = @import("../table.zig").ColumnValue;
const ColumnType = @import("../table.zig").ColumnType;
const Row = @import("../table.zig").Row;
const sql = @import("../sql.zig");
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const WalRecordType = @import("../wal.zig").WalRecordType;
const Transaction = @import("../transaction.zig");
const Operation = Transaction.Operation;
const TxRow = Transaction.Row;

/// Execute a SQL command
pub fn execute(db: *Database, query: []const u8) !QueryResult {
    var cmd = try sql.parse(db.allocator, query);
    defer cmd.deinit(db.allocator);

    return switch (cmd) {
        .create_table => |create| try executeCreateTable(db, create),
        .create_index => |create_idx| try executeCreateIndex(db, create_idx),
        .drop_index => |drop_idx| try executeDropIndex(db, drop_idx),
        .insert => |insert| try executeInsert(db, insert),
        .select => |select| try executeSelect(db, select),
        .delete => |delete| try executeDelete(db, delete),
        .update => |update| try executeUpdate(db, update),
        .begin => try executeBegin(db),
        .commit => try executeCommit(db),
        .rollback => try executeRollback(db),
    };
}

fn executeCreateTable(db: *Database, cmd: sql.CreateTableCmd) !QueryResult {
    const table_ptr = try db.allocator.create(Table);
    table_ptr.* = try Table.init(db.allocator, cmd.table_name);

    for (cmd.columns.items) |col_def| {
        try table_ptr.addColumn(col_def.name, col_def.col_type);
    }

    const owned_name = try db.allocator.dupe(u8, cmd.table_name);
    try db.tables.put(owned_name, table_ptr);

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(db.allocator, "Table '{s}' created", .{cmd.table_name});
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

fn executeCreateIndex(db: *Database, cmd: sql.CreateIndexCmd) !QueryResult {
    // Get the table
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Create the index
    try db.index_manager.createIndex(
        cmd.index_name,
        cmd.table_name,
        cmd.column_name,
        table,
    );

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(
        db.allocator,
        "Index '{s}' created on {s}({s})",
        .{ cmd.index_name, cmd.table_name, cmd.column_name },
    );
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

fn executeDropIndex(db: *Database, cmd: sql.DropIndexCmd) !QueryResult {
    // Drop the index
    try db.index_manager.dropIndex(cmd.index_name);

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(
        db.allocator,
        "Index '{s}' dropped",
        .{cmd.index_name},
    );
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

fn executeInsert(db: *Database, cmd: sql.InsertCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    var values_map = StringHashMap(ColumnValue).init(db.allocator);
    defer values_map.deinit();

    // If columns are specified, use them; otherwise use table schema order
    if (cmd.columns.items.len > 0) {
        for (cmd.columns.items, 0..) |col, i| {
            if (i < cmd.values.items.len) {
                try values_map.put(col, cmd.values.items[i]);
            }
        }
    } else {
        // Use table column order
        for (table.columns.items, 0..) |col, i| {
            if (i < cmd.values.items.len) {
                try values_map.put(col.name, cmd.values.items[i]);
            }
        }
    }

    // Check if an explicit ID is provided (must be non-negative)
    var explicit_id: ?u64 = null;
    if (values_map.get("id")) |id_value| {
        if (id_value == .int and id_value.int >= 0) {
            explicit_id = @intCast(id_value.int);
        }
    }

    // Determine the row_id that will be used
    const row_id = explicit_id orelse table.next_id;

    // WAL-Ahead Protocol: Write to WAL BEFORE modifying data
    if (db.wal != null) {
        // Create a temporary row for serialization
        var temp_row = Row.init(db.allocator, row_id);
        defer temp_row.deinit(db.allocator);

        // Populate the temporary row with all values
        var it = values_map.iterator();
        while (it.next()) |entry| {
            try temp_row.set(db.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Serialize the row
        const serialized_row = try temp_row.serialize(db.allocator);
        defer db.allocator.free(serialized_row);

        // Write to WAL using helper function
        _ = try recovery.writeWalRecord(db, WalRecordType.insert_row, cmd.table_name, row_id, serialized_row);
    }

    // Insert the row using appropriate method
    const final_row_id = if (explicit_id) |id| blk: {
        try table.insertWithId(id, values_map);
        break :blk id;
    } else try table.insert(values_map);

    // If there's an embedding column and vector search is enabled, add to index
    if (db.hnsw) |h| {
        const row = table.get(final_row_id).?;
        var it = row.values.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* == .embedding) {
                const embedding = entry.value_ptr.embedding;
                _ = try h.insert(embedding, final_row_id);
                break;
            }
        }
    }

    // Phase 1: Update B-tree indexes automatically
    const inserted_row = table.get(final_row_id).?;
    try db.index_manager.onInsert(cmd.table_name, final_row_id, inserted_row);

    // Track operation in transaction if one is active
    if (db.tx_manager.getCurrentTx()) |tx| {
        const table_name_owned = try db.allocator.dupe(u8, cmd.table_name);
        const op = Operation{
            .insert = .{
                .table_name = table_name_owned,
                .row_id = final_row_id,
            },
        };
        try tx.addOperation(op);
    }

    var result = QueryResult.init(db.allocator);
    try result.addColumn("row_id");
    var row = ArrayList(ColumnValue).init(db.allocator);
    try row.append(ColumnValue{ .int = @intCast(final_row_id) });
    try result.addRow(row);

    return result;
}

fn executeSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;
    var result = QueryResult.init(db.allocator);

    // Determine which columns to select
    const select_all = cmd.columns.items.len == 0;
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
            try result.addColumn(col);
        }
    }

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
            if (db.hnsw == null) return sql.SqlError.InvalidSyntax;

            // For semantic search, we need to generate an embedding from the text
            const query_embedding = try db.allocator.alloc(f32, 128);
            defer db.allocator.free(query_embedding);

            // Simple hash-based embedding (in real use, you'd use an actual embedding model)
            const hash = std.hash.Wyhash.hash(0, similarity_text);
            for (query_embedding, 0..) |*val, i| {
                const seed = hash +% i;
                val.* = @as(f32, @floatFromInt(seed & 0xFF)) / 255.0;
            }

            const search_results = try db.hnsw.?.search(query_embedding, cmd.limit orelse 10);
            defer db.allocator.free(search_results);

            row_ids = try db.allocator.alloc(u64, search_results.len);
            for (search_results, 0..) |res, i| {
                row_ids[i] = res.external_id;
            }
        } else if (cmd.order_by_vibes) {
            // Fun parody feature: random order!
            row_ids = try table.getAllRows(db.allocator);
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
            row_ids = try table.getAllRows(db.allocator);
        }
    }

    defer if (should_free_ids) db.allocator.free(row_ids);

    // Apply LIMIT
    const max_rows = if (cmd.limit) |lim| @min(lim, row_ids.len) else row_ids.len;

    // Process each row
    var count: usize = 0;
    for (row_ids) |row_id| {
        if (count >= max_rows) break;

        const row = table.get(row_id) orelse continue;

        // Apply WHERE filter (skip if we already used an index to filter)
        if (!use_index) {
            if (cmd.where_column) |where_col| {
                if (cmd.where_value) |where_val| {
                    const row_val = row.get(where_col) orelse continue;
                    if (!valuesEqual(row_val, where_val)) continue;
                }
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
                if (row.get(col)) |val| {
                    try result_row.append(try val.clone(db.allocator));
                } else {
                    try result_row.append(ColumnValue.null_value);
                }
            }
        }

        try result.addRow(result_row);
        count += 1;
    }

    return result;
}

fn executeDelete(db: *Database, cmd: sql.DeleteCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    var deleted_count: usize = 0;
    const row_ids = try table.getAllRows(db.allocator);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id) orelse continue;

        // Apply WHERE filter
        var should_delete = true;
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                const row_val = row.get(where_col) orelse {
                    should_delete = false;
                    continue;
                };
                if (!valuesEqual(row_val, where_val)) {
                    should_delete = false;
                }
            }
        }

        if (should_delete) {
            // Save row data for transaction rollback if needed
            var saved_row: ?TxRow = null;
            if (db.tx_manager.getCurrentTx()) |_| {
                saved_row = TxRow.init(db.allocator);
                var it = row.values.iterator();
                while (it.next()) |entry| {
                    const key = try db.allocator.dupe(u8, entry.key_ptr.*);
                    const value = try entry.value_ptr.clone(db.allocator);
                    try saved_row.?.values.put(key, value);
                }
            }
            errdefer if (saved_row) |*sr| sr.deinit();

            // WAL-Ahead Protocol: Write to WAL BEFORE deleting
            if (db.wal != null) {
                // Serialize the row being deleted (for potential recovery/undo)
                const serialized_row = try row.serialize(db.allocator);
                defer db.allocator.free(serialized_row);

                // Write to WAL using helper function
                _ = try recovery.writeWalRecord(db, WalRecordType.delete_row, cmd.table_name, row_id, serialized_row);
            }

            // Phase 1: Update B-tree indexes before deletion (need row data)
            try db.index_manager.onDelete(cmd.table_name, row_id, row);

            _ = table.delete(row_id);
            deleted_count += 1;

            // Track operation in transaction if one is active
            if (db.tx_manager.getCurrentTx()) |tx| {
                const table_name_owned = try db.allocator.dupe(u8, cmd.table_name);
                const op = Operation{
                    .delete = .{
                        .table_name = table_name_owned,
                        .row_id = row_id,
                        .saved_row = saved_row.?,
                    },
                };
                try tx.addOperation(op);
            }
        }
    }

    var result = QueryResult.init(db.allocator);
    try result.addColumn("deleted");
    var row = ArrayList(ColumnValue).init(db.allocator);
    try row.append(ColumnValue{ .int = @intCast(deleted_count) });
    try result.addRow(row);

    return result;
}

fn executeUpdate(db: *Database, cmd: sql.UpdateCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Validate all SET columns exist in table and have correct types
    for (cmd.assignments.items) |assignment| {
        var found = false;
        var col_type: ColumnType = undefined;

        for (table.columns.items) |col| {
            if (std.mem.eql(u8, col.name, assignment.column)) {
                found = true;
                col_type = col.col_type;
                break;
            }
        }

        if (!found) {
            return sql.SqlError.ColumnNotFound;
        }

        // Type validation
        const value_valid = switch (col_type) {
            .int => assignment.value == .int or assignment.value == .null_value,
            .float => assignment.value == .float or assignment.value == .int or assignment.value == .null_value,
            .text => assignment.value == .text or assignment.value == .null_value,
            .bool => assignment.value == .bool or assignment.value == .null_value,
            .embedding => blk: {
                if (assignment.value == .null_value) break :blk true;
                if (assignment.value != .embedding) break :blk false;
                // Validate dimension
                const expected_dim: usize = 768; // TODO: Make this configurable
                break :blk assignment.value.embedding.len == expected_dim;
            },
        };

        if (!value_valid) {
            return sql.SqlError.TypeMismatch;
        }
    }

    var updated_count: usize = 0;
    const row_ids = try table.getAllRows(db.allocator);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        var row = table.get(row_id) orelse continue;

        // Apply WHERE filter using expression evaluator
        var should_update = true;
        if (cmd.where_expr) |expr| {
            should_update = sql.evaluateExpr(expr, row.values);
        }

        if (!should_update) continue;

        // Phase 1: Save old row state for index updates
        var old_row_for_index = Row.init(db.allocator, row_id);
        defer old_row_for_index.deinit(db.allocator);

        // Clone old row values
        var old_it = row.values.iterator();
        while (old_it.next()) |entry| {
            try old_row_for_index.set(db.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Track if embedding column is being updated
        var old_embedding: ?[]const f32 = null;
        var new_embedding: ?[]const f32 = null;
        var embedding_changed = false;

        // Clone old embedding for potential rollback
        var old_embedding_backup: ?[]f32 = null;
        defer if (old_embedding_backup) |emb| db.allocator.free(emb);

        // Find old embedding if it exists
        if (db.hnsw != null) {
            var it = row.values.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .embedding) {
                    old_embedding = entry.value_ptr.embedding;
                    // Clone for potential rollback
                    old_embedding_backup = try db.allocator.dupe(f32, old_embedding.?);
                    break;
                }
            }
        }

        // First pass: Detect if embedding is changing and determine new embedding
        for (cmd.assignments.items) |assignment| {
            if (assignment.value == .embedding) {
                new_embedding = assignment.value.embedding;
                // Check if embedding actually changed
                if (old_embedding) |old_emb| {
                    if (old_emb.len == new_embedding.?.len) {
                        var changed = false;
                        for (old_emb, 0..) |val, i| {
                            if (val != new_embedding.?[i]) {
                                changed = true;
                                break;
                            }
                        }
                        embedding_changed = changed;
                    } else {
                        embedding_changed = true;
                    }
                } else {
                    embedding_changed = true;
                }
                break; // Only one embedding column per table
            }
        }

        // WAL-Ahead Protocol: Write to WAL BEFORE any mutations
        if (db.wal != null) {
            // Serialize the current row (old state) for recovery
            const serialized_old = try row.serialize(db.allocator);
            defer db.allocator.free(serialized_old);

            // Create a temporary row with updates to serialize new state
            var temp_row = Row.init(db.allocator, row_id);
            defer temp_row.deinit(db.allocator);

            // Copy current values to temp row
            var copy_it = row.values.iterator();
            while (copy_it.next()) |entry| {
                try temp_row.set(db.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }

            // Apply assignments to temp row
            for (cmd.assignments.items) |assignment| {
                try temp_row.set(db.allocator, assignment.column, assignment.value);
            }

            // Serialize the new state
            const serialized_new = try temp_row.serialize(db.allocator);
            defer db.allocator.free(serialized_new);

            // For UPDATE, we store both old and new row data
            // Format: [old_size:u64][old_data][new_data]
            const combined_size = 8 + serialized_old.len + serialized_new.len;
            const combined_data = try db.allocator.alloc(u8, combined_size);
            defer db.allocator.free(combined_data);

            std.mem.writeInt(u64, combined_data[0..8], serialized_old.len, .little);
            @memcpy(combined_data[8..][0..serialized_old.len], serialized_old);
            @memcpy(combined_data[8 + serialized_old.len ..][0..serialized_new.len], serialized_new);

            // Write to WAL using helper function
            _ = try recovery.writeWalRecord(db, WalRecordType.update_row, cmd.table_name, row_id, combined_data);
        }

        // Handle HNSW index updates BEFORE applying row updates
        if (embedding_changed and db.hnsw != null) {
            const h = db.hnsw.?;

            // Remove old vector from HNSW (if it existed)
            if (old_embedding_backup != null) {
                h.removeNode(row_id) catch |err| {
                    std.debug.print("Error removing node from HNSW: {}\n", .{err});
                    return err;
                };
            }

            // Insert new vector with same row_id
            if (new_embedding) |new_emb| {
                _ = h.insert(new_emb, row_id) catch |err| {
                    // Rollback: Re-insert old embedding to restore HNSW state
                    if (old_embedding_backup) |old_clone| {
                        _ = h.insert(old_clone, row_id) catch {
                            std.debug.print("CRITICAL: Failed to rollback HNSW state after insert failure\n", .{});
                        };
                    }
                    std.debug.print("Error inserting new vector to HNSW: {}\n", .{err});
                    return err;
                };
            }
        }

        // Save old row values for transaction tracking if needed
        var tx_old_values: ?TxRow = null;
        if (db.tx_manager.getCurrentTx()) |_| {
            tx_old_values = TxRow.init(db.allocator);
            var tx_it = row.values.iterator();
            while (tx_it.next()) |entry| {
                const key = try db.allocator.dupe(u8, entry.key_ptr.*);
                const value = try entry.value_ptr.clone(db.allocator);
                try tx_old_values.?.values.put(key, value);
            }
        }
        errdefer if (tx_old_values) |*tov| tov.deinit();

        // Now apply all SET assignments to the row
        for (cmd.assignments.items) |assignment| {
            try row.set(db.allocator, assignment.column, assignment.value);
        }

        // Phase 1: Update B-tree indexes after row mutation
        try db.index_manager.onUpdate(cmd.table_name, row_id, &old_row_for_index, row);

        // Track operation in transaction if one is active
        if (db.tx_manager.getCurrentTx()) |tx| {
            const table_name_owned = try db.allocator.dupe(u8, cmd.table_name);
            const op = Operation{
                .update = .{
                    .table_name = table_name_owned,
                    .row_id = row_id,
                    .old_values = tx_old_values.?,
                },
            };
            try tx.addOperation(op);
        }

        updated_count += 1;
    }

    var result = QueryResult.init(db.allocator);
    try result.addColumn("updated");
    var row = ArrayList(ColumnValue).init(db.allocator);
    try row.append(ColumnValue{ .int = @intCast(updated_count) });
    try result.addRow(row);

    return result;
}

// ============================================================================
// Transaction Commands
// ============================================================================

/// Execute BEGIN command
fn executeBegin(db: *Database) !QueryResult {
    const tx_id = try db.tx_manager.begin();

    // Write BEGIN to WAL if enabled
    if (db.wal) |w| {
        try w.writeRecord(.{
            .record_type = WalRecordType.begin_tx,
            .tx_id = tx_id,
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
        });
        try w.flush();
    }

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(db.allocator, "Transaction {d} started", .{tx_id});
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

/// Execute COMMIT command
fn executeCommit(db: *Database) !QueryResult {
    const tx = db.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;
    const tx_id = tx.id;

    // Write COMMIT to WAL if enabled
    if (db.wal) |w| {
        try w.writeRecord(.{
            .record_type = WalRecordType.commit_tx,
            .tx_id = tx_id,
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
        });
        try w.flush(); // Ensure durable
    }

    // Mark transaction as committed
    try db.tx_manager.commit(tx_id);

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(db.allocator, "Transaction {d} committed", .{tx_id});
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

/// Execute ROLLBACK command
fn executeRollback(db: *Database) !QueryResult {
    const tx = db.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;
    const tx_id = tx.id;

    // Undo operations in reverse order
    var i = tx.operations.items.len;
    while (i > 0) {
        i -= 1;
        const op = tx.operations.items[i];
        try undoOperation(db, op);
    }

    // Write ROLLBACK to WAL if enabled
    if (db.wal) |w| {
        try w.writeRecord(.{
            .record_type = WalRecordType.rollback_tx,
            .tx_id = tx_id,
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
        });
        try w.flush();
    }

    // Mark transaction as aborted
    try db.tx_manager.rollback(tx_id);

    var result = QueryResult.init(db.allocator);
    try result.addColumn("status");
    var row = ArrayList(ColumnValue).init(db.allocator);
    const msg = try std.fmt.allocPrint(db.allocator, "Transaction {d} rolled back", .{tx_id});
    try row.append(ColumnValue{ .text = msg });
    try result.addRow(row);

    return result;
}

/// Undo a single operation (helper for rollback)
fn undoOperation(db: *Database, op: Operation) !void {
    switch (op) {
        .insert => |ins| {
            // Undo INSERT: delete the inserted row
            const table = db.tables.get(ins.table_name) orelse return error.TableNotFound;
            _ = table.delete(ins.row_id);

            // Update indexes
            const row = table.get(ins.row_id);
            if (row) |r| {
                try db.index_manager.onDelete(ins.table_name, ins.row_id, r);
            }
        },
        .delete => |del| {
            // Undo DELETE: restore the deleted row
            const table = db.tables.get(del.table_name) orelse return error.TableNotFound;

            // Convert transaction.Row to table.Row and insert
            var values_map = StringHashMap(ColumnValue).init(db.allocator);
            defer values_map.deinit();

            var it = del.saved_row.values.iterator();
            while (it.next()) |entry| {
                try values_map.put(entry.key_ptr.*, entry.value_ptr.*);
            }

            // Insert the row back with its original ID
            try table.insertWithId(del.row_id, values_map);

            // Update indexes
            const row = table.get(del.row_id);
            if (row) |r| {
                try db.index_manager.onInsert(del.table_name, del.row_id, r);
            }
        },
        .update => |upd| {
            // Undo UPDATE: restore the old values
            const table = db.tables.get(upd.table_name) orelse return error.TableNotFound;
            const row = table.get(upd.row_id) orelse return error.RowNotFound;

            // Save current row state for index update
            var old_row_for_index = Row.init(db.allocator, upd.row_id);
            defer old_row_for_index.deinit(db.allocator);

            var old_it = row.values.iterator();
            while (old_it.next()) |entry| {
                try old_row_for_index.set(db.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }

            // Restore old values
            var it = upd.old_values.values.iterator();
            while (it.next()) |entry| {
                try row.set(db.allocator, entry.key_ptr.*, entry.value_ptr.*);
            }

            // Update indexes
            try db.index_manager.onUpdate(upd.table_name, upd.row_id, &old_row_for_index, row);
        },
    }
}
