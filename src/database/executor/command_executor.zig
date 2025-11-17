// ============================================================================
// Command Execution Functions (DDL and DML)
// ============================================================================
//
// This module contains functions for executing SQL commands that modify
// the database structure or data:
// - CREATE TABLE, CREATE INDEX, DROP INDEX (DDL)
// - INSERT, UPDATE, DELETE (DML)
//
// These functions handle:
// - Table and index management
// - Row insertion with WAL logging and MVCC versioning
// - Row updates with index maintenance and transaction tracking
// - Row deletion with proper cleanup
// - Transaction integration for rollback support
//
// ============================================================================

const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const ValidationMode = core.ValidationMode;
const valuesEqual = core.valuesEqual;
const recovery = @import("../recovery.zig");
const validator = @import("../validator.zig");
const ValidationResult = validator.ValidationResult;
const Table = @import("../../table.zig").Table;
const ColumnValue = @import("../../table.zig").ColumnValue;
const ColumnType = @import("../../table.zig").ColumnType;
const Row = @import("../../table.zig").Row;
const sql = @import("../../sql.zig");
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const WalRecordType = @import("../../wal.zig").WalRecordType;
const Transaction = @import("../../transaction.zig");
const Operation = Transaction.Operation;
const TxRow = Transaction.Row;
const Snapshot = Transaction.Snapshot;
const CommitLog = Transaction.CommitLog;
const Allocator = std.mem.Allocator;

// Forward declaration for expression evaluation (still in main executor)
// This will be moved to expr_evaluator once that module is fully extracted
const evaluateExprWithSubqueries = @import("../executor.zig").evaluateExprWithSubqueries;

// ============================================================================
// Validation Helpers
// ============================================================================

/// Handle validation result based on database validation mode
/// Returns error.ValidationFailed in strict mode if validation fails
/// Logs warnings in warnings mode
/// Does nothing in disabled mode
fn handleValidationResult(db: *Database, validation_result: *ValidationResult) !void {
    // If validation passed, nothing to do
    if (validation_result.valid) {
        return;
    }

    // Get validation mode
    const mode = db.getValidationMode();

    switch (mode) {
        .strict => {
            // In strict mode, validation errors block execution
            // Log the errors for user feedback
            if (validation_result.hasErrors()) {
                std.debug.print("\n=== VALIDATION ERRORS ===\n", .{});
                for (validation_result.errors.items) |*err| {
                    std.debug.print("ERROR: {s}\n", .{err.message});
                    if (err.hint) |hint| {
                        std.debug.print("HINT: {s}\n", .{hint});
                    }
                }
                std.debug.print("=========================\n\n", .{});
            }
            return sql.SqlError.ValidationFailed;
        },

        .warnings => {
            // In warnings mode, log but continue execution
            if (validation_result.hasErrors()) {
                std.debug.print("\n=== VALIDATION WARNINGS ===\n", .{});
                for (validation_result.errors.items) |*err| {
                    std.debug.print("WARNING: {s}\n", .{err.message});
                    if (err.hint) |hint| {
                        std.debug.print("HINT: {s}\n", .{hint});
                    }
                }
                std.debug.print("===========================\n\n", .{});
            }
            // Continue execution despite errors
        },

        .disabled => {
            // Validation disabled, do nothing
        },
    }
}

// ============================================================================
// DDL Commands - Table and Index Management
// ============================================================================

/// Execute CREATE TABLE command
/// Creates a new table with the specified columns and types
pub fn executeCreateTable(db: *Database, cmd: sql.CreateTableCmd) !QueryResult {
    const table_ptr = try db.allocator.create(Table);
    table_ptr.* = try Table.init(db.allocator, cmd.table_name);

    for (cmd.columns.items) |col_def| {
        try table_ptr.addColumnWithDim(col_def.name, col_def.col_type, col_def.embedding_dim);
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

/// Execute CREATE INDEX command
/// Creates a B-tree index on the specified table column
pub fn executeCreateIndex(db: *Database, cmd: sql.CreateIndexCmd) !QueryResult {
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

/// Execute DROP INDEX command
/// Removes an existing B-tree index from the database
pub fn executeDropIndex(db: *Database, cmd: sql.DropIndexCmd) !QueryResult {
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

// ============================================================================
// DML Commands - Data Manipulation
// ============================================================================

/// Execute INSERT command
/// Inserts a new row into the specified table with proper:
/// - Semantic validation (column existence, type checking)
/// - WAL logging for crash recovery
/// - MVCC versioning for transaction isolation
/// - Index updates (B-tree and HNSW vector indexes)
/// - Transaction tracking for rollback support
pub fn executeInsert(db: *Database, cmd: sql.InsertCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Phase 4: Validate query before execution (if validation enabled)
    if (db.isValidationEnabled()) {
        var validation_result = try validator.validateInsert(db.allocator, &cmd, table);
        defer validation_result.deinit();

        try handleValidationResult(db, &validation_result);
    }

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

    // Always auto-generate the row_id (don't use user's "id" column as row_id)
    // This allows multiple rows with the same "id" column value, which is SQL compliant
    // Atomically reserve the next row ID (thread-safe for concurrent inserts)
    const row_id = table.next_id.fetchAdd(1, .monotonic);

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

    // Insert the row using the pre-determined row_id (needed for WAL-Ahead protocol)
    // Phase 3: Use actual transaction ID from active transaction (or 0 for bootstrap)
    const tx_id = db.getCurrentTxId();
    try table.insertWithId(row_id, values_map, tx_id);
    const final_row_id = row_id;

    // Bug Fix #6: Rollback the table insert if any index update fails
    // This ensures atomicity between table and index operations
    errdefer {
        // Physically delete the row we just inserted (complete removal, not MVCC deletion)
        table.physicalDelete(final_row_id) catch |err| {
            std.debug.print("CRITICAL: Failed to rollback table insert for row {}: {}\n", .{ final_row_id, err });
        };
    }

    // Phase 3: Get MVCC context to retrieve the row we just inserted
    // With the visibility fix, transactions can now see their own changes
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    // If there's an embedding column, add to the appropriate dimension-specific HNSW index
    const row = table.get(final_row_id, snapshot, clog).?;
    var embedding_dims = std.ArrayList(usize).init(db.allocator); // Track all embedding dimensions for potential rollback
    defer embedding_dims.deinit();
    var it = row.values.iterator();
    while (it.next()) |entry| {
        if (entry.value_ptr.* == .embedding) {
            const embedding = entry.value_ptr.embedding;
            const dim = embedding.len;
            try embedding_dims.append(dim); // Store for potential rollback

            // Get or create HNSW index for this dimension
            const h = try db.getOrCreateHnswForDim(dim);
            _ = try h.insert(embedding, final_row_id);
            // Continue to process all embedding columns (removed break statement)
        }
    }

    // Additional rollback for HNSW if B-tree index update fails
    errdefer {
        for (embedding_dims.items) |dim| {
            if (db.hnsw_indexes.get(dim)) |h| {
                h.removeNode(final_row_id) catch |err| {
                    std.debug.print("CRITICAL: Failed to rollback HNSW insert for row {} in dim {}: {}\n", .{ final_row_id, dim, err });
                };
            }
        }
    }

    // Phase 1: Update B-tree indexes automatically
    // Reuse the row we already fetched above
    try db.index_manager.onInsert(cmd.table_name, final_row_id, row);

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
    var result_row = ArrayList(ColumnValue).init(db.allocator);
    try result_row.append(ColumnValue{ .int = @intCast(final_row_id) });
    try result.addRow(result_row);

    return result;
}

/// Execute DELETE command
/// Deletes rows matching the WHERE clause with proper:
/// - Semantic validation (WHERE clause column existence)
/// - MVCC snapshot isolation for consistent reads
/// - WAL logging for crash recovery
/// - Index updates (B-tree removal)
/// - Transaction tracking for rollback support
pub fn executeDelete(db: *Database, cmd: sql.DeleteCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Phase 4: Validate query before execution (if validation enabled)
    if (db.isValidationEnabled()) {
        var validation_result = try validator.validateDelete(db.allocator, &cmd, table);
        defer validation_result.deinit();

        try handleValidationResult(db, &validation_result);
    }

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();

    var deleted_count: usize = 0;
    const row_ids = try table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id, snapshot, clog) orelse continue;

        // Apply WHERE filter using expression evaluator (like UPDATE)
        var should_delete = true;
        if (cmd.where_expr) |expr| {
            should_delete = try evaluateExprWithSubqueries(db, expr, row.values);
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

            // Phase 3: Pass actual transaction ID from active transaction
            const tx_id = db.getCurrentTxId();
            try table.delete(row_id, tx_id, clog);
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

    // Note: Auto-VACUUM is now triggered in executor.zig after auto-commit
    // (removed from here to avoid triggering before transaction commits)

    var result = QueryResult.init(db.allocator);
    try result.addColumn("deleted");
    var row = ArrayList(ColumnValue).init(db.allocator);
    try row.append(ColumnValue{ .int = @intCast(deleted_count) });
    try result.addRow(row);

    return result;
}

/// Execute UPDATE command
/// Updates rows matching the WHERE clause with proper:
/// - Semantic validation (column existence, WHERE clause validation)
/// - Column type validation
/// - MVCC versioning for transaction isolation
/// - WAL logging for crash recovery (stores both old and new state)
/// - Index updates (B-tree and HNSW vector indexes)
/// - Transaction tracking for rollback support
/// - Atomic updates for embedding columns with rollback on failure
pub fn executeUpdate(db: *Database, cmd: sql.UpdateCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Phase 4: Validate query before execution (if validation enabled)
    if (db.isValidationEnabled()) {
        var validation_result = try validator.validateUpdate(db.allocator, &cmd, table);
        defer validation_result.deinit();

        try handleValidationResult(db, &validation_result);
    }

    // Validate all SET columns exist in table and have correct types
    for (cmd.assignments.items) |assignment| {
        var found = false;
        var col_type: ColumnType = undefined;
        var embedding_dim: ?usize = null;

        for (table.columns.items) |col| {
            if (std.mem.eql(u8, col.name, assignment.column)) {
                found = true;
                col_type = col.col_type;
                embedding_dim = col.embedding_dim;
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
                // Validate dimension matches schema
                const expected_dim = embedding_dim orelse return sql.SqlError.TypeMismatch;
                break :blk assignment.value.embedding.len == expected_dim;
            },
        };

        if (!value_valid) {
            return sql.SqlError.TypeMismatch;
        }
    }

    // Phase 3: Get MVCC context for snapshot isolation
    const snapshot = db.getCurrentSnapshot();
    const clog = db.getClog();
    const tx_id = db.getCurrentTxId();

    var updated_count: usize = 0;
    const row_ids = try table.getAllRows(db.allocator, snapshot, clog);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        var row = table.get(row_id, snapshot, clog) orelse continue;

        // Apply WHERE filter using expression evaluator with subquery support
        var should_update = true;
        if (cmd.where_expr) |expr| {
            should_update = try evaluateExprWithSubqueries(db, expr, row.values);
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

        // Track embedding column updates (support multiple embeddings per row)
        const EmbeddingUpdate = struct {
            column_name: []const u8,
            old_backup: ?[]f32,
            new_value: []const f32,
            changed: bool,
        };
        var embedding_updates = std.ArrayList(EmbeddingUpdate).init(db.allocator);
        defer {
            for (embedding_updates.items) |update| {
                if (update.old_backup) |backup| {
                    db.allocator.free(backup);
                }
            }
            embedding_updates.deinit();
        }

        // Process each assignment to find embedding updates
        for (cmd.assignments.items) |assignment| {
            if (assignment.value == .embedding) {
                const new_embedding = assignment.value.embedding;

                // Find old embedding value for this column (if it exists)
                const old_value = row.get(assignment.column);
                var old_backup: ?[]f32 = null;
                var changed = false;

                if (old_value) |old_val| {
                    if (old_val == .embedding) {
                        const old_emb = old_val.embedding;
                        // Clone for rollback
                        old_backup = try db.allocator.dupe(f32, old_emb);

                        // Check if actually changed
                        if (old_emb.len == new_embedding.len) {
                            for (old_emb, 0..) |val, i| {
                                if (val != new_embedding[i]) {
                                    changed = true;
                                    break;
                                }
                            }
                        } else {
                            changed = true;
                        }
                    } else {
                        // Column exists but wasn't an embedding before
                        changed = true;
                    }
                } else {
                    // New embedding column being added
                    changed = true;
                }

                try embedding_updates.append(.{
                    .column_name = assignment.column,
                    .old_backup = old_backup,
                    .new_value = new_embedding,
                    .changed = changed,
                });
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

        // Handle HNSW index updates BEFORE applying row updates (support multiple embeddings)
        for (embedding_updates.items) |update| {
            if (update.changed) {
                // Remove old vector from its dimension-specific HNSW (if it existed)
                if (update.old_backup) |old_emb| {
                    const old_dim = old_emb.len;
                    if (db.hnsw_indexes.get(old_dim)) |h| {
                        h.removeNode(row_id) catch |err| {
                            std.debug.print("Error removing node from HNSW (column '{s}'): {}\n", .{ update.column_name, err });
                            return err;
                        };
                    }
                }

                // Insert new vector to its dimension-specific HNSW
                const new_emb = update.new_value;
                const new_dim = new_emb.len;
                const h = try db.getOrCreateHnswForDim(new_dim);
                _ = h.insert(new_emb, row_id) catch |err| {
                    // Rollback: Re-insert old embedding to restore HNSW state
                    if (update.old_backup) |old_clone| {
                        const old_dim = old_clone.len;
                        const old_h = try db.getOrCreateHnswForDim(old_dim);
                        _ = old_h.insert(old_clone, row_id) catch {
                            std.debug.print("CRITICAL: Failed to rollback HNSW state after insert failure (column '{s}')\n", .{update.column_name});
                        };
                    }
                    std.debug.print("Error inserting new vector to HNSW (column '{s}'): {}\n", .{ update.column_name, err });
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

        // Phase 3: Apply all SET assignments using table.update() to create new versions
        // This creates one new version with all updates applied
        // Note: We call update() for each column which creates multiple versions
        // TODO Phase 4: Optimize to create single version for multi-column updates
        for (cmd.assignments.items) |assignment| {
            try table.update(row_id, assignment.column, assignment.value, tx_id, clog);
        }

        // Get the updated row for index updates
        const updated_row = table.get(row_id, snapshot, clog).?;

        // Phase 1: Update B-tree indexes after row mutation
        try db.index_manager.onUpdate(cmd.table_name, row_id, &old_row_for_index, updated_row);

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

    // Note: Auto-VACUUM is now triggered in executor.zig after auto-commit
    // (removed from here to avoid triggering before transaction commits)

    var result = QueryResult.init(db.allocator);
    try result.addColumn("updated");
    var row = ArrayList(ColumnValue).init(db.allocator);
    try row.append(ColumnValue{ .int = @intCast(updated_count) });
    try result.addRow(row);

    return result;
}

// ============================================================================
// Maintenance Commands - VACUUM
// ============================================================================

/// Execute VACUUM command
/// Removes old row versions that are no longer visible to any transaction
/// This frees up memory by cleaning up version chains
///
/// Supports:
/// - VACUUM (all tables)
/// - VACUUM table_name (specific table)
pub fn executeVacuum(db: *Database, cmd: sql.VacuumCmd) !QueryResult {
    var result = QueryResult.init(db.allocator);
    errdefer result.deinit();

    // Add result columns
    try result.addColumn("table_name");
    try result.addColumn("versions_removed");
    try result.addColumn("total_chains");
    try result.addColumn("total_versions");
    try result.addColumn("max_chain_length");

    // Get minimum visible transaction ID
    // This is the oldest transaction that might still need to see old versions
    const min_visible_txid = blk: {
        // Get the minimum transaction ID from all active transactions
        var min_txid: u64 = std.math.maxInt(u64);
        var found_any = false;

        db.tx_manager.mutex.lock();
        defer db.tx_manager.mutex.unlock();

        var it = db.tx_manager.active_txs.valueIterator();
        while (it.next()) |tx_ptr| {
            if (tx_ptr.*.id < min_txid) {
                min_txid = tx_ptr.*.id;
                found_any = true;
            }
        }

        // If no active transactions, use next_tx_id (no one can see old versions)
        if (!found_any) {
            min_txid = db.tx_manager.next_tx_id.load(.monotonic);
        }

        break :blk min_txid;
    };

    if (cmd.table_name) |table_name| {
        // VACUUM specific table
        const table = db.tables.get(table_name) orelse return sql.SqlError.TableNotFound;

        const stats = try table.vacuum(min_visible_txid, &db.tx_manager.clog);

        // Add result row
        var row = ArrayList(ColumnValue).init(db.allocator);
        const name_copy = try db.allocator.dupe(u8, table_name);
        try row.append(ColumnValue{ .text = name_copy });
        try row.append(ColumnValue{ .int = @intCast(stats.versions_removed) });
        try row.append(ColumnValue{ .int = @intCast(stats.total_chains) });
        try row.append(ColumnValue{ .int = @intCast(stats.total_versions) });
        try row.append(ColumnValue{ .int = @intCast(stats.max_chain_length) });
        try result.addRow(row);
    } else {
        // VACUUM all tables
        var it = db.tables.iterator();
        while (it.next()) |entry| {
            const table_name = entry.key_ptr.*;
            const table = entry.value_ptr.*;

            const stats = try table.vacuum(min_visible_txid, &db.tx_manager.clog);

            // Add result row
            var row = ArrayList(ColumnValue).init(db.allocator);
            const name_copy = try db.allocator.dupe(u8, table_name);
            try row.append(ColumnValue{ .text = name_copy });
            try row.append(ColumnValue{ .int = @intCast(stats.versions_removed) });
            try row.append(ColumnValue{ .int = @intCast(stats.total_chains) });
            try row.append(ColumnValue{ .int = @intCast(stats.total_versions) });
            try row.append(ColumnValue{ .int = @intCast(stats.max_chain_length) });
            try result.addRow(row);
        }
    }

    return result;
}
