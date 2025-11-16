// ============================================================================
// Transaction Commands - BEGIN, COMMIT, ROLLBACK
// ============================================================================

const std = @import("std");
const core = @import("../core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const ColumnValue = @import("../../table.zig").ColumnValue;
const ArrayList = std.array_list.Managed;
const WalRecordType = @import("../../wal.zig").WalRecordType;
const Transaction = @import("../../transaction.zig");
const Operation = Transaction.Operation;

// ============================================================================
// Transaction Lifecycle
// ============================================================================

/// Execute BEGIN command
/// NOTE: Multiple concurrent transactions are supported via MVCC.
/// Each BEGIN creates a new transaction with snapshot isolation.
pub fn executeBegin(db: *Database) !QueryResult {
    const tx_id = try db.tx_manager.begin();

    // Write BEGIN to WAL if enabled
    if (db.wal) |w| {
        _ = try w.writeRecord(.{
            .record_type = WalRecordType.begin_tx,
            .tx_id = tx_id,
            .lsn = 0, // Will be assigned by writeRecord
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
            .checksum = 0, // Will be calculated during serialization
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
pub fn executeCommit(db: *Database) !QueryResult {
    const tx = db.tx_manager.getCurrentTx() orelse return error.NoActiveTransaction;
    const tx_id = tx.id;

    // Write COMMIT to WAL if enabled
    if (db.wal) |w| {
        _ = try w.writeRecord(.{
            .record_type = WalRecordType.commit_tx,
            .tx_id = tx_id,
            .lsn = 0, // Will be assigned by writeRecord
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
            .checksum = 0, // Will be calculated during serialization
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
pub fn executeRollback(db: *Database) !QueryResult {
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
        _ = try w.writeRecord(.{
            .record_type = WalRecordType.rollback_tx,
            .tx_id = tx_id,
            .lsn = 0, // Will be assigned by writeRecord
            .table_name = "",
            .row_id = 0,
            .data = &[_]u8{},
            .checksum = 0, // Will be calculated during serialization
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

// ============================================================================
// Undo Operations (Rollback Support)
// ============================================================================

/// Undo a single operation (helper for rollback)
///
/// NOTE: This function uses null,null for table.get() to see the latest uncommitted version.
/// This is intentional during rollback - we need to undo the transaction's own changes.
///
/// TODO Phase 4: Replace physical undo with MVCC-native rollback:
/// - Mark transaction as ABORTED in CLOG
/// - Versions created by aborted tx become invisible automatically
/// - No need to physically undo operations
fn undoOperation(db: *Database, op: Operation) !void {
    switch (op) {
        .insert => |ins| {
            // Undo INSERT: physically delete the inserted row
            const table = db.tables.get(ins.table_name) orelse return error.TableNotFound;

            // Phase 3: Get row BEFORE deletion for index cleanup
            // Using null,null to see uncommitted version from this transaction
            const row = table.get(ins.row_id, null, null);
            if (row) |r| {
                try db.index_manager.onDelete(ins.table_name, ins.row_id, r);
            }

            // Physically delete the row to free memory (not just mark as deleted)
            // This is safe during rollback because the row was created by this transaction
            // and no other transaction can see it yet
            _ = table.physicalDelete(ins.row_id) catch {};
        },
        .delete => |del| {
            // Undo DELETE: restore the deleted row by clearing its deletion marker
            const table = db.tables.get(del.table_name) orelse return error.TableNotFound;

            // Simply undelete the row (clears xmax to make it visible again)
            // This is much cleaner than creating a new RowVersion which would leak memory
            try table.undelete(del.row_id);

            // Update indexes - using null,null to see just-restored version
            const row = table.get(del.row_id, null, null);
            if (row) |r| {
                try db.index_manager.onInsert(del.table_name, del.row_id, r);
            }
        },
        .update => |upd| {
            // Undo UPDATE: remove the new version and restore the old version
            const table = db.tables.get(upd.table_name) orelse return error.TableNotFound;

            // Get the new (current) version for index cleanup
            const new_row = table.get(upd.row_id, null, null);

            // Undo the update (removes new version, restores old version as chain head)
            try table.undoUpdate(upd.row_id);

            // Get the restored old version for index update
            const old_row = table.get(upd.row_id, null, null);

            // Update indexes - remove new version's index entries, add old version's entries
            if (new_row != null and old_row != null) {
                try db.index_manager.onUpdate(upd.table_name, upd.row_id, new_row.?, old_row.?);
            }
        },
    }
}
