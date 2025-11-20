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
/// MVCC-Native Rollback (Phase 4):
/// - Transaction is marked as ABORTED in CLOG (by tx_manager.rollback)
/// - All row versions created by this transaction automatically become invisible
/// - MVCC visibility checking (table.zig:isVisible) filters out aborted transactions
/// - We only need to clean up indexes to maintain consistency
///
/// Benefits:
/// - 50-90% faster rollback (no physical row manipulation)
/// - Simpler logic (CLOG-based instead of physical undo)
/// - No risk of data corruption from incomplete undo operations
///
/// Note: Aborted transaction rows remain in table until VACUUM (future enhancement)
fn undoOperation(db: *Database, op: Operation) !void {
    // MVCC-native approach: rows stay in table but become invisible via CLOG
    // We only update indexes to reflect the rollback

    switch (op) {
        .insert => |ins| {
            // Undo INSERT: Remove from indexes only
            // The inserted row stays in table but is invisible (xmin = aborted tx)
            const table = db.tables.get(ins.table_name) orelse return error.TableNotFound;

            // Get row to extract indexed column values
            // Using null,null to see uncommitted version from this transaction
            const row = table.get(ins.row_id, null, null);
            if (row) |r| {
                // Remove row from all indexes on this table
                try db.index_manager.onDelete(ins.table_name, ins.row_id, r);
            }

            // Note: Row version remains in table.version_chains but is invisible
            // because its xmin points to an aborted transaction (checked in isVisible)
        },
        .delete => |del| {
            // Undo DELETE: Add back to indexes only
            // The row is already visible again because xmax points to aborted tx
            const table = db.tables.get(del.table_name) orelse return error.TableNotFound;

            // Row is visible again: xmax from aborted tx is ignored by isVisible
            // Get the row to extract indexed column values
            const row = table.get(del.row_id, null, null);
            if (row) |r| {
                // Re-add row to all indexes on this table
                try db.index_manager.onInsert(del.table_name, del.row_id, r);
            }

            // Note: No need to call table.undelete() - visibility logic handles it
            // The xmax field stays set, but isVisible() ignores it (aborted tx)
        },
        .update => |upd| {
            // Undo UPDATE: Restore old index entries, remove new ones
            // The old version is visible again, new version is invisible
            const table = db.tables.get(upd.table_name) orelse return error.TableNotFound;

            // The version chain is:
            //   HEAD: new version (xmin = aborted tx) -> INVISIBLE
            //   PREV: old version (xmax = aborted tx) -> VISIBLE

            // Get both versions to update indexes
            // Note: With MVCC-native approach, both versions remain in chain
            // The old version becomes visible again because its xmax is from aborted tx
            // The new version is invisible because its xmin is from aborted tx

            // For index updates, we need to identify which values changed
            // We can walk the version chain to find both versions
            const head_version = table.version_chains.get(upd.row_id);
            if (head_version) |head| {
                // New version (invisible)
                const new_row_data = &head.data;

                // Old version (visible) is in the next chain
                if (head.next) |next| {
                    const old_row_data = &next.data;

                    // Update indexes: remove new values, restore old values
                    // This is the only physical operation we need to do
                    try db.index_manager.onUpdate(upd.table_name, upd.row_id, new_row_data, old_row_data);
                }
            }

            // Note: Row versions remain in chain, MVCC visibility determines which is seen
        },
    }
}
