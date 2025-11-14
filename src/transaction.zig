const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const ColumnValue = @import("table.zig").ColumnValue;

/// Transaction state management for zvdb
///
/// Provides:
/// - Transaction lifecycle (begin/commit/rollback)
/// - Operation logging for rollback
/// - Transaction ID management

// ============================================================================
// Transaction State
// ============================================================================

/// Transaction state enum
pub const TransactionState = enum {
    /// Transaction is active and accepting operations
    active,

    /// Transaction has been committed (final state)
    committed,

    /// Transaction has been rolled back (final state)
    aborted,
};

// ============================================================================
// Operation Types for Rollback
// ============================================================================

/// Row data for undo operations
pub const Row = struct {
    values: StringHashMap(ColumnValue),
    allocator: Allocator,

    pub fn init(allocator: Allocator) Row {
        return Row{
            .values = StringHashMap(ColumnValue).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Row) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            var val = entry.value_ptr.*;
            val.deinit(self.allocator);
        }
        self.values.deinit();
    }

    pub fn clone(self: *const Row, allocator: Allocator) !Row {
        var new_row = Row.init(allocator);
        var it = self.values.iterator();
        while (it.next()) |entry| {
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            const value = try entry.value_ptr.clone(allocator);
            try new_row.values.put(key, value);
        }
        return new_row;
    }
};

/// Operation types that can be rolled back
pub const Operation = union(enum) {
    /// INSERT operation - need to delete the inserted row on rollback
    insert: struct {
        table_name: []const u8,
        row_id: u64,
    },

    /// DELETE operation - need to restore the deleted row on rollback
    delete: struct {
        table_name: []const u8,
        row_id: u64,
        saved_row: Row, // The deleted row data for restoration
    },

    /// UPDATE operation - need to restore old values on rollback
    update: struct {
        table_name: []const u8,
        row_id: u64,
        old_values: Row, // The old row data before update
    },

    /// Deinitialize the operation (free owned memory)
    pub fn deinit(self: *Operation, allocator: Allocator) void {
        switch (self.*) {
            .insert => |*op| {
                allocator.free(op.table_name);
            },
            .delete => |*op| {
                allocator.free(op.table_name);
                var row = op.saved_row;
                row.deinit();
            },
            .update => |*op| {
                allocator.free(op.table_name);
                var row = op.old_values;
                row.deinit();
            },
        }
    }
};

// ============================================================================
// Transaction
// ============================================================================

/// Represents a single transaction
pub const Transaction = struct {
    /// Unique transaction ID
    id: u64,

    /// Current state of the transaction
    state: TransactionState,

    /// List of operations performed in this transaction (for rollback)
    operations: ArrayList(Operation),

    /// Allocator for memory management
    allocator: Allocator,

    /// Initialize a new transaction
    pub fn init(id: u64, allocator: Allocator) Transaction {
        return Transaction{
            .id = id,
            .state = .active,
            .operations = ArrayList(Operation).init(allocator),
            .allocator = allocator,
        };
    }

    /// Deinitialize the transaction and free all resources
    pub fn deinit(self: *Transaction) void {
        for (self.operations.items) |*op| {
            op.deinit(self.allocator);
        }
        self.operations.deinit();
    }

    /// Add an operation to the transaction log
    pub fn addOperation(self: *Transaction, op: Operation) !void {
        if (self.state != .active) {
            return error.TransactionNotActive;
        }
        try self.operations.append(op);
    }

    /// Commit the transaction (mark as committed)
    pub fn commit(self: *Transaction) !void {
        if (self.state != .active) {
            return error.TransactionNotActive;
        }
        self.state = .committed;
    }

    /// Rollback the transaction (mark as aborted)
    pub fn rollback(self: *Transaction) !void {
        if (self.state != .active) {
            return error.TransactionNotActive;
        }
        self.state = .aborted;
    }
};

// ============================================================================
// Transaction Manager
// ============================================================================

/// Manages transactions for the database
pub const TransactionManager = struct {
    /// Current active transaction (only one transaction at a time for now)
    current_tx: ?Transaction,

    /// Next transaction ID to assign
    next_tx_id: u64,

    /// Allocator for memory management
    allocator: Allocator,

    /// Initialize a new transaction manager
    pub fn init(allocator: Allocator) TransactionManager {
        return TransactionManager{
            .current_tx = null,
            .next_tx_id = 1,
            .allocator = allocator,
        };
    }

    /// Deinitialize the transaction manager
    pub fn deinit(self: *TransactionManager) void {
        if (self.current_tx) |*tx| {
            tx.deinit();
        }
    }

    /// Begin a new transaction
    /// Returns the transaction ID
    pub fn begin(self: *TransactionManager) !u64 {
        // If there's already an active transaction, return error
        if (self.current_tx) |tx| {
            if (tx.state == .active) {
                return error.TransactionAlreadyActive;
            }
        }

        // Clean up old transaction if it exists
        if (self.current_tx) |*tx| {
            tx.deinit();
        }

        // Create new transaction
        const tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        self.current_tx = Transaction.init(tx_id, self.allocator);

        return tx_id;
    }

    /// Commit the current transaction
    pub fn commit(self: *TransactionManager, tx_id: u64) !void {
        if (self.current_tx == null) {
            return error.NoActiveTransaction;
        }

        var tx = &self.current_tx.?;
        if (tx.id != tx_id) {
            return error.InvalidTransactionId;
        }

        try tx.commit();
    }

    /// Rollback the current transaction
    pub fn rollback(self: *TransactionManager, tx_id: u64) !void {
        if (self.current_tx == null) {
            return error.NoActiveTransaction;
        }

        var tx = &self.current_tx.?;
        if (tx.id != tx_id) {
            return error.InvalidTransactionId;
        }

        try tx.rollback();
    }

    /// Get the current active transaction (or null)
    pub fn getCurrentTx(self: *TransactionManager) ?*Transaction {
        if (self.current_tx) |*tx| {
            if (tx.state == .active) {
                return tx;
            }
        }
        return null;
    }

    /// Check if there's an active transaction
    pub fn hasActiveTx(self: *const TransactionManager) bool {
        if (self.current_tx) |tx| {
            return tx.state == .active;
        }
        return false;
    }
};
