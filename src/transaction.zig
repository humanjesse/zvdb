const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;
const ColumnValue = @import("table.zig").ColumnValue;

/// Transaction state management for zvdb (MVCC-enabled)
///
/// Provides:
/// - Transaction lifecycle (begin/commit/rollback)
/// - Operation logging for rollback
/// - Transaction ID management
/// - MVCC snapshots for consistent reads
/// - Transaction status tracking (CLOG)

// ============================================================================
// MVCC Snapshot
// ============================================================================

/// Snapshot represents a point-in-time view of the database
/// Used for consistent reads in MVCC
pub const Snapshot = struct {
    /// Transaction ID when this snapshot was taken
    txid: u64,

    /// List of transactions that were active when snapshot was created
    /// These transactions' changes are invisible to this snapshot
    active_txids: []const u64,

    /// Timestamp when snapshot was created (for debugging/metrics)
    timestamp: i64,

    /// Allocator for memory management
    allocator: Allocator,

    pub fn init(txid: u64, active_txids: []const u64, allocator: Allocator) !Snapshot {
        const owned_active = try allocator.dupe(u64, active_txids);
        return Snapshot{
            .txid = txid,
            .active_txids = owned_active,
            .timestamp = std.time.timestamp(),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Snapshot) void {
        self.allocator.free(self.active_txids);
    }

    /// Check if a transaction ID was active when this snapshot was taken
    pub fn wasActive(self: *const Snapshot, check_txid: u64) bool {
        for (self.active_txids) |active_txid| {
            if (active_txid == check_txid) return true;
        }
        return false;
    }
};

// ============================================================================
// Transaction Status (CLOG - Commit Log)
// ============================================================================

/// Transaction status in the commit log
pub const TxStatus = enum(u8) {
    /// Transaction is currently in progress
    in_progress = 0,

    /// Transaction has been committed
    committed = 1,

    /// Transaction has been aborted/rolled back
    aborted = 2,
};

/// Commit Log (CLOG) - tracks status of all transactions
/// In a production system, this would be persistent storage
pub const CommitLog = struct {
    /// Map of transaction ID to status
    status_map: AutoHashMap(u64, TxStatus),

    /// Mutex to protect concurrent access
    mutex: std.Thread.Mutex,

    /// Allocator for memory management
    allocator: Allocator,

    pub fn init(allocator: Allocator) CommitLog {
        return CommitLog{
            .status_map = AutoHashMap(u64, TxStatus).init(allocator),
            .mutex = std.Thread.Mutex{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *CommitLog) void {
        self.status_map.deinit();
    }

    /// Set the status of a transaction
    pub fn setStatus(self: *CommitLog, txid: u64, status: TxStatus) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        try self.status_map.put(txid, status);
    }

    /// Get the status of a transaction
    pub fn getStatus(self: *CommitLog, txid: u64) TxStatus {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.status_map.get(txid) orelse .in_progress;
    }

    /// Check if a transaction is committed
    pub fn isCommitted(self: *CommitLog, txid: u64) bool {
        // Bootstrap transaction (txid=0) is always considered committed
        // This is used for operations outside of explicit transactions
        if (txid == 0) return true;

        return self.getStatus(txid) == .committed;
    }

    /// Check if a transaction is aborted
    pub fn isAborted(self: *CommitLog, txid: u64) bool {
        // Bootstrap transaction (txid=0) is never aborted
        if (txid == 0) return false;

        return self.getStatus(txid) == .aborted;
    }

    /// Check if a transaction is in progress
    pub fn isInProgress(self: *CommitLog, txid: u64) bool {
        // Bootstrap transaction (txid=0) is never in progress (it's always committed)
        if (txid == 0) return false;

        return self.getStatus(txid) == .in_progress;
    }

    /// Save commit log to a binary file
    /// File format:
    ///   - Magic: "CLOG" (4 bytes)
    ///   - Version: 1 (4 bytes)
    ///   - Entry count (u64)
    ///   - For each entry: TX ID (u64) + Status (u8)
    pub fn save(self: *CommitLog, path: []const u8) !void {
        const utils = @import("utils.zig");

        // Ensure parent directory exists
        if (std.fs.path.dirname(path)) |dir_path| {
            std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
                error.PathAlreadyExists => {},
                else => return err,
            };
        }

        const file = try std.fs.cwd().createFile(path, .{});
        defer file.close();

        // Write header
        const magic: u32 = 0x434C_4F47; // "CLOG" in hex
        const version: u32 = 1;
        try utils.writeInt(file, u32, magic);
        try utils.writeInt(file, u32, version);

        // Lock mutex to safely iterate over status_map
        self.mutex.lock();
        defer self.mutex.unlock();

        // Write entry count
        const count = self.status_map.count();
        try utils.writeInt(file, u64, count);

        // Write each transaction entry
        var it = self.status_map.iterator();
        while (it.next()) |entry| {
            const txid = entry.key_ptr.*;
            const status = entry.value_ptr.*;

            try utils.writeInt(file, u64, txid);
            try utils.writeInt(file, u8, @intFromEnum(status));
        }
    }

    /// Load commit log from a binary file
    /// Returns a new CommitLog instance with loaded transaction states
    pub fn load(allocator: Allocator, path: []const u8) !CommitLog {
        const utils = @import("utils.zig");

        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        // Read and verify header
        const magic = try utils.readInt(file, u32);
        if (magic != 0x434C_4F47) return error.InvalidFileFormat;

        const version = try utils.readInt(file, u32);
        if (version != 1) return error.UnsupportedVersion;

        // Initialize new CommitLog
        var clog = CommitLog.init(allocator);
        errdefer clog.deinit();

        // Read entry count
        const count = try utils.readInt(file, u64);

        // Read each transaction entry
        for (0..count) |_| {
            const txid = try utils.readInt(file, u64);
            const status_int = try utils.readInt(file, u8);
            const status: TxStatus = @enumFromInt(status_int);

            try clog.status_map.put(txid, status);
        }

        return clog;
    }

    /// Merge recovered transaction state from WAL replay into this CommitLog
    /// This is used during recovery to combine checkpoint state with WAL state
    /// If a transaction exists in both, the recovered state takes precedence
    pub fn mergeRecoveredState(self: *CommitLog, recovered: AutoHashMap(u64, TxStatus)) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var it = recovered.iterator();
        while (it.next()) |entry| {
            const txid = entry.key_ptr.*;
            const status = entry.value_ptr.*;

            // Recovered state takes precedence over checkpoint state
            try self.status_map.put(txid, status);
        }
    }
};

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

    /// Snapshot for this transaction (MVCC)
    /// Captures what data is visible to this transaction
    snapshot: ?Snapshot,

    /// List of operations performed in this transaction (for rollback)
    operations: ArrayList(Operation),

    /// Allocator for memory management
    allocator: Allocator,

    /// Initialize a new transaction without snapshot (for backward compatibility)
    pub fn init(id: u64, allocator: Allocator) Transaction {
        return Transaction{
            .id = id,
            .state = .active,
            .snapshot = null,
            .operations = ArrayList(Operation).init(allocator),
            .allocator = allocator,
        };
    }

    /// Initialize a new transaction with snapshot (MVCC mode)
    pub fn initWithSnapshot(id: u64, snapshot: Snapshot, allocator: Allocator) Transaction {
        return Transaction{
            .id = id,
            .state = .active,
            .snapshot = snapshot,
            .operations = ArrayList(Operation).init(allocator),
            .allocator = allocator,
        };
    }

    /// Deinitialize the transaction and free all resources
    pub fn deinit(self: *Transaction) void {
        // Clean up snapshot if present
        if (self.snapshot) |*snapshot| {
            snapshot.deinit();
        }

        // Clean up operations
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

/// Manages transactions for the database (MVCC-enabled)
pub const TransactionManager = struct {
    /// Active transactions (transaction ID -> Transaction pointer)
    /// Protected by mutex for thread-safe concurrent access
    active_txs: AutoHashMap(u64, *Transaction),

    /// Next transaction ID to assign (atomic for thread-safety)
    next_tx_id: std.atomic.Value(u64),

    /// Commit Log for tracking transaction status
    clog: CommitLog,

    /// Mutex to protect active_txs map
    mutex: std.Thread.Mutex,

    /// Transaction stack for tracking current execution context
    /// The top of the stack (last element) is the current transaction
    /// Supports nested transactions
    tx_stack: ArrayList(u64),

    /// Allocator for memory management
    allocator: Allocator,

    /// Initialize a new transaction manager
    pub fn init(allocator: Allocator) TransactionManager {
        return TransactionManager{
            .active_txs = AutoHashMap(u64, *Transaction).init(allocator),
            .next_tx_id = std.atomic.Value(u64).init(1),
            .clog = CommitLog.init(allocator),
            .mutex = std.Thread.Mutex{},
            .tx_stack = ArrayList(u64).init(allocator),
            .allocator = allocator,
        };
    }

    /// Deinitialize the transaction manager
    pub fn deinit(self: *TransactionManager) void {
        // Clean up all active transactions
        var it = self.active_txs.valueIterator();
        while (it.next()) |tx_ptr| {
            tx_ptr.*.deinit();
            self.allocator.destroy(tx_ptr.*);
        }
        self.active_txs.deinit();
        self.clog.deinit();
        self.tx_stack.deinit();
    }

    /// Begin a new transaction (MVCC mode)
    /// Returns the transaction ID
    /// Creates a snapshot capturing all currently active transactions
    pub fn begin(self: *TransactionManager) !u64 {
        // Atomically get next transaction ID
        const tx_id = self.next_tx_id.fetchAdd(1, .monotonic);

        // Lock mutex to safely access active_txs
        self.mutex.lock();
        defer self.mutex.unlock();

        // Capture list of currently active transaction IDs for snapshot
        var active_list = ArrayList(u64).init(self.allocator);
        defer active_list.deinit();

        var it = self.active_txs.keyIterator();
        while (it.next()) |active_txid_ptr| {
            try active_list.append(active_txid_ptr.*);
        }

        // Create snapshot for this transaction
        const snapshot = try Snapshot.init(tx_id, active_list.items, self.allocator);

        // Create new transaction with snapshot
        const tx_ptr = try self.allocator.create(Transaction);
        tx_ptr.* = Transaction.initWithSnapshot(tx_id, snapshot, self.allocator);

        // Add to active transactions
        try self.active_txs.put(tx_id, tx_ptr);

        // Mark as in_progress in CLOG
        try self.clog.setStatus(tx_id, .in_progress);

        // Push transaction ID to stack (makes it the current transaction)
        try self.tx_stack.append(tx_id);

        return tx_id;
    }

    /// Commit the transaction (MVCC mode)
    pub fn commit(self: *TransactionManager, tx_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get the transaction
        const tx_ptr = self.active_txs.get(tx_id) orelse return error.NoActiveTransaction;

        // Pop transaction from stack
        // Verify it's the current transaction (top of stack)
        if (self.tx_stack.items.len > 0) {
            const top_tx_id = self.tx_stack.pop();
            if (top_tx_id != tx_id) {
                // Trying to commit non-current transaction - this is an error
                // For now, we'll allow it but log a warning
                // In production, you might want to enforce strict LIFO order
            }
        }

        // Mark transaction as committed
        try tx_ptr.commit();

        // Update CLOG before removing from active set
        try self.clog.setStatus(tx_id, .committed);

        // Remove from active transactions
        _ = self.active_txs.remove(tx_id);

        // Clean up transaction
        tx_ptr.deinit();
        self.allocator.destroy(tx_ptr);
    }

    /// Rollback the transaction (MVCC mode)
    pub fn rollback(self: *TransactionManager, tx_id: u64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Get the transaction
        const tx_ptr = self.active_txs.get(tx_id) orelse return error.NoActiveTransaction;

        // Pop transaction from stack
        // Verify it's the current transaction (top of stack)
        if (self.tx_stack.items.len > 0) {
            const top_tx_id = self.tx_stack.pop();
            if (top_tx_id != tx_id) {
                // Trying to rollback non-current transaction
                // For now, we'll allow it but this could be enforced
            }
        }

        // Mark transaction as aborted
        try tx_ptr.rollback();

        // Update CLOG before removing from active set
        try self.clog.setStatus(tx_id, .aborted);

        // Remove from active transactions
        _ = self.active_txs.remove(tx_id);

        // Clean up transaction
        tx_ptr.deinit();
        self.allocator.destroy(tx_ptr);
    }

    /// Get a specific transaction by ID
    pub fn getTransaction(self: *TransactionManager, tx_id: u64) ?*Transaction {
        self.mutex.lock();
        defer self.mutex.unlock();

        return self.active_txs.get(tx_id);
    }

    /// Get the current active transaction
    /// Returns the transaction at the top of the transaction stack
    /// This represents the most recently begun transaction (nested transaction support)
    pub fn getCurrentTx(self: *TransactionManager) ?*Transaction {
        self.mutex.lock();
        defer self.mutex.unlock();

        // Return transaction at top of stack (last element)
        if (self.tx_stack.items.len == 0) {
            return null;
        }

        const current_tx_id = self.tx_stack.items[self.tx_stack.items.len - 1];
        return self.active_txs.get(current_tx_id);
    }

    /// Check if there's at least one active transaction
    pub fn hasActiveTx(self: *const TransactionManager) bool {
        // Note: Can't lock in const method, so this is a best-effort check
        return self.active_txs.count() > 0;
    }

    /// Get count of active transactions
    pub fn activeCount(self: *const TransactionManager) usize {
        return self.active_txs.count();
    }

    /// Get snapshot for a specific transaction
    pub fn getSnapshot(self: *TransactionManager, tx_id: u64) ?*const Snapshot {
        self.mutex.lock();
        defer self.mutex.unlock();

        const tx = self.active_txs.get(tx_id) orelse return null;
        if (tx.snapshot) |*snapshot| {
            return snapshot;
        }
        return null;
    }
};
