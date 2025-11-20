const std = @import("std");
const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;
const ArrayList = std.array_list.Managed;
const Table = @import("../table.zig").Table;
const ColumnValue = @import("../table.zig").ColumnValue;
const HNSW = @import("../hnsw.zig").HNSW;
const WalWriter = @import("../wal.zig").WalWriter;
const IndexManager = @import("../index_manager.zig").IndexManager;
const TransactionManager = @import("../transaction.zig").TransactionManager;
const Snapshot = @import("../transaction.zig").Snapshot;
const CommitLog = @import("../transaction.zig").CommitLog;

// ============================================================================
// HNSW Index Key (for supporting multiple same-dimension embeddings)
// ============================================================================

/// Composite key for HNSW indexes: (dimension, column_name)
/// This allows multiple embedding columns with the same dimension in one table
/// Example: title_vec embedding(128) and content_vec embedding(128) in one table
pub const HnswIndexKey = struct {
    dimension: usize,
    column_name: []const u8,  // Owned string

    pub fn init(allocator: Allocator, dimension: usize, column_name: []const u8) !HnswIndexKey {
        const owned_name = try allocator.dupe(u8, column_name);
        return HnswIndexKey{
            .dimension = dimension,
            .column_name = owned_name,
        };
    }

    pub fn deinit(self: *HnswIndexKey, allocator: Allocator) void {
        allocator.free(self.column_name);
    }

    /// Hash function for use in HashMap
    pub fn hash(ctx: @This(), key: HnswIndexKey) u64 {
        _ = ctx;
        var hasher = std.hash.Wyhash.init(0);
        hasher.update(std.mem.asBytes(&key.dimension));
        hasher.update(key.column_name);
        return hasher.final();
    }

    /// Equality function for use in HashMap
    pub fn eql(ctx: @This(), a: HnswIndexKey, b: HnswIndexKey) bool {
        _ = ctx;
        return a.dimension == b.dimension and std.mem.eql(u8, a.column_name, b.column_name);
    }
};

// ============================================================================
// Validation Configuration
// ============================================================================

/// Validation mode determines how queries are validated
pub const ValidationMode = enum {
    /// Strict mode: validation errors cause queries to fail
    strict,

    /// Warning mode: validation errors are logged but execution continues
    warnings,

    /// Disabled mode: no validation is performed (backward compatibility)
    disabled,
};

/// Query result set
pub const QueryResult = struct {
    columns: ArrayList([]const u8),
    rows: ArrayList(ArrayList(ColumnValue)),
    allocator: Allocator,

    pub fn init(allocator: Allocator) QueryResult {
        return QueryResult{
            .columns = ArrayList([]const u8).init(allocator),
            .rows = ArrayList(ArrayList(ColumnValue)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueryResult) void {
        for (self.columns.items) |col| {
            self.allocator.free(col);
        }
        self.columns.deinit();

        for (self.rows.items) |*row| {
            for (row.items) |*val| {
                var v = val.*;
                v.deinit(self.allocator);
            }
            row.deinit();
        }
        self.rows.deinit();
    }

    pub fn addColumn(self: *QueryResult, name: []const u8) !void {
        const owned = try self.allocator.dupe(u8, name);
        try self.columns.append(owned);
    }

    pub fn addRow(self: *QueryResult, values: ArrayList(ColumnValue)) !void {
        try self.rows.append(values);
    }

    pub fn print(self: *QueryResult) !void {
        // Print header
        std.debug.print("\n", .{});
        for (self.columns.items, 0..) |col, i| {
            if (i > 0) std.debug.print(" | ", .{});
            std.debug.print("{s}", .{col});
        }
        std.debug.print("\n", .{});

        // Print separator
        for (self.columns.items, 0..) |_, i| {
            if (i > 0) std.debug.print("-+-", .{});
            std.debug.print("----------", .{});
        }
        std.debug.print("\n", .{});

        // Print rows
        for (self.rows.items) |row| {
            for (row.items, 0..) |val, i| {
                if (i > 0) std.debug.print(" | ", .{});
                std.debug.print("{any}", .{val});
            }
            std.debug.print("\n", .{});
        }

        std.debug.print("\n({d} rows)\n", .{self.rows.items.len});
    }
};

/// Auto-VACUUM configuration
pub const VacuumConfig = struct {
    /// Enable automatic VACUUM after operations
    enabled: bool = true,

    /// Trigger auto-VACUUM when version chain length exceeds this
    max_chain_length: usize = 10,

    /// Trigger auto-VACUUM every N transactions
    txn_interval: usize = 1000,
};

/// Main database with SQL and vector search
pub const Database = struct {
    tables: StringHashMap(*Table),
    hnsw_indexes: std.HashMap(HnswIndexKey, *HNSW(f32), HnswIndexKey, std.hash_map.default_max_load_percentage), // Per-(dimension,column) vector indexes
    index_manager: IndexManager, // B-tree indexes for fast queries
    allocator: Allocator,
    data_dir: ?[]const u8, // Data directory for persistence (owned)
    auto_save: bool, // Auto-save on deinit
    wal: ?*WalWriter, // Write-Ahead Log for durability (optional)
    tx_manager: TransactionManager, // Transaction manager (single source of truth for all transaction IDs)

    // Validation configuration
    enable_validation: bool, // Master switch for validation
    validation_mode: ValidationMode, // How validation errors are handled

    // Auto-VACUUM configuration (Phase 4)
    vacuum_config: VacuumConfig, // Auto-vacuum settings
    txn_count_since_vacuum: usize, // Track transactions for auto-vacuum

    // Resource limits (security)
    max_embeddings_per_row: usize, // Maximum embedding columns per row (default: 10)

    pub fn init(allocator: Allocator) Database {
        return Database{
            .tables = StringHashMap(*Table).init(allocator),
            .hnsw_indexes = std.HashMap(HnswIndexKey, *HNSW(f32), HnswIndexKey, std.hash_map.default_max_load_percentage).init(allocator),
            .index_manager = IndexManager.init(allocator),
            .allocator = allocator,
            .data_dir = null,
            .auto_save = false,
            .wal = null,
            .tx_manager = TransactionManager.init(allocator),
            .enable_validation = true, // Enabled by default for safety
            .validation_mode = .strict, // Strict mode by default
            .vacuum_config = VacuumConfig{}, // Auto-vacuum enabled with defaults
            .txn_count_since_vacuum = 0,
            .max_embeddings_per_row = 10, // Reasonable default to prevent resource exhaustion
        };
    }

    pub fn deinit(self: *Database) void {
        // Auto-save if enabled and data_dir is set
        if (self.auto_save and self.data_dir != null) {
            // Import persistence module to call saveAllMvcc (full MVCC support)
            const persistence = @import("persistence.zig");
            persistence.saveAllMvcc(self, self.data_dir.?) catch |err| {
                std.debug.print("Warning: Failed to auto-save database: {}\n", .{err});
            };
        }

        // Close WAL if enabled
        if (self.wal) |w| {
            w.deinit();
            self.allocator.destroy(w);
        }

        // Clean up transaction manager
        self.tx_manager.deinit();

        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.tables.deinit();

        // Clean up all HNSW indexes
        var hnsw_it = self.hnsw_indexes.iterator();
        while (hnsw_it.next()) |entry| {
            var key = entry.key_ptr.*;
            key.deinit(self.allocator);  // Free column_name
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.hnsw_indexes.deinit();

        // Clean up indexes
        self.index_manager.deinit();

        if (self.data_dir) |dir| {
            self.allocator.free(dir);
        }
    }

    /// Get or create HNSW index for a specific (dimension, column_name) pair
    /// This allows multiple embedding columns with the same dimension
    pub fn getOrCreateHnswForColumn(self: *Database, dim: usize, column_name: []const u8) !*HNSW(f32) {
        // Create the composite key
        const key = try HnswIndexKey.init(self.allocator, dim, column_name);
        errdefer key.deinit(self.allocator);

        // Check if HNSW index for this (dimension, column) already exists
        if (self.hnsw_indexes.get(key)) |existing_hnsw| {
            // Found existing index, free the temporary key
            var temp_key = key;
            temp_key.deinit(self.allocator);
            return existing_hnsw;
        }

        // Create new HNSW index for this (dimension, column)
        const hnsw_ptr = try self.allocator.create(HNSW(f32));
        errdefer self.allocator.destroy(hnsw_ptr);
        // Use standard HNSW parameters: M=16, efConstruction=200
        hnsw_ptr.* = HNSW(f32).init(self.allocator, 16, 200);

        // Store in map (key is now owned by the map)
        try self.hnsw_indexes.put(key, hnsw_ptr);
        return hnsw_ptr;
    }

    /// Get or create HNSW index for a specific dimension (deprecated)
    /// Use getOrCreateHnswForColumn instead for new code
    /// This is kept for backward compatibility with tests
    pub fn getOrCreateHnswForDim(self: *Database, dim: usize) !*HNSW(f32) {
        // Use a default column name for backward compatibility
        return self.getOrCreateHnswForColumn(dim, "default");
    }

    /// Initialize vector search capabilities (deprecated - use getOrCreateHnswForColumn instead)
    pub fn initVectorSearch(self: *Database, m: usize, ef_construction: usize) !void {
        // For backward compatibility, initialize a default 768-dimensional HNSW with column name "default"
        const key = try HnswIndexKey.init(self.allocator, 768, "default");
        const hnsw_ptr = try self.allocator.create(HNSW(f32));
        hnsw_ptr.* = HNSW(f32).init(self.allocator, m, ef_construction);
        try self.hnsw_indexes.put(key, hnsw_ptr);
    }

    /// Enable persistence with specified data directory
    pub fn enablePersistence(self: *Database, data_dir: []const u8, auto_save: bool) !void {
        if (self.data_dir) |old_dir| {
            self.allocator.free(old_dir);
        }
        self.data_dir = try self.allocator.dupe(u8, data_dir);
        self.auto_save = auto_save;
    }

    // ========================================================================
    // Auto-VACUUM Support (Phase 4)
    // ========================================================================

    /// Check if auto-VACUUM should be triggered and run it if needed
    /// Called after UPDATE/DELETE operations
    pub fn maybeAutoVacuum(self: *Database) void {
        if (!self.vacuum_config.enabled) return;

        // Check if we should trigger based on transaction count
        self.txn_count_since_vacuum += 1;
        const should_vacuum_by_count = self.txn_count_since_vacuum >= self.vacuum_config.txn_interval;

        // Check if any table has excessive version chain length
        var should_vacuum_by_chain = false;
        var it = self.tables.valueIterator();
        while (it.next()) |table_ptr| {
            const stats = table_ptr.*.getVacuumStats();
            if (stats.max_chain_length > self.vacuum_config.max_chain_length) {
                should_vacuum_by_chain = true;
                break;
            }
        }

        // Trigger auto-VACUUM if either condition is met
        if (should_vacuum_by_count or should_vacuum_by_chain) {
            self.runAutoVacuum() catch |err| {
                std.debug.print("Auto-VACUUM failed: {}\n", .{err});
            };
        }
    }

    /// Run auto-VACUUM on all tables
    /// Called automatically by maybeAutoVacuum when thresholds are exceeded
    fn runAutoVacuum(self: *Database) !void {
        // Get minimum visible transaction ID
        const min_visible_txid = blk: {
            var min_txid: u64 = std.math.maxInt(u64);
            var found_any = false;

            self.tx_manager.mutex.lock();
            defer self.tx_manager.mutex.unlock();

            var it = self.tx_manager.active_txs.valueIterator();
            while (it.next()) |tx_ptr| {
                if (tx_ptr.*.id < min_txid) {
                    min_txid = tx_ptr.*.id;
                    found_any = true;
                }
            }

            if (!found_any) {
                min_txid = self.tx_manager.next_tx_id.load(.monotonic);
            }

            break :blk min_txid;
        };

        // VACUUM all tables
        var table_it = self.tables.valueIterator();
        while (table_it.next()) |table_ptr| {
            _ = try table_ptr.*.vacuum(min_visible_txid, &self.tx_manager.clog);
        }

        // Reset counter
        self.txn_count_since_vacuum = 0;
    }

    // ========================================================================
    // Delegation methods for backward compatibility
    // These delegate to the modular implementations while maintaining the
    // original method-based API
    // ========================================================================

    /// Execute a SQL query - delegates to executor module
    pub fn execute(self: *Database, query: []const u8) !QueryResult {
        const executor = @import("executor.zig");
        return executor.execute(self, query);
    }

    /// Save all tables to disk - delegates to persistence module
    pub fn saveAll(self: *Database, dir_path: []const u8) !void {
        const persistence = @import("persistence.zig");
        return persistence.saveAll(self, dir_path);
    }

    /// Load all tables from disk - static method delegates to persistence module
    pub fn loadAll(allocator: Allocator, dir_path: []const u8) !Database {
        const persistence = @import("persistence.zig");
        return persistence.loadAll(allocator, dir_path);
    }

    /// Enable Write-Ahead Logging - delegates to recovery module
    pub fn enableWal(self: *Database, wal_dir: []const u8) !void {
        const recovery = @import("recovery.zig");
        return recovery.enableWal(self, wal_dir);
    }

    /// Recover from WAL after a crash - delegates to recovery module
    pub fn recoverFromWal(self: *Database, wal_dir: []const u8) !usize {
        const recovery = @import("recovery.zig");
        return recovery.recoverFromWal(self, wal_dir);
    }

    /// Write a WAL record - delegates to recovery module
    pub fn writeWalRecord(
        self: *Database,
        record_type: @import("../wal.zig").WalRecordType,
        table_name: []const u8,
        row_id: u64,
        data: []const u8,
    ) !u64 {
        const recovery = @import("recovery.zig");
        return recovery.writeWalRecord(self, record_type, table_name, row_id, data);
    }

    /// Rebuild HNSW index from table data - delegates to vector_ops module
    pub fn rebuildHnswFromTables(self: *Database) !usize {
        const vector_ops = @import("vector_ops.zig");
        return vector_ops.rebuildHnswFromTables(self);
    }

    // ========================================================================
    // MVCC Transaction Context Helpers (Phase 3)
    // ========================================================================

    /// Get current transaction ID (returns 0 if no active transaction - bootstrap mode)
    pub fn getCurrentTxId(self: *Database) u64 {
        if (self.tx_manager.getCurrentTx()) |tx| {
            return tx.id;
        }
        return 0; // Bootstrap transaction for backward compatibility
    }

    /// Get current transaction's snapshot (returns null if no active transaction)
    pub fn getCurrentSnapshot(self: *Database) ?*const Snapshot {
        if (self.tx_manager.getCurrentTx()) |tx| {
            if (tx.snapshot) |*snapshot| {
                return snapshot;
            }
        }
        return null;
    }

    /// Get CLOG (Commit Log) for visibility checks
    pub fn getClog(self: *Database) *CommitLog {
        return &self.tx_manager.clog;
    }

    // ========================================================================
    // MVCC Persistence (Phase 3)
    // ========================================================================

    /// Save all tables with full MVCC support (version chains + CommitLog)
    /// This is the Phase 3 checkpoint function that preserves transaction history
    pub fn saveAllMvcc(self: *Database, dir_path: []const u8) !void {
        const persistence = @import("persistence.zig");
        return persistence.saveAllMvcc(self, dir_path);
    }

    /// Load all tables with full MVCC support from checkpoint
    /// Falls back to v2 format for backward compatibility
    pub fn loadAllMvcc(allocator: Allocator, dir_path: []const u8) !Database {
        const persistence = @import("persistence.zig");
        return persistence.loadAllMvcc(allocator, dir_path);
    }

    // ========================================================================
    // Validation Configuration Helpers
    // ========================================================================

    /// Enable query validation
    pub fn enableValidation(self: *Database) void {
        self.enable_validation = true;
    }

    /// Disable query validation (for backward compatibility or debugging)
    pub fn disableValidation(self: *Database) void {
        self.enable_validation = false;
    }

    /// Set validation mode
    pub fn setValidationMode(self: *Database, mode: ValidationMode) void {
        self.validation_mode = mode;
    }

    /// Check if validation is currently enabled
    pub fn isValidationEnabled(self: *const Database) bool {
        return self.enable_validation;
    }

    /// Get current validation mode
    pub fn getValidationMode(self: *const Database) ValidationMode {
        return self.validation_mode;
    }
};

/// Helper function to compare column values for equality
pub fn valuesEqual(a: ColumnValue, b: ColumnValue) bool {
    return switch (a) {
        .null_value => false, // SQL Standard: NULL != NULL (NULL never equals anything, not even NULL)
        .int => |ai| b == .int and b.int == ai,
        .float => |af| b == .float and b.float == af,
        .bool => |ab| b == .bool and b.bool == ab,
        .text => |at| b == .text and std.mem.eql(u8, at, b.text),
        .embedding => false, // Don't compare embeddings directly
    };
}
