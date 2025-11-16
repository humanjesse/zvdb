const std = @import("std");
const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const Table = @import("../table.zig").Table;
const ColumnValue = @import("../table.zig").ColumnValue;
const HNSW = @import("../hnsw.zig").HNSW;
const WalWriter = @import("../wal.zig").WalWriter;
const IndexManager = @import("../index_manager.zig").IndexManager;
const TransactionManager = @import("../transaction.zig").TransactionManager;

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

/// Main database with SQL and vector search
pub const Database = struct {
    tables: StringHashMap(*Table),
    hnsw: ?*HNSW(f32), // Optional vector index
    index_manager: IndexManager, // B-tree indexes for fast queries
    allocator: Allocator,
    data_dir: ?[]const u8, // Data directory for persistence (owned)
    auto_save: bool, // Auto-save on deinit
    wal: ?*WalWriter, // Write-Ahead Log for durability (optional)
    tx_manager: TransactionManager, // Transaction manager (single source of truth for all transaction IDs)

    pub fn init(allocator: Allocator) Database {
        return Database{
            .tables = StringHashMap(*Table).init(allocator),
            .hnsw = null,
            .index_manager = IndexManager.init(allocator),
            .allocator = allocator,
            .data_dir = null,
            .auto_save = false,
            .wal = null,
            .tx_manager = TransactionManager.init(allocator),
        };
    }

    pub fn deinit(self: *Database) void {
        // Auto-save if enabled and data_dir is set
        if (self.auto_save and self.data_dir != null) {
            // Import persistence module to call saveAll
            const persistence = @import("persistence.zig");
            persistence.saveAll(self, self.data_dir.?) catch |err| {
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

        if (self.hnsw) |h| {
            h.deinit();
            self.allocator.destroy(h);
        }

        // Clean up indexes
        self.index_manager.deinit();

        if (self.data_dir) |dir| {
            self.allocator.free(dir);
        }
    }

    /// Initialize vector search capabilities
    pub fn initVectorSearch(self: *Database, m: usize, ef_construction: usize) !void {
        const hnsw_ptr = try self.allocator.create(HNSW(f32));
        hnsw_ptr.* = HNSW(f32).init(self.allocator, m, ef_construction);
        self.hnsw = hnsw_ptr;
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
