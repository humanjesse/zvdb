// Main database module - delegates to submodules for better organization
//
// This file serves as the public API for the database, bringing together:
// - core.zig: Database struct, QueryResult, basic initialization
// - executor.zig: SQL query execution
// - recovery.zig: WAL and crash recovery
// - persistence.zig: Save/load functionality
// - vector_ops.zig: HNSW vector search operations

const core = @import("database/core.zig");
const executor = @import("database/executor.zig");
const recovery = @import("database/recovery.zig");
const persistence = @import("database/persistence.zig");
const vector_ops = @import("database/vector_ops.zig");

// Re-export core types
pub const Database = core.Database;
pub const QueryResult = core.QueryResult;

// Re-export utility functions
pub const valuesEqual = core.valuesEqual;

// Add delegation methods to Database
pub const DatabaseExt = struct {
    /// Execute a SQL query
    pub fn execute(db: *Database, query: []const u8) !QueryResult {
        return executor.execute(db, query);
    }

    /// Enable Write-Ahead Logging
    pub fn enableWal(db: *Database, wal_dir: []const u8) !void {
        return recovery.enableWal(db, wal_dir);
    }

    /// Recover from WAL after a crash
    pub fn recoverFromWal(db: *Database, wal_dir: []const u8) !usize {
        return recovery.recoverFromWal(db, wal_dir);
    }

    /// Write a WAL record
    pub fn writeWalRecord(
        db: *Database,
        record_type: @import("wal.zig").WalRecordType,
        table_name: []const u8,
        row_id: u64,
        data: []const u8,
    ) !u64 {
        return recovery.writeWalRecord(db, record_type, table_name, row_id, data);
    }

    /// Rebuild HNSW index from table data
    pub fn rebuildHnswFromTables(db: *Database) !usize {
        return vector_ops.rebuildHnswFromTables(db);
    }

    /// Save all tables to disk
    pub fn saveAll(db: *Database, dir_path: []const u8) !void {
        return persistence.saveAll(db, dir_path);
    }

    /// Load all tables from disk
    pub fn loadAll(allocator: @import("std").mem.Allocator, dir_path: []const u8) !Database {
        return persistence.loadAll(allocator, dir_path);
    }
};

// For backward compatibility, we can provide these as standalone functions too
pub const execute = DatabaseExt.execute;
pub const enableWal = DatabaseExt.enableWal;
pub const recoverFromWal = DatabaseExt.recoverFromWal;
pub const writeWalRecord = DatabaseExt.writeWalRecord;
pub const rebuildHnswFromTables = DatabaseExt.rebuildHnswFromTables;
pub const saveAll = DatabaseExt.saveAll;
pub const loadAll = DatabaseExt.loadAll;
