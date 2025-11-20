const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const Table = @import("../table.zig").Table;
const HNSW = @import("../hnsw.zig").HNSW;
const CommitLog = @import("../transaction.zig").CommitLog;
const Allocator = std.mem.Allocator;

/// Save all tables and HNSW index to the data directory (v2 format - no MVCC)
///
/// ⚠️ WARNING: This method only saves the newest row versions and does NOT preserve:
///   - Transaction history (committed/aborted status)
///   - Uncommitted or in-progress transaction data
///   - MVCC version chains
///
/// For production use with full MVCC support, use saveAllMvcc() instead.
/// This method is provided for backward compatibility and simple use cases.
pub fn saveAll(db: *Database, dir_path: []const u8) !void {
    // Create directory if it doesn't exist
    std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Save each table
    var it = db.tables.iterator();
    while (it.next()) |entry| {
        const table_name = entry.key_ptr.*;
        const table = entry.value_ptr.*;

        const file_path = try std.fmt.allocPrint(
            db.allocator,
            "{s}/{s}.zvdb",
            .{ dir_path, table_name },
        );
        defer db.allocator.free(file_path);

        try table.save(file_path);
    }

    // Save all per-(dimension,column) HNSW indexes
    var hnsw_it = db.hnsw_indexes.iterator();
    while (hnsw_it.next()) |entry| {
        const key = entry.key_ptr.*;
        const h = entry.value_ptr.*;

        // Encode column name for filesystem safety (replace / with _)
        const safe_col_name = try std.mem.replaceOwned(u8, db.allocator, key.column_name, "/", "_");
        defer db.allocator.free(safe_col_name);

        const hnsw_path = try std.fmt.allocPrint(
            db.allocator,
            "{s}/vectors_{d}_{s}.hnsw",
            .{ dir_path, key.dimension, safe_col_name },
        );
        defer db.allocator.free(hnsw_path);

        try h.save(hnsw_path);
    }
}

/// Load all tables and HNSW index from the data directory
pub fn loadAll(allocator: Allocator, dir_path: []const u8) !Database {
    var db = Database.init(allocator);
    errdefer db.deinit();

    // Open directory
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            // Directory doesn't exist yet, return empty database
            return db;
        },
        else => return err,
    };
    defer dir.close();

    // Iterate through files
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;

        // Check file extension
        const ext = std.fs.path.extension(entry.name);

        if (std.mem.eql(u8, ext, ".zvdb")) {
            // Load table file
            const file_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ dir_path, entry.name },
            );
            defer allocator.free(file_path);

            var table = try Table.load(allocator, file_path);
            errdefer table.deinit();

            // Allocate owned table pointer
            const table_ptr = try allocator.create(Table);
            table_ptr.* = table;

            // Duplicate table name for the key
            const table_name_key = try allocator.dupe(u8, table.name);

            // Add to database
            try db.tables.put(table_name_key, table_ptr);
        } else if (std.mem.startsWith(u8, entry.name, "vectors_") and std.mem.endsWith(u8, entry.name, ".hnsw")) {
            // Load per-(dimension,column) HNSW index
            // Parse from filename: vectors_{dim}_{column}.hnsw
            const prefix_len = "vectors_".len;
            const suffix_len = ".hnsw".len;
            const middle_part = entry.name[prefix_len .. entry.name.len - suffix_len];

            // Find the first underscore to separate dimension from column name
            if (std.mem.indexOf(u8, middle_part, "_")) |underscore_pos| {
                const dim_str = middle_part[0..underscore_pos];
                const col_name_encoded = middle_part[underscore_pos + 1 ..];

                // Decode column name (replace _ with /)
                const col_name = try std.mem.replaceOwned(u8, allocator, col_name_encoded, "_", "/");

                const dim = try std.fmt.parseInt(usize, dim_str, 10);

                const file_path = try std.fmt.allocPrint(
                    allocator,
                    "{s}/{s}",
                    .{ dir_path, entry.name },
                );
                defer allocator.free(file_path);

                const hnsw = try allocator.create(HNSW(f32));
                hnsw.* = try HNSW(f32).load(allocator, file_path);

                const key = core.HnswIndexKey{
                    .dimension = dim,
                    .column_name = col_name, // Owned by the key
                };
                try db.hnsw_indexes.put(key, hnsw);
            } else {
                // Old format: vectors_{dim}.hnsw (backward compatibility)
                const dim_str = middle_part;
                const dim = try std.fmt.parseInt(usize, dim_str, 10);

                const file_path = try std.fmt.allocPrint(
                    allocator,
                    "{s}/{s}",
                    .{ dir_path, entry.name },
                );
                defer allocator.free(file_path);

                const hnsw = try allocator.create(HNSW(f32));
                hnsw.* = try HNSW(f32).load(allocator, file_path);

                // Use "default" as column name for old format
                const key = try core.HnswIndexKey.init(allocator, dim, "default");
                try db.hnsw_indexes.put(key, hnsw);
            }
        } else if (std.mem.eql(u8, entry.name, "vectors.hnsw")) {
            // Legacy: Load old single HNSW index as 768-dimensional for backward compatibility
            const file_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ dir_path, entry.name },
            );
            defer allocator.free(file_path);

            const hnsw = try allocator.create(HNSW(f32));
            hnsw.* = try HNSW(f32).load(allocator, file_path);

            // Use "default" as column name for legacy format
            const key = try core.HnswIndexKey.init(allocator, 768, "default");
            try db.hnsw_indexes.put(key, hnsw);
        }
    }

    return db;
}

/// Save all tables with full MVCC version chains and CommitLog state
/// This enables complete MVCC recovery including transaction history
pub fn saveAllMvcc(db: *Database, dir_path: []const u8) !void {
    // Create directory if it doesn't exist
    std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };

    // Get current checkpoint transaction ID
    const checkpoint_txid = db.getCurrentTxId();

    // Step 1: Save CommitLog state
    const clog_path = try std.fmt.allocPrint(
        db.allocator,
        "{s}/commitlog.zvdb",
        .{dir_path},
    );
    defer db.allocator.free(clog_path);

    try db.tx_manager.clog.save(clog_path);

    // Step 2: Save each table with full MVCC data
    var it = db.tables.iterator();
    while (it.next()) |entry| {
        const table_name = entry.key_ptr.*;
        const table = entry.value_ptr.*;

        const file_path = try std.fmt.allocPrint(
            db.allocator,
            "{s}/{s}.zvdb",
            .{ dir_path, table_name },
        );
        defer db.allocator.free(file_path);

        try table.saveMvcc(file_path, checkpoint_txid);
    }

    // Step 3: Save all per-(dimension,column) HNSW indexes
    var hnsw_it = db.hnsw_indexes.iterator();
    while (hnsw_it.next()) |entry| {
        const key = entry.key_ptr.*;
        const h = entry.value_ptr.*;

        // Encode column name for filesystem safety (replace / with _)
        const safe_col_name = try std.mem.replaceOwned(u8, db.allocator, key.column_name, "/", "_");
        defer db.allocator.free(safe_col_name);

        const hnsw_path = try std.fmt.allocPrint(
            db.allocator,
            "{s}/vectors_{d}_{s}.hnsw",
            .{ dir_path, key.dimension, safe_col_name },
        );
        defer db.allocator.free(hnsw_path);

        try h.save(hnsw_path);
    }
}

/// Load all tables with full MVCC version chains and CommitLog state
/// If WAL recovery is needed, call recoverFromWal() after this
pub fn loadAllMvcc(allocator: Allocator, dir_path: []const u8) !Database {
    var db = Database.init(allocator);
    errdefer db.deinit();

    // Open directory
    var dir = std.fs.cwd().openDir(dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            // Directory doesn't exist yet, return empty database
            return db;
        },
        else => return err,
    };
    defer dir.close();

    // Step 1: Try to load CommitLog if it exists
    const clog_path = try std.fmt.allocPrint(
        allocator,
        "{s}/commitlog.zvdb",
        .{dir_path},
    );
    defer allocator.free(clog_path);

    // Load CLOG or use empty one if file doesn't exist
    const clog_file_exists = blk: {
        std.fs.cwd().access(clog_path, .{}) catch {
            break :blk false;
        };
        break :blk true;
    };

    if (clog_file_exists) {
        // Load existing CLOG
        const loaded_clog = try CommitLog.load(allocator, clog_path);
        // Replace the default CLOG in tx_manager
        db.tx_manager.clog.deinit();
        db.tx_manager.clog = loaded_clog;
    }

    // Step 2: Iterate through files to load tables and indexes
    var it = dir.iterate();
    while (try it.next()) |entry| {
        if (entry.kind != .file) continue;

        // Check file extension
        const ext = std.fs.path.extension(entry.name);

        if (std.mem.eql(u8, ext, ".zvdb") and !std.mem.eql(u8, entry.name, "commitlog.zvdb")) {
            // Load table file with MVCC support
            const file_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ dir_path, entry.name },
            );
            defer allocator.free(file_path);

            // Try to load as v3 (MVCC), fall back to v2 if needed
            var table = Table.loadMvcc(allocator, file_path) catch |err| switch (err) {
                error.UnsupportedVersion => blk: {
                    // Fall back to old load method for v1/v2 files
                    break :blk try Table.load(allocator, file_path);
                },
                else => return err,
            };
            errdefer table.deinit();

            // Allocate owned table pointer
            const table_ptr = try allocator.create(Table);
            table_ptr.* = table;

            // Duplicate table name for the key
            const table_name_key = try allocator.dupe(u8, table.name);

            // Add to database
            try db.tables.put(table_name_key, table_ptr);
        } else if (std.mem.startsWith(u8, entry.name, "vectors_") and std.mem.endsWith(u8, entry.name, ".hnsw")) {
            // Load per-(dimension,column) HNSW index (same as loadAll)
            const prefix_len = "vectors_".len;
            const suffix_len = ".hnsw".len;
            const middle_part = entry.name[prefix_len .. entry.name.len - suffix_len];

            // Find the first underscore to separate dimension from column name
            if (std.mem.indexOf(u8, middle_part, "_")) |underscore_pos| {
                const dim_str = middle_part[0..underscore_pos];
                const col_name_encoded = middle_part[underscore_pos + 1 ..];

                // Decode column name (replace _ with /)
                const col_name = try std.mem.replaceOwned(u8, allocator, col_name_encoded, "_", "/");

                const dim = try std.fmt.parseInt(usize, dim_str, 10);

                const file_path = try std.fmt.allocPrint(
                    allocator,
                    "{s}/{s}",
                    .{ dir_path, entry.name },
                );
                defer allocator.free(file_path);

                const hnsw = try allocator.create(HNSW(f32));
                hnsw.* = try HNSW(f32).load(allocator, file_path);

                const key = core.HnswIndexKey{
                    .dimension = dim,
                    .column_name = col_name, // Owned by the key
                };
                try db.hnsw_indexes.put(key, hnsw);
            } else {
                // Old format: vectors_{dim}.hnsw (backward compatibility)
                const dim_str = middle_part;
                const dim = try std.fmt.parseInt(usize, dim_str, 10);

                const file_path = try std.fmt.allocPrint(
                    allocator,
                    "{s}/{s}",
                    .{ dir_path, entry.name },
                );
                defer allocator.free(file_path);

                const hnsw = try allocator.create(HNSW(f32));
                hnsw.* = try HNSW(f32).load(allocator, file_path);

                // Use "default" as column name for old format
                const key = try core.HnswIndexKey.init(allocator, dim, "default");
                try db.hnsw_indexes.put(key, hnsw);
            }
        } else if (std.mem.eql(u8, entry.name, "vectors.hnsw")) {
            // Legacy: Load old single HNSW index as 768-dimensional
            const file_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ dir_path, entry.name },
            );
            defer allocator.free(file_path);

            const hnsw = try allocator.create(HNSW(f32));
            hnsw.* = try HNSW(f32).load(allocator, file_path);

            // Use "default" as column name for legacy format
            const key = try core.HnswIndexKey.init(allocator, 768, "default");
            try db.hnsw_indexes.put(key, hnsw);
        }
    }

    return db;
}
