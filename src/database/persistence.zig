const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const Table = @import("../table.zig").Table;
const HNSW = @import("../hnsw.zig").HNSW;
const Allocator = std.mem.Allocator;

/// Save all tables and HNSW index to the data directory
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

    // Save HNSW index if it exists
    if (db.hnsw) |h| {
        const hnsw_path = try std.fmt.allocPrint(
            db.allocator,
            "{s}/vectors.hnsw",
            .{dir_path},
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
        } else if (std.mem.eql(u8, entry.name, "vectors.hnsw")) {
            // Load HNSW index
            const file_path = try std.fmt.allocPrint(
                allocator,
                "{s}/{s}",
                .{ dir_path, entry.name },
            );
            defer allocator.free(file_path);

            const hnsw = try allocator.create(HNSW(f32));
            hnsw.* = try HNSW(f32).load(allocator, file_path);
            db.hnsw = hnsw;
        }
    }

    return db;
}
