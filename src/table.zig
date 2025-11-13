const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;

// ============================================================================
// Persistence Helpers
// ============================================================================

/// Helper to write integers in little-endian format to a file
fn writeInt(file: std.fs.File, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try file.writeAll(&bytes);
}

/// Helper to read integers in little-endian format from a file
fn readInt(file: std.fs.File, comptime T: type) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    const n = try file.readAll(&bytes);
    if (n != @sizeOf(T)) return error.UnexpectedEOF;
    return std.mem.readInt(T, &bytes, .little);
}

// ============================================================================
// Data Types
// ============================================================================

/// Column value types
pub const ColumnValue = union(enum) {
    null_value,
    int: i64,
    float: f64,
    text: []const u8,
    bool: bool,
    embedding: []const f32, // Vector embedding for semantic search

    pub fn deinit(self: *ColumnValue, allocator: Allocator) void {
        switch (self.*) {
            .text => |s| allocator.free(s),
            .embedding => |e| allocator.free(e),
            else => {},
        }
    }

    pub fn clone(self: ColumnValue, allocator: Allocator) !ColumnValue {
        return switch (self) {
            .null_value => .null_value,
            .int => |i| ColumnValue{ .int = i },
            .float => |f| ColumnValue{ .float = f },
            .bool => |b| ColumnValue{ .bool = b },
            .text => |s| blk: {
                const owned = try allocator.alloc(u8, s.len);
                @memcpy(owned, s);
                break :blk ColumnValue{ .text = owned };
            },
            .embedding => |e| blk: {
                const owned = try allocator.alloc(f32, e.len);
                @memcpy(owned, e);
                break :blk ColumnValue{ .embedding = owned };
            },
        };
    }

    pub fn format(
        self: ColumnValue,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;
        switch (self) {
            .null_value => try writer.writeAll("NULL"),
            .int => |i| try writer.print("{d}", .{i}),
            .float => |f| try writer.print("{d:.2}", .{f}),
            .bool => |b| try writer.writeAll(if (b) "true" else "false"),
            .text => |s| try writer.print("\"{s}\"", .{s}),
            .embedding => |e| try writer.print("[{d} dims]", .{e.len}),
        }
    }
};

/// Column type definition
pub const ColumnType = enum {
    int,
    float,
    text,
    bool,
    embedding,
};

/// Column schema
pub const Column = struct {
    name: []const u8,
    col_type: ColumnType,

    pub fn init(allocator: Allocator, name: []const u8, col_type: ColumnType) !Column {
        const owned_name = try allocator.alloc(u8, name.len);
        @memcpy(owned_name, name);
        return Column{
            .name = owned_name,
            .col_type = col_type,
        };
    }

    pub fn deinit(self: *Column, allocator: Allocator) void {
        allocator.free(self.name);
    }
};

/// A single row in a table
pub const Row = struct {
    id: u64,
    values: StringHashMap(ColumnValue),

    pub fn init(allocator: Allocator, id: u64) Row {
        return Row{
            .id = id,
            .values = StringHashMap(ColumnValue).init(allocator),
        };
    }

    pub fn deinit(self: *Row, allocator: Allocator) void {
        var it = self.values.iterator();
        while (it.next()) |entry| {
            allocator.free(entry.key_ptr.*);
            var val = entry.value_ptr.*;
            val.deinit(allocator);
        }
        self.values.deinit();
    }

    pub fn set(self: *Row, allocator: Allocator, column: []const u8, value: ColumnValue) !void {
        const owned_key = try allocator.alloc(u8, column.len);
        @memcpy(owned_key, column);
        const owned_value = try value.clone(allocator);
        try self.values.put(owned_key, owned_value);
    }

    pub fn get(self: *Row, column: []const u8) ?ColumnValue {
        return self.values.get(column);
    }
};

/// A table with schema and rows
pub const Table = struct {
    name: []const u8,
    columns: ArrayList(Column),
    rows: AutoHashMap(u64, Row),
    next_id: u64,
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8) !Table {
        const owned_name = try allocator.alloc(u8, name.len);
        @memcpy(owned_name, name);

        return Table{
            .name = owned_name,
            .columns = ArrayList(Column).init(allocator),
            .rows = AutoHashMap(u64, Row).init(allocator),
            .next_id = 1,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Table) void {
        self.allocator.free(self.name);

        for (self.columns.items) |*col| {
            col.deinit(self.allocator);
        }
        self.columns.deinit();

        var it = self.rows.iterator();
        while (it.next()) |entry| {
            var row = entry.value_ptr.*;
            row.deinit(self.allocator);
        }
        self.rows.deinit();
    }

    pub fn addColumn(self: *Table, name: []const u8, col_type: ColumnType) !void {
        const column = try Column.init(self.allocator, name, col_type);
        try self.columns.append(column);
    }

    pub fn insert(self: *Table, values: StringHashMap(ColumnValue)) !u64 {
        const id = self.next_id;
        self.next_id += 1;

        var row = Row.init(self.allocator, id);

        var it = values.iterator();
        while (it.next()) |entry| {
            try row.set(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        try self.rows.put(id, row);
        return id;
    }

    pub fn insertWithId(self: *Table, id: u64, values: StringHashMap(ColumnValue)) !void {
        var row = Row.init(self.allocator, id);

        var it = values.iterator();
        while (it.next()) |entry| {
            try row.set(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        try self.rows.put(id, row);
        if (id >= self.next_id) {
            self.next_id = id + 1;
        }
    }

    pub fn get(self: *Table, id: u64) ?*Row {
        return self.rows.getPtr(id);
    }

    pub fn delete(self: *Table, id: u64) bool {
        if (self.rows.fetchRemove(id)) |entry| {
            var row = entry.value;
            row.deinit(self.allocator);
            return true;
        }
        return false;
    }

    pub fn count(self: *Table) usize {
        return self.rows.count();
    }

    pub fn getAllRows(self: *Table, allocator: Allocator) ![]u64 {
        var ids = try allocator.alloc(u64, self.rows.count());
        var it = self.rows.keyIterator();
        var i: usize = 0;
        while (it.next()) |id| {
            ids[i] = id.*;
            i += 1;
        }
        return ids;
    }

    /// Save table to a binary file (.zvdb format)
    pub fn save(self: *Table, path: []const u8) !void {
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
        const magic: u32 = 0x5456_4442; // "TVDB" in hex
        const version: u32 = 1;
        try writeInt(file, u32, magic);
        try writeInt(file, u32, version);

        // Write metadata
        try writeInt(file, u64, self.name.len);
        try file.writeAll(self.name);
        try writeInt(file, u64, self.next_id);

        // Write schema
        try writeInt(file, u64, self.columns.items.len);
        for (self.columns.items) |col| {
            try writeInt(file, u64, col.name.len);
            try file.writeAll(col.name);
            try writeInt(file, u8, @intFromEnum(col.col_type));
        }

        // Write rows
        try writeInt(file, u64, self.rows.count());
        var it = self.rows.iterator();
        while (it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const row = entry.value_ptr.*;

            try writeInt(file, u64, row_id);
            try writeInt(file, u64, row.values.count());

            var val_it = row.values.iterator();
            while (val_it.next()) |val_entry| {
                const col_name = val_entry.key_ptr.*;
                const value = val_entry.value_ptr.*;

                // Write column name
                try writeInt(file, u64, col_name.len);
                try file.writeAll(col_name);

                // Write value type tag
                try writeInt(file, u8, @intFromEnum(value));

                // Write value data
                switch (value) {
                    .null_value => {},
                    .int => |i| try writeInt(file, i64, i),
                    .float => |f| try file.writeAll(std.mem.asBytes(&f)),
                    .bool => |b| try writeInt(file, u8, if (b) 1 else 0),
                    .text => |s| {
                        try writeInt(file, u64, s.len);
                        try file.writeAll(s);
                    },
                    .embedding => |e| {
                        try writeInt(file, u64, e.len);
                        for (e) |val| {
                            try file.writeAll(std.mem.asBytes(&val));
                        }
                    },
                }
            }
        }
    }

    /// Load table from a binary file (.zvdb format)
    pub fn load(allocator: Allocator, path: []const u8) !Table {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        // Read and verify header
        const magic = try readInt(file, u32);
        if (magic != 0x5456_4442) return error.InvalidFileFormat;

        const version = try readInt(file, u32);
        if (version != 1) return error.UnsupportedVersion;

        // Read metadata
        const name_len = try readInt(file, u64);
        const name = try allocator.alloc(u8, name_len);
        // Handle cleanup explicitly before table owns the name
        _ = file.readAll(name) catch |err| {
            allocator.free(name);
            return err;
        };

        const next_id = readInt(file, u64) catch |err| {
            allocator.free(name);
            return err;
        };

        // Initialize table - from this point, table owns name
        var table = Table{
            .name = name,
            .columns = ArrayList(Column).init(allocator),
            .rows = AutoHashMap(u64, Row).init(allocator),
            .next_id = next_id,
            .allocator = allocator,
        };
        errdefer table.deinit();

        // Read schema
        const column_count = try readInt(file, u64);
        for (0..column_count) |_| {
            const col_name_len = try readInt(file, u64);
            const col_name = try allocator.alloc(u8, col_name_len);
            errdefer allocator.free(col_name);
            _ = try file.readAll(col_name);

            const col_type_int = try readInt(file, u8);
            const col_type: ColumnType = @enumFromInt(col_type_int);

            try table.columns.append(Column{
                .name = col_name,
                .col_type = col_type,
            });
        }

        // Read rows
        const row_count = try readInt(file, u64);
        for (0..row_count) |_| {
            const row_id = try readInt(file, u64);
            var row = Row.init(allocator, row_id);
            errdefer row.deinit(allocator);

            const value_count = try readInt(file, u64);
            for (0..value_count) |_| {
                // Read column name
                const col_name_len = try readInt(file, u64);
                const col_name = try allocator.alloc(u8, col_name_len);
                errdefer allocator.free(col_name);
                _ = try file.readAll(col_name);

                // Read value type
                const value_type_int = try readInt(file, u8);

                // Read value data based on type
                const value: ColumnValue = switch (value_type_int) {
                    0 => .null_value,
                    1 => .{ .int = try readInt(file, i64) },
                    2 => blk: {
                        var bytes: [8]u8 = undefined;
                        _ = try file.readAll(&bytes);
                        const f = std.mem.bytesToValue(f64, &bytes);
                        break :blk .{ .float = f };
                    },
                    3 => blk: { // text
                        const text_len = try readInt(file, u64);
                        const text = try allocator.alloc(u8, text_len);
                        errdefer allocator.free(text);
                        _ = try file.readAll(text);
                        break :blk .{ .text = text };
                    },
                    4 => .{ .bool = (try readInt(file, u8)) != 0 },
                    5 => blk: { // embedding
                        const emb_len = try readInt(file, u64);
                        const embedding = try allocator.alloc(f32, emb_len);
                        errdefer allocator.free(embedding);
                        for (embedding) |*val| {
                            var bytes: [4]u8 = undefined;
                            _ = try file.readAll(&bytes);
                            val.* = std.mem.bytesToValue(f32, &bytes);
                        }
                        break :blk .{ .embedding = embedding };
                    },
                    else => return error.InvalidValueType,
                };

                try row.values.put(col_name, value);
            }

            try table.rows.put(row_id, row);
        }

        return table;
    }
};
