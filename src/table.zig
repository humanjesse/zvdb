const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;
const utils = @import("utils.zig");

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
        errdefer allocator.free(owned_key);
        @memcpy(owned_key, column);

        var owned_value = try value.clone(allocator);
        errdefer owned_value.deinit(allocator);

        const result = try self.values.getOrPut(column);
        if (result.found_existing) {
            // Free old key and value
            allocator.free(result.key_ptr.*);
            var old_val = result.value_ptr.*;
            old_val.deinit(allocator);
        }
        result.key_ptr.* = owned_key;
        result.value_ptr.* = owned_value;
    }

    pub fn get(self: *const Row, column: []const u8) ?ColumnValue {
        return self.values.get(column);
    }

    /// Serialize row to bytes for WAL storage
    /// Returns owned byte slice that caller must free
    pub fn serialize(self: *const Row, allocator: Allocator) ![]u8 {
        // Calculate size needed
        var size: usize = 8; // row_id
        size += 8; // num_values

        var val_it = self.values.iterator();
        while (val_it.next()) |entry| {
            const col_name = entry.key_ptr.*;
            const value = entry.value_ptr.*;

            size += 8; // col_name length
            size += col_name.len; // col_name
            size += 1; // value type tag

            // Value data size
            switch (value) {
                .null_value => {},
                .int => size += 8,
                .float => size += 8,
                .bool => size += 1,
                .text => |s| {
                    size += 8; // text length
                    size += s.len; // text data
                },
                .embedding => |e| {
                    size += 8; // embedding length
                    size += e.len * 4; // f32 data (4 bytes each)
                },
            }
        }

        // Allocate buffer
        var buffer = try allocator.alloc(u8, size);
        errdefer allocator.free(buffer);

        var offset: usize = 0;

        // Write row_id
        std.mem.writeInt(u64, buffer[offset..][0..8], self.id, .little);
        offset += 8;

        // Write num_values
        const num_values: u64 = self.values.count();
        std.mem.writeInt(u64, buffer[offset..][0..8], num_values, .little);
        offset += 8;

        // Write each value
        var it = self.values.iterator();
        while (it.next()) |entry| {
            const col_name = entry.key_ptr.*;
            const value = entry.value_ptr.*;

            // Write column name
            std.mem.writeInt(u64, buffer[offset..][0..8], col_name.len, .little);
            offset += 8;
            @memcpy(buffer[offset..][0..col_name.len], col_name);
            offset += col_name.len;

            // Write value type tag
            buffer[offset] = @intFromEnum(std.meta.activeTag(value));
            offset += 1;

            // Write value data
            switch (value) {
                .null_value => {},
                .int => |i| {
                    std.mem.writeInt(i64, buffer[offset..][0..8], i, .little);
                    offset += 8;
                },
                .float => |f| {
                    @memcpy(buffer[offset..][0..8], std.mem.asBytes(&f));
                    offset += 8;
                },
                .bool => |b| {
                    buffer[offset] = if (b) 1 else 0;
                    offset += 1;
                },
                .text => |s| {
                    std.mem.writeInt(u64, buffer[offset..][0..8], s.len, .little);
                    offset += 8;
                    @memcpy(buffer[offset..][0..s.len], s);
                    offset += s.len;
                },
                .embedding => |e| {
                    std.mem.writeInt(u64, buffer[offset..][0..8], e.len, .little);
                    offset += 8;
                    for (e) |val| {
                        @memcpy(buffer[offset..][0..4], std.mem.asBytes(&val));
                        offset += 4;
                    }
                },
            }
        }

        std.debug.assert(offset == size);
        return buffer;
    }

    /// Deserialize row from bytes (used during WAL recovery)
    /// Returns new Row that caller owns
    pub fn deserialize(buffer: []const u8, allocator: Allocator) !Row {
        if (buffer.len < 16) return error.BufferTooSmall;

        var offset: usize = 0;

        // Read row_id
        const row_id = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read num_values
        const num_values = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        var row = Row.init(allocator, row_id);
        errdefer row.deinit(allocator);

        // Read each value
        var i: usize = 0;
        while (i < num_values) : (i += 1) {
            // Read column name
            if (offset + 8 > buffer.len) return error.BufferTooSmall;
            const col_name_len = std.mem.readInt(u64, buffer[offset..][0..8], .little);
            offset += 8;

            if (offset + col_name_len > buffer.len) return error.BufferTooSmall;
            const col_name_slice = buffer[offset..][0..col_name_len];
            offset += col_name_len;

            // Read value type tag
            if (offset >= buffer.len) return error.BufferTooSmall;
            const value_tag = buffer[offset];
            offset += 1;

            // Allocate owned column name
            const owned_col_name = try allocator.dupe(u8, col_name_slice);
            errdefer allocator.free(owned_col_name);

            // Read value data based on type
            var value = switch (value_tag) {
                0 => ColumnValue.null_value, // null_value
                1 => blk: { // int
                    if (offset + 8 > buffer.len) return error.BufferTooSmall;
                    const val = std.mem.readInt(i64, buffer[offset..][0..8], .little);
                    offset += 8;
                    break :blk ColumnValue{ .int = val };
                },
                2 => blk: { // float
                    if (offset + 8 > buffer.len) return error.BufferTooSmall;
                    const val = @as(*const f64, @ptrCast(@alignCast(buffer[offset..][0..8]))).*;
                    offset += 8;
                    break :blk ColumnValue{ .float = val };
                },
                3 => blk: { // text
                    if (offset + 8 > buffer.len) return error.BufferTooSmall;
                    const text_len = std.mem.readInt(u64, buffer[offset..][0..8], .little);
                    offset += 8;

                    if (offset + text_len > buffer.len) return error.BufferTooSmall;
                    const text = try allocator.dupe(u8, buffer[offset..][0..text_len]);
                    offset += text_len;
                    break :blk ColumnValue{ .text = text };
                },
                4 => blk: { // bool
                    if (offset >= buffer.len) return error.BufferTooSmall;
                    const val = buffer[offset] != 0;
                    offset += 1;
                    break :blk ColumnValue{ .bool = val };
                },
                5 => blk: { // embedding
                    if (offset + 8 > buffer.len) return error.BufferTooSmall;
                    const emb_len = std.mem.readInt(u64, buffer[offset..][0..8], .little);
                    offset += 8;

                    if (offset + emb_len * 4 > buffer.len) return error.BufferTooSmall;
                    const embedding = try allocator.alloc(f32, emb_len);
                    var j: usize = 0;
                    while (j < emb_len) : (j += 1) {
                        embedding[j] = @as(*const f32, @ptrCast(@alignCast(buffer[offset..][0..4]))).*;
                        offset += 4;
                    }
                    break :blk ColumnValue{ .embedding = embedding };
                },
                else => return error.InvalidValueType,
            };
            errdefer value.deinit(allocator);

            // Insert directly into the hashmap without cloning, since we already own the data
            // This avoids the double allocation that would occur with row.set()
            const result = try row.values.getOrPut(col_name_slice);
            if (result.found_existing) {
                // Free old key and value (shouldn't happen during deserialization, but be safe)
                allocator.free(result.key_ptr.*);
                var old_val = result.value_ptr.*;
                old_val.deinit(allocator);
            }
            result.key_ptr.* = owned_col_name;
            result.value_ptr.* = value;
        }

        return row;
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
            entry.value_ptr.deinit(self.allocator);
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
        try utils.writeInt(file, u32, magic);
        try utils.writeInt(file, u32, version);

        // Write metadata
        try utils.writeInt(file, u64, self.name.len);
        try file.writeAll(self.name);
        try utils.writeInt(file, u64, self.next_id);

        // Write schema
        try utils.writeInt(file, u64, self.columns.items.len);
        for (self.columns.items) |col| {
            try utils.writeInt(file, u64, col.name.len);
            try file.writeAll(col.name);
            try utils.writeInt(file, u8, @intFromEnum(col.col_type));
        }

        // Write rows
        try utils.writeInt(file, u64, self.rows.count());
        var it = self.rows.iterator();
        while (it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const row = entry.value_ptr.*;

            try utils.writeInt(file, u64, row_id);
            try utils.writeInt(file, u64, row.values.count());

            var val_it = row.values.iterator();
            while (val_it.next()) |val_entry| {
                const col_name = val_entry.key_ptr.*;
                const value = val_entry.value_ptr.*;

                // Write column name
                try utils.writeInt(file, u64, col_name.len);
                try file.writeAll(col_name);

                // Write value type tag
                try utils.writeInt(file, u8, @intFromEnum(std.meta.activeTag(value)));

                // Write value data
                switch (value) {
                    .null_value => {},
                    .int => |i| try utils.writeInt(file, i64, i),
                    .float => |f| try file.writeAll(std.mem.asBytes(&f)),
                    .bool => |b| try utils.writeInt(file, u8, if (b) 1 else 0),
                    .text => |s| {
                        try utils.writeInt(file, u64, s.len);
                        try file.writeAll(s);
                    },
                    .embedding => |e| {
                        try utils.writeInt(file, u64, e.len);
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
        const magic = try utils.readInt(file, u32);
        if (magic != 0x5456_4442) return error.InvalidFileFormat;

        const version = try utils.readInt(file, u32);
        if (version != 1) return error.UnsupportedVersion;

        // Read metadata
        const name_len = try utils.readInt(file, u64);
        const name = try allocator.alloc(u8, name_len);
        // Handle cleanup explicitly before table owns the name
        _ = file.readAll(name) catch |err| {
            allocator.free(name);
            return err;
        };

        const next_id = utils.readInt(file, u64) catch |err| {
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
        const column_count = try utils.readInt(file, u64);
        for (0..column_count) |_| {
            const col_name_len = try utils.readInt(file, u64);
            const col_name = try allocator.alloc(u8, col_name_len);
            errdefer allocator.free(col_name);
            _ = try file.readAll(col_name);

            const col_type_int = try utils.readInt(file, u8);
            const col_type: ColumnType = @enumFromInt(col_type_int);

            try table.columns.append(Column{
                .name = col_name,
                .col_type = col_type,
            });
        }

        // Read rows
        const row_count = try utils.readInt(file, u64);
        for (0..row_count) |_| {
            const row_id = try utils.readInt(file, u64);
            var row = Row.init(allocator, row_id);
            errdefer row.deinit(allocator);

            const value_count = try utils.readInt(file, u64);
            for (0..value_count) |_| {
                // Read column name
                const col_name_len = try utils.readInt(file, u64);
                const col_name = try allocator.alloc(u8, col_name_len);
                errdefer allocator.free(col_name);
                _ = try file.readAll(col_name);

                // Read value type
                const value_type_int = try utils.readInt(file, u8);

                // Read value data based on type
                const value: ColumnValue = switch (value_type_int) {
                    0 => .null_value,
                    1 => .{ .int = try utils.readInt(file, i64) },
                    2 => blk: {
                        var bytes: [8]u8 = undefined;
                        _ = try file.readAll(&bytes);
                        const f = std.mem.bytesToValue(f64, &bytes);
                        break :blk .{ .float = f };
                    },
                    3 => blk: { // text
                        const text_len = try utils.readInt(file, u64);
                        const text = try allocator.alloc(u8, text_len);
                        errdefer allocator.free(text);
                        _ = try file.readAll(text);
                        break :blk .{ .text = text };
                    },
                    4 => .{ .bool = (try utils.readInt(file, u8)) != 0 },
                    5 => blk: { // embedding
                        const emb_len = try utils.readInt(file, u64);
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
