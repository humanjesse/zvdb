const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;

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
};
