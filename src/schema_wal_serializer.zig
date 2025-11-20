const std = @import("std");
const Allocator = std.mem.Allocator;
const sql = @import("sql.zig");
const ColumnValue = @import("table.zig").ColumnValue;
const ArrayList = std.array_list.Managed;

/// Schema WAL Serialization - Phase 3
///
/// Handles serialization/deserialization of schema operations for WAL logging.
/// This ensures schema changes (CREATE TABLE, ALTER TABLE, etc.) are crash-safe.

// ============================================================================
// CREATE TABLE Serialization
// ============================================================================

/// Serialize CREATE TABLE command to binary format for WAL
///
/// Format:
///   - table_name_len (u32)
///   - table_name (bytes)
///   - if_not_exists (u8: 0 or 1)
///   - column_count (u32)
///   - For each column:
///     - column_name_len (u32)
///     - column_name (bytes)
///     - column_type (u8)
///     - embedding_dim (u32, only if column_type == embedding)
pub fn serializeCreateTable(allocator: Allocator, cmd: sql.CreateTableCmd) ![]u8 {
    var buffer = ArrayList(u8).init(allocator);
    errdefer buffer.deinit();

    const writer = buffer.writer();

    // Write table name
    try writer.writeInt(u32, @intCast(cmd.table_name.len), .little);
    try writer.writeAll(cmd.table_name);

    // Write if_not_exists flag
    try writer.writeByte(if (cmd.if_not_exists) 1 else 0);

    // Write column count
    try writer.writeInt(u32, @intCast(cmd.columns.len), .little);

    // Write each column
    for (cmd.columns) |col| {
        try writer.writeInt(u32, @intCast(col.name.len), .little);
        try writer.writeAll(col.name);
        try writer.writeByte(@intFromEnum(col.column_type));

        // If embedding type, write dimension
        if (col.column_type == .embedding) {
            try writer.writeInt(u32, @intCast(col.embedding_dim), .little);
        }
    }

    return try buffer.toOwnedSlice();
}

/// Deserialize CREATE TABLE command from WAL binary data
pub fn deserializeCreateTable(allocator: Allocator, data: []const u8) !sql.CreateTableCmd {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    // Read table name
    const table_name_len = try reader.readInt(u32, .little);
    const table_name = try allocator.alloc(u8, table_name_len);
    errdefer allocator.free(table_name);
    _ = try reader.readAll(table_name);

    // Read if_not_exists flag
    const if_not_exists = (try reader.readByte()) != 0;

    // Read column count
    const column_count = try reader.readInt(u32, .little);

    // Read columns
    var columns = ArrayList(sql.ColumnDef).init(allocator);
    errdefer {
        for (columns.items) |col| {
            allocator.free(col.name);
        }
        columns.deinit();
    }

    for (0..column_count) |_| {
        const col_name_len = try reader.readInt(u32, .little);
        const col_name = try allocator.alloc(u8, col_name_len);
        errdefer allocator.free(col_name);
        _ = try reader.readAll(col_name);

        const col_type_byte = try reader.readByte();
        const col_type: sql.ColumnType = @enumFromInt(col_type_byte);

        var embedding_dim: usize = 0;
        if (col_type == .embedding) {
            embedding_dim = try reader.readInt(u32, .little);
        }

        try columns.append(sql.ColumnDef{
            .name = col_name,
            .column_type = col_type,
            .embedding_dim = embedding_dim,
        });
    }

    return sql.CreateTableCmd{
        .table_name = table_name,
        .columns = try columns.toOwnedSlice(),
        .if_not_exists = if_not_exists,
    };
}

// ============================================================================
// DROP TABLE Serialization
// ============================================================================

/// Serialize DROP TABLE command
///
/// Format:
///   - table_name_len (u32)
///   - table_name (bytes)
///   - if_exists (u8: 0 or 1)
pub fn serializeDropTable(allocator: Allocator, cmd: sql.DropTableCmd) ![]u8 {
    var buffer = ArrayList(u8).init(allocator);
    errdefer buffer.deinit();

    const writer = buffer.writer();

    try writer.writeInt(u32, @intCast(cmd.table_name.len), .little);
    try writer.writeAll(cmd.table_name);
    try writer.writeByte(if (cmd.if_exists) 1 else 0);

    return try buffer.toOwnedSlice();
}

/// Deserialize DROP TABLE command
pub fn deserializeDropTable(allocator: Allocator, data: []const u8) !sql.DropTableCmd {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    const table_name_len = try reader.readInt(u32, .little);
    const table_name = try allocator.alloc(u8, table_name_len);
    errdefer allocator.free(table_name);
    _ = try reader.readAll(table_name);

    const if_exists = (try reader.readByte()) != 0;

    return sql.DropTableCmd{
        .table_name = table_name,
        .if_exists = if_exists,
    };
}

// ============================================================================
// ALTER TABLE Serialization
// ============================================================================

/// Serialize ALTER TABLE command
///
/// Format:
///   - table_name_len (u32)
///   - table_name (bytes)
///   - operation_type (u8: 0=add_column, 1=drop_column, 2=rename_column)
///   - operation-specific data (see each operation below)
pub fn serializeAlterTable(allocator: Allocator, cmd: sql.AlterTableCmd) ![]u8 {
    var buffer = ArrayList(u8).init(allocator);
    errdefer buffer.deinit();

    const writer = buffer.writer();

    // Write table name
    try writer.writeInt(u32, @intCast(cmd.table_name.len), .little);
    try writer.writeAll(cmd.table_name);

    // Write operation type and data
    switch (cmd.operation) {
        .add_column => |add| {
            try writer.writeByte(0); // operation type
            try writer.writeInt(u32, @intCast(add.name.len), .little);
            try writer.writeAll(add.name);
            try writer.writeByte(@intFromEnum(add.column_type));
            if (add.column_type == .embedding) {
                try writer.writeInt(u32, @intCast(add.embedding_dim), .little);
            }
        },
        .drop_column => |col_name| {
            try writer.writeByte(1); // operation type
            try writer.writeInt(u32, @intCast(col_name.len), .little);
            try writer.writeAll(col_name);
        },
        .rename_column => |rename| {
            try writer.writeByte(2); // operation type
            try writer.writeInt(u32, @intCast(rename.old_name.len), .little);
            try writer.writeAll(rename.old_name);
            try writer.writeInt(u32, @intCast(rename.new_name.len), .little);
            try writer.writeAll(rename.new_name);
        },
    }

    return try buffer.toOwnedSlice();
}

/// Deserialize ALTER TABLE command
pub fn deserializeAlterTable(allocator: Allocator, data: []const u8) !sql.AlterTableCmd {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    // Read table name
    const table_name_len = try reader.readInt(u32, .little);
    const table_name = try allocator.alloc(u8, table_name_len);
    errdefer allocator.free(table_name);
    _ = try reader.readAll(table_name);

    // Read operation type
    const op_type = try reader.readByte();

    const operation = switch (op_type) {
        0 => blk: { // add_column
            const col_name_len = try reader.readInt(u32, .little);
            const col_name = try allocator.alloc(u8, col_name_len);
            errdefer allocator.free(col_name);
            _ = try reader.readAll(col_name);

            const col_type_byte = try reader.readByte();
            const col_type: sql.ColumnType = @enumFromInt(col_type_byte);

            var embedding_dim: usize = 0;
            if (col_type == .embedding) {
                embedding_dim = try reader.readInt(u32, .little);
            }

            break :blk sql.AlterOperation{
                .add_column = sql.ColumnDef{
                    .name = col_name,
                    .column_type = col_type,
                    .embedding_dim = embedding_dim,
                },
            };
        },
        1 => blk: { // drop_column
            const col_name_len = try reader.readInt(u32, .little);
            const col_name = try allocator.alloc(u8, col_name_len);
            errdefer allocator.free(col_name);
            _ = try reader.readAll(col_name);

            break :blk sql.AlterOperation{ .drop_column = col_name };
        },
        2 => blk: { // rename_column
            const old_name_len = try reader.readInt(u32, .little);
            const old_name = try allocator.alloc(u8, old_name_len);
            errdefer allocator.free(old_name);
            _ = try reader.readAll(old_name);

            const new_name_len = try reader.readInt(u32, .little);
            const new_name = try allocator.alloc(u8, new_name_len);
            errdefer allocator.free(new_name);
            _ = try reader.readAll(new_name);

            break :blk sql.AlterOperation{
                .rename_column = .{ .old_name = old_name, .new_name = new_name },
            };
        },
        else => return error.InvalidAlterOperation,
    };

    return sql.AlterTableCmd{
        .table_name = table_name,
        .operation = operation,
    };
}

// ============================================================================
// CREATE INDEX Serialization
// ============================================================================

/// Serialize CREATE INDEX command
///
/// Format:
///   - index_name_len (u32)
///   - index_name (bytes)
///   - table_name_len (u32)
///   - table_name (bytes)
///   - column_name_len (u32)
///   - column_name (bytes)
pub fn serializeCreateIndex(allocator: Allocator, cmd: sql.CreateIndexCmd) ![]u8 {
    var buffer = ArrayList(u8).init(allocator);
    errdefer buffer.deinit();

    const writer = buffer.writer();

    try writer.writeInt(u32, @intCast(cmd.index_name.len), .little);
    try writer.writeAll(cmd.index_name);

    try writer.writeInt(u32, @intCast(cmd.table_name.len), .little);
    try writer.writeAll(cmd.table_name);

    try writer.writeInt(u32, @intCast(cmd.column_name.len), .little);
    try writer.writeAll(cmd.column_name);

    return try buffer.toOwnedSlice();
}

/// Deserialize CREATE INDEX command
pub fn deserializeCreateIndex(allocator: Allocator, data: []const u8) !sql.CreateIndexCmd {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    const index_name_len = try reader.readInt(u32, .little);
    const index_name = try allocator.alloc(u8, index_name_len);
    errdefer allocator.free(index_name);
    _ = try reader.readAll(index_name);

    const table_name_len = try reader.readInt(u32, .little);
    const table_name = try allocator.alloc(u8, table_name_len);
    errdefer allocator.free(table_name);
    _ = try reader.readAll(table_name);

    const column_name_len = try reader.readInt(u32, .little);
    const column_name = try allocator.alloc(u8, column_name_len);
    errdefer allocator.free(column_name);
    _ = try reader.readAll(column_name);

    return sql.CreateIndexCmd{
        .index_name = index_name,
        .table_name = table_name,
        .column_name = column_name,
    };
}

// ============================================================================
// DROP INDEX Serialization
// ============================================================================

/// Serialize DROP INDEX command
///
/// Format:
///   - index_name_len (u32)
///   - index_name (bytes)
pub fn serializeDropIndex(allocator: Allocator, cmd: sql.DropIndexCmd) ![]u8 {
    var buffer = ArrayList(u8).init(allocator);
    errdefer buffer.deinit();

    const writer = buffer.writer();

    try writer.writeInt(u32, @intCast(cmd.index_name.len), .little);
    try writer.writeAll(cmd.index_name);

    return try buffer.toOwnedSlice();
}

/// Deserialize DROP INDEX command
pub fn deserializeDropIndex(allocator: Allocator, data: []const u8) !sql.DropIndexCmd {
    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    const index_name_len = try reader.readInt(u32, .little);
    const index_name = try allocator.alloc(u8, index_name_len);
    errdefer allocator.free(index_name);
    _ = try reader.readAll(index_name);

    return sql.DropIndexCmd{
        .index_name = index_name,
    };
}

// ============================================================================
// Cleanup Helpers
// ============================================================================

/// Free memory allocated during deserialization of CREATE TABLE
pub fn freeCreateTableCmd(allocator: Allocator, cmd: sql.CreateTableCmd) void {
    allocator.free(cmd.table_name);
    for (cmd.columns) |col| {
        allocator.free(col.name);
    }
    allocator.free(cmd.columns);
}

/// Free memory allocated during deserialization of DROP TABLE
pub fn freeDropTableCmd(allocator: Allocator, cmd: sql.DropTableCmd) void {
    allocator.free(cmd.table_name);
}

/// Free memory allocated during deserialization of ALTER TABLE
pub fn freeAlterTableCmd(allocator: Allocator, cmd: sql.AlterTableCmd) void {
    allocator.free(cmd.table_name);
    switch (cmd.operation) {
        .add_column => |col| allocator.free(col.name),
        .drop_column => |name| allocator.free(name),
        .rename_column => |rename| {
            allocator.free(rename.old_name);
            allocator.free(rename.new_name);
        },
    }
}

/// Free memory allocated during deserialization of CREATE INDEX
pub fn freeCreateIndexCmd(allocator: Allocator, cmd: sql.CreateIndexCmd) void {
    allocator.free(cmd.index_name);
    allocator.free(cmd.table_name);
    allocator.free(cmd.column_name);
}

/// Free memory allocated during deserialization of DROP INDEX
pub fn freeDropIndexCmd(allocator: Allocator, cmd: sql.DropIndexCmd) void {
    allocator.free(cmd.index_name);
}
