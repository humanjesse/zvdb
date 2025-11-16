const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const AutoHashMap = std.AutoHashMap;
const utils = @import("utils.zig");
const transaction = @import("transaction.zig");
const Snapshot = transaction.Snapshot;
const CommitLog = transaction.CommitLog;

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
    embedding_dim: ?usize, // Dimension for embedding columns (null for non-embedding types)

    pub fn init(allocator: Allocator, name: []const u8, col_type: ColumnType) !Column {
        const owned_name = try allocator.alloc(u8, name.len);
        @memcpy(owned_name, name);
        return Column{
            .name = owned_name,
            .col_type = col_type,
            .embedding_dim = if (col_type == .embedding) 768 else null, // Default to 768 for backward compatibility
        };
    }

    pub fn initWithDim(allocator: Allocator, name: []const u8, col_type: ColumnType, dim: ?usize) !Column {
        const owned_name = try allocator.alloc(u8, name.len);
        @memcpy(owned_name, name);
        return Column{
            .name = owned_name,
            .col_type = col_type,
            .embedding_dim = dim,
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

    /// Clone a row (deep copy of all values)
    pub fn clone(self: *const Row, allocator: Allocator) !Row {
        var new_row = Row.init(allocator, self.id);
        errdefer new_row.deinit(allocator);

        var it = self.values.iterator();
        while (it.next()) |entry| {
            const key = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(key);
            const value = try entry.value_ptr.clone(allocator);
            try new_row.values.put(key, value);
        }

        return new_row;
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

// ============================================================================
// MVCC - Multi-Version Storage
// ============================================================================

/// A single version of a row in MVCC storage
/// Each row can have multiple versions forming a chain from newest to oldest
pub const RowVersion = struct {
    /// Row ID (shared across all versions of the same row)
    row_id: u64,

    /// Transaction ID that created this version (xmin)
    xmin: u64,

    /// Transaction ID that deleted/updated this version (0 = still current)
    xmax: u64,

    /// The actual row data for this version
    data: Row,

    /// Pointer to next older version (linked list)
    next: ?*RowVersion,

    /// Create a new row version
    pub fn init(allocator: Allocator, row_id: u64, tx_id: u64, data: Row) !*RowVersion {
        const version = try allocator.create(RowVersion);
        version.* = .{
            .row_id = row_id,
            .xmin = tx_id,
            .xmax = 0, // 0 means "not deleted yet"
            .data = data,
            .next = null,
        };
        return version;
    }

    /// Check if this version is visible to a given snapshot
    /// This is the CORE visibility logic for MVCC
    pub fn isVisible(self: *const RowVersion, snapshot: *const Snapshot, clog: *CommitLog) bool {
        // Version created by a transaction that started after our snapshot?
        if (self.xmin >= snapshot.txid) return false;

        // Version created by a transaction that was active when we took snapshot?
        if (snapshot.wasActive(self.xmin)) return false;

        // Version created by aborted transaction?
        if (clog.isAborted(self.xmin)) return false;

        // Check if version has been deleted/updated
        if (self.xmax != 0) {
            // Deleted by transaction that committed before our snapshot?
            if (self.xmax < snapshot.txid and clog.isCommitted(self.xmax)) {
                return false;
            }
        }

        return true;
    }

    /// Deinitialize a single version (does not follow chain)
    pub fn deinit(self: *RowVersion, allocator: Allocator) void {
        self.data.deinit(allocator);
        allocator.destroy(self);
    }

    /// Deinitialize entire version chain
    pub fn deinitChain(self: *RowVersion, allocator: Allocator) void {
        var current: ?*RowVersion = self;
        while (current) |version| {
            const next = version.next;
            version.deinit(allocator);
            current = next;
        }
    }
};

/// A table with schema and rows (MVCC-enabled)
pub const Table = struct {
    name: []const u8,
    columns: ArrayList(Column),
    /// MVCC: Version chains (row_id -> newest version)
    version_chains: AutoHashMap(u64, *RowVersion),
    next_id: std.atomic.Value(u64),
    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8) !Table {
        const owned_name = try allocator.alloc(u8, name.len);
        @memcpy(owned_name, name);

        return Table{
            .name = owned_name,
            .columns = ArrayList(Column).init(allocator),
            .version_chains = AutoHashMap(u64, *RowVersion).init(allocator),
            .next_id = std.atomic.Value(u64).init(1),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Table) void {
        self.allocator.free(self.name);

        for (self.columns.items) |*col| {
            col.deinit(self.allocator);
        }
        self.columns.deinit();

        // MVCC: Clean up all version chains
        var it = self.version_chains.iterator();
        while (it.next()) |entry| {
            const chain_head = entry.value_ptr.*;
            chain_head.deinitChain(self.allocator);
        }
        self.version_chains.deinit();
    }

    pub fn addColumn(self: *Table, name: []const u8, col_type: ColumnType) !void {
        const column = try Column.init(self.allocator, name, col_type);
        try self.columns.append(column);
    }

    /// Insert a new row (generates ID automatically)
    /// For MVCC compatibility, uses transaction ID 0 (bootstrap transaction)
    /// TODO Phase 3: Accept transaction ID parameter
    pub fn insert(self: *Table, values: StringHashMap(ColumnValue)) !u64 {
        const id = self.next_id.fetchAdd(1, .monotonic);
        try self.insertWithId(id, values, 0); // tx_id = 0 for bootstrap
        return id;
    }

    /// Insert a row with specific ID (MVCC-enabled)
    /// Creates the first version of the row with given transaction ID
    pub fn insertWithId(self: *Table, id: u64, values: StringHashMap(ColumnValue), tx_id: u64) !void {
        // Create Row data
        var row = Row.init(self.allocator, id);
        errdefer row.deinit(self.allocator);

        // Copy values into row
        var it = values.iterator();
        while (it.next()) |entry| {
            try row.set(self.allocator, entry.key_ptr.*, entry.value_ptr.*);
        }

        // Create first version for this row
        const version = try RowVersion.init(self.allocator, id, tx_id, row);
        try self.version_chains.put(id, version);

        // Atomically update next_id if needed (ensure it's at least id + 1)
        const desired_next = id + 1;
        while (true) {
            const current = self.next_id.load(.monotonic);
            if (current >= desired_next) break;
            _ = self.next_id.cmpxchgWeak(current, desired_next, .monotonic, .monotonic) orelse break;
        }
    }

    /// Get a row by ID (MVCC-enabled)
    /// Walks the version chain to find the visible version
    /// For non-MVCC mode (snapshot=null), returns the newest version
    pub fn get(self: *Table, id: u64, snapshot: ?*const Snapshot, clog: ?*CommitLog) ?*Row {
        const chain_head = self.version_chains.get(id) orelse return null;

        // Non-MVCC mode: return newest version
        if (snapshot == null or clog == null) {
            return &chain_head.data;
        }

        // MVCC mode: Walk version chain from newest to oldest
        var current: ?*RowVersion = chain_head;
        while (current) |version| {
            if (version.isVisible(snapshot.?, clog.?)) {
                return &version.data;
            }
            current = version.next;
        }

        return null; // No visible version found
    }

    /// Delete a row by ID (MVCC-enabled)
    /// Marks the current version as deleted (sets xmax) instead of physically removing it
    /// For non-MVCC mode (tx_id=0), uses maxInt(u64) as sentinel for "deleted"
    pub fn delete(self: *Table, id: u64, tx_id: u64) !void {
        const chain_head = self.version_chains.get(id) orelse return error.RowNotFound;

        // Mark the current version as deleted
        // Use maxInt as sentinel for immediate deletion (backward compatibility)
        chain_head.xmax = if (tx_id == 0) std.math.maxInt(u64) else tx_id;

        // Don't physically remove - old snapshots may still need to see it!
    }

    /// Physically delete a row and its entire version chain from the table
    /// This is used during transaction rollback to clean up rows that were inserted
    /// within a transaction that is being aborted. Unlike the regular delete(),
    /// this completely removes the row from memory.
    /// WARNING: Only use this during rollback - normal deletes should use delete()
    pub fn physicalDelete(self: *Table, id: u64) !void {
        const chain_head = self.version_chains.get(id) orelse return error.RowNotFound;

        // Remove from version chains map
        _ = self.version_chains.remove(id);

        // Free the entire version chain
        chain_head.deinitChain(self.allocator);
    }

    /// Undelete a row by clearing its xmax field
    /// This is used during transaction rollback to restore rows that were deleted
    /// within a transaction that is being aborted. Unlike insertWithId(),
    /// this doesn't create a new RowVersion but just marks the existing one as not deleted.
    /// WARNING: Only use this during rollback - normal operations should not call this
    pub fn undelete(self: *Table, id: u64) !void {
        const chain_head = self.version_chains.get(id) orelse return error.RowNotFound;

        // Clear the deletion marker (xmax = 0 means "not deleted")
        chain_head.xmax = 0;
    }

    /// Undo an update by removing the newest version and restoring the previous one
    /// This is used during transaction rollback to undo UPDATE operations.
    /// It removes the new RowVersion created by the update and makes the old version current again.
    /// WARNING: Only use this during rollback - assumes the chain has at least 2 versions
    pub fn undoUpdate(self: *Table, id: u64) !void {
        const new_version = self.version_chains.get(id) orelse return error.RowNotFound;
        const old_version = new_version.next orelse return error.InvalidVersionChain;

        // Clear the old version's xmax (it's no longer superseded)
        old_version.xmax = 0;

        // Make the old version the chain head again
        try self.version_chains.put(id, old_version);

        // Free the new version (but don't follow the chain - old_version is still valid)
        new_version.deinit(self.allocator);
    }

    /// Update a row by creating a new version (MVCC-enabled)
    /// Creates a new version with the updated value and chains it to the old version
    pub fn update(self: *Table, row_id: u64, column: []const u8, new_value: ColumnValue, tx_id: u64) !void {
        const old_version = self.version_chains.get(row_id) orelse return error.RowNotFound;

        // Mark old version as superseded by this transaction
        old_version.xmax = tx_id;

        // Clone old row data
        var new_row = try old_version.data.clone(self.allocator);
        errdefer new_row.deinit(self.allocator);

        // Apply the update
        try new_row.set(self.allocator, column, new_value);

        // Create new version
        const new_version = try RowVersion.init(self.allocator, row_id, tx_id, new_row);

        // Link new version to old version (new_version is now the head)
        new_version.next = old_version;

        // Update the chain head
        try self.version_chains.put(row_id, new_version);
    }

    /// Count number of non-deleted rows
    /// Returns count of rows where the newest version is not deleted (xmax == 0)
    pub fn count(self: *Table) usize {
        var count_active: usize = 0;
        var it = self.version_chains.iterator();
        while (it.next()) |entry| {
            const head_version = entry.value_ptr.*;
            // Count row if newest version is not deleted
            if (head_version.xmax == 0) {
                count_active += 1;
            }
        }
        return count_active;
    }

    /// Get all visible row IDs (MVCC-enabled)
    /// Filters rows by visibility according to the snapshot
    /// For non-MVCC mode (snapshot=null), returns all row IDs
    pub fn getAllRows(self: *Table, allocator: Allocator, snapshot: ?*const Snapshot, clog: ?*CommitLog) ![]u64 {
        var visible_ids = ArrayList(u64).init(allocator);
        defer visible_ids.deinit();

        var it = self.version_chains.iterator();
        while (it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const head_version = entry.value_ptr.*;

            // Non-MVCC mode: include non-deleted rows (xmax == 0)
            if (snapshot == null or clog == null) {
                if (head_version.xmax == 0) {
                    try visible_ids.append(row_id);
                }
                continue;
            }

            // MVCC mode: Walk version chain to find visible version
            var version: ?*RowVersion = entry.value_ptr.*;
            while (version) |v| {
                if (v.isVisible(snapshot.?, clog.?)) {
                    try visible_ids.append(row_id);
                    break; // Found visible version, move to next row
                }
                version = v.next;
            }
        }

        return visible_ids.toOwnedSlice();
    }

    /// Save table to a binary file (.zvdb format)
    /// NOTE: Currently saves only the newest version of each row (no MVCC history)
    /// TODO Phase 4: Save version chains for full MVCC recovery
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
        try utils.writeInt(file, u64, self.next_id.load(.monotonic));

        // Write schema
        try utils.writeInt(file, u64, self.columns.items.len);
        for (self.columns.items) |col| {
            try utils.writeInt(file, u64, col.name.len);
            try file.writeAll(col.name);
            try utils.writeInt(file, u8, @intFromEnum(col.col_type));
        }

        // Write rows (newest version only)
        try utils.writeInt(file, u64, self.version_chains.count());
        var it = self.version_chains.iterator();
        while (it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const row_version = entry.value_ptr.*;
            const row = row_version.data;

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
    /// NOTE: Loads rows as single versions with tx_id=0 (bootstrap)
    /// TODO Phase 4: Load version chains for full MVCC recovery
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
            .version_chains = AutoHashMap(u64, *RowVersion).init(allocator),
            .next_id = std.atomic.Value(u64).init(next_id),
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

            // Create version for loaded row (tx_id = 0 for bootstrap)
            const row_version = try RowVersion.init(allocator, row_id, 0, row);
            try table.version_chains.put(row_id, row_version);
        }

        return table;
    }
};
