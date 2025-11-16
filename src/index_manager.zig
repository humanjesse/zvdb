const std = @import("std");
const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const BTree = @import("btree.zig").BTree;
const ColumnValue = @import("table.zig").ColumnValue;
const Table = @import("table.zig").Table;
const Row = @import("table.zig").Row;

/// Index metadata
pub const IndexInfo = struct {
    /// Index name (unique identifier)
    name: []const u8,

    /// Table this index belongs to
    table_name: []const u8,

    /// Column being indexed
    column_name: []const u8,

    /// The actual B-tree index
    btree: BTree,

    /// Allocator for this index
    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        name: []const u8,
        table_name: []const u8,
        column_name: []const u8,
    ) !*IndexInfo {
        const info = try allocator.create(IndexInfo);
        errdefer allocator.destroy(info);

        // Create owned copies of strings
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);

        const owned_table = try allocator.dupe(u8, table_name);
        errdefer allocator.free(owned_table);

        const owned_column = try allocator.dupe(u8, column_name);
        errdefer allocator.free(owned_column);

        info.* = IndexInfo{
            .name = owned_name,
            .table_name = owned_table,
            .column_name = owned_column,
            .btree = BTree.init(allocator),
            .allocator = allocator,
        };

        return info;
    }

    pub fn deinit(self: *IndexInfo) void {
        self.allocator.free(self.name);
        self.allocator.free(self.table_name);
        self.allocator.free(self.column_name);
        self.btree.deinit();
    }
};

/// Index Manager - manages all indexes for a database
pub const IndexManager = struct {
    /// Map of index_name -> IndexInfo
    indexes: StringHashMap(*IndexInfo),

    /// Allocator
    allocator: Allocator,

    pub fn init(allocator: Allocator) IndexManager {
        return IndexManager{
            .indexes = StringHashMap(*IndexInfo).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IndexManager) void {
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;
            info.deinit();
            self.allocator.destroy(info);
        }
        self.indexes.deinit();
    }

    /// Create a new index on a table column
    /// Returns error if index name already exists
    pub fn createIndex(
        self: *IndexManager,
        index_name: []const u8,
        table_name: []const u8,
        column_name: []const u8,
        table: *Table,
    ) !void {
        // Check if index already exists
        if (self.indexes.contains(index_name)) {
            return error.IndexAlreadyExists;
        }

        // Create new index
        const info = try IndexInfo.init(
            self.allocator,
            index_name,
            table_name,
            column_name,
        );
        errdefer {
            info.deinit();
            self.allocator.destroy(info);
        }

        // Build index from existing table data
        try self.buildIndexFromTable(info, table);

        // Store index (use owned copy of name from info, not the parameter)
        try self.indexes.put(info.name, info);
    }

    /// Build index by scanning table data
    fn buildIndexFromTable(self: *IndexManager, info: *IndexInfo, table: *Table) !void {
        _ = self;

        // Iterate through all rows in the table (using version chains)
        // Note: For index building, we use the newest version of each row
        var row_it = table.version_chains.iterator();
        while (row_it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const version = entry.value_ptr.*;
            const row = &version.data;

            // Get the indexed column value
            if (row.get(info.column_name)) |value| {
                // Insert into B-tree index
                try info.btree.insert(value, row_id);
            }
            // If column doesn't exist in this row, skip (NULL)
        }
    }

    /// Drop an existing index
    /// Returns error if index doesn't exist
    pub fn dropIndex(self: *IndexManager, index_name: []const u8) !void {
        const info = self.indexes.get(index_name) orelse return error.IndexNotFound;

        _ = self.indexes.remove(index_name);
        info.deinit();
        self.allocator.destroy(info);
    }

    /// Get an index by name
    pub fn getIndex(self: *const IndexManager, index_name: []const u8) ?*IndexInfo {
        return self.indexes.get(index_name);
    }

    /// Find all indexes for a given table
    /// Caller must free the returned list
    pub fn getIndexesForTable(self: *const IndexManager, table_name: []const u8) ![][]const u8 {
        var result = ArrayList([]const u8).init(self.allocator);
        errdefer result.deinit();

        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;
            if (std.mem.eql(u8, info.table_name, table_name)) {
                try result.append(info.name);
            }
        }

        return try result.toOwnedSlice();
    }

    /// Find an index on a specific table column
    pub fn findIndexForColumn(
        self: *const IndexManager,
        table_name: []const u8,
        column_name: []const u8,
    ) ?*IndexInfo {
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;
            if (std.mem.eql(u8, info.table_name, table_name) and
                std.mem.eql(u8, info.column_name, column_name))
            {
                return info;
            }
        }
        return null;
    }

    /// Update indexes when a row is inserted
    pub fn onInsert(
        self: *IndexManager,
        table_name: []const u8,
        row_id: u64,
        row: *const Row,
    ) !void {
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;

            // Skip indexes for other tables
            if (!std.mem.eql(u8, info.table_name, table_name)) {
                continue;
            }

            // Get the indexed column value
            if (row.get(info.column_name)) |value| {
                try info.btree.insert(value, row_id);
            }
        }
    }

    /// Update indexes when a row is deleted
    pub fn onDelete(
        self: *IndexManager,
        table_name: []const u8,
        row_id: u64,
        row: *const Row,
    ) !void {
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;

            // Skip indexes for other tables
            if (!std.mem.eql(u8, info.table_name, table_name)) {
                continue;
            }

            // Get the indexed column value
            if (row.get(info.column_name)) |value| {
                _ = try info.btree.delete(value, row_id);
            }
        }
    }

    /// Update indexes when a row is updated
    pub fn onUpdate(
        self: *IndexManager,
        table_name: []const u8,
        row_id: u64,
        old_row: *const Row,
        new_row: *const Row,
    ) !void {
        var it = self.indexes.iterator();
        while (it.next()) |entry| {
            const info = entry.value_ptr.*;

            // Skip indexes for other tables
            if (!std.mem.eql(u8, info.table_name, table_name)) {
                continue;
            }

            // Check if the indexed column changed
            const old_value = old_row.get(info.column_name);
            const new_value = new_row.get(info.column_name);

            // If column didn't change, skip
            if (old_value != null and new_value != null) {
                if (std.meta.eql(old_value.?, new_value.?)) {
                    continue;
                }
            }

            // Delete old value from index
            if (old_value) |val| {
                _ = try info.btree.delete(val, row_id);
            }

            // Insert new value into index
            if (new_value) |val| {
                try info.btree.insert(val, row_id);
            }
        }
    }

    /// Query an index for exact match
    /// Returns list of row IDs with matching values
    /// Caller must free the returned list
    pub fn query(
        self: *const IndexManager,
        index_name: []const u8,
        value: ColumnValue,
    ) ![]u64 {
        const info = self.indexes.get(index_name) orelse return error.IndexNotFound;
        return try info.btree.search(value);
    }

    /// Query an index for range [min_value, max_value]
    /// Returns list of row IDs in sorted order
    /// Caller must free the returned list
    pub fn queryRange(
        self: *const IndexManager,
        index_name: []const u8,
        min_value: ColumnValue,
        max_value: ColumnValue,
    ) ![]u64 {
        const info = self.indexes.get(index_name) orelse return error.IndexNotFound;
        // Default to inclusive range [min_value, max_value]
        return try info.btree.findRange(min_value, max_value, true, true);
    }

    /// Get total number of indexes
    pub fn count(self: *const IndexManager) usize {
        return self.indexes.count();
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "IndexManager: create and drop index" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    // Add a column
    try table.addColumn("id", .int);

    // Create index
    try mgr.createIndex("idx_users_id", "users", "id", &table);

    try testing.expectEqual(@as(usize, 1), mgr.count());

    // Verify index exists
    const info = mgr.getIndex("idx_users_id");
    try testing.expect(info != null);
    try testing.expectEqualStrings("users", info.?.table_name);
    try testing.expectEqualStrings("id", info.?.column_name);

    // Drop index
    try mgr.dropIndex("idx_users_id");
    try testing.expectEqual(@as(usize, 0), mgr.count());

    // Verify index is gone
    try testing.expect(mgr.getIndex("idx_users_id") == null);
}

test "IndexManager: create index from existing data" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("name", .text);

    // Insert some data
    var values1 = StringHashMap(ColumnValue).init(allocator);
    defer values1.deinit();
    try values1.put("id", ColumnValue{ .int = 1 });
    _ = try table.insert(values1);

    var values2 = StringHashMap(ColumnValue).init(allocator);
    defer values2.deinit();
    try values2.put("id", ColumnValue{ .int = 2 });
    _ = try table.insert(values2);

    // Create index - should build from existing data
    try mgr.createIndex("idx_users_id", "users", "id", &table);

    // Verify index has data
    const results = try mgr.query("idx_users_id", ColumnValue{ .int = 1 });
    defer allocator.free(results);

    try testing.expectEqual(@as(usize, 1), results.len);
}

test "IndexManager: automatic index updates on insert" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);

    // Create index first
    try mgr.createIndex("idx_users_id", "users", "id", &table);

    // Insert data
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("id", ColumnValue{ .int = 42 });
    const row_id = try table.insert(values);

    // Update index
    // TODO Phase 3: Pass snapshot for MVCC visibility
    const row = table.get(row_id, null, null).?;
    try mgr.onInsert("users", row_id, row);

    // Query index
    const results = try mgr.query("idx_users_id", ColumnValue{ .int = 42 });
    defer allocator.free(results);

    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(row_id, results[0]);
}

test "IndexManager: automatic index updates on delete" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);

    // Insert and index
    var values = StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("id", ColumnValue{ .int = 42 });
    const row_id = try table.insert(values);

    try mgr.createIndex("idx_users_id", "users", "id", &table);

    // Delete row and update index
    // TODO Phase 3: Pass snapshot for MVCC visibility
    const row = table.get(row_id, null, null).?;
    try mgr.onDelete("users", row_id, row);
    // TODO Phase 3: Pass transaction ID
    try table.delete(row_id, 0);

    // Query index - should be empty
    const results = try mgr.query("idx_users_id", ColumnValue{ .int = 42 });
    defer allocator.free(results);

    try testing.expectEqual(@as(usize, 0), results.len);
}

test "IndexManager: find indexes for table" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("email", .text);

    // Create multiple indexes
    try mgr.createIndex("idx_users_id", "users", "id", &table);
    try mgr.createIndex("idx_users_email", "users", "email", &table);

    // Find indexes for table
    const indexes = try mgr.getIndexesForTable("users");
    defer allocator.free(indexes);

    try testing.expectEqual(@as(usize, 2), indexes.len);
}

test "IndexManager: find index for column" {
    const allocator = testing.allocator;
    var mgr = IndexManager.init(allocator);
    defer mgr.deinit();

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("email", .text);

    try mgr.createIndex("idx_users_email", "users", "email", &table);

    // Find by column
    const info = mgr.findIndexForColumn("users", "email");
    try testing.expect(info != null);
    try testing.expectEqualStrings("idx_users_email", info.?.name);

    // Non-existent column
    const info2 = mgr.findIndexForColumn("users", "age");
    try testing.expect(info2 == null);
}
