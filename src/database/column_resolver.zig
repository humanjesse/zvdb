const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const Table = @import("../table.zig").Table;
const Column = @import("../table.zig").Column;
const ColumnType = @import("../table.zig").ColumnType;

// ============================================================================
// Error Types
// ============================================================================

/// Errors that can occur during column resolution
pub const ColumnResolverError = error{
    /// Column does not exist in any tracked table
    ColumnNotFound,

    /// Column exists in multiple tables (ambiguous unqualified reference)
    AmbiguousColumn,

    /// Invalid qualified name format (e.g., "a.b.c" or malformed)
    InvalidQualifiedName,

    /// Memory allocation failure
    OutOfMemory,
};

// ============================================================================
// Column Information Types
// ============================================================================

/// Information about a column in a resolved schema
pub const ColumnInfo = struct {
    /// Table name this column belongs to
    table_name: []const u8,

    /// Column name (unqualified)
    column_name: []const u8,

    /// Column type
    column_type: ColumnType,

    /// Index of the table in the join sequence (0 = base table)
    table_index: usize,

    /// Index of the column within its table
    column_index: usize,
};

/// Result of resolving a column reference
pub const ResolvedColumn = struct {
    /// Table name the column belongs to
    table_name: []const u8,

    /// Column name (unqualified)
    column_name: []const u8,

    /// Index of the table in the join sequence (0 = base table)
    table_index: usize,

    /// Index of the column within its table
    column_index: usize,

    /// Column type
    column_type: ColumnType,
};

// ============================================================================
// Internal Schema Tracking
// ============================================================================

/// Schema information for a single table in the join sequence
const TableSchema = struct {
    /// Table name
    name: []const u8,

    /// Columns in this table
    columns: ArrayList(ColumnInfo),

    /// Table index in join sequence
    table_index: usize,

    allocator: Allocator,

    pub fn init(allocator: Allocator, name: []const u8, table_index: usize) !TableSchema {
        const owned_name = try allocator.dupe(u8, name);
        return TableSchema{
            .name = owned_name,
            .columns = ArrayList(ColumnInfo).init(allocator),
            .table_index = table_index,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *TableSchema) void {
        self.allocator.free(self.name);

        // Free column info strings
        for (self.columns.items) |*col_info| {
            self.allocator.free(col_info.table_name);
            self.allocator.free(col_info.column_name);
        }
        self.columns.deinit();
    }

    /// Add a column to this table's schema
    pub fn addColumn(
        self: *TableSchema,
        column_name: []const u8,
        column_type: ColumnType,
        column_index: usize,
    ) !void {
        const owned_table_name = try self.allocator.dupe(u8, self.name);
        errdefer self.allocator.free(owned_table_name);

        const owned_column_name = try self.allocator.dupe(u8, column_name);
        errdefer self.allocator.free(owned_column_name);

        try self.columns.append(ColumnInfo{
            .table_name = owned_table_name,
            .column_name = owned_column_name,
            .column_type = column_type,
            .table_index = self.table_index,
            .column_index = column_index,
        });
    }
};

// ============================================================================
// Column Resolver
// ============================================================================

/// Resolves column references in multi-table JOINs
/// Tracks schema from all tables and resolves qualified/unqualified column names
pub const ColumnResolver = struct {
    /// All table schemas in join order (base table first)
    table_schemas: ArrayList(TableSchema),

    /// Map from table name to table index for quick lookup
    table_name_index: StringHashMap(usize),

    /// Map from table alias to table index for alias resolution
    alias_to_table_index: StringHashMap(usize),

    allocator: Allocator,

    /// Initialize resolver with base table schema
    pub fn init(allocator: Allocator, base_table: *Table) !ColumnResolver {
        var resolver = ColumnResolver{
            .table_schemas = ArrayList(TableSchema).init(allocator),
            .table_name_index = StringHashMap(usize).init(allocator),
            .alias_to_table_index = StringHashMap(usize).init(allocator),
            .allocator = allocator,
        };
        errdefer resolver.deinit();

        // Add base table (table index 0)
        try resolver.addTableInternal(base_table, 0);

        return resolver;
    }

    pub fn deinit(self: *ColumnResolver) void {
        // Free all table schemas
        for (self.table_schemas.items) |*schema| {
            schema.deinit();
        }
        self.table_schemas.deinit();

        // Free table name index (keys are owned by TableSchema)
        self.table_name_index.deinit();

        // Free alias index (keys are owned by SelectCmd/JoinClause)
        self.alias_to_table_index.deinit();
    }

    /// Add a joined table's schema to the resolver
    /// Tables must be added in the order they appear in the JOIN sequence
    pub fn addJoinedTable(self: *ColumnResolver, table: *Table) !void {
        const table_index = self.table_schemas.items.len;
        try self.addTableInternal(table, table_index);
    }

    /// Register an alias for a table that's already been added
    /// This allows the resolver to find tables by either their real name or alias
    /// Example: registerAlias("u", "users") allows resolving "u.id" as "users.id"
    pub fn registerAlias(self: *ColumnResolver, alias: []const u8, table_name: []const u8) !void {
        // Look up the table by its real name
        const table_index = self.table_name_index.get(table_name) orelse {
            return error.TableNotFound;
        };

        // Register the alias pointing to the same table index
        try self.alias_to_table_index.put(alias, table_index);
    }

    /// Internal helper to add a table at a specific index
    fn addTableInternal(self: *ColumnResolver, table: *Table, table_index: usize) !void {
        var schema = try TableSchema.init(self.allocator, table.name, table_index);
        errdefer schema.deinit();

        // Add all columns from the table
        for (table.columns.items, 0..) |col, col_index| {
            try schema.addColumn(col.name, col.col_type, col_index);
        }

        // Add to our tracking structures
        try self.table_schemas.append(schema);
        try self.table_name_index.put(schema.name, table_index);
    }

    /// Resolve a column reference to its table and column information
    /// Supports both qualified (table.column) and unqualified (column) references
    ///
    /// For qualified references: validates table exists and has the column
    /// For unqualified references: searches all tables, errors if ambiguous
    ///
    /// Returns ColumnResolverError.ColumnNotFound if column doesn't exist
    /// Returns ColumnResolverError.AmbiguousColumn if unqualified name matches multiple tables
    /// Returns ColumnResolverError.InvalidQualifiedName if qualified name is malformed
    pub fn resolveColumn(self: *ColumnResolver, column_ref: []const u8) ColumnResolverError!ResolvedColumn {
        // Check if this is a qualified reference (contains '.')
        if (std.mem.indexOfScalar(u8, column_ref, '.')) |dot_pos| {
            return try self.resolveQualifiedColumn(column_ref, dot_pos);
        } else {
            return try self.resolveUnqualifiedColumn(column_ref);
        }
    }

    /// Resolve a qualified column reference (table.column)
    fn resolveQualifiedColumn(
        self: *ColumnResolver,
        column_ref: []const u8,
        dot_pos: usize,
    ) ColumnResolverError!ResolvedColumn {
        // Split into table and column parts
        const table_name = column_ref[0..dot_pos];
        const column_name = column_ref[dot_pos + 1 ..];

        // Validate format
        if (table_name.len == 0 or column_name.len == 0) {
            return ColumnResolverError.InvalidQualifiedName;
        }

        // Check for additional dots (e.g., "a.b.c" is invalid)
        if (std.mem.indexOfScalar(u8, column_name, '.') != null) {
            return ColumnResolverError.InvalidQualifiedName;
        }

        // Look up the table by name OR alias (aliases take precedence)
        const table_index = self.alias_to_table_index.get(table_name) orelse
            self.table_name_index.get(table_name) orelse {
            return ColumnResolverError.ColumnNotFound;
        };

        const table_schema = &self.table_schemas.items[table_index];

        // Find the column in this table
        for (table_schema.columns.items) |col_info| {
            if (std.mem.eql(u8, col_info.column_name, column_name)) {
                return ResolvedColumn{
                    .table_name = col_info.table_name,
                    .column_name = col_info.column_name,
                    .table_index = col_info.table_index,
                    .column_index = col_info.column_index,
                    .column_type = col_info.column_type,
                };
            }
        }

        // Column not found in the specified table
        return ColumnResolverError.ColumnNotFound;
    }

    /// Resolve an unqualified column reference (column)
    /// Searches all tables and errors if ambiguous
    fn resolveUnqualifiedColumn(self: *ColumnResolver, column_name: []const u8) ColumnResolverError!ResolvedColumn {
        var found_column: ?ResolvedColumn = null;

        // Search all tables for this column
        for (self.table_schemas.items) |*table_schema| {
            for (table_schema.columns.items) |col_info| {
                if (std.mem.eql(u8, col_info.column_name, column_name)) {
                    if (found_column != null) {
                        // Already found in another table - ambiguous!
                        return ColumnResolverError.AmbiguousColumn;
                    }

                    found_column = ResolvedColumn{
                        .table_name = col_info.table_name,
                        .column_name = col_info.column_name,
                        .table_index = col_info.table_index,
                        .column_index = col_info.column_index,
                        .column_type = col_info.column_type,
                    };
                }
            }
        }

        if (found_column) |col| {
            return col;
        }

        return ColumnResolverError.ColumnNotFound;
    }

    /// Check if a column exists in any tracked table
    /// Returns true for both qualified and unqualified references
    /// Does not error on ambiguous columns (just returns true)
    pub fn columnExists(self: *ColumnResolver, column_ref: []const u8) bool {
        // Try to resolve it; if it succeeds, it exists
        _ = self.resolveColumn(column_ref) catch |err| {
            // If ambiguous, it still exists (just ambiguous)
            if (err == ColumnResolverError.AmbiguousColumn) {
                return true;
            }
            return false;
        };

        return true;
    }

    /// Get the full schema for the result set
    /// Returns all columns from all tables in join order
    /// Useful for constructing SELECT * result schemas
    pub fn getFullSchema(self: *ColumnResolver) []const ColumnInfo {
        // Calculate total column count
        var total_columns: usize = 0;
        for (self.table_schemas.items) |*schema| {
            total_columns += schema.columns.items.len;
        }

        // Allocate result array
        var result = self.allocator.alloc(ColumnInfo, total_columns) catch {
            // Return empty slice on allocation failure
            return &[_]ColumnInfo{};
        };

        // Populate result
        var idx: usize = 0;
        for (self.table_schemas.items) |*schema| {
            for (schema.columns.items) |col_info| {
                result[idx] = col_info;
                idx += 1;
            }
        }

        return result;
    }

    /// Get all columns from a specific table
    /// Returns null if table not found
    pub fn getTableColumns(self: *ColumnResolver, table_name: []const u8) ?[]const ColumnInfo {
        const table_index = self.table_name_index.get(table_name) orelse return null;
        const table_schema = &self.table_schemas.items[table_index];
        return table_schema.columns.items;
    }

    /// Get the number of tables being tracked
    pub fn getTableCount(self: *ColumnResolver) usize {
        return self.table_schemas.items.len;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "ColumnResolver: basic initialization with single table" {
    const allocator = std.testing.allocator;

    // Create a simple table
    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("name", .text);
    try table.addColumn("age", .int);

    // Initialize resolver
    var resolver = try ColumnResolver.init(allocator, &table);
    defer resolver.deinit();

    // Verify table count
    try std.testing.expectEqual(@as(usize, 1), resolver.getTableCount());
}

test "ColumnResolver: resolve unqualified column" {
    const allocator = std.testing.allocator;

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("name", .text);
    try table.addColumn("email", .text);

    var resolver = try ColumnResolver.init(allocator, &table);
    defer resolver.deinit();

    // Resolve "name"
    const resolved = try resolver.resolveColumn("name");
    try std.testing.expectEqualStrings("users", resolved.table_name);
    try std.testing.expectEqualStrings("name", resolved.column_name);
    try std.testing.expectEqual(@as(usize, 0), resolved.table_index);
    try std.testing.expectEqual(@as(usize, 1), resolved.column_index);
    try std.testing.expectEqual(ColumnType.text, resolved.column_type);
}

test "ColumnResolver: resolve qualified column" {
    const allocator = std.testing.allocator;

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("name", .text);

    var resolver = try ColumnResolver.init(allocator, &table);
    defer resolver.deinit();

    // Resolve "users.name"
    const resolved = try resolver.resolveColumn("users.name");
    try std.testing.expectEqualStrings("users", resolved.table_name);
    try std.testing.expectEqualStrings("name", resolved.column_name);
    try std.testing.expectEqual(@as(usize, 0), resolved.table_index);
    try std.testing.expectEqual(ColumnType.text, resolved.column_type);
}

test "ColumnResolver: column not found" {
    const allocator = std.testing.allocator;

    var table = try Table.init(allocator, "users");
    defer table.deinit();

    try table.addColumn("id", .int);

    var resolver = try ColumnResolver.init(allocator, &table);
    defer resolver.deinit();

    // Try to resolve non-existent column
    const result = resolver.resolveColumn("nonexistent");
    try std.testing.expectError(ColumnResolverError.ColumnNotFound, result);
}

test "ColumnResolver: ambiguous column in multi-table join" {
    const allocator = std.testing.allocator;

    // Base table: users
    var users = try Table.init(allocator, "users");
    defer users.deinit();
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    // Joined table: orders (also has 'id')
    var orders = try Table.init(allocator, "orders");
    defer orders.deinit();
    try orders.addColumn("id", .int);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("amount", .float);

    var resolver = try ColumnResolver.init(allocator, &users);
    defer resolver.deinit();

    try resolver.addJoinedTable(&orders);

    // "id" exists in both tables - should be ambiguous
    const result = resolver.resolveColumn("id");
    try std.testing.expectError(ColumnResolverError.AmbiguousColumn, result);

    // But qualified references should work
    const users_id = try resolver.resolveColumn("users.id");
    try std.testing.expectEqualStrings("users", users_id.table_name);

    const orders_id = try resolver.resolveColumn("orders.id");
    try std.testing.expectEqualStrings("orders", orders_id.table_name);
}

test "ColumnResolver: unambiguous column in multi-table join" {
    const allocator = std.testing.allocator;

    var users = try Table.init(allocator, "users");
    defer users.deinit();
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    var orders = try Table.init(allocator, "orders");
    defer orders.deinit();
    try orders.addColumn("order_id", .int);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("amount", .float);

    var resolver = try ColumnResolver.init(allocator, &users);
    defer resolver.deinit();

    try resolver.addJoinedTable(&orders);

    // "name" only exists in users - should resolve
    const name = try resolver.resolveColumn("name");
    try std.testing.expectEqualStrings("users", name.table_name);
    try std.testing.expectEqualStrings("name", name.column_name);

    // "amount" only exists in orders - should resolve
    const amount = try resolver.resolveColumn("amount");
    try std.testing.expectEqualStrings("orders", amount.table_name);
    try std.testing.expectEqualStrings("amount", amount.column_name);
    try std.testing.expectEqual(@as(usize, 1), amount.table_index);
}

test "ColumnResolver: invalid qualified name formats" {
    const allocator = std.testing.allocator;

    var table = try Table.init(allocator, "users");
    defer table.deinit();
    try table.addColumn("id", .int);

    var resolver = try ColumnResolver.init(allocator, &table);
    defer resolver.deinit();

    // Empty table name
    try std.testing.expectError(
        ColumnResolverError.InvalidQualifiedName,
        resolver.resolveColumn(".id"),
    );

    // Empty column name
    try std.testing.expectError(
        ColumnResolverError.InvalidQualifiedName,
        resolver.resolveColumn("users."),
    );

    // Too many dots
    try std.testing.expectError(
        ColumnResolverError.InvalidQualifiedName,
        resolver.resolveColumn("schema.users.id"),
    );
}

test "ColumnResolver: columnExists helper" {
    const allocator = std.testing.allocator;

    var users = try Table.init(allocator, "users");
    defer users.deinit();
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    var orders = try Table.init(allocator, "orders");
    defer orders.deinit();
    try orders.addColumn("id", .int);
    try orders.addColumn("amount", .float);

    var resolver = try ColumnResolver.init(allocator, &users);
    defer resolver.deinit();
    try resolver.addJoinedTable(&orders);

    // Existing unambiguous columns
    try std.testing.expect(resolver.columnExists("name"));
    try std.testing.expect(resolver.columnExists("amount"));

    // Existing qualified columns
    try std.testing.expect(resolver.columnExists("users.id"));
    try std.testing.expect(resolver.columnExists("orders.id"));

    // Ambiguous column (still exists, just ambiguous)
    try std.testing.expect(resolver.columnExists("id"));

    // Non-existent column
    try std.testing.expect(!resolver.columnExists("nonexistent"));
    try std.testing.expect(!resolver.columnExists("users.nonexistent"));
}

test "ColumnResolver: three-table join scenario" {
    const allocator = std.testing.allocator;

    // users table
    var users = try Table.init(allocator, "users");
    defer users.deinit();
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);

    // orders table
    var orders = try Table.init(allocator, "orders");
    defer orders.deinit();
    try orders.addColumn("id", .int);
    try orders.addColumn("user_id", .int);
    try orders.addColumn("product_id", .int);

    // products table
    var products = try Table.init(allocator, "products");
    defer products.deinit();
    try products.addColumn("id", .int);
    try products.addColumn("name", .text);
    try products.addColumn("price", .float);

    var resolver = try ColumnResolver.init(allocator, &users);
    defer resolver.deinit();
    try resolver.addJoinedTable(&orders);
    try resolver.addJoinedTable(&products);

    // Verify table count
    try std.testing.expectEqual(@as(usize, 3), resolver.getTableCount());

    // "id" is in all three tables - ambiguous
    try std.testing.expectError(
        ColumnResolverError.AmbiguousColumn,
        resolver.resolveColumn("id"),
    );

    // "name" is in users and products - ambiguous
    try std.testing.expectError(
        ColumnResolverError.AmbiguousColumn,
        resolver.resolveColumn("name"),
    );

    // Unique columns should resolve
    const user_id = try resolver.resolveColumn("user_id");
    try std.testing.expectEqualStrings("orders", user_id.table_name);
    try std.testing.expectEqual(@as(usize, 1), user_id.table_index);

    const price = try resolver.resolveColumn("price");
    try std.testing.expectEqualStrings("products", price.table_name);
    try std.testing.expectEqual(@as(usize, 2), price.table_index);

    // Qualified references should work
    const users_name = try resolver.resolveColumn("users.name");
    try std.testing.expectEqual(@as(usize, 0), users_name.table_index);

    const products_name = try resolver.resolveColumn("products.name");
    try std.testing.expectEqual(@as(usize, 2), products_name.table_index);
}

test "ColumnResolver: getTableColumns" {
    const allocator = std.testing.allocator;

    var users = try Table.init(allocator, "users");
    defer users.deinit();
    try users.addColumn("id", .int);
    try users.addColumn("name", .text);
    try users.addColumn("email", .text);

    var resolver = try ColumnResolver.init(allocator, &users);
    defer resolver.deinit();

    // Get columns for users table
    const cols = resolver.getTableColumns("users") orelse unreachable;
    try std.testing.expectEqual(@as(usize, 3), cols.len);
    try std.testing.expectEqualStrings("id", cols[0].column_name);
    try std.testing.expectEqualStrings("name", cols[1].column_name);
    try std.testing.expectEqualStrings("email", cols[2].column_name);

    // Non-existent table
    try std.testing.expectEqual(@as(?[]const ColumnInfo, null), resolver.getTableColumns("nonexistent"));
}
