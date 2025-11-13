const std = @import("std");
const Allocator = std.mem.Allocator;
const StringHashMap = std.StringHashMap;
const ArrayList = std.array_list.Managed;
const Table = @import("table.zig").Table;
const ColumnValue = @import("table.zig").ColumnValue;
const ColumnType = @import("table.zig").ColumnType;
const Row = @import("table.zig").Row;
const sql = @import("sql.zig");
const SqlCommand = sql.SqlCommand;
const HNSW = @import("hnsw.zig").HNSW;

/// Query result set
pub const QueryResult = struct {
    columns: ArrayList([]const u8),
    rows: ArrayList(ArrayList(ColumnValue)),
    allocator: Allocator,

    pub fn init(allocator: Allocator) QueryResult {
        return QueryResult{
            .columns = ArrayList([]const u8).init(allocator),
            .rows = ArrayList(ArrayList(ColumnValue)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *QueryResult) void {
        for (self.columns.items) |col| {
            self.allocator.free(col);
        }
        self.columns.deinit();

        for (self.rows.items) |*row| {
            for (row.items) |*val| {
                var v = val.*;
                v.deinit(self.allocator);
            }
            row.deinit();
        }
        self.rows.deinit();
    }

    pub fn addColumn(self: *QueryResult, name: []const u8) !void {
        const owned = try self.allocator.dupe(u8, name);
        try self.columns.append(owned);
    }

    pub fn addRow(self: *QueryResult, values: ArrayList(ColumnValue)) !void {
        try self.rows.append(values);
    }

    pub fn print(self: *QueryResult) !void {
        // Print header
        std.debug.print("\n", .{});
        for (self.columns.items, 0..) |col, i| {
            if (i > 0) std.debug.print(" | ", .{});
            std.debug.print("{s}", .{col});
        }
        std.debug.print("\n", .{});

        // Print separator
        for (self.columns.items, 0..) |_, i| {
            if (i > 0) std.debug.print("-+-", .{});
            std.debug.print("----------", .{});
        }
        std.debug.print("\n", .{});

        // Print rows
        for (self.rows.items) |row| {
            for (row.items, 0..) |val, i| {
                if (i > 0) std.debug.print(" | ", .{});
                std.debug.print("{any}", .{val});
            }
            std.debug.print("\n", .{});
        }

        std.debug.print("\n({d} rows)\n", .{self.rows.items.len});
    }
};

/// Main database with SQL and vector search
pub const Database = struct {
    tables: StringHashMap(*Table),
    hnsw: ?*HNSW(f32), // Optional vector index
    allocator: Allocator,
    data_dir: ?[]const u8, // Data directory for persistence (owned)
    auto_save: bool, // Auto-save on deinit

    pub fn init(allocator: Allocator) Database {
        return Database{
            .tables = StringHashMap(*Table).init(allocator),
            .hnsw = null,
            .allocator = allocator,
            .data_dir = null,
            .auto_save = false,
        };
    }

    pub fn deinit(self: *Database) void {
        // Auto-save if enabled and data_dir is set
        if (self.auto_save and self.data_dir != null) {
            self.saveAll(self.data_dir.?) catch |err| {
                std.debug.print("Warning: Failed to auto-save database: {}\n", .{err});
            };
        }

        var it = self.tables.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.tables.deinit();

        if (self.hnsw) |h| {
            h.deinit();
            self.allocator.destroy(h);
        }

        if (self.data_dir) |dir| {
            self.allocator.free(dir);
        }
    }

    /// Initialize vector search capabilities
    pub fn initVectorSearch(self: *Database, m: usize, ef_construction: usize) !void {
        const hnsw_ptr = try self.allocator.create(HNSW(f32));
        hnsw_ptr.* = HNSW(f32).init(self.allocator, m, ef_construction);
        self.hnsw = hnsw_ptr;
    }

    /// Execute a SQL command
    pub fn execute(self: *Database, query: []const u8) !QueryResult {
        var cmd = try sql.parse(self.allocator, query);
        defer cmd.deinit(self.allocator);

        return switch (cmd) {
            .create_table => |create| try self.executeCreateTable(create),
            .insert => |insert| try self.executeInsert(insert),
            .select => |select| try self.executeSelect(select),
            .delete => |delete| try self.executeDelete(delete),
            .update => |update| try self.executeUpdate(update),
        };
    }

    fn executeCreateTable(self: *Database, cmd: sql.CreateTableCmd) !QueryResult {
        const table_ptr = try self.allocator.create(Table);
        table_ptr.* = try Table.init(self.allocator, cmd.table_name);

        for (cmd.columns.items) |col_def| {
            try table_ptr.addColumn(col_def.name, col_def.col_type);
        }

        const owned_name = try self.allocator.dupe(u8, cmd.table_name);
        try self.tables.put(owned_name, table_ptr);

        var result = QueryResult.init(self.allocator);
        try result.addColumn("status");
        var row = ArrayList(ColumnValue).init(self.allocator);
        const msg = try std.fmt.allocPrint(self.allocator, "Table '{s}' created", .{cmd.table_name});
        try row.append(ColumnValue{ .text = msg });
        try result.addRow(row);

        return result;
    }

    fn executeInsert(self: *Database, cmd: sql.InsertCmd) !QueryResult {
        const table = self.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

        var values_map = StringHashMap(ColumnValue).init(self.allocator);
        defer values_map.deinit();

        // If columns are specified, use them; otherwise use table schema order
        if (cmd.columns.items.len > 0) {
            for (cmd.columns.items, 0..) |col, i| {
                if (i < cmd.values.items.len) {
                    try values_map.put(col, cmd.values.items[i]);
                }
            }
        } else {
            // Use table column order
            for (table.columns.items, 0..) |col, i| {
                if (i < cmd.values.items.len) {
                    try values_map.put(col.name, cmd.values.items[i]);
                }
            }
        }

        const row_id = try table.insert(values_map);

        // If there's an embedding column and vector search is enabled, add to index
        if (self.hnsw) |h| {
            const row = table.get(row_id).?;
            var it = row.values.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.* == .embedding) {
                    const embedding = entry.value_ptr.embedding;
                    _ = try h.insert(embedding, row_id);
                    break;
                }
            }
        }

        var result = QueryResult.init(self.allocator);
        try result.addColumn("row_id");
        var row = ArrayList(ColumnValue).init(self.allocator);
        try row.append(ColumnValue{ .int = @intCast(row_id) });
        try result.addRow(row);

        return result;
    }

    fn executeSelect(self: *Database, cmd: sql.SelectCmd) !QueryResult {
        const table = self.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;
        var result = QueryResult.init(self.allocator);

        // Determine which columns to select
        const select_all = cmd.columns.items.len == 0;
        if (select_all) {
            try result.addColumn("id");
            for (table.columns.items) |col| {
                try result.addColumn(col.name);
            }
        } else {
            for (cmd.columns.items) |col| {
                try result.addColumn(col);
            }
        }

        // Get rows to process
        var row_ids: []u64 = undefined;
        const should_free_ids = true;

        // Handle ORDER BY SIMILARITY TO "text"
        if (cmd.order_by_similarity) |similarity_text| {
            if (self.hnsw == null) return sql.SqlError.InvalidSyntax;

            // For semantic search, we need to generate an embedding from the text
            // For now, we'll use a simple hash-based mock embedding
            const query_embedding = try self.allocator.alloc(f32, 128);
            defer self.allocator.free(query_embedding);

            // Simple hash-based embedding (in real use, you'd use an actual embedding model)
            const hash = std.hash.Wyhash.hash(0, similarity_text);
            for (query_embedding, 0..) |*val, i| {
                const seed = hash +% i;
                val.* = @as(f32, @floatFromInt(seed & 0xFF)) / 255.0;
            }

            const search_results = try self.hnsw.?.search(query_embedding, cmd.limit orelse 10);
            defer self.allocator.free(search_results);

            row_ids = try self.allocator.alloc(u64, search_results.len);
            for (search_results, 0..) |res, i| {
                row_ids[i] = res.external_id;
            }
        } else if (cmd.order_by_vibes) {
            // Fun parody feature: random order!
            row_ids = try table.getAllRows(self.allocator);
            var prng = std.Random.DefaultPrng.init(@intCast(std.time.timestamp()));
            const random = prng.random();

            // Shuffle
            for (row_ids, 0..) |_, i| {
                const j = random.intRangeLessThan(usize, 0, row_ids.len);
                const temp = row_ids[i];
                row_ids[i] = row_ids[j];
                row_ids[j] = temp;
            }
        } else {
            row_ids = try table.getAllRows(self.allocator);
        }

        defer if (should_free_ids) self.allocator.free(row_ids);

        // Apply LIMIT
        const max_rows = if (cmd.limit) |lim| @min(lim, row_ids.len) else row_ids.len;

        // Process each row
        var count: usize = 0;
        for (row_ids) |row_id| {
            if (count >= max_rows) break;

            const row = table.get(row_id) orelse continue;

            // Apply WHERE filter
            if (cmd.where_column) |where_col| {
                if (cmd.where_value) |where_val| {
                    const row_val = row.get(where_col) orelse continue;
                    if (!valuesEqual(row_val, where_val)) continue;
                }
            }

            // Apply SIMILAR TO filter (semantic search on text columns)
            if (cmd.similar_to_column) |_| {
                // In a real implementation, this would do semantic similarity
                // For now, we'll do simple text matching
                // Skip for simplicity in this demo
            }

            // Add row to results
            var result_row = ArrayList(ColumnValue).init(self.allocator);

            if (select_all) {
                try result_row.append(ColumnValue{ .int = @intCast(row_id) });
                for (table.columns.items) |col| {
                    if (row.get(col.name)) |val| {
                        try result_row.append(try val.clone(self.allocator));
                    } else {
                        try result_row.append(ColumnValue.null_value);
                    }
                }
            } else {
                for (cmd.columns.items) |col| {
                    if (row.get(col)) |val| {
                        try result_row.append(try val.clone(self.allocator));
                    } else {
                        try result_row.append(ColumnValue.null_value);
                    }
                }
            }

            try result.addRow(result_row);
            count += 1;
        }

        return result;
    }

    fn executeDelete(self: *Database, cmd: sql.DeleteCmd) !QueryResult {
        const table = self.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

        var deleted_count: usize = 0;
        const row_ids = try table.getAllRows(self.allocator);
        defer self.allocator.free(row_ids);

        for (row_ids) |row_id| {
            const row = table.get(row_id) orelse continue;

            // Apply WHERE filter
            var should_delete = true;
            if (cmd.where_column) |where_col| {
                if (cmd.where_value) |where_val| {
                    const row_val = row.get(where_col) orelse {
                        should_delete = false;
                        continue;
                    };
                    if (!valuesEqual(row_val, where_val)) {
                        should_delete = false;
                    }
                }
            }

            if (should_delete) {
                _ = table.delete(row_id);
                deleted_count += 1;
            }
        }

        var result = QueryResult.init(self.allocator);
        try result.addColumn("deleted");
        var row = ArrayList(ColumnValue).init(self.allocator);
        try row.append(ColumnValue{ .int = @intCast(deleted_count) });
        try result.addRow(row);

        return result;
    }

    fn executeUpdate(self: *Database, cmd: sql.UpdateCmd) !QueryResult {
        const table = self.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

        // Validate all SET columns exist in table
        for (cmd.assignments.items) |assignment| {
            var found = false;
            for (table.columns.items) |col| {
                if (std.mem.eql(u8, col.name, assignment.column)) {
                    found = true;
                    // Validate embedding dimension if it's an embedding column
                    if (col.col_type == .embedding and assignment.value == .embedding) {
                        const expected_dim: usize = 768; // TODO: Make this configurable
                        if (assignment.value.embedding.len != expected_dim) {
                            return sql.SqlError.DimensionMismatch;
                        }
                    }
                    break;
                }
            }
            if (!found) {
                return sql.SqlError.ColumnNotFound;
            }
        }

        var updated_count: usize = 0;
        const row_ids = try table.getAllRows(self.allocator);
        defer self.allocator.free(row_ids);

        for (row_ids) |row_id| {
            var row = table.get(row_id) orelse continue;

            // Apply WHERE filter using expression evaluator
            var should_update = true;
            if (cmd.where_expr) |expr| {
                should_update = sql.evaluateExpr(expr, row.values);
            }

            if (!should_update) continue;

            // Track if embedding column is being updated
            var old_embedding: ?[]const f32 = null;
            var new_embedding: ?[]const f32 = null;
            var embedding_changed = false;

            // Find old embedding if it exists
            if (self.hnsw != null) {
                var it = row.values.iterator();
                while (it.next()) |entry| {
                    if (entry.value_ptr.* == .embedding) {
                        old_embedding = entry.value_ptr.embedding;
                        break;
                    }
                }
            }

            // Apply all SET assignments
            for (cmd.assignments.items) |assignment| {
                // Check if this is updating an embedding column
                if (assignment.value == .embedding) {
                    new_embedding = assignment.value.embedding;
                    // Check if embedding actually changed
                    if (old_embedding) |old_emb| {
                        if (old_emb.len == new_embedding.?.len) {
                            var changed = false;
                            for (old_emb, 0..) |val, i| {
                                if (val != new_embedding.?[i]) {
                                    changed = true;
                                    break;
                                }
                            }
                            embedding_changed = changed;
                        } else {
                            embedding_changed = true;
                        }
                    } else {
                        embedding_changed = true;
                    }
                }

                // Update the row
                try row.set(self.allocator, assignment.column, assignment.value);
            }

            // Handle HNSW index updates if embedding changed
            if (embedding_changed and self.hnsw != null) {
                const h = self.hnsw.?;

                // Remove old vector from HNSW (if it existed)
                if (old_embedding != null) {
                    h.removeNode(row_id) catch |err| {
                        // Rollback: This is a critical error, we should revert changes
                        // For now, we'll just log the error
                        std.debug.print("Error removing node from HNSW: {}\n", .{err});
                        return err;
                    };
                }

                // Insert new vector with same row_id
                if (new_embedding) |new_emb| {
                    _ = h.insert(new_emb, row_id) catch |err| {
                        // Critical error: we've already removed the old vector
                        std.debug.print("Error inserting new vector to HNSW: {}\n", .{err});
                        return err;
                    };
                }
            }

            updated_count += 1;
        }

        var result = QueryResult.init(self.allocator);
        try result.addColumn("updated");
        var row = ArrayList(ColumnValue).init(self.allocator);
        try row.append(ColumnValue{ .int = @intCast(updated_count) });
        try result.addRow(row);

        return result;
    }

    /// Enable persistence with specified data directory
    pub fn enablePersistence(self: *Database, data_dir: []const u8, auto_save: bool) !void {
        if (self.data_dir) |old_dir| {
            self.allocator.free(old_dir);
        }
        self.data_dir = try self.allocator.dupe(u8, data_dir);
        self.auto_save = auto_save;
    }

    /// Save all tables and HNSW index to the data directory
    pub fn saveAll(self: *Database, dir_path: []const u8) !void {
        // Create directory if it doesn't exist
        std.fs.cwd().makePath(dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Save each table
        var it = self.tables.iterator();
        while (it.next()) |entry| {
            const table_name = entry.key_ptr.*;
            const table = entry.value_ptr.*;

            const file_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/{s}.zvdb",
                .{ dir_path, table_name },
            );
            defer self.allocator.free(file_path);

            try table.save(file_path);
        }

        // Save HNSW index if it exists
        if (self.hnsw) |h| {
            const hnsw_path = try std.fmt.allocPrint(
                self.allocator,
                "{s}/vectors.hnsw",
                .{dir_path},
            );
            defer self.allocator.free(hnsw_path);

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

    fn valuesEqual(a: ColumnValue, b: ColumnValue) bool {
        return switch (a) {
            .null_value => b == .null_value,
            .int => |ai| b == .int and b.int == ai,
            .float => |af| b == .float and b.float == af,
            .bool => |ab| b == .bool and b.bool == ab,
            .text => |at| b == .text and std.mem.eql(u8, at, b.text),
            .embedding => false, // Don't compare embeddings directly
        };
    }
};
