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

    pub fn init(allocator: Allocator) Database {
        return Database{
            .tables = StringHashMap(*Table).init(allocator),
            .hnsw = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Database) void {
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
