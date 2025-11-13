const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "SQL: CREATE TABLE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
    try testing.expect(db.tables.count() == 1);
}

test "SQL: INSERT and SELECT" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer insert_result.deinit();

    var select_result = try db.execute("SELECT * FROM users");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 1);
    try testing.expect(select_result.columns.items.len == 4); // id + 3 columns
}

test "SQL: INSERT with column names" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, bio text)");
    defer create_result.deinit();
    var insert_result = try db.execute("INSERT INTO users (name, id, bio) VALUES (\"Bob\", 2, \"Developer\")");
    defer insert_result.deinit();

    var result = try db.execute("SELECT * FROM users");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
}

test "SQL: SELECT with WHERE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE products (id int, name text, price float)");
    defer create_result.deinit();
    var insert1 = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 29.99)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO products VALUES (3, \"Gizmo\", 19.99)");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM products WHERE price = 19.99");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
}

test "SQL: SELECT with LIMIT" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE items (id int, value int)");
    defer create_result.deinit();
    var insert1 = try db.execute("INSERT INTO items VALUES (1, 100)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO items VALUES (2, 200)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO items VALUES (3, 300)");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO items VALUES (4, 400)");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM items LIMIT 2");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
}

test "SQL: DELETE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE temp (id int, status text)");
    defer create_result.deinit();
    var insert1 = try db.execute("INSERT INTO temp VALUES (1, \"active\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO temp VALUES (2, \"inactive\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO temp VALUES (3, \"active\")");
    defer insert3.deinit();

    var delete_result = try db.execute("DELETE FROM temp WHERE status = \"inactive\"");
    defer delete_result.deinit();

    try testing.expect(delete_result.rows.items[0].items[0].int == 1);

    var select_result = try db.execute("SELECT * FROM temp");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 2);
}

test "SQL: SELECT specific columns" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, email text, age int)");
    defer create_result.deinit();
    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\", \"alice@example.com\", 25)");
    defer insert_result.deinit();

    var result = try db.execute("SELECT name, age FROM users");
    defer result.deinit();

    try testing.expect(result.columns.items.len == 2);
    try testing.expect(std.mem.eql(u8, result.columns.items[0], "name"));
    try testing.expect(std.mem.eql(u8, result.columns.items[1], "age"));
}

test "SQL: Multiple data types" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE mixed (id int, name text, price float, active bool)");
    defer create_result.deinit();
    var insert1 = try db.execute("INSERT INTO mixed VALUES (1, \"Item1\", 99.99, true)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO mixed VALUES (2, \"Item2\", 49.99, false)");
    defer insert2.deinit();

    var result = try db.execute("SELECT * FROM mixed");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
}

test "SQL: ORDER BY VIBES (random)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE fun (id int, name text)");
    defer create_result.deinit();
    var insert1 = try db.execute("INSERT INTO fun VALUES (1, \"First\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO fun VALUES (2, \"Second\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO fun VALUES (3, \"Third\")");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM fun ORDER BY VIBES");
    defer result.deinit();

    // Should return all rows in random order
    try testing.expect(result.rows.items.len == 3);
}

test "SQL: Semantic search with embeddings" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initialize vector search
    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE documents (id int, title text, content text, embedding embedding)");
    defer create_result.deinit();

    // Create a simple embedding
    var embedding1 = [_]f32{0.1} ** 128;
    var embedding2 = [_]f32{0.9} ** 128;
    _ = [_]f32{0.5} ** 128; // embedding3 - reserved for future use

    // In a real scenario, you'd generate these from text
    // For testing, we'll directly insert with embeddings

    // Note: The parser doesn't support array literals yet, so we test the table API directly
    const table = db.tables.get("documents").?;

    var values1 = std.StringHashMap(ColumnValue).init(testing.allocator);
    defer values1.deinit();
    try values1.put("id", ColumnValue{ .int = 1 });
    try values1.put("title", ColumnValue{ .text = "Doc1" });
    const emb1 = try testing.allocator.dupe(f32, &embedding1);
    defer testing.allocator.free(emb1);
    try values1.put("embedding", ColumnValue{ .embedding = emb1 });

    const id1 = try table.insert(values1);
    _ = try db.hnsw.?.insert(&embedding1, id1);

    var values2 = std.StringHashMap(ColumnValue).init(testing.allocator);
    defer values2.deinit();
    try values2.put("id", ColumnValue{ .int = 2 });
    try values2.put("title", ColumnValue{ .text = "Doc2" });
    const emb2 = try testing.allocator.dupe(f32, &embedding2);
    defer testing.allocator.free(emb2);
    try values2.put("embedding", ColumnValue{ .embedding = emb2 });

    const id2 = try table.insert(values2);
    _ = try db.hnsw.?.insert(&embedding2, id2);

    // Semantic search
    var result = try db.execute("SELECT * FROM documents ORDER BY SIMILARITY TO \"test query\" LIMIT 2");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
}

test "SQL: Case insensitive keywords" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("create table test (id int)");
    defer create_result.deinit();
    var insert_result = try db.execute("insert into test values (1)");
    defer insert_result.deinit();
    var result = try db.execute("select * from test");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
}

test "SQL: NULL values" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE nullable (id int, value text)");
    defer create_result.deinit();
    var insert_result = try db.execute("INSERT INTO nullable VALUES (1, NULL)");
    defer insert_result.deinit();

    var result = try db.execute("SELECT * FROM nullable");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
    try testing.expect(result.rows.items[0].items[2] == .null_value);
}

// =============================================================================
// Persistence Tests
// =============================================================================

test "Persistence: Save and load empty table" {
    const test_dir = "test_data/empty_table";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create and save empty table
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer create_result.deinit();

        try db.saveAll(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        try testing.expect(db.tables.count() == 1);
        const table = db.tables.get("users").?;
        try testing.expect(table.count() == 0);
        try testing.expect(table.columns.items.len == 3);
        try testing.expectEqualStrings("users", table.name);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: Save and load table with data" {
    const test_dir = "test_data/with_data";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create, populate, and save
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE products (id int, name text, price float, available bool)");
        defer create_result.deinit();

        var insert1 = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99, true)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 29.99, false)");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO products VALUES (3, \"Gizmo\", 39.99, true)");
        defer insert3.deinit();

        try db.saveAll(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        try testing.expect(db.tables.count() == 1);

        var result = try db.execute("SELECT * FROM products");
        defer result.deinit();

        try testing.expect(result.rows.items.len == 3);

        // Verify we can query the loaded data
        var where_result = try db.execute("SELECT * FROM products WHERE available = true");
        defer where_result.deinit();

        try testing.expect(where_result.rows.items.len == 2);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: Multiple tables" {
    const test_dir = "test_data/multi_tables";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create multiple tables and save
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create1 = try db.execute("CREATE TABLE users (id int, name text)");
        defer create1.deinit();
        var create2 = try db.execute("CREATE TABLE products (id int, price float)");
        defer create2.deinit();
        var create3 = try db.execute("CREATE TABLE orders (id int, quantity int)");
        defer create3.deinit();

        var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
        defer insert1.deinit();
        var insert2 = try db.execute("INSERT INTO products VALUES (1, 99.99)");
        defer insert2.deinit();
        var insert3 = try db.execute("INSERT INTO orders VALUES (1, 5)");
        defer insert3.deinit();

        try db.saveAll(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        try testing.expect(db.tables.count() == 3);
        try testing.expect(db.tables.contains("users"));
        try testing.expect(db.tables.contains("products"));
        try testing.expect(db.tables.contains("orders"));

        var result1 = try db.execute("SELECT * FROM users");
        defer result1.deinit();
        try testing.expect(result1.rows.items.len == 1);

        var result2 = try db.execute("SELECT * FROM products");
        defer result2.deinit();
        try testing.expect(result2.rows.items.len == 1);

        var result3 = try db.execute("SELECT * FROM orders");
        defer result3.deinit();
        try testing.expect(result3.rows.items.len == 1);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: All data types" {
    const test_dir = "test_data/all_types";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create table with all types
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE mixed (id int, name text, price float, active bool)");
        defer create_result.deinit();

        var insert1 = try db.execute("INSERT INTO mixed VALUES (42, \"Test Item\", 123.45, true)");
        defer insert1.deinit();
        var insert2 = try db.execute("INSERT INTO mixed VALUES (-99, \"Another\", -0.5, false)");
        defer insert2.deinit();
        var insert3 = try db.execute("INSERT INTO mixed VALUES (0, \"Empty\", 0.0, false)");
        defer insert3.deinit();

        try db.saveAll(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        var result = try db.execute("SELECT * FROM mixed");
        defer result.deinit();

        try testing.expect(result.rows.items.len == 3);

        // Verify specific values preserved
        var where_result = try db.execute("SELECT * FROM mixed WHERE id = 42");
        defer where_result.deinit();
        try testing.expect(where_result.rows.items.len == 1);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: NULL values" {
    const test_dir = "test_data/nulls";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create table with NULLs
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE nullable (id int, value text)");
        defer create_result.deinit();

        var insert1 = try db.execute("INSERT INTO nullable VALUES (1, NULL)");
        defer insert1.deinit();
        var insert2 = try db.execute("INSERT INTO nullable VALUES (2, \"Not Null\")");
        defer insert2.deinit();

        try db.saveAll(test_dir);
    }

    // Load and verify NULLs preserved
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        var result = try db.execute("SELECT * FROM nullable");
        defer result.deinit();

        try testing.expect(result.rows.items.len == 2);
        // First row should have NULL value
        try testing.expect(result.rows.items[0].items[2] == .null_value);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: Row IDs preserved" {
    const test_dir = "test_data/row_ids";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    var original_ids: [3]u64 = undefined;

    // Create and save with specific row IDs
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE tracking (id int, name text)");
        defer create_result.deinit();

        var insert1 = try db.execute("INSERT INTO tracking VALUES (1, \"First\")");
        defer insert1.deinit();
        var insert2 = try db.execute("INSERT INTO tracking VALUES (2, \"Second\")");
        defer insert2.deinit();
        var insert3 = try db.execute("INSERT INTO tracking VALUES (3, \"Third\")");
        defer insert3.deinit();

        // Capture row IDs
        const table = db.tables.get("tracking").?;
        const ids = try table.getAllRows(testing.allocator);
        defer testing.allocator.free(ids);
        @memcpy(&original_ids, ids);

        try db.saveAll(test_dir);
    }

    // Load and verify row IDs match
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        const table = db.tables.get("tracking").?;
        const loaded_ids = try table.getAllRows(testing.allocator);
        defer testing.allocator.free(loaded_ids);

        try testing.expect(loaded_ids.len == original_ids.len);

        // Verify all original row IDs exist
        for (original_ids) |orig_id| {
            var found = false;
            for (loaded_ids) |loaded_id| {
                if (orig_id == loaded_id) {
                    found = true;
                    break;
                }
            }
            try testing.expect(found);
        }
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: Next ID counter preserved" {
    const test_dir = "test_data/next_id";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create, insert, and save
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create_result = try db.execute("CREATE TABLE sequences (id int, value int)");
        defer create_result.deinit();

        var insert1 = try db.execute("INSERT INTO sequences VALUES (1, 100)");
        defer insert1.deinit();
        var insert2 = try db.execute("INSERT INTO sequences VALUES (2, 200)");
        defer insert2.deinit();

        try db.saveAll(test_dir);
    }

    // Load and insert new row - ID should continue from where it left off
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        const table_before = db.tables.get("sequences").?;
        const next_id_before = table_before.next_id;

        var insert3 = try db.execute("INSERT INTO sequences VALUES (3, 300)");
        defer insert3.deinit();

        // Verify new row got expected ID
        const table_after = db.tables.get("sequences").?;
        try testing.expect(table_after.rows.count() == 3);
        try testing.expect(table_after.next_id == next_id_before + 1);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}

test "Persistence: Load from non-existent directory" {
    const test_dir = "test_data/does_not_exist";

    // Ensure directory doesn't exist
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Loading from non-existent directory should return empty database
    var db = try Database.loadAll(testing.allocator, test_dir);
    defer db.deinit();

    try testing.expect(db.tables.count() == 0);
    try testing.expect(db.hnsw == null);
}

test "Persistence: Auto-save on deinit" {
    const test_dir = "test_data/auto_save";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(test_dir) catch {};

    // Create database with auto-save enabled
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enablePersistence(test_dir, true);

        var create_result = try db.execute("CREATE TABLE autosave (id int, data text)");
        defer create_result.deinit();

        var insert_result = try db.execute("INSERT INTO autosave VALUES (1, \"Auto saved data\")");
        defer insert_result.deinit();

        // deinit will trigger auto-save
    }

    // Verify data was auto-saved
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        try testing.expect(db.tables.count() == 1);

        var result = try db.execute("SELECT * FROM autosave");
        defer result.deinit();

        try testing.expect(result.rows.items.len == 1);
    }

    // Clean up
    std.fs.cwd().deleteTree(test_dir) catch {};
}
