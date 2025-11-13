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

// ============================================================================
// UPDATE Statement Tests
// ============================================================================

test "SQL: Basic UPDATE single column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer insert2.deinit();

    // Update single column with WHERE
    var update_result = try db.execute("UPDATE users SET name = \"Alicia\" WHERE id = 1");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1); // 1 row updated

    // Verify the update
    var select_result = try db.execute("SELECT * FROM users WHERE id = 1");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 1);
    try testing.expect(std.mem.eql(u8, select_result.rows.items[0].items[2].text, "Alicia"));
}

test "SQL: UPDATE multiple columns" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE products (id int, name text, price float, stock int)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99, 100)");
    defer insert_result.deinit();

    // Update multiple columns
    var update_result = try db.execute("UPDATE products SET price = 24.99, stock = 150 WHERE id = 1");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    // Verify both columns updated
    var select_result = try db.execute("SELECT * FROM products WHERE id = 1");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items[0].items[3].float == 24.99);
    try testing.expect(select_result.rows.items[0].items[4].int == 150);
}

test "SQL: UPDATE with WHERE AND condition" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE items (id int, category text, price float)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO items VALUES (1, \"electronics\", 99.99)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO items VALUES (2, \"electronics\", 49.99)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO items VALUES (3, \"books\", 19.99)");
    defer insert3.deinit();

    // Update with AND condition
    var update_result = try db.execute("UPDATE items SET price = 89.99 WHERE category = \"electronics\" AND price > 50");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1); // Only 1 row matches

    // Verify correct row updated
    var select_result = try db.execute("SELECT * FROM items WHERE id = 1");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items[0].items[3].float == 89.99);
}

test "SQL: UPDATE with WHERE OR condition" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE tasks (id int, status text, priority int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO tasks VALUES (1, \"pending\", 1)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO tasks VALUES (2, \"active\", 3)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO tasks VALUES (3, \"pending\", 5)");
    defer insert3.deinit();

    // Update with OR condition
    var update_result = try db.execute("UPDATE tasks SET status = \"completed\" WHERE priority = 1 OR priority = 5");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 2); // 2 rows updated

    // Verify both rows updated
    var select_result = try db.execute("SELECT * FROM tasks WHERE status = \"completed\"");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 2);
}

test "SQL: UPDATE with comparison operators" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE inventory (id int, quantity int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO inventory VALUES (1, 5)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO inventory VALUES (2, 15)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO inventory VALUES (3, 25)");
    defer insert3.deinit();

    // Test >= operator
    var update1 = try db.execute("UPDATE inventory SET quantity = 20 WHERE quantity >= 15");
    defer update1.deinit();
    try testing.expect(update1.rows.items[0].items[0].int == 2);

    // Test < operator
    var update2 = try db.execute("UPDATE inventory SET quantity = 10 WHERE quantity < 15");
    defer update2.deinit();
    try testing.expect(update2.rows.items[0].items[0].int == 1);
}

test "SQL: UPDATE with IS NULL" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE contacts (id int, email text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO contacts VALUES (1, \"alice@example.com\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO contacts VALUES (2, NULL)");
    defer insert2.deinit();

    // Update rows where email IS NULL
    var update_result = try db.execute("UPDATE contacts SET email = \"unknown@example.com\" WHERE email IS NULL");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    // Verify update
    var select_result = try db.execute("SELECT * FROM contacts WHERE id = 2");
    defer select_result.deinit();

    try testing.expect(std.mem.eql(u8, select_result.rows.items[0].items[2].text, "unknown@example.com"));
}

test "SQL: UPDATE with IS NOT NULL" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE profiles (id int, bio text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO profiles VALUES (1, \"Software developer\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO profiles VALUES (2, NULL)");
    defer insert2.deinit();

    // Update rows where bio IS NOT NULL
    var update_result = try db.execute("UPDATE profiles SET bio = \"Updated bio\" WHERE bio IS NOT NULL");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1);
}

test "SQL: UPDATE with no matching rows" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert_result.deinit();

    // Try to update non-existent row
    var update_result = try db.execute("UPDATE users SET name = \"Bob\" WHERE id = 999");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 0); // 0 rows updated
}

test "SQL: UPDATE all rows (no WHERE)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE settings (id int, enabled bool)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO settings VALUES (1, true)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO settings VALUES (2, true)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO settings VALUES (3, false)");
    defer insert3.deinit();

    // Update all rows
    var update_result = try db.execute("UPDATE settings SET enabled = false");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 3); // All 3 rows updated

    // Verify all updated
    var select_result = try db.execute("SELECT * FROM settings");
    defer select_result.deinit();

    for (select_result.rows.items) |row| {
        try testing.expect(row.items[2].bool == false);
    }
}

test "SQL: UPDATE with NOT operator" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE flags (id int, active bool)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO flags VALUES (1, true)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO flags VALUES (2, false)");
    defer insert2.deinit();

    // Update with NOT operator
    var update_result = try db.execute("UPDATE flags SET active = true WHERE NOT active = true");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    // Verify all are now true
    var select_result = try db.execute("SELECT * FROM flags");
    defer select_result.deinit();

    for (select_result.rows.items) |row| {
        try testing.expect(row.items[2].bool == true);
    }
}

test "SQL: UPDATE with complex nested conditions" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE records (id int, value int, status text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO records VALUES (1, 10, \"active\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO records VALUES (2, 20, \"inactive\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO records VALUES (3, 30, \"active\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO records VALUES (4, 40, \"inactive\")");
    defer insert4.deinit();

    // Complex condition: (value > 15 AND status = "active") OR value < 15
    var update_result = try db.execute("UPDATE records SET status = \"updated\" WHERE value > 15 AND status = \"active\" OR value < 15");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 2); // rows 1 and 3

    // Verify correct rows updated
    var select_result = try db.execute("SELECT * FROM records WHERE status = \"updated\"");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 2);
}

test "SQL: UPDATE with embeddings - vector changed" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initialize vector search
    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE documents (id int, title text, embedding embedding)");
    defer create_result.deinit();

    // Create embeddings
    var old_embedding = [_]f32{0.1} ** 768;
    var new_embedding = [_]f32{0.9} ** 768;

    // Insert document with embedding using table API
    const table = db.tables.get("documents").?;

    var values = std.StringHashMap(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.put("id", ColumnValue{ .int = 1 });
    try values.put("title", ColumnValue{ .text = "Test Doc" });
    const emb_old = try testing.allocator.dupe(f32, &old_embedding);
    defer testing.allocator.free(emb_old);
    try values.put("embedding", ColumnValue{ .embedding = emb_old });

    const row_id = try table.insert(values);
    _ = try db.hnsw.?.insert(&old_embedding, row_id);

    // Verify embedding is in HNSW
    var internal_id = db.hnsw.?.getInternalId(row_id);
    try testing.expect(internal_id != null);

    // Now update the embedding using table API to simulate UPDATE
    const row = table.get(row_id).?;
    const emb_new = try testing.allocator.dupe(f32, &new_embedding);
    defer testing.allocator.free(emb_new);
    try row.set(testing.allocator, "embedding", ColumnValue{ .embedding = emb_new });

    // Simulate UPDATE execution: remove old, insert new
    try db.hnsw.?.removeNode(row_id);
    _ = try db.hnsw.?.insert(&new_embedding, row_id);

    // Verify new embedding is in HNSW
    internal_id = db.hnsw.?.getInternalId(row_id);
    try testing.expect(internal_id != null);

    // Verify embedding changed
    const updated_row = table.get(row_id).?;
    const updated_emb = updated_row.get("embedding").?;
    try testing.expect(updated_emb.embedding[0] == 0.9);
}

test "SQL: UPDATE persistence" {
    const test_dir = "test_update_persistence";
    std.fs.cwd().deleteTree(test_dir) catch {};
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create database and enable persistence
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enablePersistence(test_dir, false);

        var create_result = try db.execute("CREATE TABLE config (key text, value int)");
        defer create_result.deinit();

        var insert_result = try db.execute("INSERT INTO config VALUES (\"timeout\", 30)");
        defer insert_result.deinit();

        var update_result = try db.execute("UPDATE config SET value = 60 WHERE key = \"timeout\"");
        defer update_result.deinit();

        try testing.expect(update_result.rows.items[0].items[0].int == 1);

        // Save manually
        try db.saveAll(test_dir);
    }

    // Load and verify update persisted
    {
        var db = try Database.loadAll(testing.allocator, test_dir);
        defer db.deinit();

        var result = try db.execute("SELECT * FROM config WHERE key = \"timeout\"");
        defer result.deinit();

        try testing.expect(result.rows.items.len == 1);
        try testing.expect(result.rows.items[0].items[2].int == 60);
    }
}

test "SQL: UPDATE error on non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert_result.deinit();

    // Try to update non-existent column
    const result = db.execute("UPDATE users SET nonexistent = \"value\" WHERE id = 1");

    try testing.expectError(error.ColumnNotFound, result);
}

test "SQL: UPDATE with inequality operators" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE scores (id int, score int, grade text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO scores VALUES (1, 95, \"TBD\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO scores VALUES (2, 75, \"TBD\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO scores VALUES (3, 55, \"TBD\")");
    defer insert3.deinit();

    // Update with != operator
    var update_result = try db.execute("UPDATE scores SET grade = \"Pass\" WHERE score != 55");
    defer update_result.deinit();

    try testing.expect(update_result.rows.items[0].items[0].int == 2);

    // Verify updates
    var select_result = try db.execute("SELECT * FROM scores WHERE grade = \"Pass\"");
    defer select_result.deinit();

    try testing.expect(select_result.rows.items.len == 2);
}

// ============================================================================
// WAL Integration Tests (Phase 2.3)
// ============================================================================

test "WAL: Basic integration with INSERT/DELETE/UPDATE" {
    const wal_dir = "test_data/wal_basic";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Enable WAL
    try db.enableWal(wal_dir);

    // Create table
    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    // Test INSERT with WAL enabled
    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer insert1.deinit();

    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer insert2.deinit();

    // Test UPDATE with WAL enabled
    var update_result = try db.execute("UPDATE users SET age = 26 WHERE id = 1");
    defer update_result.deinit();
    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    // Test DELETE with WAL enabled
    var delete_result = try db.execute("DELETE FROM users WHERE id = 2");
    defer delete_result.deinit();
    try testing.expect(delete_result.rows.items[0].items[0].int == 1);

    // Verify data is still correct
    var select_result = try db.execute("SELECT * FROM users");
    defer select_result.deinit();
    try testing.expect(select_result.rows.items.len == 1);
    try testing.expect(select_result.rows.items[0].items[3].int == 26); // Updated age

    // Verify WAL directory was created
    var wal_dir_handle = try std.fs.cwd().openDir(wal_dir, .{});
    wal_dir_handle.close();
}

test "WAL: Transaction ID increments" {
    const wal_dir = "test_data/wal_txid";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.enableWal(wal_dir);

    // Create table
    var create_result = try db.execute("CREATE TABLE test (id int, value int)");
    defer create_result.deinit();

    // Transaction ID should start at 0
    try testing.expect(db.current_tx_id == 0);

    // Perform INSERT - should increment tx_id
    var insert1 = try db.execute("INSERT INTO test VALUES (1, 100)");
    defer insert1.deinit();
    try testing.expect(db.current_tx_id == 1);

    // Perform UPDATE - should increment tx_id again
    var update1 = try db.execute("UPDATE test SET value = 200 WHERE id = 1");
    defer update1.deinit();
    try testing.expect(db.current_tx_id == 2);

    // Perform DELETE - should increment tx_id again
    var delete1 = try db.execute("DELETE FROM test WHERE id = 1");
    defer delete1.deinit();
    try testing.expect(db.current_tx_id == 3);
}

test "WAL: Database works without WAL (optional)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // WAL should be null by default
    try testing.expect(db.wal == null);

    // Operations should work without WAL
    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert_result.deinit();

    var update_result = try db.execute("UPDATE users SET name = \"Alicia\" WHERE id = 1");
    defer update_result.deinit();
    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    var delete_result = try db.execute("DELETE FROM users WHERE id = 1");
    defer delete_result.deinit();
    try testing.expect(delete_result.rows.items[0].items[0].int == 1);

    // Verify database still works correctly
    var select_result = try db.execute("SELECT * FROM users");
    defer select_result.deinit();
    try testing.expect(select_result.rows.items.len == 0);
}

test "WAL: Embeddings are preserved across INSERT/UPDATE with WAL enabled" {
    const wal_dir = "test_data/wal_embeddings";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initialize vector search and WAL
    try db.initVectorSearch(16, 200);
    try db.enableWal(wal_dir);

    // Create table with embedding column
    var create_result = try db.execute("CREATE TABLE docs (id int, title text, embedding embedding)");
    defer create_result.deinit();

    // Create test embeddings
    var embedding1 = [_]f32{0.1} ** 768;
    var embedding2 = [_]f32{0.9} ** 768;

    // Test INSERT with regular columns first (this triggers WAL)
    var insert_result = try db.execute("INSERT INTO docs VALUES (1, \"Document 1\", NULL)");
    defer insert_result.deinit();

    // Now manually add the embedding to test preservation
    // (SQL parser doesn't support embedding array literals, so we use table API for embeddings only)
    const table = db.tables.get("docs").?;
    const row_id: u64 = 1;
    const row = table.get(row_id).?;

    const emb1 = try testing.allocator.dupe(f32, &embedding1);
    defer testing.allocator.free(emb1);
    try row.set(testing.allocator, "embedding", ColumnValue{ .embedding = emb1 });
    _ = try db.hnsw.?.insert(&embedding1, row_id);

    // Verify WAL file was created from the INSERT command
    var wal_dir_handle = try std.fs.cwd().openDir(wal_dir, .{ .iterate = true });
    defer wal_dir_handle.close();

    var wal_file_found = false;
    var wal_iter = wal_dir_handle.iterate();
    while (try wal_iter.next()) |entry| {
        if (std.mem.startsWith(u8, entry.name, "wal.")) {
            wal_file_found = true;
            // Check that WAL file is not empty
            const file_stat = try wal_dir_handle.statFile(entry.name);
            try testing.expect(file_stat.size > 36); // Header is 36 bytes, should have more
            break;
        }
    }
    try testing.expect(wal_file_found);

    // Test UPDATE to change title (this triggers WAL)
    var update_result = try db.execute("UPDATE docs SET title = \"Updated Document\" WHERE id = 1");
    defer update_result.deinit();
    try testing.expect(update_result.rows.items[0].items[0].int == 1);

    // Update the embedding manually
    const emb2 = try testing.allocator.dupe(f32, &embedding2);
    defer testing.allocator.free(emb2);
    try row.set(testing.allocator, "embedding", ColumnValue{ .embedding = emb2 });
    try db.hnsw.?.removeNode(row_id);
    _ = try db.hnsw.?.insert(&embedding2, row_id);

    // Verify embedding was updated
    const updated_emb = row.get("embedding").?;
    try testing.expect(updated_emb.embedding[0] == 0.9);

    // Verify HNSW index still contains the row
    const internal_id = db.hnsw.?.getInternalId(row_id);
    try testing.expect(internal_id != null);
}

test "WAL: Multiple embedding insertions with WAL" {
    const wal_dir = "test_data/wal_multi_embeddings";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initialize vector search and WAL
    try db.initVectorSearch(16, 200);
    try db.enableWal(wal_dir);

    // Create table
    var create_result = try db.execute("CREATE TABLE docs (id int, content text, vec embedding)");
    defer create_result.deinit();

    const table = db.tables.get("docs").?;

    // Insert multiple rows using SQL (this triggers WAL)
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        // Use SQL INSERT to trigger WAL
        const id = i + 1;
        var insert_query_buf: [100]u8 = undefined;
        const insert_query = try std.fmt.bufPrint(&insert_query_buf, "INSERT INTO docs VALUES ({d}, \"Test document\", NULL)", .{id});
        var insert_result = try db.execute(insert_query);
        defer insert_result.deinit();

        // Now add the embedding manually (parser doesn't support embedding literals)
        var embedding = [_]f32{@as(f32, @floatFromInt(i)) * 0.1} ** 768;
        const row = table.get(id).?;
        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try row.set(testing.allocator, "vec", ColumnValue{ .embedding = emb });
        _ = try db.hnsw.?.insert(&embedding, id);
    }

    // Verify all rows inserted
    try testing.expect(table.count() == 5);

    // Verify transaction IDs incremented for each insert
    // Since we have WAL enabled, each INSERT command incremented the tx_id
    // We inserted 5 rows, so tx_id should now be 5 (started at 0)
    try testing.expect(db.current_tx_id == 5);

    // Verify HNSW has all vectors
    for (1..6) |row_num| {
        const internal_id = db.hnsw.?.getInternalId(row_num);
        try testing.expect(internal_id != null);
    }
}
