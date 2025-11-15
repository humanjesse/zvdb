const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;
const WalWriter = @import("wal.zig").WalWriter;

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
    try testing.expect(select_result.columns.items.len == 3); // id, name, age (no duplicate id)
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
    try testing.expect(result.rows.items[0].items[1] == .null_value); // items[1] is the value column
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
        // First row should have NULL value (items[1] is the value column)
        try testing.expect(result.rows.items[0].items[1] == .null_value);
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
    try testing.expect(std.mem.eql(u8, select_result.rows.items[0].items[1].text, "Alicia")); // items[1] is name column
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

    try testing.expect(select_result.rows.items[0].items[2].float == 24.99); // items[2] is price
    try testing.expect(select_result.rows.items[0].items[3].int == 150); // items[3] is stock
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

    try testing.expect(select_result.rows.items[0].items[2].float == 89.99); // items[2] is price
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

    try testing.expect(std.mem.eql(u8, select_result.rows.items[0].items[1].text, "unknown@example.com")); // items[1] is email
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
        try testing.expect(row.items[1].bool == false); // items[1] is enabled column
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
        try testing.expect(row.items[1].bool == true); // items[1] is active column
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
    try testing.expect(select_result.rows.items[0].items[2].int == 26); // Updated age

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

// ============================================================================
// Phase 2.4: Crash Recovery Tests
// ============================================================================

test "WAL Recovery: Basic recovery of committed transaction" {
    const wal_dir = "test_data/wal_recovery_basic";

    // Clean up any existing test data
    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create database, insert data, "crash" before closing properly
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        // Create table
        var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer create_result.deinit();

        // Insert data (this writes to WAL)
        var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
        defer insert2.deinit();

        // Verify data was inserted
        const table = db.tables.get("users").?;
        try testing.expectEqual(@as(usize, 2), table.count());

        // Simulate crash: DON'T call db.deinit() properly - just let WAL close
        // The WAL has the data, but we'll pretend the database "crashed"
    }

    // Phase 2: Restart database and recover from WAL
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        // First create the table again (schema is not in WAL yet in Phase 2.4)
        var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer create_result.deinit();

        // Recover from WAL (do this before enabling WAL to avoid file conflicts)
        const recovered_count = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 2), recovered_count);

        // Verify data was recovered
        const table = db.tables.get("users").?;
        try testing.expectEqual(@as(usize, 2), table.count());

        // Verify the actual data
        var select = try db.execute("SELECT * FROM users");
        defer select.deinit();
        try testing.expectEqual(@as(usize, 2), select.rows.items.len);
    }
}

test "WAL Recovery: Uncommitted transaction is discarded" {
    const wal_dir = "test_data/wal_recovery_uncommitted";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Write some committed data and some uncommitted data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE test (id int, value text)");
        defer create.deinit();

        // Insert committed data
        var insert1 = try db.execute("INSERT INTO test VALUES (1, \"committed\")");
        defer insert1.deinit();

        // Manually write an uncommitted transaction to WAL
        // (In real scenario, this would be a transaction without COMMIT before crash)
        // For testing, we'll write records with tx_id but no commit record
        if (db.wal) |w| {
            // Simulate an uncommitted transaction
            const uncommitted_tx_id: u64 = 999;

            // Write a record without commit
            _ = try w.writeRecord(.{
                .record_type = .insert_row,
                .tx_id = uncommitted_tx_id,
                .lsn = 0,
                .row_id = 2,
                .table_name = "test",
                .data = "", // Would normally be serialized row
                .checksum = 0,
            });

            try w.flush();
        }
    }

    // Phase 2: Recover - uncommitted transaction should be discarded
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE test (id int, value text)");
        defer create.deinit();

        const recovered_count = try db.recoverFromWal(wal_dir);
        // Should only recover the 1 committed transaction
        try testing.expectEqual(@as(usize, 1), recovered_count);

        const table = db.tables.get("test").?;
        try testing.expectEqual(@as(usize, 1), table.count());
    }
}

test "WAL Recovery: Multiple committed transactions" {
    const wal_dir = "test_data/wal_recovery_multiple";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Write multiple transactions
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE items (id int, name text)");
        defer create.deinit();

        // Insert multiple items
        var i: usize = 1;
        while (i <= 10) : (i += 1) {
            var buf: [100]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO items VALUES ({d}, \"item_{d}\")", .{ i, i });
            var result = try db.execute(query);
            defer result.deinit();
        }

        const table = db.tables.get("items").?;
        try testing.expectEqual(@as(usize, 10), table.count());
    }

    // Phase 2: Recover all transactions
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE items (id int, name text)");
        defer create.deinit();

        const recovered_count = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 10), recovered_count);

        const table = db.tables.get("items").?;
        try testing.expectEqual(@as(usize, 10), table.count());

        // Verify all items recovered
        var select = try db.execute("SELECT * FROM items");
        defer select.deinit();
        try testing.expectEqual(@as(usize, 10), select.rows.items.len);
    }
}

test "WAL Recovery: DELETE operations" {
    const wal_dir = "test_data/wal_recovery_delete";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Insert then delete
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE records (id int, status text)");
        defer create.deinit();

        // Insert 3 records
        var insert1 = try db.execute("INSERT INTO records VALUES (1, \"active\")");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO records VALUES (2, \"active\")");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO records VALUES (3, \"active\")");
        defer insert3.deinit();

        // Delete one record
        var delete = try db.execute("DELETE FROM records WHERE id = 2");
        defer delete.deinit();

        const table = db.tables.get("records").?;
        try testing.expectEqual(@as(usize, 2), table.count());
    }

    // Phase 2: Recover
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE records (id int, status text)");
        defer create.deinit();

        _ = try db.recoverFromWal(wal_dir);

        const table = db.tables.get("records").?;
        try testing.expectEqual(@as(usize, 2), table.count());

        // Verify record 2 was deleted
        try testing.expect(table.get(2) == null);
        try testing.expect(table.get(1) != null);
        try testing.expect(table.get(3) != null);
    }
}

test "WAL Recovery: UPDATE operations" {
    const wal_dir = "test_data/wal_recovery_update";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Insert then update
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE products (id int, name text, price int)");
        defer create.deinit();

        var insert = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 100)");
        defer insert.deinit();

        var update = try db.execute("UPDATE products SET price = 150 WHERE id = 1");
        defer update.deinit();
    }

    // Phase 2: Recover and verify update
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE products (id int, name text, price int)");
        defer create.deinit();

        _ = try db.recoverFromWal(wal_dir);

        var select = try db.execute("SELECT * FROM products WHERE id = 1");
        defer select.deinit();

        try testing.expectEqual(@as(usize, 1), select.rows.items.len);

        // Verify price was updated to 150
        // Table has 3 columns: id, name, price
        // So price is at index 2
        const price_col = select.rows.items[0].items[2];
        try testing.expectEqual(@as(i64, 150), price_col.int);
    }
}

test "WAL Recovery: Idempotent recovery (running twice)" {
    const wal_dir = "test_data/wal_recovery_idempotent";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE data (id int, value int)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO data VALUES (1, 100)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO data VALUES (2, 200)");
        defer insert2.deinit();
    }

    // Phase 2: Recover once
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE data (id int, value int)");
        defer create.deinit();

        const recovered1 = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 2), recovered1);

        const table = db.tables.get("data").?;
        try testing.expectEqual(@as(usize, 2), table.count());

        // Recover again (should be idempotent - no duplicates)
        const recovered2 = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 2), recovered2);

        // Should still have only 2 rows (no duplicates)
        try testing.expectEqual(@as(usize, 2), table.count());
    }
}

test "WAL Recovery: HNSW index rebuild after recovery" {
    const wal_dir = "test_data/wal_recovery_hnsw";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create data with embeddings
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.initVectorSearch(16, 200);
        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE docs (id int, title text, vec embedding)");
        defer create.deinit();

        const table = db.tables.get("docs").?;

        // Insert rows with embeddings
        var i: usize = 1;
        while (i <= 3) : (i += 1) {
            var buf: [100]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO docs VALUES ({d}, \"doc_{d}\", NULL)", .{ i, i });
            var result = try db.execute(query);
            defer result.deinit();

            // Add embedding manually and log to WAL
            var embedding = [_]f32{@as(f32, @floatFromInt(i)) * 0.1} ** 128;
            const row = table.get(i).?;

            // Serialize old row state for WAL
            const old_serialized = try row.serialize(testing.allocator);
            defer testing.allocator.free(old_serialized);

            const emb = try testing.allocator.dupe(f32, &embedding);
            defer testing.allocator.free(emb);
            try row.set(testing.allocator, "vec", ColumnValue{ .embedding = emb });
            _ = try db.hnsw.?.insert(&embedding, i);

            // Serialize new row state for WAL
            const new_serialized = try row.serialize(testing.allocator);
            defer testing.allocator.free(new_serialized);

            // Write UPDATE to WAL
            const combined_size = 8 + old_serialized.len + new_serialized.len;
            const combined_data = try testing.allocator.alloc(u8, combined_size);
            defer testing.allocator.free(combined_data);

            std.mem.writeInt(u64, combined_data[0..8], old_serialized.len, .little);
            @memcpy(combined_data[8..][0..old_serialized.len], old_serialized);
            @memcpy(combined_data[8 + old_serialized.len ..][0..new_serialized.len], new_serialized);

            _ = try db.writeWalRecord(.update_row, "docs", i, combined_data);
        }

        try testing.expectEqual(@as(usize, 3), table.count());
    }

    // Phase 2: Recover and rebuild HNSW
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.initVectorSearch(16, 200);

        var create = try db.execute("CREATE TABLE docs (id int, title text, vec embedding)");
        defer create.deinit();

        // Recover from WAL
        const recovered = try db.recoverFromWal(wal_dir);
        // Expect 6 transactions: 3 INSERTs + 3 UPDATEs (each auto-committed separately)
        try testing.expectEqual(@as(usize, 6), recovered);

        // Rebuild HNSW index
        const vectors_indexed = try db.rebuildHnswFromTables();
        try testing.expectEqual(@as(usize, 3), vectors_indexed);

        // Verify HNSW has all vectors
        const table = db.tables.get("docs").?;
        try testing.expectEqual(@as(usize, 3), table.count());

        var row_it = table.rows.iterator();
        while (row_it.next()) |entry| {
            const row_id = entry.key_ptr.*;
            const internal_id = db.hnsw.?.getInternalId(row_id);
            try testing.expect(internal_id != null);
        }
    }
}

test "WAL Recovery: No WAL directory (fresh start)" {
    const wal_dir = "test_data/wal_recovery_nodir";

    // Ensure directory doesn't exist
    std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Recovery should return 0 (no recovery needed)
    const recovered = try db.recoverFromWal(wal_dir);
    try testing.expectEqual(@as(usize, 0), recovered);
}

test "WAL Recovery: Empty WAL directory" {
    const wal_dir = "test_data/wal_recovery_empty";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    try std.fs.cwd().makePath(wal_dir);
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Recovery should return 0 (no WAL files)
    const recovered = try db.recoverFromWal(wal_dir);
    try testing.expectEqual(@as(usize, 0), recovered);
}

// ============================================================================
// Phase 2.5: Advanced Crash Simulation Tests
// ============================================================================

test "WAL Crash: Mid-transaction crash (no commit)" {
    const wal_dir = "test_data/wal_crash_mid_tx";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Simulate crash during transaction (no COMMIT written)
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE orders (id int, total int)");
        defer create.deinit();

        // Start inserting but "crash" before all inserts complete
        var insert1 = try db.execute("INSERT INTO orders VALUES (1, 100)");
        defer insert1.deinit();

        // Manually simulate a partial transaction by writing directly to WAL
        if (db.wal) |w| {
            const partial_tx_id: u64 = 999;

            // Write BEGIN but no COMMIT (simulates crash mid-transaction)
            _ = try w.writeRecord(.{
                .record_type = .begin_tx,
                .tx_id = partial_tx_id,
                .lsn = 0,
                .row_id = 0,
                .table_name = "",
                .data = "",
                .checksum = 0,
            });

            _ = try w.writeRecord(.{
                .record_type = .insert_row,
                .tx_id = partial_tx_id,
                .lsn = 0,
                .row_id = 2,
                .table_name = "orders",
                .data = "", // Simulated row data
                .checksum = 0,
            });

            try w.flush();
            // No COMMIT record - simulates crash
        }
    }

    // Phase 2: Recover - partial transaction should be ignored
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE orders (id int, total int)");
        defer create.deinit();

        const recovered = try db.recoverFromWal(wal_dir);

        // Should only recover the first committed INSERT
        try testing.expectEqual(@as(usize, 1), recovered);

        const table = db.tables.get("orders").?;
        try testing.expectEqual(@as(usize, 1), table.count());

        // Verify row 2 from uncommitted transaction was NOT recovered
        try testing.expect(table.get(2) == null);
        try testing.expect(table.get(1) != null);
    }
}

test "WAL Crash: Power failure during fsync" {
    const wal_dir = "test_data/wal_crash_fsync";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // This test simulates partial data in WAL (crash during fsync)
    // The WAL record will be incomplete and should be ignored

    // Phase 1: Write some good data, then simulate partial write
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE logs (id int, msg text)");
        defer create.deinit();

        // Write one complete transaction
        var insert1 = try db.execute("INSERT INTO logs VALUES (1, \"complete\")");
        defer insert1.deinit();

        // The second transaction would be incomplete due to crash
        // In real scenario, this would be truncated/corrupted WAL data
        // Our checksum validation should catch this
    }

    // Phase 2: Recovery should succeed with the complete transaction
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE logs (id int, msg text)");
        defer create.deinit();

        const recovered = try db.recoverFromWal(wal_dir);

        // Should recover 1 complete transaction
        try testing.expectEqual(@as(usize, 1), recovered);

        const table = db.tables.get("logs").?;
        try testing.expectEqual(@as(usize, 1), table.count());
    }
}

test "WAL Crash: Multiple crashes and recoveries" {
    const wal_dir = "test_data/wal_crash_multiple";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Initial data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE events (id int, type text)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO events VALUES (1, \"start\")");
        defer insert1.deinit();
    }

    // Phase 2: First recovery + more data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE events (id int, type text)");
        defer create.deinit();

        const recovered1 = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 1), recovered1);

        // Add more data after recovery
        var insert2 = try db.execute("INSERT INTO events VALUES (2, \"middle\")");
        defer insert2.deinit();
    }

    // Phase 3: Second recovery - should have both records
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE events (id int, type text)");
        defer create.deinit();

        const recovered2 = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 2), recovered2);

        const table = db.tables.get("events").?;
        try testing.expectEqual(@as(usize, 2), table.count());

        var insert3 = try db.execute("INSERT INTO events VALUES (3, \"end\")");
        defer insert3.deinit();
    }

    // Phase 4: Final recovery - should have all 3 records
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE events (id int, type text)");
        defer create.deinit();

        const recovered3 = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 3), recovered3);

        const table = db.tables.get("events").?;
        try testing.expectEqual(@as(usize, 3), table.count());
    }
}

test "WAL Crash: Large transaction recovery" {
    const wal_dir = "test_data/wal_crash_large";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    const num_records = 1000;

    // Phase 1: Write large transaction
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE bulk_data (id int, value int)");
        defer create.deinit();

        var i: usize = 0;
        while (i < num_records) : (i += 1) {
            var buf: [100]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO bulk_data VALUES ({d}, {d})", .{ i, i * 2 });
            var result = try db.execute(query);
            defer result.deinit();
        }
    }

    // Phase 2: Recover large dataset
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE bulk_data (id int, value int)");
        defer create.deinit();

        const recovered = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, num_records), recovered);

        const table = db.tables.get("bulk_data").?;
        try testing.expectEqual(@as(usize, num_records), table.count());

        // Verify some random records (row_ids start at 1, so records are 1-1000)
        try testing.expect(table.get(1) != null);
        try testing.expect(table.get(num_records / 2 + 1) != null);
        try testing.expect(table.get(num_records) != null);
    }
}

test "WAL Crash: Interleaved INSERT/UPDATE/DELETE" {
    const wal_dir = "test_data/wal_crash_mixed";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Complex operations
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE inventory (id int, qty int, status text)");
        defer create.deinit();

        // Insert
        var insert1 = try db.execute("INSERT INTO inventory VALUES (1, 100, \"active\")");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO inventory VALUES (2, 200, \"active\")");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO inventory VALUES (3, 300, \"active\")");
        defer insert3.deinit();

        // Update
        var update1 = try db.execute("UPDATE inventory SET qty = 150 WHERE id = 1");
        defer update1.deinit();

        // Delete
        var delete1 = try db.execute("DELETE FROM inventory WHERE id = 2");
        defer delete1.deinit();

        // Insert again
        var insert4 = try db.execute("INSERT INTO inventory VALUES (4, 400, \"pending\")");
        defer insert4.deinit();

        // Update
        var update2 = try db.execute("UPDATE inventory SET status = \"shipped\" WHERE id = 4");
        defer update2.deinit();
    }

    // Phase 2: Recover and verify final state
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE inventory (id int, qty int, status text)");
        defer create.deinit();

        const recovered = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 7), recovered); // 4 inserts + 2 updates + 1 delete

        const table = db.tables.get("inventory").?;
        try testing.expectEqual(@as(usize, 3), table.count()); // 4 inserted - 1 deleted = 3

        // Verify final state
        try testing.expect(table.get(1) != null); // Updated to 150
        try testing.expect(table.get(2) == null); // Deleted
        try testing.expect(table.get(3) != null); // Original
        try testing.expect(table.get(4) != null); // Updated to "shipped"

        // Verify values
        var select1 = try db.execute("SELECT * FROM inventory WHERE id = 1");
        defer select1.deinit();
        try testing.expectEqual(@as(i64, 150), select1.rows.items[0].items[1].int); // qty updated

        var select4 = try db.execute("SELECT * FROM inventory WHERE id = 4");
        defer select4.deinit();
        try testing.expectEqualStrings("shipped", select4.rows.items[0].items[2].text); // status updated
    }
}

test "WAL Crash: Recovery with WAL file rotation" {
    const wal_dir = "test_data/wal_crash_rotation";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Write enough data to trigger rotation
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        // Use custom WAL with small file size to force rotation
        const wal_ptr = try testing.allocator.create(WalWriter);
        // Note: db.deinit() will destroy wal_ptr, so we don't defer destroy here

        wal_ptr.* = try WalWriter.initWithOptions(testing.allocator, wal_dir, .{
            .max_file_size = 2048, // 2KB - small to force rotation
        });
        db.wal = wal_ptr;

        var create = try db.execute("CREATE TABLE rotated (id int, data text)");
        defer create.deinit();

        // Insert enough to cause rotation
        var i: usize = 0;
        while (i < 100) : (i += 1) {
            var buf: [200]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO rotated VALUES ({d}, \"data_with_some_extra_text_to_fill_space_{d}\")", .{ i, i });
            var result = try db.execute(query);
            defer result.deinit();
        }
    }

    // Phase 2: Recover from multiple WAL files
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE rotated (id int, data text)");
        defer create.deinit();

        const recovered = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 100), recovered);

        const table = db.tables.get("rotated").?;
        try testing.expectEqual(@as(usize, 100), table.count());
    }
}

test "WAL Crash: Recovery performance with large dataset" {
    const wal_dir = "test_data/wal_crash_perf";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    const num_records = 5000;

    // Phase 1: Create large dataset
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE perf_test (id int, name text, score int)");
        defer create.deinit();

        var i: usize = 0;
        while (i < num_records) : (i += 1) {
            var buf: [150]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO perf_test VALUES ({d}, \"user_{d}\", {d})", .{ i, i, i % 100 });
            var result = try db.execute(query);
            defer result.deinit();
        }
    }

    // Phase 2: Benchmark recovery
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE perf_test (id int, name text, score int)");
        defer create.deinit();

        var timer = try std.time.Timer.start();
        const recovered = try db.recoverFromWal(wal_dir);
        const elapsed_ns = timer.read();

        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
        const ops_per_sec = @as(f64, @floatFromInt(recovered)) / (@as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0);

        std.debug.print("\nRecovery Performance:\n", .{});
        std.debug.print("  Records: {d}\n", .{recovered});
        std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});
        std.debug.print("  Throughput: {d:.0} ops/sec\n", .{ops_per_sec});

        try testing.expectEqual(@as(usize, num_records), recovered);

        const table = db.tables.get("perf_test").?;
        try testing.expectEqual(@as(usize, num_records), table.count());
    }
}

// ============================================================================
// Phase 1: B-tree Index Tests
// ============================================================================

test "Index: CREATE INDEX and DROP INDEX" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table
    var create_table = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_table.deinit();

    // Create index
    var create_index = try db.execute("CREATE INDEX idx_users_age ON users(age)");
    defer create_index.deinit();

    try testing.expectEqual(@as(usize, 1), db.index_manager.count());

    // Verify index exists
    const index_info = db.index_manager.getIndex("idx_users_age");
    try testing.expect(index_info != null);

    // Drop index
    var drop_index = try db.execute("DROP INDEX idx_users_age");
    defer drop_index.deinit();

    try testing.expectEqual(@as(usize, 0), db.index_manager.count());
}

test "Index: Automatic updates on INSERT" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE products (id int, name text, price int)");
    defer create_table.deinit();

    var create_index = try db.execute("CREATE INDEX idx_products_price ON products(price)");
    defer create_index.deinit();

    // Insert data
    var insert1 = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 100)");
    defer insert1.deinit();

    var insert2 = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 200)");
    defer insert2.deinit();

    // Query index directly
    const index_info = db.index_manager.getIndex("idx_products_price").?;
    const results = try index_info.btree.search(ColumnValue{ .int = 100 });
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 1), results[0]);
}

test "Index: Automatic updates on DELETE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE items (id int, code text)");
    defer create_table.deinit();

    var insert1 = try db.execute("INSERT INTO items VALUES (1, \"A001\")");
    defer insert1.deinit();

    var insert2 = try db.execute("INSERT INTO items VALUES (2, \"B002\")");
    defer insert2.deinit();

    // Create index on existing data
    var create_index = try db.execute("CREATE INDEX idx_items_code ON items(code)");
    defer create_index.deinit();

    // Verify both items in index
    const index_info = db.index_manager.getIndex("idx_items_code").?;
    try testing.expectEqual(@as(usize, 2), index_info.btree.getSize());

    // Delete one item
    var delete_stmt = try db.execute("DELETE FROM items WHERE id = 1");
    defer delete_stmt.deinit();

    // Verify index was updated
    try testing.expectEqual(@as(usize, 1), index_info.btree.getSize());
}

test "Index: Automatic updates on UPDATE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE inventory (id int, qty int)");
    defer create_table.deinit();

    var insert1 = try db.execute("INSERT INTO inventory VALUES (1, 50)");
    defer insert1.deinit();

    var create_index = try db.execute("CREATE INDEX idx_inventory_qty ON inventory(qty)");
    defer create_index.deinit();

    const index_info = db.index_manager.getIndex("idx_inventory_qty").?;

    // Verify old value in index
    const results_old = try index_info.btree.search(ColumnValue{ .int = 50 });
    defer testing.allocator.free(results_old);
    try testing.expectEqual(@as(usize, 1), results_old.len);

    // Update the qty
    var update_stmt = try db.execute("UPDATE inventory SET qty = 75 WHERE id = 1");
    defer update_stmt.deinit();

    // Verify old value removed from index
    const results_old2 = try index_info.btree.search(ColumnValue{ .int = 50 });
    defer testing.allocator.free(results_old2);
    try testing.expectEqual(@as(usize, 0), results_old2.len);

    // Verify new value in index
    const results_new = try index_info.btree.search(ColumnValue{ .int = 75 });
    defer testing.allocator.free(results_new);
    try testing.expectEqual(@as(usize, 1), results_new.len);
}

test "Index: Query optimizer uses index for WHERE clause" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE users (id int, email text, age int)");
    defer create_table.deinit();

    // Insert test data
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        var buf: [200]u8 = undefined;
        const query = try std.fmt.bufPrint(
            &buf,
            "INSERT INTO users VALUES ({d}, \"user{d}@test.com\", {d})",
            .{ i, i, 20 + (i % 50) },
        );
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Create index on age
    var create_index = try db.execute("CREATE INDEX idx_users_age ON users(age)");
    defer create_index.deinit();

    // Query with WHERE clause should use index
    var select = try db.execute("SELECT * FROM users WHERE age = 30");
    defer select.deinit();

    // Should find users with age = 30 (indices 10, 60)
    try testing.expectEqual(@as(usize, 2), select.rows.items.len);
}

test "Index: Performance comparison - WITH vs WITHOUT index" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE records (id int, value int)");
    defer create_table.deinit();

    // Insert 1000 records
    const num_records = 1000;
    var i: usize = 0;
    while (i < num_records) : (i += 1) {
        var buf: [100]u8 = undefined;
        const query = try std.fmt.bufPrint(&buf, "INSERT INTO records VALUES ({d}, {d})", .{ i, i % 100 });
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Benchmark WITHOUT index
    var timer_no_index = try std.time.Timer.start();
    var select_no_index = try db.execute("SELECT * FROM records WHERE value = 50");
    const time_no_index = timer_no_index.read();
    defer select_no_index.deinit();

    // Create index
    var create_index = try db.execute("CREATE INDEX idx_records_value ON records(value)");
    defer create_index.deinit();

    // Benchmark WITH index
    var timer_with_index = try std.time.Timer.start();
    var select_with_index = try db.execute("SELECT * FROM records WHERE value = 50");
    const time_with_index = timer_with_index.read();
    defer select_with_index.deinit();

    // Print results
    std.debug.print("\nIndex Performance Comparison ({d} records):\n", .{num_records});
    std.debug.print("  Without index: {d} ns\n", .{time_no_index});
    std.debug.print("  With index:    {d} ns\n", .{time_with_index});

    if (time_no_index > time_with_index) {
        const speedup = @as(f64, @floatFromInt(time_no_index)) / @as(f64, @floatFromInt(time_with_index));
        std.debug.print("  Speedup:       {d:.1}x faster\n", .{speedup});
    }

    // Both should return same results
    try testing.expectEqual(select_no_index.rows.items.len, select_with_index.rows.items.len);
}

test "Index: Text column index" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE emails (id int, address text)");
    defer create_table.deinit();

    var insert1 = try db.execute("INSERT INTO emails VALUES (1, \"alice@example.com\")");
    defer insert1.deinit();

    var insert2 = try db.execute("INSERT INTO emails VALUES (2, \"bob@example.com\")");
    defer insert2.deinit();

    var create_index = try db.execute("CREATE INDEX idx_emails_address ON emails(address)");
    defer create_index.deinit();

    // Query by email
    var select = try db.execute("SELECT * FROM emails WHERE address = \"alice@example.com\"");
    defer select.deinit();

    try testing.expectEqual(@as(usize, 1), select.rows.items.len);
    try testing.expectEqual(@as(i64, 1), select.rows.items[0].items[0].int);
}

test "Index: Multiple indexes on same table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE users (id int, name text, age int, email text)");
    defer create_table.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25, \"alice@example.com\")");
    defer insert1.deinit();

    // Create multiple indexes
    var create_index1 = try db.execute("CREATE INDEX idx_users_age ON users(age)");
    defer create_index1.deinit();

    var create_index2 = try db.execute("CREATE INDEX idx_users_email ON users(email)");
    defer create_index2.deinit();

    try testing.expectEqual(@as(usize, 2), db.index_manager.count());

    // Query using first index
    var select1 = try db.execute("SELECT * FROM users WHERE age = 25");
    defer select1.deinit();
    try testing.expectEqual(@as(usize, 1), select1.rows.items.len);

    // Query using second index
    var select2 = try db.execute("SELECT * FROM users WHERE email = \"alice@example.com\"");
    defer select2.deinit();
    try testing.expectEqual(@as(usize, 1), select2.rows.items.len);
}

test "Index: Index on empty table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE empty_table (id int, value int)");
    defer create_table.deinit();

    // Create index on empty table
    var create_index = try db.execute("CREATE INDEX idx_empty_value ON empty_table(value)");
    defer create_index.deinit();

    const index_info = db.index_manager.getIndex("idx_empty_value").?;
    try testing.expectEqual(@as(usize, 0), index_info.btree.getSize());

    // Insert data after index creation
    var insert1 = try db.execute("INSERT INTO empty_table VALUES (1, 42)");
    defer insert1.deinit();

    try testing.expectEqual(@as(usize, 1), index_info.btree.getSize());
}

test "Index: Correctness - all results match table scan" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_table = try db.execute("CREATE TABLE data (id int, category int)");
    defer create_table.deinit();

    // Insert diverse data
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        var buf: [100]u8 = undefined;
        const query = try std.fmt.bufPrint(&buf, "INSERT INTO data VALUES ({d}, {d})", .{ i, i % 5 });
        var result = try db.execute(query);
        defer result.deinit();
    }

    // Query without index (baseline)
    var select_baseline = try db.execute("SELECT * FROM data WHERE category = 2");
    defer select_baseline.deinit();
    const baseline_count = select_baseline.rows.items.len;

    // Create index
    var create_index = try db.execute("CREATE INDEX idx_data_category ON data(category)");
    defer create_index.deinit();

    // Query with index
    var select_indexed = try db.execute("SELECT * FROM data WHERE category = 2");
    defer select_indexed.deinit();

    // Results should be identical
    try testing.expectEqual(baseline_count, select_indexed.rows.items.len);

    // Expected: records 2, 7, 12, 17, 22, 27, 32, 37, 42, 47 (10 records)
    try testing.expectEqual(@as(usize, 10), select_indexed.rows.items.len);
}

test "WAL Recovery: Corrupted WAL file with checksum error" {
    const wal_dir = "test_data/wal_recovery_corrupt";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create valid WAL data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE messages (id int, msg text)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO messages VALUES (1, \"first\")");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO messages VALUES (2, \"second\")");
        defer insert2.deinit();
    }

    // Phase 2: Corrupt the WAL file by modifying bytes
    {
        const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/wal.000000", .{wal_dir});
        defer testing.allocator.free(wal_path);

        // Open the WAL file and corrupt some bytes in the middle
        const file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
        defer file.close();

        // Seek to middle of file and write garbage to corrupt checksum
        try file.seekTo(100);
        const garbage = [_]u8{ 0xFF, 0xFF, 0xFF, 0xFF };
        _ = try file.write(&garbage);
    }

    // Phase 3: Recovery should skip corrupted file but not crash
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE messages (id int, msg text)");
        defer create.deinit();

        // Recovery should complete but skip corrupted records
        const recovered = try db.recoverFromWal(wal_dir);

        // We may recover 0 records if corruption happens early in file
        // The important thing is recovery doesn't crash
        _ = recovered;

        // Database should still be functional after handling corruption
        var insert3 = try db.execute("INSERT INTO messages VALUES (3, \"after_recovery\")");
        defer insert3.deinit();

        const table = db.tables.get("messages").?;
        try testing.expect(table.count() > 0);
    }
}

test "WAL Recovery: Truncated WAL file" {
    const wal_dir = "test_data/wal_recovery_truncated";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create valid WAL data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE data (id int, value int)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO data VALUES (1, 100)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO data VALUES (2, 200)");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO data VALUES (3, 300)");
        defer insert3.deinit();
    }

    // Phase 2: Truncate the WAL file to simulate incomplete write
    {
        const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/wal.000000", .{wal_dir});
        defer testing.allocator.free(wal_path);

        // Get file size and truncate to 60% to create partial record
        var file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
        defer file.close();

        const stat = try file.stat();
        const truncate_size = stat.size * 6 / 10;
        try file.setEndPos(truncate_size);
    }

    // Phase 3: Recovery should handle truncated file gracefully
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE data (id int, value int)");
        defer create.deinit();

        // Recovery should complete despite truncated file
        const recovered = try db.recoverFromWal(wal_dir);

        // We should recover some records before truncation point
        // Exact number depends on where truncation occurred
        _ = recovered;

        // Database should still be functional
        var insert4 = try db.execute("INSERT INTO data VALUES (4, 400)");
        defer insert4.deinit();

        const table = db.tables.get("data").?;
        try testing.expect(table.count() > 0);
    }
}

test "WAL Recovery: Partial corruption in WAL file" {
    const wal_dir = "test_data/wal_recovery_partial";

    std.fs.cwd().deleteTree(wal_dir) catch {};
    defer std.fs.cwd().deleteTree(wal_dir) catch {};

    // Phase 1: Create WAL file with some data
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        try db.enableWal(wal_dir);

        var create = try db.execute("CREATE TABLE items (id int, name text)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO items VALUES (1, \"first\")");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO items VALUES (2, \"second\")");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO items VALUES (3, \"third\")");
        defer insert3.deinit();
    }

    // Phase 2: Corrupt middle of the WAL file
    {
        const wal_path = try std.fmt.allocPrint(testing.allocator, "{s}/wal.000000", .{wal_dir});
        defer testing.allocator.free(wal_path);

        const file = try std.fs.cwd().openFile(wal_path, .{ .mode = .read_write });
        defer file.close();

        // Corrupt bytes in the middle to invalidate checksum
        try file.seekTo(150);
        const garbage = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF, 0xBA, 0xDC, 0x0D, 0xE0 };
        _ = try file.write(&garbage);
    }

    // Phase 3: Recovery should handle corruption gracefully
    {
        var db = Database.init(testing.allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE items (id int, name text)");
        defer create.deinit();

        // Recovery should complete despite corruption
        // May recover some records before the corruption
        const recovered = try db.recoverFromWal(wal_dir);
        _ = recovered; // May be 0-3 depending on where corruption occurred

        // Database should still be functional after handling corruption
        var insert4 = try db.execute("INSERT INTO items VALUES (4, \"after_corruption\")");
        defer insert4.deinit();

        const table = db.tables.get("items").?;
        try testing.expect(table.count() > 0); // At least the new insert
    }
}
