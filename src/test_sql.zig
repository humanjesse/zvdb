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
