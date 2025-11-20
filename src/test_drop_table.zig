const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "DROP TABLE: basic table drop" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create a table
    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();
    try testing.expect(db.tables.count() == 1);

    // Insert some data
    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert_result.deinit();

    // Drop the table
    var drop_result = try db.execute("DROP TABLE users");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);
    try testing.expectEqualStrings("Table 'users' dropped", drop_result.rows.items[0].items[0].text);
}

test "DROP TABLE: IF EXISTS when table exists" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create a table
    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();
    try testing.expect(db.tables.count() == 1);

    // Drop with IF EXISTS
    var drop_result = try db.execute("DROP TABLE IF EXISTS users");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);
    try testing.expectEqualStrings("Table 'users' dropped", drop_result.rows.items[0].items[0].text);
}

test "DROP TABLE: IF EXISTS when table doesn't exist" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Drop non-existent table with IF EXISTS (should not error)
    var drop_result = try db.execute("DROP TABLE IF EXISTS nonexistent");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);

    // NOTE: Current implementation returns "Table 'X' dropped" even when table
    // doesn't exist with IF EXISTS. This is arguably misleading - ideally it should
    // return "Table 'X' does not exist (skipped)" or similar.
    // See: src/database/executor/command_executor.zig:320-336
    // TODO: Consider improving the message to distinguish between "actually dropped"
    // vs "didn't exist but IF EXISTS was used"
    try testing.expectEqualStrings("Table 'nonexistent' dropped", drop_result.rows.items[0].items[0].text);
}

test "DROP TABLE: without IF EXISTS on missing table (error)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Drop non-existent table without IF EXISTS (should error)
    const result = db.execute("DROP TABLE nonexistent");
    try testing.expectError(error.TableNotFound, result);
}

test "DROP TABLE: drops associated B-tree indexes" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table with indexes
    var create_result = try db.execute("CREATE TABLE users (id int, name text, email text)");
    defer create_result.deinit();

    var insert_result = try db.execute("INSERT INTO users VALUES (1, \"Alice\", \"alice@example.com\")");
    defer insert_result.deinit();

    // Create indexes
    var idx1_result = try db.execute("CREATE INDEX idx_id ON users (id)");
    defer idx1_result.deinit();
    var idx2_result = try db.execute("CREATE INDEX idx_name ON users (name)");
    defer idx2_result.deinit();

    try testing.expect(db.index_manager.count() == 2);

    // Drop table
    var drop_result = try db.execute("DROP TABLE users");
    defer drop_result.deinit();

    // Verify table is gone
    try testing.expect(db.tables.count() == 0);

    // Verify indexes are gone
    try testing.expect(db.index_manager.count() == 0);
}

test "DROP TABLE: table with data" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table
    var create_result = try db.execute("CREATE TABLE products (id int, name text, price float)");
    defer create_result.deinit();

    // Insert multiple rows
    var insert1 = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 29.99)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO products VALUES (3, \"Doohickey\", 39.99)");
    defer insert3.deinit();

    // Verify data exists
    var select_result = try db.execute("SELECT * FROM products");
    defer select_result.deinit();
    try testing.expect(select_result.rows.items.len == 3);

    // Drop table
    var drop_result = try db.execute("DROP TABLE products");
    defer drop_result.deinit();

    // Verify table is gone
    try testing.expect(db.tables.count() == 0);
}

test "DROP TABLE: multiple tables, drop one" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create multiple tables
    var create1 = try db.execute("CREATE TABLE users (id int, name text)");
    defer create1.deinit();
    var create2 = try db.execute("CREATE TABLE products (id int, name text)");
    defer create2.deinit();
    var create3 = try db.execute("CREATE TABLE orders (id int, user_id int)");
    defer create3.deinit();

    try testing.expect(db.tables.count() == 3);

    // Drop one table
    var drop_result = try db.execute("DROP TABLE products");
    defer drop_result.deinit();

    // Verify only one is gone
    try testing.expect(db.tables.count() == 2);
    try testing.expect(db.tables.get("users") != null);
    try testing.expect(db.tables.get("products") == null);
    try testing.expect(db.tables.get("orders") != null);
}

test "DROP TABLE: then recreate with same name" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table
    var create1 = try db.execute("CREATE TABLE temp (id int, value text)");
    defer create1.deinit();

    var insert1 = try db.execute("INSERT INTO temp VALUES (1, \"old\")");
    defer insert1.deinit();

    // Drop table
    var drop_result = try db.execute("DROP TABLE temp");
    defer drop_result.deinit();

    // Recreate with different schema
    var create2 = try db.execute("CREATE TABLE temp (id int, name text, age int)");
    defer create2.deinit();

    var insert2 = try db.execute("INSERT INTO temp VALUES (1, \"new\", 25)");
    defer insert2.deinit();

    // Verify new table has new schema
    var select_result = try db.execute("SELECT * FROM temp");
    defer select_result.deinit();

    try testing.expect(select_result.columns.items.len == 3); // id, name, age
    try testing.expect(select_result.rows.items.len == 1);
    try testing.expectEqualStrings("new", select_result.rows.items[0].items[1].text);
}

test "DROP TABLE: empty table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create empty table
    var create_result = try db.execute("CREATE TABLE empty_table (id int, name text)");
    defer create_result.deinit();

    // Verify it's empty
    var select_result = try db.execute("SELECT * FROM empty_table");
    defer select_result.deinit();
    try testing.expect(select_result.rows.items.len == 0);

    // Drop it
    var drop_result = try db.execute("DROP TABLE empty_table");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);
}

test "DROP TABLE: chat app messages table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create messages table (chat app use case)
    var create_result = try db.execute("CREATE TABLE messages (id int, user_id int, content text, timestamp int)");
    defer create_result.deinit();

    // Add some messages
    var insert1 = try db.execute("INSERT INTO messages VALUES (1, 101, \"Hello\", 1000)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO messages VALUES (2, 102, \"Hi there\", 1001)");
    defer insert2.deinit();

    // Create index on user_id
    var idx_result = try db.execute("CREATE INDEX idx_user ON messages (user_id)");
    defer idx_result.deinit();

    try testing.expect(db.tables.count() == 1);
    try testing.expect(db.index_manager.count() == 1);

    // Drop messages table
    var drop_result = try db.execute("DROP TABLE messages");
    defer drop_result.deinit();

    // Verify everything is cleaned up
    try testing.expect(db.tables.count() == 0);
    try testing.expect(db.index_manager.count() == 0);
}

test "DROP TABLE: case insensitive" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table
    var create_result = try db.execute("CREATE TABLE MyTable (id int, name text)");
    defer create_result.deinit();

    // Drop with different case
    var drop_result = try db.execute("drop table MyTable");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);
}

test "DROP TABLE: with ALTER TABLE history" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create table
    var create_result = try db.execute("CREATE TABLE evolving (id int, name text)");
    defer create_result.deinit();

    // Alter it
    var alter_result = try db.execute("ALTER TABLE evolving ADD COLUMN age int");
    defer alter_result.deinit();

    var insert_result = try db.execute("INSERT INTO evolving VALUES (1, \"Alice\", 25)");
    defer insert_result.deinit();

    // Drop it
    var drop_result = try db.execute("DROP TABLE evolving");
    defer drop_result.deinit();

    try testing.expect(db.tables.count() == 0);
}
