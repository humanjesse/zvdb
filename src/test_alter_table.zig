const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "ALTER TABLE: ADD COLUMN to empty table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var alter_result = try db.execute("ALTER TABLE users ADD COLUMN age int");
    defer alter_result.deinit();

    // Verify the column was added
    const table = db.tables.get("users").?;
    try testing.expectEqual(@as(usize, 3), table.columns.items.len);
    try testing.expectEqualStrings("age", table.columns.items[2].name);
}

test "ALTER TABLE: ADD COLUMN to table with existing data" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE messages (id int, content text)");
    defer create_result.deinit();

    // Insert some data
    var insert1 = try db.execute("INSERT INTO messages VALUES (1, \"Hello\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO messages VALUES (2, \"World\")");
    defer insert2.deinit();

    // Add new column
    var alter_result = try db.execute("ALTER TABLE messages ADD COLUMN attachments text");
    defer alter_result.deinit();

    // Verify existing rows have NULL for the new column
    var select_result = try db.execute("SELECT * FROM messages");
    defer select_result.deinit();

    try testing.expectEqual(@as(usize, 2), select_result.rows.items.len);
    try testing.expectEqual(@as(usize, 3), select_result.columns.items.len);

    // First row should have NULL for attachments
    const first_row = select_result.rows.items[0];
    try testing.expectEqual(ColumnValue.null_value, first_row.items[2]);
}

test "ALTER TABLE: ADD COLUMN then INSERT with new column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE messages (id int, content text)");
    defer create_result.deinit();

    // Add edit_count column (chat app use case!)
    var alter_result = try db.execute("ALTER TABLE messages ADD COLUMN edit_count int");
    defer alter_result.deinit();

    // Insert a row with the new column
    var insert_result = try db.execute("INSERT INTO messages VALUES (1, \"Hello\", 0)");
    defer insert_result.deinit();

    var select_result = try db.execute("SELECT * FROM messages");
    defer select_result.deinit();

    try testing.expectEqual(@as(usize, 1), select_result.rows.items.len);
    const row = select_result.rows.items[0];
    try testing.expectEqual(@as(i64, 0), row.items[2].int);
}

test "ALTER TABLE: ADD COLUMN with embedding type" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE docs (id int, title text)");
    defer create_result.deinit();

    // Add embedding column
    var alter_result = try db.execute("ALTER TABLE docs ADD COLUMN embedding embedding(384)");
    defer alter_result.deinit();

    const table = db.tables.get("docs").?;
    try testing.expectEqual(@as(usize, 3), table.columns.items.len);
    try testing.expectEqual(@as(?usize, 384), table.columns.items[2].embedding_dim);
}

test "ALTER TABLE: ADD COLUMN fails for duplicate column name" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    // Try to add a column that already exists
    const result = db.execute("ALTER TABLE users ADD COLUMN name text");
    try testing.expectError(error.InvalidSyntax, result);
}

test "ALTER TABLE: ADD COLUMN allows duplicate embedding dimension" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE docs (id int, vec1 embedding(128))");
    defer create_result.deinit();

    // Add another embedding column with same dimension - now allowed!
    var result = try db.execute("ALTER TABLE docs ADD COLUMN vec2 embedding(128)");
    defer result.deinit();

    // Verify the column was added
    const table = db.tables.get("docs").?;
    const has_vec2 = for (table.columns.items) |col| {
        if (std.mem.eql(u8, col.name, "vec2")) break true;
    } else false;
    try testing.expect(has_vec2);
}

test "ALTER TABLE: DROP COLUMN" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var alter_result = try db.execute("ALTER TABLE users DROP COLUMN age");
    defer alter_result.deinit();

    const table = db.tables.get("users").?;
    try testing.expectEqual(@as(usize, 2), table.columns.items.len);
    try testing.expectEqualStrings("id", table.columns.items[0].name);
    try testing.expectEqualStrings("name", table.columns.items[1].name);
}

test "ALTER TABLE: DROP COLUMN with existing data" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 25)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 30)");
    defer insert2.deinit();

    // Drop the age column
    var alter_result = try db.execute("ALTER TABLE users DROP COLUMN age");
    defer alter_result.deinit();

    // Verify data is still accessible
    var select_result = try db.execute("SELECT * FROM users");
    defer select_result.deinit();

    try testing.expectEqual(@as(usize, 2), select_result.rows.items.len);
    try testing.expectEqual(@as(usize, 2), select_result.columns.items.len);
}

test "ALTER TABLE: DROP COLUMN fails for non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    const result = db.execute("ALTER TABLE users DROP COLUMN age");
    try testing.expectError(error.ColumnNotFound, result);
}

test "ALTER TABLE: RENAME COLUMN" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, username text)");
    defer create_result.deinit();

    var alter_result = try db.execute("ALTER TABLE users RENAME COLUMN username TO name");
    defer alter_result.deinit();

    const table = db.tables.get("users").?;
    try testing.expectEqual(@as(usize, 2), table.columns.items.len);
    try testing.expectEqualStrings("name", table.columns.items[1].name);
}

test "ALTER TABLE: RENAME COLUMN with existing data" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, old_name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\")");
    defer insert2.deinit();

    // Rename the column
    var alter_result = try db.execute("ALTER TABLE users RENAME COLUMN old_name TO new_name");
    defer alter_result.deinit();

    // Verify we can query with new column name
    var select_result = try db.execute("SELECT new_name FROM users");
    defer select_result.deinit();

    try testing.expectEqual(@as(usize, 2), select_result.rows.items.len);
    try testing.expectEqualStrings("new_name", select_result.columns.items[0]);
}

test "ALTER TABLE: RENAME COLUMN fails for non-existent column" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    const result = db.execute("ALTER TABLE users RENAME COLUMN age TO years");
    try testing.expectError(error.ColumnNotFound, result);
}

test "ALTER TABLE: RENAME COLUMN fails when new name conflicts" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    const result = db.execute("ALTER TABLE users RENAME COLUMN name TO id");
    try testing.expectError(error.InvalidSyntax, result);
}

test "ALTER TABLE: Chat app schema evolution use case" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initial schema
    var create_result = try db.execute("CREATE TABLE messages (id int, user_id int, content text, created_at int)");
    defer create_result.deinit();

    // Insert initial message
    var insert1 = try db.execute("INSERT INTO messages VALUES (1, 100, \"Hello world\", 1234567890)");
    defer insert1.deinit();

    // Evolve schema: Add attachments column (requested feature!)
    var alter1 = try db.execute("ALTER TABLE messages ADD COLUMN attachments text");
    defer alter1.deinit();

    // Evolve schema: Add edit_count column (requested feature!)
    var alter2 = try db.execute("ALTER TABLE messages ADD COLUMN edit_count int");
    defer alter2.deinit();

    // Insert new message with all columns
    var insert2 = try db.execute("INSERT INTO messages VALUES (2, 101, \"Check this out\", 1234567900, \"file.pdf\", 0)");
    defer insert2.deinit();

    // Verify both old and new messages are accessible
    var select_result = try db.execute("SELECT * FROM messages");
    defer select_result.deinit();

    try testing.expectEqual(@as(usize, 2), select_result.rows.items.len);
    try testing.expectEqual(@as(usize, 6), select_result.columns.items.len);

    // First row (old data) should have NULLs for new columns
    const old_row = select_result.rows.items[0];
    try testing.expectEqual(ColumnValue.null_value, old_row.items[4]); // attachments
    try testing.expectEqual(ColumnValue.null_value, old_row.items[5]); // edit_count

    // Second row (new data) should have values
    const new_row = select_result.rows.items[1];
    try testing.expectEqualStrings("file.pdf", new_row.items[4].text);
    try testing.expectEqual(@as(i64, 0), new_row.items[5].int);
}

test "ALTER TABLE: fails on non-existent table" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    const result = db.execute("ALTER TABLE nonexistent ADD COLUMN col int");
    try testing.expectError(error.TableNotFound, result);
}

test "ALTER TABLE: Multiple operations in sequence" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE test (id int, col1 text)");
    defer create_result.deinit();

    // Add a column
    var alter1 = try db.execute("ALTER TABLE test ADD COLUMN col2 int");
    defer alter1.deinit();

    // Rename a column
    var alter2 = try db.execute("ALTER TABLE test RENAME COLUMN col1 TO name");
    defer alter2.deinit();

    // Add another column
    var alter3 = try db.execute("ALTER TABLE test ADD COLUMN col3 float");
    defer alter3.deinit();

    // Drop a column
    var alter4 = try db.execute("ALTER TABLE test DROP COLUMN col2");
    defer alter4.deinit();

    const table = db.tables.get("test").?;
    try testing.expectEqual(@as(usize, 3), table.columns.items.len);
    try testing.expectEqualStrings("id", table.columns.items[0].name);
    try testing.expectEqualStrings("name", table.columns.items[1].name);
    try testing.expectEqualStrings("col3", table.columns.items[2].name);
}
