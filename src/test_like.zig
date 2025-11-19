const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "LIKE: exact match" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\")");
    defer insert2.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"Alice\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
    try testing.expectEqualStrings("Alice", result.rows.items[0].items[1].text);
}

test "LIKE: % at end matches prefix" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Alex\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Alexander\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"Al%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // Alice, Alex, Alexander
}

test "LIKE: % at start matches suffix" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Johnson\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Person\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Anderson\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Smith\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"%son\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // Johnson, Person, Anderson
}

test "LIKE: % in middle matches substring" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"John\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Jason\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Justin\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Ryan\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"J%n\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // John, Jason, Justin
}

test "LIKE: _ matches single character" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"John\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Joan\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Jean\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Johnson\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"Jo_n\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2); // John, Joan (not Jean, not Johnson)
}

test "LIKE: combined wildcards _%n" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Jon\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"John\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Jason\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO users VALUES (4, \"Justin\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"J_%n\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // John, Jason, Justin (not Jon - too short)
}

test "LIKE: multiple % wildcards" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE products (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO products VALUES (1, \"Apple iPhone 14\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO products VALUES (2, \"Apple iPad Pro\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO products VALUES (3, \"Samsung Galaxy\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO products VALUES (4, \"Apple Watch\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM products WHERE name LIKE \"%Apple%P%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2); // iPhone, iPad
}

test "LIKE: empty pattern matches empty string only" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE items (id int, value text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO items VALUES (1, \"\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO items VALUES (2, \"something\")");
    defer insert2.deinit();

    var result = try db.execute("SELECT * FROM items WHERE value LIKE \"\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1);
    try testing.expect(result.rows.items[0].items[0].int == 1);
}

test "LIKE: % alone matches everything" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE items (id int, value text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO items VALUES (1, \"\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO items VALUES (2, \"a\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO items VALUES (3, \"anything\")");
    defer insert3.deinit();

    var result = try db.execute("SELECT * FROM items WHERE value LIKE \"%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // Matches all strings
}

test "LIKE: no match returns empty result" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\")");
    defer insert2.deinit();

    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"Z%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 0);
}

test "LIKE: chat app message search use case" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Create messages table
    var create_result = try db.execute("CREATE TABLE messages (id int, user_id int, content text, timestamp int)");
    defer create_result.deinit();

    // Insert sample messages
    var insert1 = try db.execute("INSERT INTO messages VALUES (1, 101, \"Hello everyone!\", 1000)");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO messages VALUES (2, 102, \"This is important information\", 1001)");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO messages VALUES (3, 101, \"Please review the important docs\", 1002)");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO messages VALUES (4, 103, \"Just saying hi\", 1003)");
    defer insert4.deinit();
    var insert5 = try db.execute("INSERT INTO messages VALUES (5, 102, \"Meeting at 3pm\", 1004)");
    defer insert5.deinit();

    // Search for messages containing "important"
    var result = try db.execute("SELECT * FROM messages WHERE content LIKE \"%important%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
    try testing.expect(result.rows.items[0].items[0].int == 2);
    try testing.expect(result.rows.items[1].items[0].int == 3);
}

test "LIKE: case-sensitive matching" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE users (id int, name text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO users VALUES (2, \"alice\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO users VALUES (3, \"ALICE\")");
    defer insert3.deinit();

    // Case-sensitive search (lowercase 'a')
    var result = try db.execute("SELECT * FROM users WHERE name LIKE \"a%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 1); // Only "alice" matches
    try testing.expectEqualStrings("alice", result.rows.items[0].items[1].text);
}

test "LIKE: multiple underscore wildcards" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE codes (id int, value text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO codes VALUES (1, \"AB123\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO codes VALUES (2, \"AC456\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO codes VALUES (3, \"AD789\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO codes VALUES (4, \"XY999\")");
    defer insert4.deinit();

    var result = try db.execute("SELECT * FROM codes WHERE value LIKE \"A__%\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 3); // AB123, AC456, AD789 (starts with A + at least 2 more chars)
}

test "LIKE: complex pattern matching" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    var create_result = try db.execute("CREATE TABLE emails (id int, address text)");
    defer create_result.deinit();

    var insert1 = try db.execute("INSERT INTO emails VALUES (1, \"user@example.com\")");
    defer insert1.deinit();
    var insert2 = try db.execute("INSERT INTO emails VALUES (2, \"admin@example.com\")");
    defer insert2.deinit();
    var insert3 = try db.execute("INSERT INTO emails VALUES (3, \"user@test.org\")");
    defer insert3.deinit();
    var insert4 = try db.execute("INSERT INTO emails VALUES (4, \"info@example.net\")");
    defer insert4.deinit();

    // Find all example.com emails
    var result = try db.execute("SELECT * FROM emails WHERE address LIKE \"%@example.com\"");
    defer result.deinit();

    try testing.expect(result.rows.items.len == 2);
}
