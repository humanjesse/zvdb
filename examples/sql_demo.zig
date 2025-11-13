const std = @import("std");
const zvdb = @import("zvdb");
const Database = zvdb.Database;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const stdout = std.io.getStdOut().writer();

    try stdout.writeAll("\n=== Welcome to ZVDB SQL Demo! ===\n");
    try stdout.writeAll("A SQL parody database with semantic search superpowers!\n\n");

    // Create database
    var db = Database.init(allocator);
    defer db.deinit();

    // Initialize vector search for semantic queries
    try db.initVectorSearch(16, 200);

    try stdout.writeAll("--- Creating a 'users' table ---\n");
    var result1 = try db.execute("CREATE TABLE users (id int, name text, bio text)");
    defer result1.deinit();
    try result1.print();

    try stdout.writeAll("--- Inserting some users ---\n");
    _ = try db.execute("INSERT INTO users VALUES (1, \"Alice\", \"Loves Zig programming and databases\")");
    _ = try db.execute("INSERT INTO users VALUES (2, \"Bob\", \"Backend developer interested in vector search\")");
    _ = try db.execute("INSERT INTO users VALUES (3, \"Charlie\", \"Machine learning engineer and data scientist\")");
    _ = try db.execute("INSERT INTO users VALUES (4, \"Diana\", \"Frontend developer who enjoys building UIs\")");

    try stdout.writeAll("\n--- SELECT * FROM users ---\n");
    var result2 = try db.execute("SELECT * FROM users");
    defer result2.deinit();
    try result2.print();

    try stdout.writeAll("--- SELECT with WHERE clause ---\n");
    var result3 = try db.execute("SELECT name, bio FROM users WHERE id = 2");
    defer result3.deinit();
    try result3.print();

    try stdout.writeAll("--- SELECT with LIMIT ---\n");
    var result4 = try db.execute("SELECT * FROM users LIMIT 2");
    defer result4.deinit();
    try result4.print();

    try stdout.writeAll("--- Fun feature: ORDER BY VIBES (random order!) ---\n");
    var result5 = try db.execute("SELECT * FROM users ORDER BY VIBES LIMIT 3");
    defer result5.deinit();
    try result5.print();

    try stdout.writeAll("--- Creating a products table ---\n");
    _ = try db.execute("CREATE TABLE products (id int, name text, price float, in_stock bool)");
    _ = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99, true)");
    _ = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 29.99, true)");
    _ = try db.execute("INSERT INTO products VALUES (3, \"Gizmo\", 39.99, false)");
    _ = try db.execute("INSERT INTO products VALUES (4, \"Doohickey\", 9.99, true)");

    try stdout.writeAll("\n--- SELECT * FROM products ---\n");
    var result6 = try db.execute("SELECT * FROM products");
    defer result6.deinit();
    try result6.print();

    try stdout.writeAll("--- DELETE products that are out of stock ---\n");
    var result7 = try db.execute("DELETE FROM products WHERE in_stock = false");
    defer result7.deinit();
    try result7.print();

    try stdout.writeAll("--- Products after deletion ---\n");
    var result8 = try db.execute("SELECT * FROM products");
    defer result8.deinit();
    try result8.print();

    try stdout.writeAll("--- Semantic search: ORDER BY SIMILARITY TO \"database tutorial\" ---\n");
    var result9 = try db.execute("SELECT * FROM users ORDER BY SIMILARITY TO \"database tutorial\" LIMIT 2");
    defer result9.deinit();
    try result9.print();

    try stdout.writeAll("\n=== Demo complete! ===\n");
    try stdout.writeAll("Try these features in your own code:\n");
    try stdout.writeAll("- CREATE TABLE with int, float, text, bool, and embedding types\n");
    try stdout.writeAll("- INSERT with values or column names\n");
    try stdout.writeAll("- SELECT with WHERE, LIMIT, and column selection\n");
    try stdout.writeAll("- DELETE with WHERE conditions\n");
    try stdout.writeAll("- ORDER BY SIMILARITY TO \"query\" for semantic search\n");
    try stdout.writeAll("- ORDER BY VIBES for random ordering (for fun!)\n\n");
}
