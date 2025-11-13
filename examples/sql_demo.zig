const std = @import("std");
const zvdb = @import("zvdb");
const Database = zvdb.Database;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Welcome to ZVDB SQL Demo! ===\n", .{});
    std.debug.print("A SQL parody database with semantic search superpowers!\n\n", .{});

    // Create database
    var db = Database.init(allocator);
    defer db.deinit();

    // Initialize vector search for semantic queries
    try db.initVectorSearch(16, 200);

    std.debug.print("--- Creating a 'users' table ---\n", .{});
    var result1 = try db.execute("CREATE TABLE users (id int, name text, bio text)");
    defer result1.deinit();
    try result1.print();

    std.debug.print("--- Inserting some users ---\n", .{});
    _ = try db.execute("INSERT INTO users VALUES (1, \"Alice\", \"Loves Zig programming and databases\")");
    _ = try db.execute("INSERT INTO users VALUES (2, \"Bob\", \"Backend developer interested in vector search\")");
    _ = try db.execute("INSERT INTO users VALUES (3, \"Charlie\", \"Machine learning engineer and data scientist\")");
    _ = try db.execute("INSERT INTO users VALUES (4, \"Diana\", \"Frontend developer who enjoys building UIs\")");

    std.debug.print("\n--- SELECT * FROM users ---\n", .{});
    var result2 = try db.execute("SELECT * FROM users");
    defer result2.deinit();
    try result2.print();

    std.debug.print("--- SELECT with WHERE clause ---\n", .{});
    var result3 = try db.execute("SELECT name, bio FROM users WHERE id = 2");
    defer result3.deinit();
    try result3.print();

    std.debug.print("--- SELECT with LIMIT ---\n", .{});
    var result4 = try db.execute("SELECT * FROM users LIMIT 2");
    defer result4.deinit();
    try result4.print();

    std.debug.print("--- Fun feature: ORDER BY VIBES (random order!) ---\n", .{});
    var result5 = try db.execute("SELECT * FROM users ORDER BY VIBES LIMIT 3");
    defer result5.deinit();
    try result5.print();

    std.debug.print("--- Creating a products table ---\n", .{});
    _ = try db.execute("CREATE TABLE products (id int, name text, price float, in_stock bool)");
    _ = try db.execute("INSERT INTO products VALUES (1, \"Widget\", 19.99, true)");
    _ = try db.execute("INSERT INTO products VALUES (2, \"Gadget\", 29.99, true)");
    _ = try db.execute("INSERT INTO products VALUES (3, \"Gizmo\", 39.99, false)");
    _ = try db.execute("INSERT INTO products VALUES (4, \"Doohickey\", 9.99, true)");

    std.debug.print("\n--- SELECT * FROM products ---\n", .{});
    var result6 = try db.execute("SELECT * FROM products");
    defer result6.deinit();
    try result6.print();

    std.debug.print("--- DELETE products that are out of stock ---\n", .{});
    var result7 = try db.execute("DELETE FROM products WHERE in_stock = false");
    defer result7.deinit();
    try result7.print();

    std.debug.print("--- Products after deletion ---\n", .{});
    var result8 = try db.execute("SELECT * FROM products");
    defer result8.deinit();
    try result8.print();

    std.debug.print("--- Semantic search: ORDER BY SIMILARITY TO \"database tutorial\" ---\n", .{});
    var result9 = try db.execute("SELECT * FROM users ORDER BY SIMILARITY TO \"database tutorial\" LIMIT 2");
    defer result9.deinit();
    try result9.print();

    std.debug.print("\n=== Demo complete! ===\n", .{});
    std.debug.print("Try these features in your own code:\n", .{});
    std.debug.print("- CREATE TABLE with int, float, text, bool, and embedding types\n", .{});
    std.debug.print("- INSERT with values or column names\n", .{});
    std.debug.print("- SELECT with WHERE, LIMIT, and column selection\n", .{});
    std.debug.print("- DELETE with WHERE conditions\n", .{});
    std.debug.print("- ORDER BY SIMILARITY TO \"query\" for semantic search\n", .{});
    std.debug.print("- ORDER BY VIBES for random ordering (for fun!)\n\n", .{});
}
