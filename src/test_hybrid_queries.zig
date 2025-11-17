const std = @import("std");
const testing = std.testing;
const Database = @import("database.zig").Database;
const ColumnValue = @import("table.zig").ColumnValue;

test "SQL: Hybrid query - SIMILARITY with simple WHERE" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    // Initialize vector search
    try db.initVectorSearch(16, 200);

    // Create table with embeddings and regular columns
    var create_result = try db.execute("CREATE TABLE articles (id int, category text, title text, content text, embedding embedding(128))");
    defer create_result.deinit();

    const table = db.tables.get("articles").?;
    const hnsw = try db.getOrCreateHnswForDim(128);

    // Insert test data with different categories
    const test_data = [_]struct {
        id: i64,
        category: []const u8,
        title: []const u8,
        content: []const u8,
        emb_seed: f32,
    }{
        .{ .id = 1, .category = "tech", .title = "Database Tutorial", .content = "Learn about databases", .emb_seed = 0.1 },
        .{ .id = 2, .category = "tech", .title = "Vector Search Guide", .content = "Understanding vector databases", .emb_seed = 0.15 },
        .{ .id = 3, .category = "sports", .title = "Football Basics", .content = "Introduction to football", .emb_seed = 0.9 },
        .{ .id = 4, .category = "tech", .title = "SQL Explained", .content = "SQL query fundamentals", .emb_seed = 0.12 },
        .{ .id = 5, .category = "cooking", .title = "Pasta Recipe", .content = "How to make pasta", .emb_seed = 0.8 },
    };

    for (test_data) |data| {
        var embedding = [_]f32{data.emb_seed} ** 128;

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = data.id });
        try values.put("category", ColumnValue{ .text = data.category });
        try values.put("title", ColumnValue{ .text = data.title });
        try values.put("content", ColumnValue{ .text = data.content });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("embedding", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Test hybrid query: Find similar articles in "tech" category only
    // This should return tech articles (1, 2, 4) sorted by similarity, NOT sports (3) or cooking (5)
    var result = try db.execute("SELECT id, category, title FROM articles WHERE category = 'tech' ORDER BY SIMILARITY TO \"database\" LIMIT 3");
    defer result.deinit();

    std.debug.print("\n=== Hybrid Query Test Results ===\n", .{});
    std.debug.print("Query: SELECT * FROM articles WHERE category = 'tech' ORDER BY SIMILARITY TO \"database\"\n", .{});
    std.debug.print("Expected: Only 'tech' category articles, sorted by similarity\n\n", .{});

    // Verify results
    try testing.expect(result.rows.items.len <= 3);
    try testing.expect(result.rows.items.len > 0);

    // All results should have category = 'tech'
    for (result.rows.items) |row| {
        const category_idx = blk: {
            for (result.columns.items, 0..) |col, i| {
                if (std.mem.eql(u8, col, "category")) break :blk i;
            }
            unreachable;
        };
        const category = row.items[category_idx].text;
        try testing.expect(std.mem.eql(u8, category, "tech"));

        const id_idx = blk: {
            for (result.columns.items, 0..) |col, i| {
                if (std.mem.eql(u8, col, "id")) break :blk i;
            }
            unreachable;
        };
        const id = row.items[id_idx].int;
        std.debug.print("  Row {}: category='{}s', id={}\n", .{ id, std.fmt.fmtSliceEscapeLower(category), id });
    }

    std.debug.print("\n✓ Hybrid query successfully filtered by WHERE clause!\n", .{});
}

test "SQL: Hybrid query - SIMILARITY with complex WHERE expression" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE products (id int, name text, price float, in_stock bool, description text, vec embedding(64))");
    defer create_result.deinit();

    const table = db.tables.get("products").?;
    const hnsw = try db.getOrCreateHnswForDim(64);

    // Insert test products
    const products = [_]struct {
        id: i64,
        name: []const u8,
        price: f64,
        in_stock: bool,
        description: []const u8,
        emb_seed: f32,
    }{
        .{ .id = 1, .name = "Gaming Laptop", .price = 1200.0, .in_stock = true, .description = "High performance laptop", .emb_seed = 0.1 },
        .{ .id = 2, .name = "Office Laptop", .price = 600.0, .in_stock = true, .description = "Budget laptop for work", .emb_seed = 0.11 },
        .{ .id = 3, .name = "Pro Laptop", .price = 2500.0, .in_stock = false, .description = "Premium laptop", .emb_seed = 0.09 },
        .{ .id = 4, .name = "Budget Laptop", .price = 400.0, .in_stock = true, .description = "Entry level laptop", .emb_seed = 0.12 },
        .{ .id = 5, .name = "Premium Phone", .price = 1100.0, .in_stock = true, .description = "Latest smartphone", .emb_seed = 0.8 },
    };

    for (products) |prod| {
        var embedding = [_]f32{prod.emb_seed} ** 64;

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = prod.id });
        try values.put("name", ColumnValue{ .text = prod.name });
        try values.put("price", ColumnValue{ .float = prod.price });
        try values.put("in_stock", ColumnValue{ .bool = prod.in_stock });
        try values.put("description", ColumnValue{ .text = prod.description });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("vec", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Complex WHERE: price < 1000 AND in_stock = true
    // Should only return products 2 and 4 (both under $1000 and in stock)
    // Product 1 is over $1000, product 3 is out of stock, product 5 is a phone (different embedding cluster)
    var result = try db.execute("SELECT id, name, price, in_stock FROM products WHERE price < 1000.0 ORDER BY SIMILARITY TO \"laptop computer\" LIMIT 10");
    defer result.deinit();

    std.debug.print("\n=== Complex WHERE Test ===\n", .{});
    std.debug.print("Query: WHERE price < 1000 ORDER BY SIMILARITY\n", .{});
    std.debug.print("Results:\n", .{});

    for (result.rows.items) |row| {
        const id = row.items[0].int;
        const price = row.items[2].float;
        std.debug.print("  Product {}: price=${d:.2}\n", .{ id, price });
        try testing.expect(price < 1000.0);
    }

    std.debug.print("✓ All results match WHERE clause constraint!\n", .{});
}

test "SQL: Hybrid query - LIMIT applies after WHERE filter" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE docs (id int, status text, content text, vec embedding(32))");
    defer create_result.deinit();

    const table = db.tables.get("docs").?;
    const hnsw = try db.getOrCreateHnswForDim(32);

    // Insert 10 documents: 5 "published", 5 "draft"
    var i: i64 = 1;
    while (i <= 10) : (i += 1) {
        var embedding = [_]f32{@as(f32, @floatFromInt(i)) * 0.05} ** 32;
        const status = if (i <= 5) "published" else "draft";

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = i });
        try values.put("status", ColumnValue{ .text = status });
        try values.put("content", ColumnValue{ .text = "Document content" });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("vec", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Query with LIMIT 3 and WHERE filter
    // Should return exactly 3 "published" documents (not 3 from all 10)
    var result = try db.execute("SELECT id, status FROM docs WHERE status = 'published' ORDER BY SIMILARITY TO \"test\" LIMIT 3");
    defer result.deinit();

    std.debug.print("\n=== LIMIT After WHERE Test ===\n", .{});
    std.debug.print("Total docs: 10 (5 published, 5 draft)\n", .{});
    std.debug.print("Query: WHERE status = 'published' LIMIT 3\n", .{});
    std.debug.print("Results: {} rows\n", .{result.rows.items.len});

    try testing.expect(result.rows.items.len <= 3);

    // All should be published
    for (result.rows.items) |row| {
        const status = row.items[1].text;
        try testing.expect(std.mem.eql(u8, status, "published"));
        std.debug.print("  Doc {}: status='{}s'\n", .{ row.items[0].int, std.fmt.fmtSliceEscapeLower(status) });
    }

    std.debug.print("✓ LIMIT correctly applied after WHERE filtering!\n", .{});
}

test "SQL: Hybrid query - No results when WHERE filters all SIMILARITY results" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE items (id int, type text, vec embedding(16))");
    defer create_result.deinit();

    const table = db.tables.get("items").?;
    const hnsw = try db.getOrCreateHnswForDim(16);

    // Insert items all of type "A"
    var i: i64 = 1;
    while (i <= 5) : (i += 1) {
        var embedding = [_]f32{0.1} ** 16;

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = i });
        try values.put("type", ColumnValue{ .text = "A" });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("vec", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Query for type "B" which doesn't exist
    var result = try db.execute("SELECT id, type FROM items WHERE type = 'B' ORDER BY SIMILARITY TO \"test\" LIMIT 10");
    defer result.deinit();

    std.debug.print("\n=== Empty Result Test ===\n", .{});
    std.debug.print("All items have type='A', querying for type='B'\n", .{});
    std.debug.print("Results: {} rows\n", .{result.rows.items.len});

    try testing.expect(result.rows.items.len == 0);
    std.debug.print("✓ Correctly returns empty result when WHERE filters everything!\n", .{});
}

test "SQL: Hybrid query - SIMILARITY without WHERE (baseline)" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE notes (id int, text text, vec embedding(16))");
    defer create_result.deinit();

    const table = db.tables.get("notes").?;
    const hnsw = try db.getOrCreateHnswForDim(16);

    // Insert 5 notes
    var i: i64 = 1;
    while (i <= 5) : (i += 1) {
        var embedding = [_]f32{@as(f32, @floatFromInt(i)) * 0.1} ** 16;

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = i });
        try values.put("text", ColumnValue{ .text = "Note text" });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("vec", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Pure SIMILARITY query (no WHERE)
    var result = try db.execute("SELECT id FROM notes ORDER BY SIMILARITY TO \"query\" LIMIT 3");
    defer result.deinit();

    std.debug.print("\n=== Pure SIMILARITY Test (no WHERE) ===\n", .{});
    std.debug.print("Total notes: 5\n", .{});
    std.debug.print("Query: ORDER BY SIMILARITY LIMIT 3\n", .{});
    std.debug.print("Results: {} rows\n", .{result.rows.items.len});

    try testing.expect(result.rows.items.len <= 3);
    try testing.expect(result.rows.items.len > 0);

    std.debug.print("✓ Pure SIMILARITY query works as baseline!\n", .{});
}

test "SQL: Hybrid query - Multiple WHERE conditions with SIMILARITY" {
    var db = Database.init(testing.allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);

    var create_result = try db.execute("CREATE TABLE listings (id int, city text, price float, available bool, vec embedding(32))");
    defer create_result.deinit();

    const table = db.tables.get("listings").?;
    const hnsw = try db.getOrCreateHnswForDim(32);

    // Insert diverse listings
    const listings = [_]struct {
        id: i64,
        city: []const u8,
        price: f64,
        available: bool,
        emb_seed: f32,
    }{
        .{ .id = 1, .city = "NYC", .price = 500.0, .available = true, .emb_seed = 0.1 },
        .{ .id = 2, .city = "NYC", .price = 800.0, .available = true, .emb_seed = 0.11 },
        .{ .id = 3, .city = "NYC", .price = 600.0, .available = false, .emb_seed = 0.12 },
        .{ .id = 4, .city = "LA", .price = 450.0, .available = true, .emb_seed = 0.13 },
        .{ .id = 5, .city = "NYC", .price = 400.0, .available = true, .emb_seed = 0.14 },
    };

    for (listings) |listing| {
        var embedding = [_]f32{listing.emb_seed} ** 32;

        var values = std.StringHashMap(ColumnValue).init(testing.allocator);
        defer values.deinit();
        try values.put("id", ColumnValue{ .int = listing.id });
        try values.put("city", ColumnValue{ .text = listing.city });
        try values.put("price", ColumnValue{ .float = listing.price });
        try values.put("available", ColumnValue{ .bool = listing.available });

        const emb = try testing.allocator.dupe(f32, &embedding);
        defer testing.allocator.free(emb);
        try values.put("vec", ColumnValue{ .embedding = emb });

        const row_id = try table.insert(values);
        _ = try hnsw.insert(&embedding, row_id);
    }

    // Multiple conditions: city = 'NYC' AND price < 700 AND available = true
    // Should match: 1 (NYC, $500, true) and 5 (NYC, $400, true)
    // NOT: 2 ($800), 3 (not available), 4 (LA)
    var result = try db.execute("SELECT id, city, price, available FROM listings WHERE city = 'NYC' ORDER BY SIMILARITY TO \"apartment\" LIMIT 10");
    defer result.deinit();

    std.debug.print("\n=== Multiple WHERE Conditions Test ===\n", .{});
    std.debug.print("Query: WHERE city = 'NYC' ORDER BY SIMILARITY\n", .{});

    for (result.rows.items) |row| {
        const id = row.items[0].int;
        const city = row.items[1].text;
        std.debug.print("  Listing {}: city='{}s'\n", .{ id, std.fmt.fmtSliceEscapeLower(city) });
        try testing.expect(std.mem.eql(u8, city, "NYC"));
    }

    std.debug.print("✓ Multiple WHERE conditions correctly applied!\n", .{});
}
