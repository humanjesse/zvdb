# ZVDB Examples

Real-world usage patterns and applications.

## Document Search System

Semantic search over text documents with metadata.

```zig
const std = @import("std");
const zvdb = @import("zvdb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var db = zvdb.Database.init(allocator);
    defer db.deinit();

    // Create schema
    try db.execute(
        \\CREATE TABLE documents (
        \\    id int,
        \\    title text,
        \\    content text,
        \\    embedding embedding(384)
        \\)
    );

    // Insert documents (embeddings from your model)
    try db.execute("INSERT INTO documents VALUES (1, 'Intro to Zig', '...', [0.1, 0.2, ...])");
    try db.execute("INSERT INTO documents VALUES (2, 'HNSW Algorithm', '...', [0.3, 0.4, ...])");

    // Semantic search
    const results = try db.execute(
        "SELECT id, title FROM documents ORDER BY SIMILARITY TO 'vector search' LIMIT 5"
    );
    defer results.deinit();

    try results.print();
}
```

## Code Knowledge Graph

Index codebase with function relationships.

```zig
const std = @import("std");
const zvdb = @import("zvdb");

pub fn indexCodebase(allocator: std.mem.Allocator, source_dir: []const u8) !void {
    var hnsw = zvdb.HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Parse source files...
    var functions = std.ArrayList(Function).init(allocator);
    defer functions.deinit();

    // For each function, create node
    for (functions.items) |func| {
        var attrs = std.StringHashMap(zvdb.MetadataValue).init(allocator);
        try attrs.put("name", .{ .string = func.name });
        try attrs.put("lines", .{ .int = func.line_count });
        try attrs.put("complexity", .{ .int = func.cyclomatic_complexity });

        const metadata = zvdb.NodeMetadata{
            .node_type = "function",
            .content_ref = func.file_path,
            .start_offset = func.start_byte,
            .end_offset = func.end_byte,
            .attributes = attrs,
        };

        const id = try hnsw.insertWithMetadata(
            func.embedding,  // From code embedding model
            null,
            metadata
        );

        // Track ID for edge creation
        func.graph_id = id;
    }

    // Add call relationships
    for (functions.items) |caller| {
        for (caller.calls) |callee_name| {
            const callee = findFunction(functions, callee_name);
            if (callee) |c| {
                try hnsw.addEdge(caller.graph_id, c.graph_id, "calls", 1.0);
            }
        }
    }

    // Save index
    try hnsw.save("/tmp/code_index.bin");
}

pub fn searchCode(allocator: std.mem.Allocator, query: []const u8) !void {
    var hnsw = try zvdb.HNSW(f32).load(allocator, "/tmp/code_index.bin");
    defer hnsw.deinit();

    // Generate query embedding...
    const query_emb: []f32 = getEmbedding(query);

    // Find similar functions
    const similar = try hnsw.searchByType(query_emb, 10, "function");
    defer allocator.free(similar);

    for (similar) |result| {
        std.debug.print("ID: {}, Distance: {}\n", .{result.external_id, result.distance});
    }

    // Explore call graph from top result
    if (similar.len > 0) {
        const callees = try hnsw.traverse(similar[0].external_id, 2, "calls");
        defer allocator.free(callees);

        std.debug.print("Functions called (2 hops): {any}\n", .{callees});
    }
}
```

## RAG Pipeline

Retrieval-Augmented Generation with context.

```zig
const std = @import("std");
const zvdb = @import("zvdb");

const RagContext = struct {
    db: *zvdb.Database,
    model: EmbeddingModel,  // Your embedding model
    llm: LanguageModel,     // Your LLM

    pub fn query(self: *RagContext, question: []const u8) ![]const u8 {
        // 1. Generate question embedding
        const q_emb = try self.model.encode(question);
        defer self.model.allocator.free(q_emb);

        // 2. Retrieve relevant chunks
        const sql = std.fmt.allocPrint(
            self.db.allocator,
            "SELECT content FROM docs ORDER BY SIMILARITY TO ? LIMIT 5",
            .{}
        );
        defer self.db.allocator.free(sql);

        const results = try self.db.execute(sql);
        defer results.deinit();

        // 3. Build context
        var context = std.ArrayList(u8).init(self.db.allocator);
        defer context.deinit();

        for (results.rows.items) |row| {
            const content = row.items[0].text;
            try context.appendSlice(content);
            try context.appendSlice("\n\n");
        }

        // 4. Generate answer with context
        const prompt = try std.fmt.allocPrint(
            self.db.allocator,
            "Context:\n{s}\n\nQuestion: {s}\n\nAnswer:",
            .{context.items, question}
        );
        defer self.db.allocator.free(prompt);

        return try self.llm.generate(prompt);
    }
};
```

## Multi-Tenant Database

Isolate data by tenant with row-level filtering.

```zig
pub fn setupMultiTenant(db: *zvdb.Database) !void {
    try db.execute(
        \\CREATE TABLE tenants (
        \\    id int,
        \\    name text
        \\)
    );

    try db.execute(
        \\CREATE TABLE documents (
        \\    id int,
        \\    tenant_id int,
        \\    title text,
        \\    content text,
        \\    embedding embedding(384)
        \\)
    );

    try db.execute("CREATE INDEX idx_tenant ON documents (tenant_id)");
}

pub fn queryTenant(db: *zvdb.Database, tenant_id: i64, query: []const u8) !zvdb.QueryResult {
    const sql = try std.fmt.allocPrint(
        db.allocator,
        \\SELECT title, content FROM documents
        \\WHERE tenant_id = {d}
        \\ORDER BY SIMILARITY TO '{s}'
        \\LIMIT 10
        ,
        .{tenant_id, query}
    );
    defer db.allocator.free(sql);

    return try db.execute(sql);
}
```

## Transaction Retry Pattern

Handle write-write conflicts gracefully.

```zig
pub fn updateWithRetry(db: *zvdb.Database, user_id: i64, delta: i64) !void {
    const max_retries = 5;
    var attempt: usize = 0;

    while (attempt < max_retries) : (attempt += 1) {
        // Start transaction
        db.execute("BEGIN") catch {
            std.time.sleep(std.time.ns_per_ms * 10);  // Brief backoff
            continue;
        };

        // Read current balance
        const sql_read = try std.fmt.allocPrint(
            db.allocator,
            "SELECT balance FROM accounts WHERE user_id = {d}",
            .{user_id}
        );
        defer db.allocator.free(sql_read);

        const result = try db.execute(sql_read);
        defer result.deinit();

        if (result.rows.items.len == 0) return error.UserNotFound;

        const current_balance = result.rows.items[0].items[0].int;
        const new_balance = current_balance + delta;

        // Update balance
        const sql_update = try std.fmt.allocPrint(
            db.allocator,
            "UPDATE accounts SET balance = {d} WHERE user_id = {d}",
            .{new_balance, user_id}
        );
        defer db.allocator.free(sql_update);

        db.execute(sql_update) catch |err| {
            _ = db.execute("ROLLBACK") catch {};
            if (err == error.WriteWriteConflict) {
                std.time.sleep(std.time.ns_per_ms * (10 * (attempt + 1)));  // Exponential backoff
                continue;
            }
            return err;
        };

        // Commit
        db.execute("COMMIT") catch |err| {
            if (err == error.WriteWriteConflict) {
                std.time.sleep(std.time.ns_per_ms * (10 * (attempt + 1)));
                continue;
            }
            return err;
        };

        // Success!
        return;
    }

    return error.MaxRetriesExceeded;
}
```

## Batch Operations

Efficient bulk inserts.

```zig
pub fn bulkInsert(db: *zvdb.Database, docs: []Document) !void {
    try db.execute("BEGIN");
    errdefer _ = db.execute("ROLLBACK") catch {};

    for (docs, 0..) |doc, i| {
        const sql = try std.fmt.allocPrint(
            db.allocator,
            "INSERT INTO documents VALUES ({d}, '{s}', '{s}', {any})",
            .{i, doc.title, doc.content, doc.embedding}
        );
        defer db.allocator.free(sql);

        try db.execute(sql);
    }

    try db.execute("COMMIT");
}
```

## Hybrid Query

Combine filters with semantic search.

```zig
pub fn searchWithFilters(
    db: *zvdb.Database,
    query: []const u8,
    category: []const u8,
    min_date: i64
) !zvdb.QueryResult {
    const sql = try std.fmt.allocPrint(
        db.allocator,
        \\SELECT * FROM documents
        \\WHERE category = '{s}' AND created_date > {d}
        \\ORDER BY SIMILARITY TO '{s}'
        \\LIMIT 20
        ,
        .{category, min_date, query}
    );
    defer db.allocator.free(sql);

    return try db.execute(sql);
}
```

## Persistence Pattern

Save and restore with WAL.

```zig
pub fn initPersistent(allocator: std.mem.Allocator, data_dir: []const u8) !zvdb.Database {
    var db = zvdb.Database.init(allocator);

    // Enable WAL
    const wal_path = try std.fmt.allocPrint(allocator, "{s}/zvdb.wal", .{data_dir});
    defer allocator.free(wal_path);
    try db.enableWal(wal_path);

    // Try to load existing data
    db.loadAll(data_dir) catch |err| {
        if (err != error.FileNotFound) return err;
        // First run - no data to load
    };

    // Configure auto-save
    db.data_dir = try allocator.dupe(u8, data_dir);
    db.auto_save = true;

    return db;
}
```

## Testing Helper

Setup and teardown for tests.

```zig
pub fn testDatabase() !struct { db: zvdb.Database, allocator: std.mem.Allocator } {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var db = zvdb.Database.init(allocator);

    // Create test schema
    try db.execute(
        \\CREATE TABLE test_docs (
        \\    id int,
        \\    content text,
        \\    embedding embedding(128)
        \\)
    );

    return .{ .db = db, .allocator = allocator };
}

test "semantic search" {
    var ctx = try testDatabase();
    defer ctx.db.deinit();

    try ctx.db.execute("INSERT INTO test_docs VALUES (1, 'hello', [0.1, 0.2, ...])");

    const results = try ctx.db.execute("SELECT * FROM test_docs ORDER BY SIMILARITY TO 'hi' LIMIT 1");
    defer results.deinit();

    try std.testing.expect(results.rows.items.len == 1);
}
```

## See Also

- [User Guide](USER_GUIDE.md) - Feature reference
- [API Reference](api/) - Function documentation
- [Architecture](ARCHITECTURE.md) - System design
