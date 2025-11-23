# Getting Started with ZVDB

Quick guide to building and using ZVDB.

## Prerequisites

- Zig 0.15.2 or later
- No other dependencies

## Installation

Clone and build:

```bash
git clone https://github.com/yourrepo/zvdb
cd zvdb
zig build
```

Run tests to verify:

```bash
zig build test
```

## Your First Database

Create `example.zig`:

```zig
const std = @import("std");
const zvdb = @import("zvdb");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Initialize database
    var db = zvdb.Database.init(allocator);
    defer db.deinit();

    // Create table
    try db.execute("CREATE TABLE users (id int, name text)");

    // Insert data
    try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    try db.execute("INSERT INTO users VALUES (2, 'Bob')");

    // Query
    const result = try db.execute("SELECT * FROM users WHERE id > 0");
    defer result.deinit();

    try result.print();
}
```

Build and run:

```bash
zig build-exe example.zig --dep zvdb --mod zvdb:/path/to/zvdb/src/zvdb.zig
./example
```

## Vector Search

Add embedding column and search:

```zig
// Create table with vector column
try db.execute("CREATE TABLE docs (id int, text text, embedding embedding(384))");

// Insert with vector
try db.execute("INSERT INTO docs VALUES (1, 'hello world', [0.1, 0.2, ...])");

// Semantic search
const results = try db.execute(
    "SELECT * FROM docs ORDER BY SIMILARITY TO 'greeting' LIMIT 5"
);
defer results.deinit();
```

## Transactions

Ensure data consistency:

```zig
try db.execute("BEGIN");

try db.execute("INSERT INTO users VALUES (3, 'Charlie')");
try db.execute("UPDATE users SET name = 'Charles' WHERE id = 3");

try db.execute("COMMIT");  // or ROLLBACK to undo
```

## Persistence

Save and load database:

```zig
// Enable WAL for durability
try db.enableWal("/tmp/zvdb.wal");

// Save all tables
try db.saveAll("/tmp/zvdb_data");

// Load from disk
var db2 = zvdb.Database.init(allocator);
try db2.loadAll("/tmp/zvdb_data");
```

## GraphRAG

Build knowledge graphs:

```zig
const zvdb = @import("zvdb");

var hnsw = zvdb.HNSW(f32).init(allocator, 16, 200);
defer hnsw.deinit();

// Create node with metadata
var attrs = std.StringHashMap(zvdb.MetadataValue).init(allocator);
try attrs.put("name", .{ .string = "main" });

const metadata = zvdb.NodeMetadata{
    .node_type = "function",
    .content_ref = "src/main.zig",
    .start_offset = 0,
    .end_offset = 100,
    .attributes = attrs,
};

const id = try hnsw.insertWithMetadata(embedding, null, metadata);

// Add relationship
try hnsw.addEdge(id, other_id, "calls", 1.0);

// Hybrid search: vector + graph
const results = try hnsw.searchThenTraverse(query_vec, 5, 2, "calls");
```

## Next Steps

- [User Guide](USER_GUIDE.md) - Comprehensive feature reference
- [API Reference](api/) - Function-level documentation
- [Examples](EXAMPLES.md) - Real-world patterns
- [Architecture](ARCHITECTURE.md) - System internals
