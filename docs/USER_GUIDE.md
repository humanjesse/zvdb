# ZVDB User Guide

Comprehensive reference for using ZVDB.

## Data Types

### Supported Types

- `int` - 64-bit signed integer (i64)
- `float` - 64-bit floating point (f64)
- `text` - UTF-8 string
- `bool` - Boolean true/false
- `embedding(N)` - N-dimensional vector (f32 array)

### Type Constraints

- Table can have up to 10 embedding columns (configurable)
- All embeddings in same column must have same dimension
- Text values owned by table - don't modify after insert

## SQL Reference

### DDL (Data Definition)

**CREATE TABLE**

```sql
CREATE TABLE name (col1 type, col2 type, ...)

CREATE TABLE docs (
    id int,
    content text,
    vec1 embedding(384),
    vec2 embedding(768)
)
```

**DROP TABLE**

```sql
DROP TABLE name
```

**ALTER TABLE**

```sql
ALTER TABLE name ADD COLUMN col_name type
```

**CREATE INDEX**

```sql
CREATE INDEX idx_name ON table_name (column)
```

B-tree index for fast lookups on int/float/text columns.

**DROP INDEX**

```sql
DROP INDEX idx_name
```

### DML (Data Manipulation)

**INSERT**

```sql
INSERT INTO table VALUES (val1, val2, ...)

INSERT INTO docs VALUES (1, 'text', [0.1, 0.2, 0.3])
```

Array literals for embeddings. Values must match schema order.

**UPDATE**

```sql
UPDATE table SET column = value WHERE condition
```

**DELETE**

```sql
DELETE FROM table WHERE condition
```

### DQL (Data Query)

**SELECT**

```sql
SELECT col1, col2 FROM table
    WHERE condition
    JOIN other ON table.col = other.col
    GROUP BY col
    HAVING aggregate_condition
    ORDER BY col [ASC|DESC]
    LIMIT n
```

**WHERE Clause**

Operators: `=`, `!=`, `<`, `>`, `<=`, `>=`, `AND`, `OR`, `NOT`, `IN`, `NOT IN`, `EXISTS`, `NOT EXISTS`

**JOIN Types**

- `INNER JOIN` - Matching rows only
- `LEFT JOIN` - All left rows + matches
- `RIGHT JOIN` - All right rows + matches
- `CROSS JOIN` - Cartesian product

**Aggregates**

`COUNT(*)`, `SUM(col)`, `AVG(col)`, `MIN(col)`, `MAX(col)`

**Subqueries**

```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
SELECT * FROM docs WHERE EXISTS (SELECT 1 FROM refs WHERE refs.doc_id = docs.id)
```

**Semantic Search**

```sql
ORDER BY SIMILARITY TO 'query text'
```

Searches HNSW index by text similarity. Built-in text-to-embedding uses hash-based placeholder (demo/testing only). For production, integrate real embedding model and insert pre-computed vectors.

**Random Ordering**

```sql
ORDER BY VIBES
```

Random result ordering (for fun).

### Transactions

```sql
BEGIN
-- your queries --
COMMIT  -- or ROLLBACK
```

Snapshot isolation. See [api/transactions.md](api/transactions.md).

### Maintenance

**VACUUM**

```sql
VACUUM
```

Remove old row versions. Run periodically or enable auto-VACUUM.

## Vector Search

### Creating Indexes

HNSW indexes created automatically for embedding columns on table creation.

Index key: `(dimension, column_name)` - allows multiple same-dimension embeddings per table.

### Inserting Vectors

```sql
INSERT INTO docs VALUES (1, 'text', [0.1, 0.2, 0.3, ...])
```

Dimension must match column definition. Generate embeddings using your embedding model (e.g., sentence-transformers, OpenAI, etc.) before insertion.

### Searching

```sql
SELECT * FROM docs ORDER BY SIMILARITY TO 'search query' LIMIT k
```

Returns k nearest neighbors by cosine similarity.

### Multiple Embeddings

```sql
CREATE TABLE docs (
    id int,
    title_vec embedding(384),
    content_vec embedding(768)
)
```

Note: SIMILARITY TO searches use the first embedding column found in the table. For multi-embedding scenarios, use direct HNSW API to search specific columns.

### HNSW Parameters

Default: M=16, ef_construction=200

Rebuild with custom parameters via direct API:

```zig
var hnsw = zvdb.HNSW(f32).init(allocator, 32, 400);  // Higher quality
```

## GraphRAG

### Node Types

Organize nodes semantically:
- `doc_chunk` - Document segments
- `function` - Code functions
- `entity` - Named entities
- Custom types allowed

### Adding Nodes

```zig
var attrs = std.StringHashMap(zvdb.MetadataValue).init(allocator);
try attrs.put("title", .{ .string = "Introduction" });
try attrs.put("page", .{ .int = 1 });

const metadata = zvdb.NodeMetadata{
    .node_type = "doc_chunk",
    .content_ref = "manual.pdf",
    .start_offset = 0,
    .end_offset = 512,
    .attributes = attrs,
};

const id = try hnsw.insertWithMetadata(embedding, null, metadata);
```

### Edges

```zig
// Create relationships
try hnsw.addEdge(parent_id, child_id, "child_of", 1.0);
try hnsw.addEdge(doc_id, entity_id, "mentions", 0.8);

// Query edges
const edges = try hnsw.getEdges(node_id, "child_of");
```

### Traversal

```zig
// BFS from node up to depth 3
const neighbors = try hnsw.traverse(start_id, 3, null);

// Follow only specific edge type
const callees = try hnsw.traverse(func_id, 2, "calls");
```

### Hybrid Queries

```zig
// Vector search + graph expansion
const results = try hnsw.searchThenTraverse(
    query_embedding,
    5,           // Initial k-NN results
    2,           // Expand 2 hops
    "related"    // Edge type filter
);
```

### Type Filtering

```zig
// Search only specific node type
const funcs = try hnsw.searchByType(query, 10, "function");

// Get all nodes of type
const all_entities = try hnsw.getNodesByType("entity");
```

## Persistence

### Write-Ahead Logging

Enable for crash recovery:

```zig
try db.enableWal("/path/to/wal.log");

// After crash
try db.recoverFromWal("/path/to/wal.log");
```

All writes logged before execution. Replay on recovery.

### Saving Data

```zig
try db.saveAll("/data/directory");
```

Creates:
- `{table_name}.table` - Table data with MVCC versions
- `hnsw_{dim}_{col}.idx` - Vector indexes

### Loading Data

```zig
var db = try zvdb.Database.loadAll(allocator, "/data/directory");
defer db.deinit();
```

Returns new Database instance with tables and indexes reconstructed from disk.

### Auto-Save

```zig
db.data_dir = "/data/directory";
db.auto_save = true;  // Save on deinit
```

## Configuration

### Validation

```zig
db.enable_validation = true;
db.validation_mode = .strict;  // .strict, .warnings, .disabled
```

Catches errors early: invalid columns, type mismatches, schema violations.

### VACUUM

```zig
db.vacuum_config = .{
    .enabled = true,
    .max_chain_length = 10,
    .txn_interval = 1000,
};
```

### Resource Limits

```zig
db.max_embeddings_per_row = 10;  // Prevent memory exhaustion
```

## Performance

### Indexing

- Create B-tree indexes on frequently filtered columns
- HNSW indexes automatic for embeddings
- Hash joins used automatically for large tables

### Query Optimization

- Cost-based optimizer chooses join strategy
- Push filters down in execution plan
- Use LIMIT to reduce result size

### VACUUM Strategy

- Run after bulk deletes/updates
- Monitor version chain length
- Enable auto-VACUUM for steady workloads

### Batch Operations

Insert multiple rows in single transaction for better throughput.

## Limitations

- No foreign key constraints
- No check constraints
- No stored procedures
- No triggers
- Write skew possible under snapshot isolation
- Savepoints not supported

## Error Handling

Common errors:
- `TableNotFound` - Check table name spelling
- `ColumnNotFound` - Verify schema matches query
- `TypeMismatch` - Check value types match column types
- `ParseError` - Syntax error in SQL
- `WriteWriteConflict` - Retry transaction
- `ValidationError` - Query validation failed (check schema)

Retry pattern for conflicts:

```zig
var retries: usize = 0;
while (retries < 3) : (retries += 1) {
    db.execute("BEGIN") catch continue;
    db.execute("UPDATE ...") catch |err| {
        if (err == error.WriteWriteConflict) continue;
        return err;
    };
    db.execute("COMMIT") catch |err| {
        if (err == error.WriteWriteConflict) continue;
        return err;
    };
    break;
}
```

## See Also

- [API Reference](api/) - Function signatures and details
- [Examples](EXAMPLES.md) - Real-world patterns
- [Architecture](ARCHITECTURE.md) - System design
