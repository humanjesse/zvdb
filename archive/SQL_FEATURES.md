# SQL Features in ZVDB

## Overview

ZVDB now includes a SQL interface that combines traditional relational database operations with powerful vector search capabilities. This creates a unique "SQL parody" database that's both familiar and futuristic!

## Architecture

### Core Components

1. **table.zig** - Table storage engine
   - `Table`: Schema and row storage
   - `Row`: Individual data rows with column values
   - `ColumnValue`: Union type supporting int, float, text, bool, and embedding
   - `ColumnType`: Type definitions for schema

2. **sql.zig** - SQL parser
   - Tokenizer for SQL commands
   - Parser for CREATE, INSERT, SELECT, DELETE
   - Support for WHERE, LIMIT, ORDER BY
   - Special semantic search syntax

3. **database.zig** - Database engine
   - `Database`: Manages multiple tables and HNSW index
   - `QueryResult`: Result sets with pretty-printing
   - SQL execution engine
   - Integration between tables and vector search

## Data Types

| Type | Description | Example |
|------|-------------|---------|
| `int` | 64-bit signed integer | `42`, `-100` |
| `float` | 64-bit floating point | `3.14`, `-0.5` |
| `text` | UTF-8 string | `"Hello"`, `"World"` |
| `bool` | Boolean value | `true`, `false` |
| `embedding` | Float32 vector | `[0.1, 0.2, ...]` |

## SQL Commands

### CREATE TABLE

Create a new table with a schema.

```sql
CREATE TABLE table_name (col1 type1, col2 type2, ...)
```

**Examples:**
```sql
CREATE TABLE users (id int, name text, age int)
CREATE TABLE products (id int, name text, price float, in_stock bool)
CREATE TABLE docs (id int, content text, embedding embedding)
```

### INSERT

Insert data into a table.

```sql
-- Insert with all columns in order
INSERT INTO table_name VALUES (val1, val2, ...)

-- Insert with specific columns
INSERT INTO table_name (col1, col2) VALUES (val1, val2)
```

**Examples:**
```sql
INSERT INTO users VALUES (1, "Alice", 25)
INSERT INTO products (name, price) VALUES ("Widget", 19.99)
INSERT INTO users VALUES (2, "Bob", NULL)
```

**Features:**
- Auto-incrementing row IDs
- NULL value support
- Type checking against schema
- Automatic vector indexing for embedding columns

### SELECT

Query data from tables.

```sql
-- Select all columns
SELECT * FROM table_name

-- Select specific columns
SELECT col1, col2 FROM table_name

-- With WHERE clause
SELECT * FROM table_name WHERE col = value

-- With LIMIT
SELECT * FROM table_name LIMIT 10

-- Semantic search (requires initVectorSearch)
SELECT * FROM table_name ORDER BY SIMILARITY TO "query text" LIMIT 5

-- Random ordering (parody feature!)
SELECT * FROM table_name ORDER BY VIBES
```

**Examples:**
```sql
SELECT * FROM users
SELECT name, age FROM users WHERE age = 25
SELECT * FROM products LIMIT 10
SELECT * FROM posts ORDER BY SIMILARITY TO "database tutorial" LIMIT 5
SELECT * FROM users ORDER BY VIBES LIMIT 3
```

**Features:**
- Column projection (select specific columns or *)
- WHERE filtering with = operator
- LIMIT for pagination
- Semantic search via HNSW integration
- Random ordering for fun

### DELETE

Remove rows from a table.

```sql
-- Delete all rows
DELETE FROM table_name

-- Delete with condition
DELETE FROM table_name WHERE col = value
```

**Examples:**
```sql
DELETE FROM users WHERE id = 5
DELETE FROM products WHERE in_stock = false
DELETE FROM temp
```

## Semantic Search Integration

The killer feature of ZVDB's SQL interface is seamless integration with vector search!

### How It Works

1. **Initialize vector search** on the database:
   ```zig
   try db.initVectorSearch(16, 200); // M=16, ef_construction=200
   ```

2. **Store embeddings** in your tables:
   ```sql
   CREATE TABLE documents (id int, text text, embedding embedding)
   ```

3. **Query with semantic similarity**:
   ```sql
   SELECT * FROM documents ORDER BY SIMILARITY TO "your search query" LIMIT 10
   ```

### Under the Hood

When you use `ORDER BY SIMILARITY TO "query"`:

1. The query text is converted to an embedding vector (currently using a simple hash-based approach for demo; in production, use a real embedding model)
2. The HNSW index performs approximate nearest neighbor search
3. Results are ranked by cosine similarity
4. Standard SQL filters (WHERE, LIMIT) are applied

### Best Practices

- Store pre-computed embeddings in the `embedding` column type
- Use `ORDER BY SIMILARITY` for semantic queries
- Combine with WHERE clauses for hybrid search
- Use LIMIT to control result set size

## Query Results

All queries return a `QueryResult` object that can be:

1. **Printed** with `result.print()` for formatted output
2. **Inspected** programmatically via `result.columns` and `result.rows`
3. **Freed** with `result.deinit()` when done

Example output:
```
id | name | age
----------+----------+----------
1 | Alice | 25
2 | Bob | 30

(2 rows)
```

## Fun Parody Features

### ORDER BY VIBES

Because sometimes you just want random results!

```sql
SELECT * FROM users ORDER BY VIBES LIMIT 5
```

This shuffles the results randomly, perfect for:
- "Feeling lucky" features
- Random sampling
- Keeping things interesting

### Future Parody Ideas

- `SELECT * FROM users WHERE vibe = "immaculate"`
- `INSERT INTO users VALUES (...) WITH GOOD_VIBES`
- `DELETE FROM posts WHERE vibe = "off"`

## Performance Characteristics

### Table Operations
- **INSERT**: O(1) for table, O(log n) if adding to HNSW
- **SELECT without WHERE**: O(n) table scan
- **SELECT with WHERE**: O(n) filtered scan
- **DELETE**: O(n) scan + O(k) deletions

### Semantic Search
- **ORDER BY SIMILARITY**: O(log n) via HNSW
- **Hybrid queries**: O(log n) + O(k) filter

### Memory Usage
- Each table stores rows in a hash map
- Each row stores column values
- Embeddings are stored both in tables and HNSW index
- String values are owned and need cleanup

## Limitations & Future Work

### Current Limitations
1. No JOINs (yet!)
2. Limited WHERE operators (only =)
3. No aggregations (SUM, COUNT, AVG)
4. No indexes on non-embedding columns
5. No transactions or ACID guarantees
6. No persistence for tables (only HNSW)
7. Embedding generation is mock (use real models in production)

### Planned Features
1. **JOINs**: Use GraphRAG edges for relationships
2. **More operators**: <, >, <=, >=, LIKE, IN
3. **Aggregations**: COUNT, SUM, AVG, MIN, MAX, GROUP BY
4. **Indexes**: B-tree indexes for fast lookups
5. **Persistence**: Save/load tables to disk
6. **SIMILAR TO**: Semantic similarity in WHERE clauses
7. **Real embeddings**: Integration with embedding models
8. **Batch operations**: INSERT multiple rows
9. **UPDATE**: Modify existing rows
10. **ALTER TABLE**: Change schema

## Examples

### Basic CRUD
```zig
var db = Database.init(allocator);
defer db.deinit();

_ = try db.execute("CREATE TABLE users (id int, name text, email text)");
_ = try db.execute("INSERT INTO users VALUES (1, \"Alice\", \"alice@example.com\")");
var results = try db.execute("SELECT * FROM users WHERE id = 1");
defer results.deinit();
_ = try db.execute("DELETE FROM users WHERE id = 1");
```

### Semantic Search
```zig
var db = Database.init(allocator);
defer db.deinit();
try db.initVectorSearch(16, 200);

_ = try db.execute("CREATE TABLE posts (id int, title text, content text)");
_ = try db.execute("INSERT INTO posts VALUES (1, \"DB Guide\", \"Learn databases...\")");
_ = try db.execute("INSERT INTO posts VALUES (2, \"Zig Tutorial\", \"Learn Zig...\")");

var results = try db.execute(
    "SELECT * FROM posts ORDER BY SIMILARITY TO \"database tutorial\" LIMIT 5"
);
defer results.deinit();
try results.print();
```

### Hybrid Queries
```zig
// Combine WHERE filtering with semantic search
var results = try db.execute(
    "SELECT title, score FROM articles " ++
    "WHERE published = true " ++
    "ORDER BY SIMILARITY TO \"machine learning\" LIMIT 10"
);
defer results.deinit();
```

## Testing

Run the comprehensive test suite:
```bash
zig build test
```

Tests cover:
- CREATE TABLE with various schemas
- INSERT with/without column names
- SELECT with *, specific columns, WHERE, LIMIT
- DELETE with WHERE
- NULL values
- Multiple data types
- Semantic search
- ORDER BY VIBES
- Case insensitive keywords

## Contributing

Ideas for improving the SQL interface:

1. **Parser improvements**: Better error messages, more SQL features
2. **Type system**: Stronger type checking, type inference
3. **Optimizations**: Query planning, index usage, caching
4. **Features**: JOINs, aggregations, subqueries
5. **Persistence**: Table serialization/deserialization
6. **Parody elements**: More fun features like VIBES!

## Conclusion

ZVDB's SQL interface bridges the gap between traditional databases and modern vector search. It's familiar enough for anyone who knows SQL, but powerful enough to handle semantic search at scale.

Whether you're building a:
- **Document search engine** with semantic ranking
- **Recommendation system** with hybrid queries
- **Knowledge graph** with SQL + GraphRAG
- **Fun side project** that needs ORDER BY VIBES

ZVDB's SQL interface has you covered! 🚀
