# ZVDB

A hybrid vector-relational database written in pure Zig. Combines SQL operations, vector similarity search, and graph relationships in a single ACID-compliant system.

## Features

**Vector Search**
- HNSW algorithm for fast k-NN search
- Cosine similarity for embeddings
- Supports 128-1024+ dimensional vectors
- Per-dimension indexing

**SQL Database**
- Full SQL support (DDL, DML, DQL)
- MVCC transactions with snapshot isolation
- B-tree and hash join optimization
- Query validation and cost-based optimizer

**GraphRAG**
- Typed nodes with rich metadata
- Typed edges with weights
- Graph traversal (BFS with filtering)
- Hybrid vector + graph queries

**Persistence**
- Write-ahead logging (WAL)
- Crash recovery
- Binary persistence format
- Zero external dependencies

## Quick Start

```zig
const zvdb = @import("zvdb");

// Initialize database
var db = zvdb.Database.init(allocator);
defer db.deinit();

// Create table with embedding column
try db.execute("CREATE TABLE docs (id int, content text, embedding embedding(384))");

// Insert with vector
try db.execute("INSERT INTO docs VALUES (1, 'hello', [0.1, 0.2, ...])");

// Semantic search (uses hash-based embedding for demo - integrate real model for production)
const result = try db.execute("SELECT * FROM docs ORDER BY SIMILARITY TO 'search query' LIMIT 5");
defer result.deinit();
```

## Building

**Requirements:** Zig 0.15.2+

```bash
zig build                  # Build library
zig build test             # Run tests
zig build demo             # Run SQL demo
```

## Documentation

- [Getting Started](docs/GETTING_STARTED.md) - Installation and first steps
- [User Guide](docs/USER_GUIDE.md) - Comprehensive usage reference
- [API Reference](docs/api/) - Function-level documentation
- [Architecture](docs/ARCHITECTURE.md) - System design for contributors
- [Contributing](docs/CONTRIBUTING.md) - Development guide
- [Examples](docs/EXAMPLES.md) - Real-world patterns

## Status

Production-ready features:
- SQL operations (CREATE, INSERT, SELECT, UPDATE, DELETE)
- MVCC transactions (BEGIN, COMMIT, ROLLBACK)
- Vector search with HNSW
- Graph relationships and traversal
- Hash joins and query optimization
- WAL and persistence

See [docs/USER_GUIDE.md](docs/USER_GUIDE.md) for feature details and limitations.

## License

MIT
