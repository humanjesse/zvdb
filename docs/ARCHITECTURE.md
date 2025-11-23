# ZVDB Architecture

System design and implementation overview for contributors.

## Component Overview

```
┌─────────────────────────────────────────────────┐
│                Database (database.zig)          │
│  - SQL execution coordinator                    │
│  - Table registry                               │
│  - Transaction management                       │
└────────┬───────────────────────┬────────────────┘
         │                       │
    ┌────▼────┐            ┌─────▼─────┐
    │  SQL    │            │  HNSW     │
    │ Parser  │            │  Indexes  │
    └────┬────┘            └───────────┘
         │
    ┌────▼────────┐
    │  Validator  │
    └────┬────────┘
         │
    ┌────▼─────────┐
    │  Executor    │
    │  (7 modules) │
    └────┬─────────┘
         │
    ┌────▼────┐        ┌──────────┐
    │  Table  │◄───────┤   WAL    │
    │ Storage │        │  Writer  │
    └────┬────┘        └──────────┘
         │
    ┌────▼────┐
    │  B-tree │
    │ Indexes │
    └─────────┘
```

## Core Modules

### database.zig (Database)

Main entry point. Coordinates all subsystems.

**Responsibilities:**
- Parse SQL and route to appropriate executor
- Manage table lifecycle
- Coordinate HNSW indexes (per dimension+column)
- Transaction management via TransactionManager
- Validation configuration
- Persistence orchestration

**Key structures:**
- `tables: StringHashMap(*Table)` - Table registry
- `hnsw_indexes: HashMap(HnswIndexKey, *HNSW(f32))` - Vector indexes
- `tx_manager: TransactionManager` - Transaction state
- `index_manager: IndexManager` - B-tree indexes

### sql.zig (Parser)

77K+ lines. Tokenizes and parses SQL into AST.

**Tokenizer:**
- Lexical analysis: keywords, identifiers, literals, operators
- Array literal support: `[0.1, 0.2, 0.3]`

**Parser:**
- Recursive descent parsing
- Produces AST nodes: SelectStmt, InsertStmt, CreateTableStmt, etc.
- Expression parsing with operator precedence

**AST Nodes:**
- Statement types (DDL, DML, DQL, transaction)
- Expression types (binary op, function call, column ref, subquery)
- Join specifications
- WHERE/HAVING conditions

### validator.zig (Query Validation)

Semantic analysis before execution.

**Checks:**
- Table existence
- Column existence and types
- Type compatibility in expressions
- Aggregate usage rules
- Schema constraints

**Modes:**
- Strict: Errors fail query
- Warnings: Logs issues but continues
- Disabled: No validation

### Executor (database/executor/)

7 specialized modules:

1. **ddl.zig** - CREATE/DROP TABLE, ALTER TABLE, CREATE/DROP INDEX
2. **insert.zig** - INSERT statements with embedding support
3. **select.zig** - SELECT with WHERE, ORDER BY, LIMIT
4. **update.zig** - UPDATE with MVCC version creation
5. **delete.zig** - DELETE with MVCC soft delete
6. **join.zig** - JOIN execution with hash/nested-loop strategies
7. **aggregate.zig** - GROUP BY and aggregate functions

**Execution flow:**
1. Receive validated AST
2. Access tables and indexes
3. Apply MVCC visibility rules
4. Execute operation
5. Return QueryResult

### table.zig (Table Storage)

57K+ lines. Row storage with MVCC.

**Row structure:**
```zig
Row {
    id: u64,
    values: StringHashMap(ColumnValue),
    xmin: u64,       // Creating transaction
    xmax: ?u64,      // Deleting transaction
    next_version: ?*Row,  // Version chain
}
```

**Operations:**
- Insert: Append new row
- Update: Create new version, link via next_version
- Delete: Set xmax, physical removal deferred to VACUUM
- Get: Apply visibility rules based on snapshot

**MVCC visibility:**
Row visible to transaction T if:
- `xmin` committed before T's snapshot
- `xmax` is null OR `xmax` committed after T's snapshot

### hnsw.zig (Vector Index)

60K+ lines. HNSW algorithm for k-NN search.

**Algorithm:**
- Hierarchical graph: Multiple layers with decreasing density
- Greedy search: Descend layers, find nearest at each level
- Insert: Random level assignment, connect to nearest neighbors

**Parameters:**
- `M`: Max connections per node (typical: 16-32)
- `ef_construction`: Candidate list size during build (typical: 200-400)

**GraphRAG extensions:**
- Node metadata: Type, content ref, attributes
- Typed edges: Separate from HNSW connections
- Type indexing: HashMap(node_type → []external_id)
- File path indexing: HashMap(file_path → []external_id)

**Persistence:**
Binary format (v2):
- Header: Magic number, version, metadata
- Nodes: Point data + connections + metadata
- Edges: Graph edges separate from HNSW structure

### transaction.zig (MVCC Transactions)

20K+ lines. Snapshot isolation implementation.

**TransactionManager:**
- Issues transaction IDs (monotonic counter)
- Tracks active transactions
- Creates snapshots
- Maintains commit log

**Snapshot:**
```zig
Snapshot {
    tx_id: u64,
    active_tx_ids: []const u64,  // Active at snapshot creation
}
```

**CommitLog:**
Maps tx_id → status (in_progress, committed, aborted)

**Conflict detection:**
Write-write conflicts detected by comparing xmin/xmax during update.

### btree.zig (B-tree Indexes)

53K+ lines. B-tree implementation for fast lookups.

**Structure:**
- Internal nodes: Keys + child pointers
- Leaf nodes: Keys + row IDs
- Order: Configurable fanout

**Operations:**
- Insert: O(log N)
- Search: O(log N)
- Range scan: O(log N + k)

**Key types:**
Supports int, float, text via generic interface.

### wal.zig (Write-Ahead Log)

65K+ lines. Durability via logging.

**Format:**
Binary records:
- Record type (insert, update, delete, commit, abort)
- Transaction ID
- Table name
- Row data

**Recovery:**
1. Read WAL from beginning
2. Replay operations in order
3. Rebuild in-memory state
4. Continue from last committed transaction

**Checkpointing:**
Periodic flush to table files, truncate WAL.

## Data Flow

### Query Execution

```
SQL string
  → Tokenizer
  → Parser (AST)
  → Validator (semantic check)
  → Executor (execute plan)
  → Table operations
  → MVCC visibility check
  → Result set
```

### Insert with Vector

```
INSERT INTO docs VALUES (1, 'text', [0.1, 0.2])
  → Parser recognizes array literal
  → Executor extracts embedding column
  → Table.insert(row_data)
  → HNSW.insert(embedding, row_id)
  → WAL.log(insert_record)  [if enabled]
```

### Semantic Search

```
SELECT * FROM docs ORDER BY SIMILARITY TO 'query'
  → Parser identifies SIMILARITY
  → Validator ensures embedding column exists
  → Generate embedding for 'query' (placeholder)
  → HNSW.search(query_embedding, k)
  → Retrieve rows by IDs
  → Apply MVCC visibility
  → Return results
```

### Transaction Commit

```
COMMIT
  → Get transaction ID from context
  → Mark as committed in CommitLog
  → Release locks (if any)
  → Flush WAL [if enabled]
  → Return success
```

## Index Architecture

### HNSW Index Keys

Composite key: `(dimension, column_name)`

Allows multiple same-dimension embeddings per table:

```sql
CREATE TABLE docs (
    title_vec embedding(384),
    summary_vec embedding(384)  -- Both use separate indexes
)
```

Stored in: `HashMap(HnswIndexKey, *HNSW(f32))`

### B-tree Indexes

Created explicitly:

```sql
CREATE INDEX idx_name ON table(column)
```

Managed by IndexManager. Used for:
- WHERE clause filters
- JOIN conditions
- ORDER BY columns

## Query Optimization

### Cost-Based Join Selection

Executor chooses strategy based on table sizes:

- Hash join: O(n+m) for large tables
- Nested loop: O(n×m) but simpler for small tables

Threshold: ~100 rows

### Hash Join Implementation

1. Build phase: Smaller table → hash map
2. Probe phase: Larger table, lookup matches
3. Result: O(n+m) instead of O(n×m)

Up to 5000x speedup for large joins.

### Filter Pushdown

WHERE conditions applied early to reduce data scanned.

## Concurrency Model

### Thread Safety

- **HNSW:** Thread-safe via mutexes. Concurrent insert/search supported.
- **Table:** Not thread-safe. Database layer serializes access.
- **Database:** Single-threaded execution model.

### MVCC Concurrency

- Readers never block writers
- Writers never block readers
- Write-write conflicts detected and rejected
- No locking required for reads

## Memory Management

### Ownership

- Database owns tables and indexes
- Tables own rows
- Rows own column values
- Caller owns QueryResult (must deinit)

### Allocator Usage

- Single allocator passed at Database.init
- All subsystems use same allocator
- Arena allocators used for query execution (fast cleanup)

## File Organization

```
src/
├── zvdb.zig              # Public API exports
├── database.zig          # Main database (wraps core.zig)
├── database/
│   ├── core.zig          # Database implementation
│   ├── executor/         # Query executors
│   ├── persistence.zig   # Save/load
│   ├── recovery.zig      # WAL recovery
│   └── validator.zig     # Query validation
├── hnsw.zig              # Vector search
├── sql.zig               # Parser
├── table.zig             # Row storage
├── transaction.zig       # MVCC
├── btree.zig             # B-tree indexes
├── wal.zig               # Write-ahead log
└── test_*.zig            # 35+ test files
```

## Testing Strategy

- **Unit tests:** Per-module functionality
- **Integration tests:** Cross-module workflows
- **SQL tests:** End-to-end query execution
- **MVCC tests:** Concurrent transaction scenarios
- **Performance benchmarks:** 6 benchmark files

Run: `zig build test`

## Build System

build.zig defines:
- Library target: libzvdb.a
- Test targets: 35+ test suites
- Benchmark targets: Performance tests
- Example targets: Demo applications

## Extension Points

Adding features:

1. **New SQL command:** Add to parser → validator → executor
2. **New data type:** Add to ColumnType enum → executor modules → serialization
3. **New aggregate:** Add to aggregate.zig → parser
4. **New join strategy:** Add to join.zig cost model

## Performance Characteristics

- **Insert:** O(log N) for B-tree + O(log N) for HNSW
- **SELECT (no index):** O(N) table scan
- **SELECT (indexed):** O(log N) lookup
- **JOIN (hash):** O(n+m)
- **Vector search:** Sub-linear with HNSW
- **Transaction begin/commit:** O(1)
- **VACUUM:** O(versions)

## Future Directions

Potential improvements:
- Parallel query execution
- Serializable isolation level
- Foreign keys and constraints
- Query plan caching
- Compression
- Incremental VACUUM

See GitHub issues for detailed proposals.
