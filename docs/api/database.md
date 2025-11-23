# Database API Reference

SQL database with MVCC transactions, vector search, and persistence.

## Type

```zig
pub const Database = struct
```

Main database instance managing tables, indexes, and transactions.

## Initialization

### `init(allocator) Database`

Create new database with default configuration:
- Validation enabled (strict mode)
- Auto-VACUUM enabled
- Max 10 embedding columns per table
- WAL disabled

### `deinit(self) void`

Clean up resources. Auto-saves if `auto_save` enabled.

## SQL Execution

### `execute(self, sql) !QueryResult`

Execute SQL statement. Returns result set for queries, empty result for DDL/DML.

Supported commands:
- **DDL**: `CREATE TABLE`, `DROP TABLE`, `ALTER TABLE`, `CREATE INDEX`, `DROP INDEX`
- **DML**: `INSERT`, `UPDATE`, `DELETE`
- **DQL**: `SELECT` with `WHERE`, `JOIN`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`
- **Transactions**: `BEGIN`, `COMMIT`, `ROLLBACK`
- **Maintenance**: `VACUUM`

Caller must call `result.deinit()` after use.

## Persistence

### `enableWal(self, wal_path) !void`

Enable write-ahead logging for durability.

- `wal_path`: Path to WAL file

All subsequent writes logged before execution.

### `recoverFromWal(self, wal_path) !void`

Replay WAL to restore database state after crash.

### `saveAll(self, data_dir) !void`

Save all tables and indexes to directory.

- Creates `{data_dir}/{table_name}.table` for each table
- Creates `{data_dir}/hnsw_{dimension}_{column}.idx` for vector indexes
- Preserves full MVCC version chains

### `loadAll(self, data_dir) !void`

Load database from directory. Reconstructs tables and indexes.

### `rebuildHnswFromTables(self) !void`

Rebuild all HNSW indexes from table data. Use after bulk operations or corruption.

## Transaction Management

Managed automatically via SQL commands:

```zig
try db.execute("BEGIN");
try db.execute("INSERT INTO users VALUES (1, 'alice')");
try db.execute("COMMIT");  // or ROLLBACK
```

Transaction isolation level: Snapshot Isolation

See [transactions.md](transactions.md) for details.

## Configuration

### Validation

```zig
db.enable_validation = true;              // Master switch
db.validation_mode = .strict;             // .strict, .warnings, .disabled
```

Strict mode: Invalid queries fail immediately
Warning mode: Logs issues but continues
Disabled: No validation (backward compatibility)

### Auto-VACUUM

```zig
db.vacuum_config = .{
    .enabled = true,
    .max_chain_length = 10,    // Trigger after version chain exceeds this
    .txn_interval = 1000,       // Trigger every N transactions
};
```

### Resource Limits

```zig
db.max_embeddings_per_row = 10;  // Prevent resource exhaustion
```

### Auto-Save

```zig
db.data_dir = "/path/to/data";
db.auto_save = true;  // Save on deinit
```

## Query Result

### `QueryResult`

```zig
struct {
    columns: ArrayList([]const u8),
    rows: ArrayList(ArrayList(ColumnValue)),
    allocator: Allocator,
}
```

### `deinit(self) void`

Free result memory. Must call after `execute()`.

### `print(self) !void`

Pretty-print result to stderr as formatted table.

## Semantic Search

```sql
SELECT * FROM docs
ORDER BY SIMILARITY TO 'search query'
LIMIT 5
```

Automatically generates embedding for query text and searches HNSW index.

## Error Handling

Common errors:
- `error.TableNotFound`: Reference to non-existent table
- `error.ColumnNotFound`: Invalid column name
- `error.TypeMismatch`: Incompatible types in operation
- `error.ParseError`: Malformed SQL syntax
- `error.WriteWriteConflict`: Transaction conflict (retry transaction)
- `error.ValidationError`: Query validation failed (strict mode)

## Internal Fields

Do not access directly unless extending:

- `tables`: StringHashMap of Table pointers
- `hnsw_indexes`: HashMap of HNSW indexes by (dimension, column_name)
- `index_manager`: B-tree indexes
- `tx_manager`: Transaction ID generation and snapshot management
- `wal`: Optional WAL writer
