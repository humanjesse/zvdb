# Table API Reference

Low-level table storage with MVCC support. Typically accessed via SQL, but can be used directly.

## Type

```zig
pub const Table = struct
```

Stores rows with versioning for MVCC transactions.

## Row Operations

### `insert(self, values) !u64`

Insert new row. Auto-generates row ID.

- `values`: StringHashMap(ColumnValue) of column→value mappings
- Returns: Generated row ID

### `insertWithId(self, id, values, tx_id) !void`

Insert row with specific ID and transaction ID. Used by executor.

### `get(self, id, snapshot, clog) ?*Row`

Retrieve row by ID with MVCC visibility rules.

- `id`: Row ID
- `snapshot`: Transaction snapshot (null = current)
- `clog`: Commit log for visibility checks
- Returns: Visible row or null

### `update(self, row_id, column, new_value, tx_id, clog) !void`

Update single column value. Creates new row version.

### `delete(self, id, tx_id, clog) !void`

Mark row as deleted by setting `xmax` to transaction ID. Does not remove physically until VACUUM.

### `getAllRows(self, allocator, snapshot, clog) ![]u64`

Get all visible row IDs for snapshot. Caller owns returned slice.

## Schema

### `Column`

```zig
struct {
    name: []const u8,
    column_type: ColumnType,
}
```

### `ColumnType`

```zig
enum {
    int,
    float,
    text,
    bool,
    embedding,  // Vector type with dimension
}
```

### `ColumnValue`

```zig
union(enum) {
    int: i64,
    float: f64,
    text: []const u8,
    bool: bool,
    embedding: []const f32,
    null_value,
}
```

## Row Structure

### `Row`

```zig
struct {
    id: u64,
    values: StringHashMap(ColumnValue),
    xmin: u64,       // Creating transaction
    xmax: ?u64,      // Deleting transaction (null = alive)
    next_version: ?*Row,  // Next row version in chain
}
```

### `get(self, column) ?ColumnValue`

Get column value from row.

## VACUUM

### `vacuum(self, min_visible_txid, clog) !VacuumStats`

Remove old row versions no longer visible to any transaction.

- `min_visible_txid`: Oldest active transaction ID
- Returns: Statistics about cleaned versions

### `getVacuumStats(self) VacuumStats`

Query current version chain statistics without modifying data.

### `VacuumStats`

```zig
struct {
    dead_versions: usize,
    live_versions: usize,
    reclaimed_bytes: usize,
}
```

## Typical Usage

Most applications use SQL instead of direct table operations:

```zig
// Via SQL (recommended)
try db.execute("INSERT INTO users VALUES (1, 'alice')");

// Direct API (advanced)
var values = StringHashMap(ColumnValue).init(allocator);
try values.put("id", .{ .int = 1 });
try values.put("name", .{ .text = "alice" });
_ = try table.insert(values);
```

## Thread Safety

Not thread-safe. Synchronization handled by Database layer.

## MVCC Details

- Each update creates new row version linked via `next_version`
- Deletes mark row with `xmax`, physical removal requires VACUUM
- Snapshot determines version visibility
- See [transactions.md](transactions.md) for isolation semantics
