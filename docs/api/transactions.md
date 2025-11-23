# Transactions API Reference

MVCC (Multi-Version Concurrency Control) transactions with snapshot isolation.

## SQL Interface

Primary method for using transactions:

```zig
try db.execute("BEGIN");
try db.execute("INSERT INTO users VALUES (1, 'alice')");
try db.execute("UPDATE users SET name = 'Alice' WHERE id = 1");
try db.execute("COMMIT");  // or ROLLBACK
```

## Isolation Level

**Snapshot Isolation**: Each transaction sees consistent snapshot of database from transaction start.

### Guarantees

- Read operations never block write operations
- Write operations never block read operations
- Concurrent transactions see independent snapshots
- Write-write conflicts detected and rejected

### Anomaly Prevention

Prevents:
- Dirty reads ✓
- Non-repeatable reads ✓
- Phantom reads ✓

Does not prevent:
- Write skew (use explicit locking if needed)

## Transaction Lifecycle

### BEGIN

Start new transaction. Allocates transaction ID and creates snapshot.

```sql
BEGIN
```

Snapshot captures all committed transactions at this instant.

### COMMIT

Persist transaction changes. Marks transaction as committed.

```sql
COMMIT
```

Fails if write-write conflict detected.

### ROLLBACK

Discard transaction changes. Marks transaction as aborted.

```sql
ROLLBACK
```

## Conflict Detection

Write-write conflicts occur when two transactions modify the same row:

```zig
// Transaction T1
try db.execute("BEGIN");
try db.execute("UPDATE users SET balance = 100 WHERE id = 1");

// Transaction T2 (concurrent)
try db.execute("BEGIN");
try db.execute("UPDATE users SET balance = 200 WHERE id = 1");  // Conflict!

try db.execute("COMMIT");  // T1 commits first
try db.execute("COMMIT");  // T2 fails with error.WriteWriteConflict
```

Application should retry conflicted transactions.

## MVCC Internals

### Row Versioning

Each row has:
- `xmin`: Transaction ID that created this version
- `xmax`: Transaction ID that deleted/updated this version (null = current)
- `next_version`: Pointer to next row version

Updates create new row version. Deletes mark `xmax` without physical removal.

### Visibility Rules

Row version visible to transaction T if:
1. `xmin` committed before T started
2. `xmax` is null OR `xmax` committed after T started

### Snapshot

```zig
struct {
    tx_id: u64,                    // Transaction ID
    active_tx_ids: []const u64,    // Active transactions at snapshot time
}
```

Used by read operations to determine row visibility.

## VACUUM

Old row versions accumulate over time. Use VACUUM to reclaim space:

```sql
VACUUM
```

### Auto-VACUUM

Configurable automatic cleanup:

```zig
db.vacuum_config = .{
    .enabled = true,
    .max_chain_length = 10,    // Trigger when version chain exceeds length
    .txn_interval = 1000,       // Trigger every N transactions
};
```

### Manual VACUUM

```zig
const stats = try table.vacuum(min_active_tx_id, commit_log);
// Returns: { dead_versions, live_versions, reclaimed_bytes }
```

## Transaction Manager

Internal component managing transaction IDs and snapshots. Not directly accessed by users.

### TransactionManager

```zig
struct {
    next_tx_id: u64,
    active_transactions: AutoHashMap(u64, void),
    commit_log: CommitLog,
}
```

### CommitLog

Tracks transaction states:

```zig
enum {
    in_progress,
    committed,
    aborted,
}
```

## Performance Characteristics

- Read-only transactions: No overhead (snapshot creation is cheap)
- Write transactions: Version chain append (no locking)
- Conflicts: O(1) detection via xmin/xmax checks
- VACUUM: O(versions) scan

## Error Handling

```zig
error.WriteWriteConflict  // Concurrent update to same row
error.NoActiveTransaction  // COMMIT/ROLLBACK without BEGIN
```

## Limitations

- No savepoints (partial rollback)
- No read-only optimization flag
- No serializable isolation level
- VACUUM required for long-running workloads

## Best Practices

1. Keep transactions short
2. Retry on `WriteWriteConflict`
3. Run VACUUM periodically or enable auto-VACUUM
4. Use indexes to minimize scanned rows
5. Batch operations when possible
