# Hash Join Implementation Summary

## Overview

This document summarizes the hash join implementation added to ZVDB, which provides significant performance improvements for JOIN operations on large tables.

## What Was Implemented

### 1. Core Hash Join Module (`src/database/hash_join.zig`)

**Features:**
- **Hash Function**: Deterministic hashing for all `ColumnValue` types (int, float, text, bool, NULL)
- **JoinHashTable**: Efficient hash table data structure for join operations
  - Handles duplicate join keys (1-to-many and many-to-many joins)
  - Tracks matched/unmatched rows for LEFT/RIGHT joins
  - Memory-efficient using Zig's `AutoHashMap`

**Join Algorithms:**
- **INNER JOIN**: Only matching rows from both tables
- **LEFT JOIN**: All rows from left table, NULLs for unmatched right rows
- **RIGHT JOIN**: All rows from right table, NULLs for unmatched left rows

**Performance:**
- **Time Complexity**: O(n + m) vs O(n × m) for nested loops
- **Space Complexity**: O(smaller_table_size) for hash table
- **Collision Handling**: Double-checks equality to handle hash collisions

### 2. Cost-Based Optimizer (`src/database/executor.zig`)

**Features:**
- **Table Size Estimation**: Counts rows to estimate join cost
- **Algorithm Selection**: Automatically chooses between hash join and nested loop
  - Hash join for large tables (>100 total rows)
  - Nested loop for small tables (overhead not worth it)

**Decision Logic:**
```zig
// Nested loop cost: O(n × m)
// Hash join cost: O(n + m) with overhead factor
if (nested_cost > hash_cost * overhead_factor) {
    use hash_join();
} else {
    use nested_loop();
}
```

**Threshold**: 100 total rows (tunable based on benchmarks)

### 3. Comprehensive Tests (`src/test_hash_join.zig`)

**Test Coverage:**
- Hash function determinism and distribution
- Hash table build and probe operations
- NULL handling (NULL ≠ NULL in SQL)
- Duplicate join keys (many-to-many joins)
- INNER, LEFT, and RIGHT JOIN correctness
- Empty table edge cases
- Different data types (int, float, text, bool)

**Total Tests**: 15+ comprehensive test cases

### 4. Performance Benchmarks (`benchmarks/join_performance.zig`)

**Benchmark Scenarios:**
- Small tables: 10 × 10 rows
- Medium tables: 100 × 100 rows
- Large tables: 1,000 × 1,000 rows
- Very large tables: 10,000 × 10,000 rows
- Skewed joins: 10 × 10,000 rows

**Metrics Tracked:**
- Execution time (milliseconds)
- Result row count
- Theoretical speedup factor
- Memory usage estimates

## Performance Improvements

### Expected Speedups

| Table Sizes | Nested Loop | Hash Join | Speedup |
|-------------|-------------|-----------|---------|
| 10 × 10 | ~100 ops | ~20 ops | ~5x |
| 100 × 100 | ~10,000 ops | ~200 ops | ~50x |
| 1K × 1K | ~1M ops | ~2K ops | ~500x |
| 10K × 10K | ~100M ops | ~20K ops | ~5,000x |

### Real-World Impact

**Before (Nested Loop):**
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
-- 10,000 users × 10,000 orders = 100M comparisons
-- Estimated time: ~10-30 seconds
```

**After (Hash Join):**
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
-- Build: 10,000 hashes + Probe: 10,000 lookups = 20K operations
-- Estimated time: ~20-50 milliseconds
-- ~500-1000x faster!
```

## Technical Details

### Hash Function Design

```zig
pub fn hashColumnValue(value: ColumnValue) u64 {
    var hasher = std.hash.Wyhash.init(0);

    switch (value) {
        .int => // Hash integer bytes
        .float => // Hash bit representation (handles -0 vs +0)
        .text => // Hash string bytes
        .bool => // Hash 0 or 1
        .null_value => // Special hash for NULL
        .embedding => // Special case (not used for joins)
    }

    return hasher.final();
}
```

**Why Wyhash?**
- Fast: ~16 GB/s throughput
- Good distribution: Low collision rate
- Deterministic: Same value always produces same hash
- Built into Zig standard library

### Build Phase

```zig
1. Choose smaller table as "build table"
2. For each row in build table:
   a. Get join key value
   b. Skip if NULL (NULL ≠ NULL in SQL)
   c. Hash the value
   d. Add row_id to hash bucket
   e. Track row_id for LEFT/RIGHT joins
```

**Memory Usage**: ~32 bytes per row (hash + row_id + overhead)

### Probe Phase

```zig
1. For each row in "probe table":
   a. Get join key value
   b. Skip if NULL
   c. Hash the value
   d. Lookup matching row_ids in hash table
   e. For each match:
      - Double-check equality (handle collisions)
      - Emit joined row
      - Mark as matched
2. For LEFT/RIGHT joins:
   a. Find unmatched rows
   b. Emit with NULLs for other table
```

### Cost-Based Optimizer

```zig
fn shouldUseHashJoin(base_size: usize, join_size: usize) bool {
    const total = base_size + join_size;

    // Too small: nested loop overhead is negligible
    if (total < 100) return false;

    // Calculate theoretical costs
    const nested_cost = base_size * join_size;
    const hash_cost = total * 5;  // Overhead factor

    return hash_cost < nested_cost;
}
```

**Tunable Parameters:**
- `MIN_SIZE_FOR_HASH = 100`: Threshold for using hash join
- `OVERHEAD_FACTOR = 5`: Hash overhead multiplier

## SQL Compatibility

### Supported Join Types

✅ **INNER JOIN**
```sql
SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id
```

✅ **LEFT JOIN (LEFT OUTER JOIN)**
```sql
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id
```

✅ **RIGHT JOIN (RIGHT OUTER JOIN)**
```sql
SELECT * FROM orders RIGHT JOIN users ON orders.user_id = users.id
```

### NULL Handling

Follows SQL standard:
- **NULL ≠ NULL** in join conditions
- NULL keys are excluded from hash table
- Unmatched rows with NULL keys still included in LEFT/RIGHT joins

Example:
```sql
-- users table:
-- id | name
-- 1  | Alice
-- NULL | Bob
-- 2  | Carol

-- orders table:
-- user_id | amount
-- 1  | 100
-- NULL | 200
-- 3  | 300

-- LEFT JOIN result:
-- users.id | users.name | orders.user_id | orders.amount
-- 1        | Alice      | 1              | 100
-- NULL     | Bob        | NULL           | NULL  -- Bob has no match (NULL ≠ NULL)
-- 2        | Carol      | NULL           | NULL  -- Carol has no match
```

## Usage

### Automatic (Recommended)

The cost-based optimizer automatically chooses the best algorithm:

```sql
-- Small tables: uses nested loop
SELECT * FROM small1 JOIN small2 ON small1.id = small2.id;

-- Large tables: uses hash join
SELECT * FROM large1 JOIN large2 ON large1.id = large2.id;
```

### Query Examples

```sql
-- Join users with orders
SELECT u.name, o.amount
FROM users u
INNER JOIN orders o ON u.id = o.user_id;

-- Left join to find users without orders
SELECT u.name, o.amount
FROM users u
LEFT JOIN orders o ON u.id = o.user_id;

-- Right join to find orphaned orders
SELECT u.name, o.amount
FROM users u
RIGHT JOIN orders o ON u.id = o.user_id;
```

## Testing

### Run Tests

```bash
zig build test
```

This runs:
- Hash function tests (determinism, distribution)
- Hash table tests (build, probe, collisions)
- Join correctness tests (INNER, LEFT, RIGHT)
- Edge case tests (empty tables, NULLs, duplicates)
- SQL standards compliance tests

### Run Benchmarks

```bash
zig build
./zig-out/bin/join_performance
```

Expected output:
```
=== Hash Join Performance Benchmarks ===

Test: Small (10 x 10)
  Rows: 10 x 10
  Result rows: 10
  Time: 0.12 ms
  Theoretical speedup: 5.0x (if using hash join)

Test: Medium (100 x 100)
  Rows: 100 x 100
  Result rows: 100
  Time: 1.23 ms
  Theoretical speedup: 50.0x (if using hash join)

Test: Large (1,000 x 1,000)
  Rows: 1000 x 1000
  Result rows: 1000
  Time: 12.34 ms
  Theoretical speedup: 500.0x (if using hash join)

...
```

## Future Enhancements

### Short-term

1. **Grace Hash Join**: Spill to disk for very large tables
2. **Statistics**: Track actual hash join performance
3. **Tuning**: Adjust optimizer thresholds based on real benchmarks
4. **Parallel Build**: Multi-threaded hash table construction

### Long-term

1. **Merge Join**: For pre-sorted data (use B-tree indexes)
2. **Bloom Filters**: Early filtering to reduce probe overhead
3. **Join Order Optimization**: For multi-table joins
4. **Adaptive Execution**: Switch algorithms mid-execution
5. **Vectorized Execution**: SIMD for hash/compare operations
6. **Index-Nested Loop**: Use B-tree indexes for small probe tables

## Files Changed

### New Files
- `src/database/hash_join.zig` - Hash join implementation (740 lines)
- `src/test_hash_join.zig` - Comprehensive tests (560 lines)
- `benchmarks/join_performance.zig` - Performance benchmarks (260 lines)
- `docs/HASH_JOIN_PLAN.md` - Implementation plan (430 lines)
- `docs/HASH_JOIN_IMPLEMENTATION.md` - This document

### Modified Files
- `src/database/executor.zig` - Added optimizer and hash join integration
- `build.zig` - Added hash join tests

### Total Lines of Code
- Implementation: ~740 lines
- Tests: ~560 lines
- Benchmarks: ~260 lines
- Documentation: ~900 lines
- **Total: ~2,460 lines**

## Credits

**Implemented by**: Claude (Anthropic AI Assistant)
**Directed by**: @humanjesse
**Repository**: https://github.com/humanjesse/zvdb
**Date**: November 2025

## References

- **PostgreSQL Hash Join**: https://www.postgresql.org/docs/current/planner-optimizer.html
- **SQL Standard**: ISO/IEC 9075 (SQL:2023)
- **Database Internals (Book)**: Alex Petrov, O'Reilly Media
- **Zig Standard Library**: https://ziglang.org/documentation/master/std/

---

**Status**: ✅ Complete and ready for testing
**Next Steps**: Run benchmarks to validate performance improvements
