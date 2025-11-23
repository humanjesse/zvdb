# Hash Join Implementation Plan

## Overview

This document outlines the plan for implementing hash joins in ZVDB, a performance optimization that will reduce join time complexity from **O(n × m)** to **O(n + m)** for equi-joins.

## Current State

### Existing Implementation (Nested Loop Join)
- **Location**: `src/database/executor.zig:414-735` (`executeJoinSelect`)
- **Algorithm**: Nested loop - iterates all combinations of rows
- **Time Complexity**: O(n × m) where n, m are table sizes
- **Space Complexity**: O(1) - no additional memory
- **Supported Joins**: INNER, LEFT, RIGHT
- **Limitations**: Only 2-table joins, no multi-join support

### Current Performance Profile
```
10 rows × 10 rows = 100 comparisons
1,000 rows × 1,000 rows = 1,000,000 comparisons
10,000 rows × 10,000 rows = 100,000,000 comparisons (very slow!)
```

## Hash Join Design

### Algorithm Overview

**Two Phases: Build and Probe**

1. **Build Phase**: Hash the smaller table
   - Select the "build table" (ideally the smaller one)
   - For each row, hash the join key
   - Store row in hash table: `hash(join_key) -> [rows]`
   - Skip NULL keys (NULL ≠ NULL in SQL)

2. **Probe Phase**: Scan the larger table
   - For each row in "probe table"
   - Hash the join key and lookup in hash table
   - For each match, emit combined row
   - Track matched/unmatched for LEFT/RIGHT joins

**Time Complexity**: O(n + m) - linear in total table size
**Space Complexity**: O(min(n, m)) - hash table for smaller table

### Performance Improvement
```
10 rows × 10 rows = 20 operations (10 build + 10 probe)
1,000 rows × 1,000 rows = 2,000 operations (50x faster!)
10,000 rows × 10,000 rows = 20,000 operations (5000x faster!)
```

## Implementation Structure

### File Organization

```
src/database/
├── hash_join.zig          # New: Hash join implementation
├── executor.zig           # Modified: Add hash join path
└── core.zig               # Modified: Add hash utilities
```

### New Module: `src/database/hash_join.zig`

**Exported Types:**

```zig
/// Hash table for join operations
pub const JoinHashTable = struct {
    /// Hash buckets: hash -> array of row indices
    buckets: AutoHashMap(u64, ArrayList(u64)),
    allocator: Allocator,

    pub fn init(allocator: Allocator) JoinHashTable;
    pub fn deinit(self: *JoinHashTable) void;

    /// Add a row to the hash table
    pub fn insert(self: *JoinHashTable, key: ColumnValue, row_id: u64) !void;

    /// Lookup rows with matching key
    pub fn probe(self: *JoinHashTable, key: ColumnValue) ?[]const u64;

    /// Get all row IDs that were inserted (for tracking unmatched rows)
    pub fn getAllRowIds(self: *JoinHashTable, allocator: Allocator) ![]u64;
};

/// Hash a ColumnValue for join operations
pub fn hashColumnValue(value: ColumnValue) u64;

/// Execute a hash join (INNER, LEFT, or RIGHT)
pub fn executeHashJoin(
    allocator: Allocator,
    base_table: *Table,
    join_table: *Table,
    join_type: JoinType,
    left_column: []const u8,
    right_column: []const u8,
    select_all: bool,
    columns: []ColumnSpec,
) !QueryResult;
```

## Implementation Details

### 1. Hash Function (`hashColumnValue`)

**Requirements:**
- Deterministic: same value always produces same hash
- Fast: simple operations
- Good distribution: minimize collisions
- Handle all ColumnValue types

**Implementation Strategy:**

```zig
pub fn hashColumnValue(value: ColumnValue) u64 {
    var hasher = std.hash.Wyhash.init(0);

    switch (value) {
        .null_value => {
            // Special hash for NULL (though we skip NULLs in practice)
            hasher.update(&[_]u8{0});
        },
        .int => |i| {
            const bytes = std.mem.asBytes(&i);
            hasher.update(bytes);
        },
        .float => |f| {
            // Use bit representation to handle float equality
            const bits = @as(u64, @bitCast(f));
            const bytes = std.mem.asBytes(&bits);
            hasher.update(bytes);
        },
        .text => |s| {
            hasher.update(s);
        },
        .bool => |b| {
            hasher.update(&[_]u8{if (b) 1 else 0});
        },
        .embedding => {
            // Embeddings shouldn't be join keys, but handle anyway
            // Hash first few elements for simplicity
            // (In practice, joining on embeddings is nonsensical)
            return 0; // Or error
        },
    }

    return hasher.final();
}
```

**Edge Cases:**
- **NULL values**: Skip during build/probe (NULL ≠ NULL)
- **Float precision**: Use bit-exact representation
- **String case**: Case-sensitive hashing (SQL standard)
- **Type mismatches**: Use `valuesEqual()` for final verification

### 2. JoinHashTable Structure

**Data Structure:**

```zig
pub const JoinHashTable = struct {
    /// Hash buckets: hash value -> list of row IDs
    buckets: AutoHashMap(u64, ArrayList(u64)),

    /// Track all inserted row IDs (for LEFT/RIGHT join tracking)
    all_row_ids: ArrayList(u64),

    /// Track which rows were matched (for LEFT/RIGHT joins)
    matched_rows: AutoHashMap(u64, bool),

    allocator: Allocator,
};
```

**Build Phase Implementation:**

```zig
pub fn build(
    allocator: Allocator,
    table: *Table,
    join_column: []const u8,
) !JoinHashTable {
    var hash_table = JoinHashTable.init(allocator);
    errdefer hash_table.deinit();

    // Get all rows from the build table
    const row_ids = try table.getAllRows(allocator);
    defer allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id) orelse continue;
        const key_value = row.get(join_column) orelse continue;

        // Skip NULL keys (NULL doesn't match NULL in SQL)
        if (key_value == .null_value) continue;

        // Hash the join key
        const hash = hashColumnValue(key_value);

        // Add to hash table
        try hash_table.insert(hash, row_id);
        try hash_table.all_row_ids.append(row_id);
    }

    return hash_table;
}
```

**Probe Phase Implementation:**

```zig
pub fn probe(
    self: *JoinHashTable,
    key: ColumnValue,
) ?[]const u64 {
    // Skip NULL (NULL doesn't match)
    if (key == .null_value) return null;

    const hash = hashColumnValue(key);

    if (self.buckets.get(hash)) |row_ids| {
        return row_ids.items;
    }

    return null;
}
```

### 3. Hash Join Execution

**INNER JOIN:**

```zig
// Build phase: hash the smaller table
const build_hash = try JoinHashTable.build(allocator, join_table, right_column);
defer build_hash.deinit();

// Probe phase: scan the larger table
for (base_row_ids) |base_id| {
    const base_row = base_table.get(base_id) orelse continue;
    const probe_key = base_row.get(left_column) orelse continue;

    // Lookup matches in hash table
    if (build_hash.probe(probe_key)) |matching_ids| {
        for (matching_ids) |join_id| {
            const join_row = join_table.get(join_id) orelse continue;

            // Double-check equality (handle hash collisions)
            const right_val = join_row.get(right_column) orelse continue;
            if (!valuesEqual(probe_key, right_val)) continue;

            // Emit matched row
            try emitJoinedRow(result, base_row, join_row, ...);
        }
    }
}
```

**LEFT JOIN:**

```zig
// Same as INNER JOIN, but track unmatched base rows

var matched_base_rows = AutoHashMap(u64, bool).init(allocator);
defer matched_base_rows.deinit();

// Probe phase (same as INNER)
for (base_row_ids) |base_id| {
    const base_row = base_table.get(base_id) orelse continue;
    const probe_key = base_row.get(left_column) orelse {
        // Base row has NULL key - emit with NULLs for join table
        try emitLeftJoinRow(result, base_row, null, ...);
        continue;
    };

    if (build_hash.probe(probe_key)) |matching_ids| {
        for (matching_ids) |join_id| {
            const join_row = join_table.get(join_id) orelse continue;
            const right_val = join_row.get(right_column) orelse continue;

            if (valuesEqual(probe_key, right_val)) {
                try emitJoinedRow(result, base_row, join_row, ...);
                try matched_base_rows.put(base_id, true);
            }
        }
    }
}

// Emit unmatched base rows with NULLs
for (base_row_ids) |base_id| {
    if (!matched_base_rows.contains(base_id)) {
        const base_row = base_table.get(base_id) orelse continue;
        try emitLeftJoinRow(result, base_row, null, ...);
    }
}
```

**RIGHT JOIN:**

```zig
// Build from base table, probe with join table (reverse roles)
// Track unmatched join table rows

const build_hash = try JoinHashTable.build(allocator, base_table, left_column);
defer build_hash.deinit();

var matched_join_rows = AutoHashMap(u64, bool).init(allocator);
defer matched_join_rows.deinit();

// Probe with join table
for (join_row_ids) |join_id| {
    const join_row = join_table.get(join_id) orelse continue;
    const probe_key = join_row.get(right_column) orelse {
        try emitRightJoinRow(result, null, join_row, ...);
        continue;
    };

    if (build_hash.probe(probe_key)) |matching_ids| {
        for (matching_ids) |base_id| {
            const base_row = base_table.get(base_id) orelse continue;
            const left_val = base_row.get(left_column) orelse continue;

            if (valuesEqual(left_val, probe_key)) {
                try emitJoinedRow(result, base_row, join_row, ...);
                try matched_join_rows.put(join_id, true);
            }
        }
    }
}

// Emit unmatched join rows with NULLs
for (join_row_ids) |join_id| {
    if (!matched_join_rows.contains(join_id)) {
        const join_row = join_table.get(join_id) orelse continue;
        try emitRightJoinRow(result, null, join_row, ...);
    }
}
```

### 4. Cost-Based Optimizer

**Table Size Estimation:**

```zig
fn estimateTableSize(table: *Table) !usize {
    // Use B-tree metadata for fast size estimation
    // Fallback: count rows (slower but accurate)
    return table.row_count; // Assuming we track this
}
```

**Join Algorithm Selection:**

```zig
fn shouldUseHashJoin(base_table_size: usize, join_table_size: usize) bool {
    const total_size = base_table_size + join_table_size;

    // Thresholds (tunable):
    const MIN_SIZE_FOR_HASH = 100; // Below this, nested loop is fine
    const HASH_SIZE_RATIO = 0.1;   // Build table should be < 10% of memory

    // Use hash join if:
    // 1. Tables are large enough to benefit
    if (total_size < MIN_SIZE_FOR_HASH) return false;

    // 2. We have memory for the hash table
    const smaller_size = @min(base_table_size, join_table_size);
    const estimated_memory = smaller_size * 32; // ~32 bytes per entry
    const available_memory = 100_000_000; // 100MB threshold (configurable)

    if (estimated_memory > available_memory) return false;

    // 3. Hash join is expected to be faster
    // Nested loop: n*m comparisons
    // Hash join: n+m operations + some overhead
    const nested_cost = base_table_size * join_table_size;
    const hash_cost = total_size * 5; // Factor for hash overhead

    return hash_cost < nested_cost;
}
```

**Integration in Executor:**

```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // ... existing code ...

    const base_size = try estimateTableSize(base_table);
    const join_size = try estimateTableSize(join_table);

    if (shouldUseHashJoin(base_size, join_size)) {
        // Use hash join
        const hash_join = @import("hash_join.zig");
        return hash_join.executeHashJoin(
            db.allocator,
            base_table,
            join_table,
            join.join_type,
            left_parts.column,
            right_parts.column,
            select_all,
            cmd.columns.items,
        );
    } else {
        // Use existing nested loop join
        // ... existing nested loop code ...
    }
}
```

## Testing Strategy

### Unit Tests (`src/test_hash_join.zig`)

**1. Hash Function Tests**
- Test hash determinism (same value = same hash)
- Test hash distribution (different values = different hashes, mostly)
- Test all ColumnValue types
- Test NULL handling
- Test collision handling

**2. Hash Table Tests**
- Test build phase with small table
- Test probe phase with matches
- Test probe phase with no matches
- Test multiple rows per key (1-to-many join)
- Test empty tables
- Test NULL keys (should skip)

**3. Correctness Tests**
- Compare hash join results with nested loop (should be identical)
- Test INNER JOIN with various data
- Test LEFT JOIN with unmatched rows
- Test RIGHT JOIN with unmatched rows
- Test with duplicate join keys
- Test with all NULLs in join column
- Test mixed data types (int, float, text, bool)

**4. Edge Cases**
- Empty left table
- Empty right table
- Both tables empty
- All rows match (cross product)
- No rows match
- Single row in each table
- Large number of duplicates in join key

### Performance Benchmarks (`src/bench_joins.zig`)

**Benchmark Scenarios:**

```zig
test "benchmark: small tables (100 x 100)" {
    // Expect: nested loop comparable or faster
}

test "benchmark: medium tables (1K x 1K)" {
    // Expect: hash join 10-50x faster
}

test "benchmark: large tables (10K x 10K)" {
    // Expect: hash join 100-1000x faster
}

test "benchmark: skewed tables (10 x 10K)" {
    // Expect: hash join significantly faster
}

test "benchmark: many duplicates" {
    // Test: 1000 rows, 10 unique join keys
}
```

**Metrics to Track:**
- Execution time (nanoseconds)
- Memory usage (bytes allocated)
- Number of comparisons
- Hash collisions
- Cache efficiency (if measurable)

### Integration Tests

**Use Existing Test Suite:**
- Run `src/test_joins.zig` with hash join enabled
- All tests should pass with identical results
- Verify no regressions in correctness
- Add new tests for large data sets

## Implementation Phases

### Phase 1: Core Hash Join (Days 1-2)
1. Create `src/database/hash_join.zig`
2. Implement `hashColumnValue()` function
3. Implement `JoinHashTable` struct
4. Write unit tests for hash function and hash table

**Deliverable:** Working hash table with tests

### Phase 2: INNER JOIN (Day 3)
1. Implement `executeHashJoin()` for INNER JOIN
2. Integrate with `executor.zig` (add hash join path)
3. Write correctness tests (compare with nested loop)
4. Basic benchmarks

**Deliverable:** Hash join for INNER JOIN, verified correct

### Phase 3: LEFT and RIGHT JOINs (Day 4)
1. Extend `executeHashJoin()` for LEFT JOIN
2. Extend `executeHashJoin()` for RIGHT JOIN
3. Add tests for unmatched row handling
4. Test edge cases (all NULL, empty tables, etc.)

**Deliverable:** Complete hash join for all join types

### Phase 4: Optimizer Integration (Day 5)
1. Implement `estimateTableSize()`
2. Implement `shouldUseHashJoin()` decision logic
3. Add optimizer to `executeJoinSelect()`
4. Tune thresholds based on benchmarks
5. Add logging for optimizer decisions (debug mode)

**Deliverable:** Automatic hash join selection

### Phase 5: Testing and Benchmarking (Day 6)
1. Run full test suite
2. Performance benchmarks at multiple scales
3. Stress tests (100K+ rows)
4. Memory leak detection
5. Document performance improvements

**Deliverable:** Validated, benchmarked implementation

## Success Criteria

### Correctness
- All existing JOIN tests pass
- Hash join produces identical results to nested loop
- NULL handling matches SQL standard
- Edge cases handled correctly

### Performance
- **10x faster** for 1K × 1K joins
- **100x faster** for 10K × 10K joins
- **No regression** for small tables (< 100 rows)
- **Memory efficient**: hash table stays under 100MB for reasonable data

### Code Quality
- Clean, readable code with comments
- Comprehensive test coverage (>90%)
- No memory leaks
- Error handling for edge cases

## Future Enhancements

### Short-term
- **Grace Hash Join**: Spill to disk for very large tables
- **Parallel Hash Join**: Multi-threaded build/probe
- **Bloom Filters**: Early filtering to reduce probes
- **Join Order Optimization**: For multi-table joins

### Long-term
- **Merge Join**: For pre-sorted data
- **Adaptive Join**: Switch algorithms mid-execution
- **Index-Nested Loop Join**: Use B-tree indexes for small probes
- **Vectorized Execution**: SIMD for hash/compare operations

## References

- **PostgreSQL Hash Join**: https://www.postgresql.org/docs/current/planner-optimizer.html
- **SQL Server Hash Join**: https://learn.microsoft.com/en-us/sql/relational-databases/query-processing-architecture-guide
- **Database Internals (Book)**: Chapter on Join Algorithms
- **CMU Database Systems Course**: Lecture on Join Algorithms

---

**Status**: Ready for implementation
**Assignee**: TBD
**Estimated Time**: 5-6 days
**Priority**: High (significant performance impact)
