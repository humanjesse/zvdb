# WHERE Clause Post-Join Evaluation Design

## Executive Summary

This document provides a detailed analysis of the current WHERE clause implementation in ZVDB and proposes a design for moving WHERE clause evaluation to the post-join phase to support N-table joins with filtering.

**Key Finding**: Currently, WHERE clauses are **NOT implemented** for JOIN queries. They are only supported for single-table SELECT queries.

---

## 1. Current WHERE Clause Implementation

### 1.1 Data Structures (src/sql.zig)

**SelectCmd Structure (Lines 118-159)**
```zig
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn),
    joins: ArrayList(JoinClause),
    where_column: ?[]const u8,      // Simple WHERE: column name
    where_value: ?ColumnValue,       // Simple WHERE: value to match
    similar_to_column: ?[]const u8,
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8,
    order_by_vibes: bool,
    group_by: ArrayList([]const u8),
    limit: ?usize,
};
```

**Expression Tree for Complex WHERE (Lines 261-285)**
```zig
pub const Expr = union(enum) {
    literal: ColumnValue,      // Constant value
    column: []const u8,        // Column reference
    binary: *BinaryExpr,       // AND, OR, =, !=, <, >, <=, >=
    unary: *UnaryExpr,         // NOT, IS NULL, IS NOT NULL
};
```

**Key Observation**: ZVDB supports two WHERE clause representations:
1. **Simple WHERE**: `where_column` + `where_value` (equality only)
2. **Complex WHERE**: `Expr` tree (used in UPDATE/DELETE commands)

However, `SelectCmd` currently uses only the simple WHERE representation.

### 1.2 Single-Table WHERE Implementation (src/database/executor.zig)

**Location**: `executeSelect()` function, lines 934-940

```zig
// Apply WHERE filter (skip if we already used an index to filter)
if (!use_index) {
    if (cmd.where_column) |where_col| {
        if (cmd.where_value) |where_val| {
            const row_val = row.get(where_col) orelse continue;
            if (!valuesEqual(row_val, where_val)) continue;
        }
    }
}
```

**Execution Flow**:
1. Get all row IDs from table (or from index if available)
2. For each row:
   - **Apply WHERE filter** ← Filtering happens HERE
   - If row matches, add to result
3. Apply LIMIT

**Performance**: WHERE is applied DURING row iteration, not after all rows are collected.

### 1.3 JOIN Query WHERE Implementation

**Location**: `executeJoinSelect()` function, lines 447-788

**Critical Finding**: **NO WHERE CLAUSE EVALUATION EXISTS**

Searching through the entire `executeJoinSelect()` function:
- No references to `cmd.where_column`
- No references to `cmd.where_value`
- No filtering based on WHERE conditions

Similarly, the hash join implementation (`src/database/hash_join.zig`, lines 1-664) also has:
- **NO WHERE clause evaluation**
- Only JOIN condition evaluation

**Conclusion**: WHERE clauses are currently **not supported** for JOIN queries. This is a missing feature.

---

## 2. Post-Join Row Format Analysis

### 2.1 Row Structure After 2-Table Join

**For SELECT * (Lines 488-507 in executor.zig)**:
```
Row Format: [base_table.col1, base_table.col2, ..., join_table.col1, join_table.col2, ...]
Column Names: ["users.id", "users.name", "orders.id", "orders.user_id", "orders.total"]
```

**For Specific Columns (Lines 554-574)**:
```
Row Format: [selected_col1, selected_col2, ...]
Column resolution:
  - Qualified names (table.column) → resolve to specific table
  - Unqualified names (column) → search base_table first, then join_table
```

### 2.2 Row Structure After N-Table Join (Proposed)

For a 3-table join: `users JOIN orders JOIN products`

**Memory Layout**:
```
Combined Row: [
    users.id, users.name,           // Table 1 (base)
    orders.id, orders.user_id,      // Table 2 (join 1)
    products.id, products.name      // Table 3 (join 2)
]
```

**Metadata Required for Column Resolution**:
```zig
// Track which columns come from which table
const TableBoundary = struct {
    table_name: []const u8,
    start_index: usize,  // First column index in combined row
    end_index: usize,    // Last column index + 1
};

// Example for 3-table join:
table_boundaries = [
    { "users", 0, 2 },      // columns[0..2]
    { "orders", 2, 4 },     // columns[2..4]
    { "products", 4, 6 },   // columns[4..6]
]
```

---

## 3. Post-Join WHERE Evaluation Design

### 3.1 Architectural Overview

```
┌─────────────────┐
│  Parse SQL      │
│  WHERE clause   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Execute Joins  │ ◄─── Currently: WHERE ignored
│  (All N tables) │      Proposed: No filtering here
└────────┬────────┘
         │
         ▼
┌─────────────────────────┐
│  Post-Join Filtering    │ ◄─── NEW PHASE
│  - Build column map     │
│  - Evaluate WHERE expr  │
│  - Filter joined rows   │
└────────┬────────────────┘
         │
         ▼
┌─────────────────┐
│  Project        │
│  (SELECT cols)  │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Apply LIMIT    │
└─────────────────┘
```

### 3.2 Column Resolution Strategy

**Problem**: After joining N tables, how do we resolve `WHERE users.age > 25 AND orders.total > 100`?

**Solution: JoinedRow Wrapper**

```zig
/// Represents a row formed by joining N tables
const JoinedRow = struct {
    /// Combined values from all tables (flattened)
    values: ArrayList(ColumnValue),

    /// Metadata: which table owns which column ranges
    table_metadata: []const TableMetadata,

    allocator: Allocator,

    const TableMetadata = struct {
        table_name: []const u8,
        column_names: []const []const u8,  // Original column names
        start_idx: usize,                   // Index in values array
        count: usize,                       // Number of columns
    };

    /// Get a column value by qualified name (e.g., "users.name")
    pub fn getQualified(self: *const JoinedRow, table_name: []const u8, col_name: []const u8) ?ColumnValue {
        // Find the table
        for (self.table_metadata) |meta| {
            if (std.mem.eql(u8, meta.table_name, table_name)) {
                // Find column within table
                for (meta.column_names, 0..) |name, i| {
                    if (std.mem.eql(u8, name, col_name)) {
                        return self.values.items[meta.start_idx + i];
                    }
                }
            }
        }
        return null;
    }

    /// Get a column value by unqualified name (searches all tables)
    pub fn getUnqualified(self: *const JoinedRow, col_name: []const u8) ?ColumnValue {
        // Search all tables in order
        for (self.table_metadata) |meta| {
            for (meta.column_names, 0..) |name, i| {
                if (std.mem.eql(u8, name, col_name)) {
                    return self.values.items[meta.start_idx + i];
                }
            }
        }
        return null;
    }

    /// Convert to a StringHashMap for compatibility with existing evaluateExpr
    pub fn toHashMap(self: *const JoinedRow) !StringHashMap(ColumnValue) {
        var map = StringHashMap(ColumnValue).init(self.allocator);

        for (self.table_metadata) |meta| {
            for (meta.column_names, 0..) |name, i| {
                const idx = meta.start_idx + i;
                const value = self.values.items[idx];

                // Store both qualified and unqualified names
                const qualified = try std.fmt.allocPrint(
                    self.allocator,
                    "{s}.{s}",
                    .{ meta.table_name, name },
                );
                try map.put(qualified, value);

                // Also store unqualified (if not ambiguous)
                if (!map.contains(name)) {
                    try map.put(name, value);
                }
            }
        }

        return map;
    }
};
```

### 3.3 WHERE Expression Evaluation

**Leverage Existing Infrastructure** (src/sql.zig, lines 1102-1260):

```zig
/// Evaluate an expression against a row's values
pub fn evaluateExpr(expr: Expr, row_values: anytype) bool {
    // This already exists! Just needs row_values to have all joined columns
}
```

**Key Insight**: The `evaluateExpr` function already exists and can handle:
- Column references
- Literal values
- Binary operators (AND, OR, =, !=, <, >, <=, >=)
- Unary operators (NOT, IS NULL, IS NOT NULL)

**What's Needed**: Pass a `StringHashMap` with all joined columns (qualified names).

### 3.4 Integration Points

**Current executeJoinSelect() structure**:
```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // 1. Get tables
    // 2. Execute join (nested loop or hash join)
    // 3. Return result
}
```

**Proposed executeJoinSelect() with WHERE**:
```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // 1. Get tables
    const base_table = db.tables.get(cmd.table_name) orelse return error.TableNotFound;

    // 2. Execute join to get intermediate result (all joined rows)
    var join_result = if (shouldUseHashJoin(...)) {
        try hash_join.executeHashJoin(...)
    } else {
        try executeNestedLoopJoin(...)
    };

    // 3. NEW: Apply WHERE clause to filter joined rows
    if (cmd.where_column != null or cmd.where_expr != null) {
        join_result = try applyWhereFilter(db.allocator, join_result, cmd, table_metadata);
    }

    // 4. Return filtered result
    return join_result;
}
```

---

## 4. Implementation Plan

### Phase 1: Add WHERE Support to SelectCmd

**File**: `src/sql.zig`

**Change 1**: Add `where_expr` field to `SelectCmd` (line 118)
```zig
pub const SelectCmd = struct {
    // ... existing fields ...
    where_column: ?[]const u8,      // Keep for backward compatibility
    where_value: ?ColumnValue,      // Keep for backward compatibility
    where_expr: ?Expr,              // NEW: Complex WHERE expressions
    // ... rest of fields ...
};
```

**Change 2**: Update `parseSelect()` to parse complex WHERE clauses (line 619)
```zig
// Parse WHERE clause
if (i < tokens.len and eqlIgnoreCase(tokens[i].text, "WHERE")) {
    i += 1;

    // Try to parse as complex expression
    where_expr = try parseExpr(allocator, tokens, &i);

    // For backward compatibility, also extract simple where_column/where_value
    // if the expression is a simple equality
    if (where_expr) |expr| {
        if (expr == .binary and expr.binary.op == .eq) {
            if (expr.binary.left == .column and expr.binary.right == .literal) {
                where_column = try allocator.dupe(u8, expr.binary.left.column);
                where_value = expr.binary.right.literal;
            }
        }
    }
}
```

### Phase 2: Create Post-Join Filter Function

**File**: `src/database/executor.zig`

**New Function** (add after `executeJoinSelect`, around line 789):
```zig
/// Apply WHERE clause filter to joined result
fn applyWhereFilter(
    allocator: Allocator,
    join_result: QueryResult,
    cmd: sql.SelectCmd,
    table_names: []const []const u8,    // ["users", "orders", "products"]
    table_columns: []const []const []const u8,  // [["id","name"], ["id","user_id"], ...]
) !QueryResult {
    var filtered_result = QueryResult.init(allocator);

    // Copy column definitions from join_result
    for (join_result.columns.items) |col_name| {
        try filtered_result.addColumn(col_name);
    }

    // Build metadata for column resolution
    var table_metadata = ArrayList(JoinedRow.TableMetadata).init(allocator);
    defer table_metadata.deinit();

    var col_idx: usize = 0;
    for (table_names, 0..) |table_name, i| {
        const cols = table_columns[i];
        try table_metadata.append(.{
            .table_name = table_name,
            .column_names = cols,
            .start_idx = col_idx,
            .count = cols.len,
        });
        col_idx += cols.len;
    }

    // Filter rows based on WHERE expression
    for (join_result.rows.items) |row| {
        // Create JoinedRow wrapper
        const joined_row = JoinedRow{
            .values = row,
            .table_metadata = table_metadata.items,
            .allocator = allocator,
        };

        // Convert to HashMap for evaluateExpr
        var row_map = try joined_row.toHashMap();
        defer {
            var it = row_map.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
            }
            row_map.deinit();
        }

        // Evaluate WHERE expression
        const matches = if (cmd.where_expr) |expr|
            sql.evaluateExpr(expr, row_map)
        else if (cmd.where_column) |where_col|
            blk: {
                // Fallback to simple WHERE
                if (cmd.where_value) |where_val| {
                    const row_val = joined_row.getUnqualified(where_col) orelse break :blk false;
                    break :blk valuesEqual(row_val, where_val);
                }
                break :blk true;
            }
        else
            true;

        if (matches) {
            // Clone the row and add to filtered result
            var filtered_row = ArrayList(ColumnValue).init(allocator);
            for (row.items) |val| {
                try filtered_row.append(try val.clone(allocator));
            }
            try filtered_result.addRow(filtered_row);
        }
    }

    return filtered_result;
}
```

### Phase 3: Modify executeJoinSelect to Use Post-Join Filtering

**File**: `src/database/executor.zig`
**Function**: `executeJoinSelect()`, line 447

**Changes**:

1. **Track table and column metadata** (add after line 458):
```zig
// Track table names and columns for WHERE resolution
var joined_table_names = ArrayList([]const u8).init(db.allocator);
defer joined_table_names.deinit();
var joined_table_columns = ArrayList([]const []const u8).init(db.allocator);
defer joined_table_columns.deinit();

try joined_table_names.append(cmd.table_name);
try joined_table_names.append(join.table_name);

// Collect column names from each table
var base_cols = ArrayList([]const u8).init(db.allocator);
defer base_cols.deinit();
for (base_table.columns.items) |col| {
    try base_cols.append(col.name);
}
try joined_table_columns.append(base_cols.items);

var join_cols = ArrayList([]const u8).init(db.allocator);
defer join_cols.deinit();
for (join_table.columns.items) |col| {
    try join_cols.append(col.name);
}
try joined_table_columns.append(join_cols.items);
```

2. **Apply WHERE filter before returning** (add before line 787):
```zig
// Apply WHERE clause filter if present
if (cmd.where_column != null or cmd.where_expr != null) {
    const filtered = try applyWhereFilter(
        db.allocator,
        result,
        cmd,
        joined_table_names.items,
        joined_table_columns.items,
    );
    result.deinit();
    result = filtered;
}

return result;
```

### Phase 4: Extend to N-Table Joins

**Prerequisite**: Multi-join support (currently limited to 1 join, line 455)

When N-table joins are implemented, the metadata tracking becomes:

```zig
// For each table in the join chain
for (cmd.joins.items) |join| {
    const join_table = db.tables.get(join.table_name) orelse return error.TableNotFound;
    try joined_table_names.append(join.table_name);

    var cols = ArrayList([]const u8).init(db.allocator);
    for (join_table.columns.items) |col| {
        try cols.append(col.name);
    }
    try joined_table_columns.append(cols.items);
}
```

The `applyWhereFilter` function will work unchanged for N tables.

---

## 5. Performance Implications

### 5.1 Memory Overhead

**Current (2-table join without WHERE)**:
- Memory: O(R * C) where R = result rows, C = combined columns

**Proposed (N-table join with WHERE)**:
- Memory: O(R * C) + O(T * C_avg) for metadata
  - Where T = number of tables, C_avg = avg columns per table
  - Metadata overhead is negligible: ~O(100 bytes) per table

**Conclusion**: Memory overhead is minimal.

### 5.2 Time Complexity

**Option A: Filter During Join** (Traditional approach)
```
Time: O(T1 × T2 × ... × TN)  [Join product size]
Rows processed: Full cartesian product (filtered early)
```

**Option B: Filter After Join** (Proposed)
```
Time: O(T1 × T2 × ... × TN) + O(R_joined × E)
      [Join]                   [WHERE evaluation]

Where:
- R_joined = number of rows after join (before WHERE)
- E = complexity of WHERE expression (typically O(1) to O(log C))
```

**Analysis**:
- **Pros of Post-Join Filtering**:
  - Simpler code (separation of concerns)
  - WHERE clause can reference columns from any table
  - Works with hash join optimization
  - Easier to extend to N tables

- **Cons of Post-Join Filtering**:
  - May produce intermediate result larger than final result
  - Cannot use indexes on WHERE columns during join

**Optimization Opportunity**:
For WHERE clauses that reference only the base table (e.g., `WHERE users.age > 25`), we could still apply filtering before the join. This is a future optimization.

### 5.3 Index Utilization

**Current**: Single-table queries can use B-tree indexes for WHERE clauses (line 867-877)

**Proposed**:
- **Short-term**: WHERE on joins cannot use indexes (post-join filtering)
- **Future enhancement**: Push down WHERE predicates
  ```sql
  SELECT * FROM users JOIN orders ON users.id = orders.user_id
  WHERE users.age > 25  -- Could filter users BEFORE join
    AND orders.total > 100  -- Must filter AFTER join
  ```

  This requires **predicate pushdown** optimization (advanced topic).

---

## 6. Edge Cases & NULL Handling

### 6.1 NULL Values in WHERE Clause

**SQL Standard**: NULL comparisons always return FALSE (except IS NULL / IS NOT NULL)

**Example**:
```sql
SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id
WHERE orders.total > 100
```

Result: Excludes users with no orders (because `NULL > 100` is FALSE)

**Implementation**: Already handled correctly by `evaluateExpr()` and `compareValues()` (sql.zig, lines 1193-1260)

### 6.2 Ambiguous Column Names

**Example**:
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
WHERE id > 10  -- Which id? users.id or orders.id?
```

**Proposed Behavior**:
- Search tables in join order (users first)
- First match wins
- Emit warning if ambiguous (future enhancement)

**Better Practice**: Require qualified names in WHERE clause

### 6.3 Column Not Found

**Example**:
```sql
SELECT * FROM users JOIN orders ON users.id = orders.user_id
WHERE products.name = 'Widget'  -- products not in query!
```

**Proposed Behavior**:
- `getQualified()` returns `null`
- `evaluateExpr()` treats missing column as `null_value`
- Comparison with null returns `false`
- Row is excluded from result

**Alternative**: Return error "Column not found" (more user-friendly)

---

## 7. Test Cases

### 7.1 Basic WHERE with JOIN

**Test File**: `src/test_joins.zig`

```zig
test "INNER JOIN with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Setup tables
    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 30)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 25)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    _ = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 2, 200.0)");
    _ = try db.execute("INSERT INTO orders VALUES (3, 3, 50.0)");

    // Test: Filter by base table column
    var result = try db.execute(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id WHERE users.age > 28"
    );
    defer result.deinit();

    // Should return Alice (30) and Charlie (35), not Bob (25)
    try expectEqual(@as(usize, 2), result.rows.items.len);
}
```

### 7.2 WHERE with Qualified Column Names

```zig
test "JOIN with WHERE using qualified column names" {
    // ...
    var result = try db.execute(
        "SELECT users.name, orders.total FROM users INNER JOIN orders " ++
        "ON users.id = orders.user_id WHERE orders.total > 150"
    );

    // Should return only orders with total > 150
    try expectEqual(@as(usize, 1), result.rows.items.len);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Bob"));
}
```

### 7.3 WHERE with LEFT JOIN and NULL

```zig
test "LEFT JOIN with WHERE clause handling NULL" {
    // ...
    var result = try db.execute(
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id " ++
        "WHERE orders.total > 100"
    );

    // Should exclude users with no orders (NULL total)
    // Only include users with orders.total > 100
}
```

### 7.4 WHERE with AND/OR

```zig
test "JOIN with complex WHERE (AND/OR)" {
    // ...
    var result = try db.execute(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id " ++
        "WHERE users.age > 25 AND orders.total > 100"
    );

    // Both conditions must be true
}
```

### 7.5 WHERE with Unqualified Column (Ambiguous)

```zig
test "JOIN with WHERE using unqualified column name" {
    // ...
    var result = try db.execute(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id " ++
        "WHERE name = 'Alice'"  // Unqualified - should find users.name
    );

    try expectEqual(@as(usize, 1), result.rows.items.len);
}
```

### 7.6 WHERE with IS NULL

```zig
test "LEFT JOIN with WHERE IS NULL" {
    // ...
    var result = try db.execute(
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id " ++
        "WHERE orders.id IS NULL"
    );

    // Should return only users with no orders
}
```

### 7.7 WHERE Referencing Multiple Tables

```zig
test "JOIN with WHERE referencing both tables" {
    // ...
    var result = try db.execute(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id " ++
        "WHERE users.age > 25 OR orders.total < 100"
    );

    // Should include rows where either condition is true
}
```

---

## 8. Migration Path

### Phase 1: Simple WHERE Support (Week 1)
- Add `where_expr` to `SelectCmd`
- Update parser to build expression tree from WHERE clause
- Implement `applyWhereFilter()` for 2-table joins
- Add tests for basic WHERE with JOIN

### Phase 2: Hash Join Integration (Week 2)
- Ensure hash join returns metadata for column resolution
- Test WHERE with both nested loop and hash join paths
- Performance benchmarks

### Phase 3: Complex WHERE Expressions (Week 3)
- Support AND, OR, NOT
- Support IS NULL, IS NOT NULL
- Support comparison operators (<, >, <=, >=, !=)
- Add comprehensive test suite

### Phase 4: N-Table Join Support (Week 4)
- Extend metadata tracking to N tables
- Test with 3+ table joins
- Edge case handling

### Phase 5: Optimizations (Future)
- Predicate pushdown (filter base table before join)
- Index utilization for WHERE clauses
- Cost-based decision: filter before or after join?

---

## 9. Code Changes Summary

### Files to Modify

1. **src/sql.zig** (Lines to change: 118-159, 619-871)
   - Add `where_expr: ?Expr` to `SelectCmd`
   - Extend `parseSelect()` to parse complex WHERE into expression tree
   - Update `deinit()` to free `where_expr`

2. **src/database/executor.zig** (Lines to add: after 788)
   - Add `JoinedRow` struct
   - Add `applyWhereFilter()` function
   - Modify `executeJoinSelect()` to call filter
   - Track table/column metadata during join

3. **src/database/hash_join.zig** (Lines to modify: 273-305)
   - Return table metadata with result (or pass through)
   - Ensure column order is preserved and documented

4. **src/test_joins.zig** (Lines to add: after 397)
   - Add 7 new test cases (see section 7)

### Estimated Lines of Code
- New code: ~200 lines
- Modified code: ~50 lines
- Test code: ~150 lines
- **Total**: ~400 lines

---

## 10. Conclusion

### Summary

The current ZVDB implementation **does not support WHERE clauses for JOIN queries**. This document proposes a **post-join filtering** architecture that:

1. ✅ **Separates concerns**: Join logic remains simple and focused
2. ✅ **Scales to N tables**: Works with any number of joined tables
3. ✅ **Leverages existing code**: Reuses `evaluateExpr()` from sql.zig
4. ✅ **Handles edge cases**: NULL values, qualified names, complex expressions
5. ✅ **Minimal overhead**: Metadata tracking is O(T × C_avg), negligible

### Trade-offs

**Advantages**:
- Simple implementation
- Easy to extend to N-table joins
- Works with both nested loop and hash join
- Clear separation between join and filter phases

**Disadvantages**:
- Cannot use indexes for WHERE columns (yet)
- May produce large intermediate results
- Future optimization needed for predicate pushdown

### Recommendation

**Implement post-join filtering as proposed** because:
1. It's the simplest correct implementation
2. It unblocks N-table join development
3. Optimizations (predicate pushdown) can be added later
4. Performance is acceptable for typical workloads

The alternative (filtering during join) is more complex and couples join logic with filter logic, making N-table joins harder to implement.

---

## Appendix A: File References

**Key Locations in Codebase:**

| File | Lines | Description |
|------|-------|-------------|
| `src/sql.zig` | 118-159 | SelectCmd structure |
| `src/sql.zig` | 261-285 | Expr tree definition |
| `src/sql.zig` | 619-871 | parseSelect() function |
| `src/sql.zig` | 1102-1260 | evaluateExpr() and helpers |
| `src/database/executor.zig` | 447-788 | executeJoinSelect() |
| `src/database/executor.zig` | 790-995 | executeSelect() (single table) |
| `src/database/executor.zig` | 934-940 | WHERE filter in single table |
| `src/database/hash_join.zig` | 261-356 | executeHashJoin() entry point |
| `src/database/hash_join.zig` | 185-254 | emitJoinedRow() helper |
| `src/test_joins.zig` | 1-397 | Existing JOIN tests |

---

## Appendix B: Example SQL Queries

**Queries that will work after implementation:**

```sql
-- Simple WHERE on base table
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE users.age > 25;

-- Simple WHERE on join table
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE orders.total > 100;

-- WHERE with AND
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE users.age > 25 AND orders.total > 100;

-- WHERE with OR
SELECT * FROM users
LEFT JOIN orders ON users.id = orders.user_id
WHERE users.name = 'Alice' OR orders.total > 200;

-- WHERE with IS NULL (find users with no orders)
SELECT * FROM users
LEFT JOIN orders ON users.id = orders.user_id
WHERE orders.id IS NULL;

-- WHERE with complex expression
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
WHERE (users.age > 30 OR users.name = 'Bob') AND orders.total > 50;

-- 3-table join with WHERE (future)
SELECT * FROM users
INNER JOIN orders ON users.id = orders.user_id
INNER JOIN products ON orders.product_id = products.id
WHERE users.age > 25 AND products.price > 100;
```

---

**Document Version**: 1.0
**Date**: 2025-11-15
**Author**: Claude (Analysis of ZVDB)
**Status**: Design Proposal
