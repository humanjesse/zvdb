# ZVDB Validation Strategy

## Overview

ZVDB implements a **deferred validation strategy** based on PostgreSQL and SQLite best practices. Column and expression validation happens during **semantic analysis** (after parsing, before execution), not at parse time or during execution.

## Architecture

### Three-Layer System

1. **column_matching.zig** - Low-level column name resolution utilities
2. **column_resolver.zig** - Multi-table schema resolution for JOINs
3. **validator.zig** - High-level semantic validation with error messages

### Validation Flow

```
SQL Query
    ↓
[1] Parser (syntax only)
    ↓
[2] Semantic Analyzer (validation) ← validator.zig
    ↓
[3] Query Planner
    ↓
[4] Executor (runtime resolution) ← column_matching.zig
```

## Why Deferred Validation?

### What We Learned from PostgreSQL

PostgreSQL validates queries in the **Analyzer phase** (Phase 2), which happens after parsing but before planning/execution. This allows:

- Full query context (JOINs, aliases, subqueries)
- Better error messages with line numbers and hints
- Type inference alongside validation
- Performance optimization through early failure

### What We Learned from SQLite

SQLite performs similar validation during **parse analysis**, with some key insights:

- Quoted identifiers can mask errors (we avoid this)
- Case sensitivity matters (we preserve case)
- Validation needs full schema context

## Validation Rules

### Literals (Always Valid)

```sql
SELECT 1 FROM users;                    -- Numeric literal
SELECT 'active' FROM users;             -- String literal (parsed as .literal)
SELECT 42, name, 'test' FROM employees; -- Mixed
```

**Why:** Literals are valid SQL expressions and don't correspond to actual columns.

### Aggregates (Context-Dependent)

```sql
-- ✅ Valid
SELECT COUNT(*) FROM users;
SELECT AVG(salary) FROM employees;
SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 5;

-- ❌ Invalid
SELECT * FROM users WHERE COUNT(*) > 5;  -- Aggregate in WHERE
```

**Rules:**
- ✅ Allowed in: SELECT, HAVING, ORDER BY (with GROUP BY)
- ❌ Not allowed in: WHERE, GROUP BY

### Column Resolution (Multi-Phase)

When looking for a column, we try three phases:

1. **Exact match**: `name` == `name`
2. **Unqualified fallback**: `users.name` → `name`
3. **Qualified scan**: `u.name` → `users.name` (extracts column part)

**Implementation:** See `src/database/column_matching.zig`

### Example: Alias Resolution

```sql
SELECT u.name, o.total
FROM users u
JOIN orders o ON u.id = o.user_id
WHERE u.status = 'active'
```

**What happens:**
1. Parser creates AST with aliases (`u`, `o`)
2. Validator resolves `u.name` → `users.name`
3. Executor looks up `users.name` in result map
4. column_matching.resolveColumnValue handles the resolution

## Error Messages

### Design Principles

Following PostgreSQL's approach:

```
ERROR: column "invalid_col" does not exist
LINE 1: SELECT invalid_col FROM users;
                ^
```

Our error messages include:

- **What:** Clear description of the error
- **Where:** Column/table name mentioned
- **Hint:** Suggestions when possible (future enhancement)

### Current Implementation

```zig
pub fn formatErrorMessage(
    allocator: Allocator,
    err: ValidationError,
    col_name: ?[]const u8,
) ![]const u8
```

**Error Types:**
- `ColumnNotFound` - Column doesn't exist in schema
- `AmbiguousColumn` - Column in multiple tables (needs qualification)
- `AggregateInWhere` - Aggregate used in WHERE clause
- `TableNotFound` - Referenced table doesn't exist

## Integration Points

### 1. Schema Creation (executor.zig)

```zig
// NO validation here - intentionally deferred
.regular => |col_name| {
    try result.addColumn(col_name);
},
```

**Why:** Schema creation doesn't have full query context yet.

### 2. Expression Evaluation (executor.zig)

```zig
.column => |col| {
    // Use column_matching helper for runtime resolution
    if (column_matching.resolveColumnValue(col, row_values)) |value| {
        return value;
    }
    return ColumnValue.null_value;
},
```

**Why:** Runtime resolution needs to be fast and handle all edge cases.

### 3. Semantic Validation (validator.zig) - FUTURE

```zig
// Example usage (not yet integrated):
pub fn validateQuery(db: *Database, cmd: sql.SelectCmd) !void {
    const table = db.tables.get(cmd.table_name) orelse return error.TableNotFound;

    try validator.validateSelectColumns(
        db.allocator,
        &cmd,
        table,
        null, // joined tables if applicable
    );
}
```

## Code Organization

### Files and Responsibilities

| File | Purpose | Lines | Tests |
|------|---------|-------|-------|
| `column_matching.zig` | Low-level name resolution | ~250 | 13 tests |
| `column_resolver.zig` | Multi-table schema tracking | ~650 | 15 tests |
| `validator.zig` | Semantic validation + errors | ~450 | 8 tests |

### Key Functions

**column_matching.zig:**
- `extractColumnPart()` - Parse qualified names
- `matchColumnName()` - Simple name matching
- `resolveColumnValue()` - Hash map resolution
- `findColumnIndex()` - Array-based lookup

**column_resolver.zig:**
- `ColumnResolver.init()` - Initialize with base table
- `addJoinedTable()` - Add JOIN target
- `resolveColumn()` - Resolve with full context
- `columnExists()` - Quick existence check

**validator.zig:**
- `isNumericLiteral()` - Detect numeric constants
- `isAggregate()` - Detect aggregate functions
- `validateColumnReference()` - Validate single column
- `validateExpression()` - Recursive validation
- `formatErrorMessage()` - User-friendly errors

## Testing Strategy

### Unit Tests

Each module has comprehensive tests:

```bash
$ zig build test
...
Build Summary: 34/37 steps succeeded; 1134/1134 tests passed
```

**Test Coverage:**
- ✅ column_matching.zig: 13 tests
- ✅ column_resolver.zig: 15 tests
- ✅ validator.zig: 8 tests
- ✅ Integration: 1098 existing tests

### Edge Cases Tested

1. **Qualified vs Unqualified**
   - `users.id` vs `id`
   - `u.name` vs `users.name` (alias mismatch)

2. **Literals**
   - Numeric: `1`, `42`, `3.14`, `-5`
   - String: `'text'` (handled by parser)

3. **Ambiguity**
   - `id` exists in both `users` and `orders`
   - Error: "column is ambiguous"

4. **Not Found**
   - `invalid_column` doesn't exist
   - Error: "column does not exist"

## Future Enhancements

### Phase 1: Soft Integration (Current)
- ✅ Modules created
- ✅ Tests passing
- ✅ Documentation complete
- ⏳ Optional validation calls (not enforced)

### Phase 2: Hard Integration
- Call validator from executor before execution
- Catch errors early with better messages
- Add "Did you mean?" suggestions

### Phase 3: Advanced Features
- Type checking (ensure `id = 'text'` fails)
- Function validation (`UPPER(name)` checks `name` exists)
- Subquery validation (recursive)
- Performance optimization (cache validation results)

## Performance Considerations

### Current Implementation

- **Parse time:** No validation (fast)
- **Execution time:** column_matching lookups (3-phase fallback)
- **Memory:** Minimal (no caching yet)

### Optimization Opportunities

1. **Cache validation results**
   - Prepared statements could skip re-validation
   - Trade memory for speed

2. **Index qualified columns**
   - Build hash map of qualified → unqualified
   - O(1) lookups instead of O(n) scans

3. **Parallel validation**
   - Multiple SELECT columns can be validated concurrently
   - Zig's async could help here

## Best Practices

### For Contributors

1. **Don't validate at schema creation**
   - Schema creation lacks full query context
   - Validation belongs in semantic analysis

2. **Use column_matching for resolution**
   - Don't reimplement the 3-phase fallback
   - Handles all edge cases consistently

3. **Provide helpful error messages**
   - Include column/table names
   - Suggest fixes when possible

### For Users

1. **Use qualified names in JOINs**
   ```sql
   -- ❌ Ambiguous
   SELECT id, name FROM users JOIN orders ON user_id = id

   -- ✅ Clear
   SELECT users.id, users.name
   FROM users JOIN orders ON orders.user_id = users.id
   ```

2. **Don't use aggregates in WHERE**
   ```sql
   -- ❌ Invalid
   SELECT * FROM users WHERE COUNT(*) > 0

   -- ✅ Use HAVING
   SELECT status, COUNT(*) FROM users GROUP BY status HAVING COUNT(*) > 0
   ```

## References

1. **PostgreSQL Query Processing**
   - https://www.postgresql.org/docs/current/query-path.html

2. **SQLite Expression Syntax**
   - https://www.sqlite.org/lang_expr.html

3. **Meta Engineering: Static Analysis of SQL**
   - https://engineering.fb.com/2022/11/30/data-infrastructure/static-analysis-sql-queries/

4. **A Formal Semantics of SQL Queries (VLDB)**
   - http://www.vldb.org/pvldb/vol11/p27-guagliardo.pdf

## Conclusion

ZVDB's validation strategy balances:

- **Correctness:** Matches PostgreSQL/SQLite semantics
- **Performance:** Fast 3-phase resolution at runtime
- **User Experience:** Clear error messages (when validation runs)
- **Maintainability:** Clean separation of concerns

The deferred approach allows complex SQL features (literals, aggregates, expressions, subqueries) while providing production-quality error detection.

**Current Status:** ✅ All infrastructure in place, ready for hard integration when needed.
