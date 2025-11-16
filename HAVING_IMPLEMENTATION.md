# HAVING Clause Implementation Summary

## Overview
Successfully implemented HAVING clause parsing support for zvdb SQL database. HAVING filters grouped results AFTER aggregation, while WHERE filters individual rows BEFORE grouping.

## Changes Made

### 1. Added HavingWithoutGroupBy Error
**File:** `/home/user/zvdb/src/sql.zig` (Line 25)
- Added new error type: `HavingWithoutGroupBy`
- Enforces SQL requirement that HAVING can only be used with GROUP BY

### 2. Updated SelectCmd Structure
**File:** `/home/user/zvdb/src/sql.zig` (Line 160)
- Added field: `having_expr: ?Expr`
- Stores the HAVING clause expression for filtering grouped results

### 3. Updated SelectCmd.deinit()
**File:** `/home/user/zvdb/src/sql.zig` (Lines 200-204)
- Added cleanup code for `having_expr` field
- Ensures proper memory management with `expr.deinit(allocator)`

### 4. Implemented parseHaving() Function
**File:** `/home/user/zvdb/src/sql.zig` (Lines 1613-1642)
- New parsing function for HAVING clauses
- Similar structure to WHERE clause parsing
- Finds clause boundaries (before ORDER BY or LIMIT)
- Returns both expression and next token index

```zig
fn parseHaving(allocator: Allocator, tokens: []const Token, start_idx: usize) !struct { expr: Expr, next_idx: usize } {
    var idx = start_idx;

    // Expect "HAVING"
    if (idx >= tokens.len or !eqlIgnoreCase(tokens[idx].text, "HAVING")) {
        return error.InvalidSyntax;
    }
    idx += 1;

    // Find end of HAVING clause (before ORDER BY or LIMIT)
    var having_end = idx;
    while (having_end < tokens.len) {
        if (eqlIgnoreCase(tokens[having_end].text, "ORDER") or
            eqlIgnoreCase(tokens[having_end].text, "LIMIT"))
        {
            break;
        }
        having_end += 1;
    }

    // Parse the condition expression (same as WHERE expressions)
    var expr_idx = idx;
    const expr = try parseExpr(allocator, tokens[0..having_end], &expr_idx);

    return .{
        .expr = expr,
        .next_idx = having_end,
    };
}
```

### 5. Updated parseSelect() Function
**File:** `/home/user/zvdb/src/sql.zig`

#### 5a. Added having_expr Variable (Line 748)
```zig
var having_expr: ?Expr = null;
```

#### 5b. Updated GROUP BY Parsing (Line 870)
- Added HAVING to the break conditions when parsing GROUP BY columns
- Ensures GROUP BY parsing stops when HAVING is encountered

#### 5c. Added HAVING Parsing Block (Lines 883-886)
```zig
} else if (eqlIgnoreCase(tokens[i].text, "HAVING")) {
    const having_result = try parseHaving(allocator, tokens, i);
    having_expr = having_result.expr;
    i = having_result.next_idx;
}
```

#### 5d. Updated SelectCmd Initialization (Lines 924-949)
- Added `.having_expr = having_expr` field
- Added validation to ensure HAVING only used with GROUP BY
- Proper cleanup if validation fails

```zig
// Build the SelectCmd
var cmd = SelectCmd{
    // ... other fields ...
    .having_expr = having_expr,
    // ... other fields ...
};

// Validate HAVING only used with GROUP BY
if (cmd.having_expr != null and cmd.group_by.items.len == 0) {
    // Clean up before returning error
    cmd.deinit(allocator);
    return error.HavingWithoutGroupBy;
}

return cmd;
```

## SQL Execution Order
The implementation respects standard SQL execution order:
```
FROM → JOIN → WHERE → GROUP BY → HAVING → ORDER BY → LIMIT
                 ↑                    ↑
            filters rows      filters groups
```

## Supported Query Examples

### Example 1: Filter by Aggregate Count
```sql
SELECT department, COUNT(*)
FROM employees
GROUP BY department
HAVING COUNT(*) > 2
```
Filters groups to only show departments with more than 2 employees.

### Example 2: Filter by Aggregate Sum
```sql
SELECT product, SUM(amount)
FROM sales
GROUP BY product
HAVING SUM(amount) > 300.0
```
Shows only products with total sales exceeding 300.

### Example 3: Multiple Conditions
```sql
SELECT department, AVG(salary)
FROM employees
GROUP BY department
HAVING AVG(salary) > 50000 AND COUNT(*) > 5
```
Shows departments with average salary > 50000 AND more than 5 employees.

### Example 4: HAVING with ORDER BY
```sql
SELECT category, SUM(price) as total
FROM products
GROUP BY category
HAVING SUM(price) > 1000
ORDER BY total DESC
```
Filters and sorts grouped results.

### Example 5: HAVING with LIMIT
```sql
SELECT region, COUNT(*) as count
FROM stores
GROUP BY region
HAVING COUNT(*) > 10
LIMIT 5
```
Filters, then limits results.

## Error Handling

### Invalid: HAVING Without GROUP BY
```sql
SELECT name, salary
FROM employees
HAVING salary > 50000
```
**Error:** `HavingWithoutGroupBy`
- HAVING requires GROUP BY to be present
- Use WHERE instead for filtering individual rows

## Key Features

1. **Expression Reuse**: HAVING uses the same expression parser as WHERE, supporting:
   - Comparison operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
   - Logical operators: `AND`, `OR`, `NOT`
   - Aggregate functions: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`
   - Column references
   - Literal values

2. **Proper Parsing Order**: HAVING is parsed:
   - AFTER GROUP BY
   - BEFORE ORDER BY
   - BEFORE LIMIT

3. **Memory Safety**:
   - Proper cleanup in `deinit()`
   - Cleanup before returning validation errors
   - Uses Zig's error handling

4. **Validation**:
   - Enforces GROUP BY requirement
   - Proper error messages
   - Clean error handling with resource cleanup

## Testing Recommendations

To test the HAVING implementation:

1. **Basic HAVING clause**:
   ```sql
   SELECT department, COUNT(*) FROM employees GROUP BY department HAVING COUNT(*) > 2
   ```

2. **HAVING with multiple aggregates**:
   ```sql
   SELECT category, SUM(price), AVG(price) FROM products GROUP BY category HAVING SUM(price) > 1000
   ```

3. **HAVING with complex conditions**:
   ```sql
   SELECT dept, AVG(salary) FROM employees GROUP BY dept HAVING AVG(salary) > 50000 AND COUNT(*) > 5
   ```

4. **Error case - HAVING without GROUP BY**:
   ```sql
   SELECT name FROM employees HAVING name = 'John'
   ```
   Should return: `HavingWithoutGroupBy` error

5. **Full query with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT**:
   ```sql
   SELECT dept, COUNT(*) as count
   FROM employees
   WHERE active = true
   GROUP BY dept
   HAVING COUNT(*) > 3
   ORDER BY count DESC
   LIMIT 10
   ```

## Implementation Notes

- The HAVING expression is stored as `having_expr: ?Expr` in SelectCmd
- Parsing reuses existing expression parser (`parseExpr`)
- Validation happens at parse time, not execution time
- Clean separation between parsing and execution logic
- Follows existing code patterns and conventions

## Next Steps (Execution Phase)

This implementation provides the **parsing layer** for HAVING clauses. The next phase would be to implement the **execution logic** in the query executor to:

1. Apply WHERE filters to individual rows
2. Group rows by GROUP BY columns
3. Calculate aggregates for each group
4. Apply HAVING filters to grouped results
5. Apply ORDER BY and LIMIT to final results

The parsing foundation is now in place to support this execution pipeline.
