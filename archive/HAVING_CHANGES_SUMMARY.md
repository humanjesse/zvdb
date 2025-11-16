# HAVING Clause Implementation - Code Changes Summary

## File Modified
**Path:** `/home/user/zvdb/src/sql.zig`

## Change Summary
- **Lines Changed:** 7 locations
- **Lines Added:** ~35 new lines
- **New Function:** `parseHaving()` (30 lines)
- **New Error Type:** `HavingWithoutGroupBy`

---

## Detailed Changes

### 1. SqlError Enum - Added New Error Type
**Location:** Line 25
**Change:** Added `HavingWithoutGroupBy` error

```zig
pub const SqlError = error{
    InvalidSyntax,
    UnknownCommand,
    MissingTableName,
    MissingValues,
    MissingColumn,
    InvalidColumnType,
    TableNotFound,
    ColumnNotFound,
    OutOfMemory,
    DimensionMismatch,
    InvalidExpression,
    TypeMismatch,
    InvalidCharacter,
    Overflow,
    HavingWithoutGroupBy,  // ← ADDED
};
```

---

### 2. SelectCmd Structure - Added Field
**Location:** Line 160
**Change:** Added `having_expr` field after `group_by`

```zig
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn),
    joins: ArrayList(JoinClause),
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    where_expr: ?Expr,
    similar_to_column: ?[]const u8,
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8,
    order_by_vibes: bool,
    order_by: ?OrderByClause,
    group_by: ArrayList([]const u8),
    having_expr: ?Expr,  // ← ADDED
    limit: ?usize,
```

---

### 3. SelectCmd.deinit() - Added Cleanup
**Location:** Lines 200-204
**Change:** Added cleanup code for `having_expr`

```zig
    // Free GROUP BY columns
    for (self.group_by.items) |col| {
        allocator.free(col);
    }
    self.group_by.deinit();

    // ↓ ADDED ↓
    // Free HAVING expression
    if (self.having_expr) |*expr| {
        var e = expr.*;
        e.deinit(allocator);
    }
```

---

### 4. parseSelect() - Variable Initialization
**Location:** Line 748
**Change:** Added `having_expr` variable

```zig
    var joins = ArrayList(JoinClause).init(allocator);
    const where_column: ?[]const u8 = null;
    const where_value: ?ColumnValue = null;
    var where_expr: ?Expr = null;
    var similar_to_column: ?[]const u8 = null;
    var similar_to_text: ?[]const u8 = null;
    var order_by_similarity: ?[]const u8 = null;
    var order_by_vibes = false;
    var order_by: ?OrderByClause = null;
    var group_by = ArrayList([]const u8).init(allocator);
    var having_expr: ?Expr = null;  // ← ADDED
    var limit: ?usize = null;
```

---

### 5. parseSelect() - GROUP BY Parsing Update
**Location:** Line 870
**Change:** Added HAVING to break conditions

```zig
    // Parse comma-separated list of columns
    while (i < tokens.len) {
        if (eqlIgnoreCase(tokens[i].text, "HAVING") or  // ← ADDED
            eqlIgnoreCase(tokens[i].text, "ORDER") or
            eqlIgnoreCase(tokens[i].text, "LIMIT"))
        {
            break;
        }
```

---

### 6. parseSelect() - HAVING Parsing Block
**Location:** Lines 883-886
**Change:** Added HAVING clause parsing (NEW BLOCK)

```zig
    } else if (eqlIgnoreCase(tokens[i].text, "HAVING")) {
        const having_result = try parseHaving(allocator, tokens, i);
        having_expr = having_result.expr;
        i = having_result.next_idx;
    } else if (eqlIgnoreCase(tokens[i].text, "ORDER")) {
```

---

### 7. parseSelect() - SelectCmd Build and Validation
**Location:** Lines 924-949
**Change:** Changed from direct return to build + validate pattern

**BEFORE:**
```zig
    return SelectCmd{
        .table_name = table_name,
        .columns = columns,
        // ... other fields ...
        .group_by = group_by,
        .limit = limit,
    };
```

**AFTER:**
```zig
    // Build the SelectCmd
    var cmd = SelectCmd{
        .table_name = table_name,
        .columns = columns,
        .joins = joins,
        .where_column = where_column,
        .where_value = where_value,
        .where_expr = where_expr,
        .similar_to_column = similar_to_column,
        .similar_to_text = similar_to_text,
        .order_by_similarity = order_by_similarity,
        .order_by_vibes = order_by_vibes,
        .order_by = order_by,
        .group_by = group_by,
        .having_expr = having_expr,  // ← ADDED
        .limit = limit,
    };

    // ↓ ADDED VALIDATION ↓
    // Validate HAVING only used with GROUP BY
    if (cmd.having_expr != null and cmd.group_by.items.len == 0) {
        // Clean up before returning error
        cmd.deinit(allocator);
        return error.HavingWithoutGroupBy;
    }

    return cmd;
```

---

### 8. New Function: parseHaving()
**Location:** Lines 1613-1642
**Change:** Complete new function (NEW)

```zig
/// Parse HAVING clause (similar to WHERE but for GROUP BY results)
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

---

## Verification Checklist

- [x] Added `HavingWithoutGroupBy` error to `SqlError` enum
- [x] Added `having_expr: ?Expr` field to `SelectCmd` structure
- [x] Updated `SelectCmd.deinit()` to clean up `having_expr`
- [x] Implemented `parseHaving()` function
- [x] Added `having_expr` variable initialization in `parseSelect()`
- [x] Updated GROUP BY parsing to break on HAVING
- [x] Added HAVING parsing block in `parseSelect()`
- [x] Added `having_expr` to SelectCmd initialization
- [x] Added validation: HAVING requires GROUP BY
- [x] Proper memory cleanup on validation error

## Files Created

1. `/home/user/zvdb/src/sql.zig` - Modified (main implementation)
2. `/home/user/zvdb/HAVING_IMPLEMENTATION.md` - Created (detailed documentation)
3. `/home/user/zvdb/HAVING_CHANGES_SUMMARY.md` - Created (this file)

## Next Steps for Integration

1. **Compile and Test:** Run `zig build` to verify compilation
2. **Unit Tests:** Create tests in test suite for HAVING clause parsing
3. **Execution Layer:** Implement HAVING evaluation in query executor
4. **Integration Tests:** Test with real queries combining WHERE, GROUP BY, HAVING, ORDER BY

## Example Test Cases

```zig
// Test 1: Basic HAVING
"SELECT dept, COUNT(*) FROM emp GROUP BY dept HAVING COUNT(*) > 2"

// Test 2: HAVING with complex expression
"SELECT category, SUM(price) FROM products GROUP BY category HAVING SUM(price) > 1000 AND COUNT(*) > 5"

// Test 3: Error case - should fail
"SELECT name FROM emp HAVING age > 30"  // No GROUP BY - should error

// Test 4: Full query
"SELECT dept, AVG(salary) FROM emp WHERE active = true GROUP BY dept HAVING AVG(salary) > 50000 ORDER BY dept LIMIT 10"
```

---

**Implementation Complete!** ✓

All parsing infrastructure for HAVING clause is now in place. The SQL parser can now:
- Parse HAVING clauses after GROUP BY
- Validate that HAVING requires GROUP BY
- Store HAVING expressions in the AST
- Properly manage memory for HAVING expressions
