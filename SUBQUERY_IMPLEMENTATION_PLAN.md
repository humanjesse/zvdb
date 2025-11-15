# Subquery Implementation Plan

**Date:** 2025-11-15
**Goal:** Add proper subquery support to zvdb SQL engine
**Estimated Effort:** 1 week (5-7 days)
**Difficulty:** Medium

---

## Executive Summary

This plan implements subquery support for zvdb, enabling nested SELECT statements in WHERE clauses. This is Phase 4 of the Enhanced SQL Plan, completing the core SQL feature set.

**What we're building:**
```sql
-- Subquery with IN operator
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);

-- Subquery with comparison operators
SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products);

-- Subquery with EXISTS
SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id);
```

**Current State:**
✅ Expression system supports: literal, column, binary, unary
✅ WHERE clause evaluation via `sql.evaluateExpr()`
✅ Complex JOIN queries with WHERE expressions
✅ Aggregate functions (COUNT, SUM, AVG, MIN, MAX)
✅ GROUP BY support

**Target State:**
- ✅ Add `subquery` variant to `Expr` union
- ✅ Add `in_op`, `exists_op`, `not_exists_op` to `BinaryOp`
- ✅ Parser recognizes and parses nested SELECT statements
- ✅ Executor evaluates subqueries recursively
- ✅ Support for scalar and list subqueries
- ✅ Comprehensive test coverage

---

## Phase 1: Extend SQL Parser (2-3 days)

### 1.1: Add Subquery Support to Expression Types

**File:** `src/sql.zig`

**Tasks:**

#### A. Update `BinaryOp` enum (lines 226-235)

Add new operators for subquery operations:

```zig
pub const BinaryOp = enum {
    eq,      // =
    neq,     // !=
    lt,      // <
    gt,      // >
    lte,     // <=
    gte,     // >=
    and_op,  // AND
    or_op,   // OR
    in_op,   // IN (for subqueries and lists)
    not_in_op,     // NOT IN
    exists_op,     // EXISTS
    not_exists_op, // NOT EXISTS
};
```

#### B. Update `Expr` union (lines 267-290)

Add `subquery` variant:

```zig
pub const Expr = union(enum) {
    literal: ColumnValue,
    column: []const u8,
    binary: *BinaryExpr,
    unary: *UnaryExpr,
    subquery: *SelectCmd,  // NEW: Nested SELECT statement

    pub fn deinit(self: *Expr, allocator: Allocator) void {
        switch (self.*) {
            .literal => |*val| {
                var v = val.*;
                v.deinit(allocator);
            },
            .column => |col| allocator.free(col),
            .binary => |bin| {
                bin.deinit(allocator);
                allocator.destroy(bin);
            },
            .unary => |un| {
                un.deinit(allocator);
                allocator.destroy(un);
            },
            .subquery => |sq| {
                sq.deinit(allocator);
                allocator.destroy(sq);
            },
        }
    }
};
```

**Acceptance Criteria:**
- ✅ Expr union compiles with new subquery variant
- ✅ deinit() properly cleans up subquery memory
- ✅ New operators compile without errors

---

### 1.2: Implement Subquery Parser

**File:** `src/sql.zig`

Find the expression parsing functions (search for `parseExpr` or similar). We need to:

#### A. Add helper to detect subquery start

```zig
/// Check if token sequence starts a subquery
fn isSubqueryStart(tokens: []const Token, idx: usize) bool {
    // Subquery starts with ( SELECT
    if (idx >= tokens.len) return false;
    if (!std.mem.eql(u8, tokens[idx].text, "(")) return false;
    if (idx + 1 >= tokens.len) return false;
    return eqlIgnoreCase(tokens[idx + 1].text, "SELECT");
}
```

#### B. Add subquery parser function

```zig
/// Parse a subquery: (SELECT ...)
/// Returns the subquery and advances idx past the closing )
fn parseSubquery(allocator: Allocator, tokens: []const Token, idx: *usize) !*SelectCmd {
    // Expect opening (
    if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, "(")) {
        return SqlError.InvalidSyntax;
    }
    idx.* += 1; // Skip (

    // Find matching closing parenthesis
    var depth: usize = 1;
    var end_idx = idx.*;
    while (end_idx < tokens.len and depth > 0) {
        if (std.mem.eql(u8, tokens[end_idx].text, "(")) {
            depth += 1;
        } else if (std.mem.eql(u8, tokens[end_idx].text, ")")) {
            depth -= 1;
        }
        end_idx += 1;
    }

    if (depth != 0) {
        return SqlError.InvalidSyntax; // Unmatched parentheses
    }

    // Parse SELECT from tokens[idx] to tokens[end_idx-1]
    const subquery_tokens = tokens[idx.*..end_idx - 1];
    const subquery = try allocator.create(SelectCmd);
    errdefer allocator.destroy(subquery);

    subquery.* = try parseSelect(allocator, subquery_tokens);

    idx.* = end_idx; // Move past closing )
    return subquery;
}
```

#### C. Update primary expression parser

Find the function that parses primary expressions (likely called `parsePrimaryExpr` or similar in the expression parsing section). Update it to handle subqueries:

```zig
// Inside parsePrimaryExpr or similar function:
fn parsePrimaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) !Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    // Check for subquery: ( SELECT ...
    if (isSubqueryStart(tokens, idx.*)) {
        const subquery = try parseSubquery(allocator, tokens, idx);
        return Expr{ .subquery = subquery };
    }

    // ... existing primary expression parsing (literals, columns, etc.)
}
```

#### D. Update IN operator parsing

Find where binary operators are parsed and add support for IN:

```zig
// In binary operator parsing:
if (eqlIgnoreCase(tokens[idx.*].text, "IN")) {
    idx.* += 1;
    const op = .in_op;

    // Parse right side (must be a subquery or list)
    const right = try parsePrimaryExpr(allocator, tokens, idx);

    // Create binary expression
    const bin_expr = try allocator.create(BinaryExpr);
    bin_expr.* = .{
        .op = op,
        .left = left_expr,
        .right = right,
    };
    return Expr{ .binary = bin_expr };
}

// Similar for NOT IN
if (eqlIgnoreCase(tokens[idx.*].text, "NOT")) {
    if (idx.* + 1 < tokens.len and eqlIgnoreCase(tokens[idx.* + 1].text, "IN")) {
        idx.* += 2; // Skip NOT IN
        // ... parse as above with .not_in_op
    }
}
```

#### E. Add EXISTS operator parsing

```zig
// In unary/prefix operator parsing:
if (eqlIgnoreCase(tokens[idx.*].text, "EXISTS")) {
    idx.* += 1;

    // EXISTS must be followed by a subquery
    if (!isSubqueryStart(tokens, idx.*)) {
        return SqlError.InvalidSyntax;
    }

    const subquery = try parseSubquery(allocator, tokens, idx);

    // EXISTS is a unary operator on a subquery
    // We can represent this as a binary op with null left side
    // OR create a new expression variant
    // For simplicity, use binary with literal true as left:
    const bin_expr = try allocator.create(BinaryExpr);
    bin_expr.* = .{
        .op = .exists_op,
        .left = Expr{ .literal = ColumnValue{ .bool = true } },
        .right = Expr{ .subquery = subquery },
    };
    return Expr{ .binary = bin_expr };
}
```

**Acceptance Criteria:**
- ✅ Parser recognizes `(SELECT ...)` as subquery
- ✅ Parser handles nested parentheses correctly
- ✅ Parser supports `IN (subquery)` syntax
- ✅ Parser supports `EXISTS (subquery)` syntax
- ✅ Parser supports `NOT IN` and `NOT EXISTS`
- ✅ Error messages for invalid subquery syntax

---

## Phase 2: Implement Subquery Execution (2-3 days)

### 2.1: Add Subquery Evaluation to Expression Evaluator

**File:** `src/sql.zig`

Update the `evaluateExpr` function (around line 1108) to handle subqueries:

```zig
pub fn evaluateExpr(expr: Expr, row_values: anytype) bool {
    switch (expr) {
        .literal => |val| {
            // Existing code...
        },
        .column => {
            // Existing code...
        },
        .binary => |bin| {
            return evaluateBinaryExpr(bin, row_values);
        },
        .unary => |un| {
            return evaluateUnaryExpr(un, row_values);
        },
        .subquery => {
            // Subqueries should not be evaluated standalone
            // They must be part of a binary expression (IN, EXISTS, etc.)
            return false;
        },
    }
}
```

**However**, we need to pass the Database context for subquery execution. This requires refactoring!

### 2.2: Refactor Expression Evaluation to Accept Database Context

**IMPORTANT CHANGE**: Current `evaluateExpr` only has access to row values, but subqueries need to execute SELECT statements, which requires database access.

#### A. Update `evaluateExpr` signature

```zig
// OLD:
pub fn evaluateExpr(expr: Expr, row_values: anytype) bool

// NEW:
pub fn evaluateExpr(
    expr: Expr,
    row_values: anytype,
    db: ?*Database  // Optional database for subquery execution
) bool
```

**NOTE:** This is a breaking change. We need to update all call sites!

#### B. Find and update all call sites

Search for `evaluateExpr` calls in:
- `src/database/executor.zig` (line 482 and others)
- Any other files

Update them to pass `db`:
```zig
// Before:
return sql.evaluateExpr(expr, row_map);

// After:
return sql.evaluateExpr(expr, row_map, db);
```

#### C. Implement subquery evaluation helpers

**File:** `src/sql.zig`

Add helper functions for subquery evaluation:

```zig
/// Execute a subquery and return result set
fn executeSubquery(
    subquery: *const SelectCmd,
    db: *Database,
    allocator: Allocator,
) !QueryResult {
    // Import executor to call executeSelect
    const executor = @import("database/executor.zig");
    return executor.executeSelect(db, subquery.*);
}

/// Evaluate IN operator with subquery
fn evaluateInSubquery(
    left_val: ColumnValue,
    subquery: *const SelectCmd,
    db: *Database,
    allocator: Allocator,
    negate: bool,  // true for NOT IN
) !bool {
    // Execute subquery
    const result = try executeSubquery(subquery, db, allocator);
    defer result.deinit();

    // Subquery for IN must return a single column
    if (result.columns.items.len != 1) {
        return SqlError.InvalidSubquery;
    }

    // Check if left_val is in the result set
    for (result.rows.items) |row| {
        if (row.items.len > 0) {
            const val = row.items[0];
            if (valuesEqual(left_val, val)) {
                return !negate;  // Found match
            }
        }
    }

    return negate;  // No match found
}

/// Evaluate EXISTS operator
fn evaluateExistsSubquery(
    subquery: *const SelectCmd,
    db: *Database,
    allocator: Allocator,
    negate: bool,  // true for NOT EXISTS
) !bool {
    // Execute subquery
    const result = try executeSubquery(subquery, db, allocator);
    defer result.deinit();

    // EXISTS returns true if result has at least one row
    const has_rows = result.rows.items.len > 0;
    return if (negate) !has_rows else has_rows;
}

/// Evaluate scalar subquery (returns single value)
fn evaluateScalarSubquery(
    subquery: *const SelectCmd,
    db: *Database,
    allocator: Allocator,
) !ColumnValue {
    const result = try executeSubquery(subquery, db, allocator);
    defer result.deinit();

    // Scalar subquery must return exactly 1 row, 1 column
    if (result.columns.items.len != 1) {
        return SqlError.InvalidSubquery;
    }
    if (result.rows.items.len != 1) {
        // SQL standard: return NULL if no rows, error if > 1 row
        if (result.rows.items.len == 0) {
            return ColumnValue.null_value;
        }
        return SqlError.InvalidSubquery; // Too many rows
    }

    return result.rows.items[0].items[0].clone(allocator);
}
```

**NOTE:** These functions need access to `Database` and the executor. We may need to move them to `executor.zig` or create a new module.

---

### 2.3: Update Binary Expression Evaluator for Subquery Operators

**File:** `src/sql.zig`

Find `evaluateBinaryExpr` or where binary operations are evaluated. Update to handle new operators:

```zig
fn evaluateBinaryExpr(bin: *const BinaryExpr, row_values: anytype, db: ?*Database) bool {
    switch (bin.op) {
        .in_op => {
            // IN operator
            if (db == null) return false; // Need database for subquery

            // Evaluate left side to get value
            const left_val = getExprValue(bin.left, row_values, db);

            // Right side must be subquery
            if (bin.right != .subquery) {
                return false; // Invalid: IN without subquery
            }

            const allocator = db.?.allocator;
            return evaluateInSubquery(
                left_val,
                bin.right.subquery,
                db.?,
                allocator,
                false  // not negated
            ) catch false;
        },

        .not_in_op => {
            // Similar to in_op but negated
            if (db == null) return false;
            const left_val = getExprValue(bin.left, row_values, db);
            if (bin.right != .subquery) return false;

            const allocator = db.?.allocator;
            return evaluateInSubquery(
                left_val,
                bin.right.subquery,
                db.?,
                allocator,
                true  // negated
            ) catch false;
        },

        .exists_op => {
            if (db == null) return false;
            if (bin.right != .subquery) return false;

            const allocator = db.?.allocator;
            return evaluateExistsSubquery(
                bin.right.subquery,
                db.?,
                allocator,
                false  // not negated
            ) catch false;
        },

        .not_exists_op => {
            if (db == null) return false;
            if (bin.right != .subquery) return false;

            const allocator = db.?.allocator;
            return evaluateExistsSubquery(
                bin.right.subquery,
                db.?,
                allocator,
                true  // negated
            ) catch false;
        },

        // For comparison operators with scalar subqueries
        .eq, .neq, .lt, .gt, .lte, .gte => {
            var left_val = getExprValue(bin.left, row_values, db);
            var right_val: ColumnValue = undefined;

            // If right is a subquery, execute it to get scalar value
            if (bin.right == .subquery) {
                if (db == null) return false;
                right_val = evaluateScalarSubquery(
                    bin.right.subquery,
                    db.?,
                    db.?.allocator
                ) catch return false;
            } else {
                right_val = getExprValue(bin.right, row_values, db);
            }

            // Now compare left_val and right_val
            return compareValues(left_val, right_val, bin.op);
        },

        // ... existing operators (and_op, or_op)
    }
}

/// Helper to extract value from expression
fn getExprValue(expr: Expr, row_values: anytype, db: ?*Database) ColumnValue {
    switch (expr) {
        .literal => |val| return val,
        .column => |col| {
            // Look up column in row_values
            var it = row_values.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, col)) {
                    return entry.value_ptr.*;
                }
            }
            return ColumnValue.null_value;
        },
        .subquery => {
            // Scalar subquery
            if (db == null) return ColumnValue.null_value;
            return evaluateScalarSubquery(
                expr.subquery,
                db.?,
                db.?.allocator
            ) catch ColumnValue.null_value;
        },
        .binary, .unary => {
            // For nested expressions, we'd need to evaluate them
            // This is complex - for now, return null
            return ColumnValue.null_value;
        },
    }
}
```

**Acceptance Criteria:**
- ✅ Subqueries execute correctly
- ✅ IN operator returns correct boolean result
- ✅ EXISTS operator returns correct boolean result
- ✅ Scalar subqueries work with comparison operators
- ✅ Proper error handling for invalid subqueries

---

## Phase 3: Handle Correlated Subqueries (Optional - 1 day)

**Correlated subqueries** reference columns from the outer query. Example:

```sql
SELECT * FROM users
WHERE EXISTS (
    SELECT 1 FROM orders
    WHERE orders.user_id = users.id  -- Correlation!
);
```

This is more complex because we need to pass outer query context into the subquery.

### 3.1: Pass Outer Context to Subquery Execution

**File:** `src/sql.zig` (or `src/database/executor.zig`)

Update subquery execution to accept outer row context:

```zig
/// Execute a subquery with outer row context for correlation
fn executeSubqueryWithContext(
    subquery: *const SelectCmd,
    db: *Database,
    outer_row_values: anytype,  // Values from outer query
    allocator: Allocator,
) !QueryResult {
    // This is complex! The subquery's WHERE clause may reference
    // columns from outer_row_values

    // One approach: Merge outer_row_values into the subquery's context
    // when evaluating WHERE clauses

    // TODO: Implement correlation support
    // For now, execute without correlation
    return executeSubquery(subquery, db, allocator);
}
```

**Decision:** Correlation support adds significant complexity. Recommend implementing as a **future enhancement** after basic subqueries work.

---

## Phase 4: Testing (1-2 days)

### 4.1: Unit Tests for Parser

**File:** `src/test_subqueries.zig` (new file)

```zig
const std = @import("std");
const expect = std.testing.expect;
const sql = @import("sql.zig");
const Database = @import("database/core.zig").Database;

test "parser: simple subquery in IN" {
    const query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    const allocator = std.testing.allocator;

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    // Verify it's a SELECT command
    try expect(cmd == .select);

    // Verify WHERE expression exists and contains subquery
    const select_cmd = cmd.select;
    try expect(select_cmd.where_expr != null);

    // WHERE expr should be a binary expression with IN operator
    const where_expr = select_cmd.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .in_op);

    // Right side should be a subquery
    try expect(where_expr.binary.right == .subquery);
}

test "parser: EXISTS subquery" {
    const query = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)";
    const allocator = std.testing.allocator;

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .exists_op);
    try expect(where_expr.binary.right == .subquery);
}

test "parser: scalar subquery comparison" {
    const query = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)";
    const allocator = std.testing.allocator;

    var cmd = try sql.parse(allocator, query);
    defer cmd.deinit(allocator);

    const where_expr = cmd.select.where_expr.?;
    try expect(where_expr == .binary);
    try expect(where_expr.binary.op == .gt);
    try expect(where_expr.binary.right == .subquery);
}
```

### 4.2: Integration Tests

**File:** `src/test_subqueries.zig`

```zig
test "subquery: IN operator" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // Setup tables
    _ = try db.execute("CREATE TABLE users (id int, name text)");
    _ = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");

    // Insert data
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie')");

    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 1, 200.0)");
    _ = try db.execute("INSERT INTO orders VALUES (3, 2, 50.0)");

    // Query: Find users who have orders
    var result = try db.execute(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
    );
    defer result.deinit();

    // Should return Alice and Bob (ids 1 and 2)
    try expect(result.rows.items.len == 2);

    // Verify Alice is in results
    var found_alice = false;
    for (result.rows.items) |row| {
        if (row.items[1] == .text) {
            if (std.mem.eql(u8, row.items[1].text, "Alice")) {
                found_alice = true;
            }
        }
    }
    try expect(found_alice);
}

test "subquery: NOT IN operator" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // ... setup same as above ...

    // Query: Find users who have NO orders
    var result = try db.execute(
        "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)"
    );
    defer result.deinit();

    // Should return only Charlie (id 3)
    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[1].text, "Charlie"));
}

test "subquery: EXISTS operator" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // ... setup ...

    // Query: Find users who have at least one order
    var result = try db.execute(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)"
    );
    defer result.deinit();

    // Should return Alice and Bob
    try expect(result.rows.items.len == 2);
}

test "subquery: scalar comparison" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE products (id int, name text, price float)");
    _ = try db.execute("INSERT INTO products VALUES (1, 'Widget', 10.0)");
    _ = try db.execute("INSERT INTO products VALUES (2, 'Gadget', 20.0)");
    _ = try db.execute("INSERT INTO products VALUES (3, 'Doohickey', 30.0)");

    // Find products more expensive than average
    var result = try db.execute(
        "SELECT name FROM products WHERE price > (SELECT AVG(price) FROM products)"
    );
    defer result.deinit();

    // Average is 20, so should return Doohickey (30)
    try expect(result.rows.items.len == 1);
    try expect(std.mem.eql(u8, result.rows.items[0].items[0].text, "Doohickey"));
}

test "subquery: with WHERE in subquery" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    // ... setup users and orders ...

    // Find users who have orders > 100
    var result = try db.execute(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100)"
    );
    defer result.deinit();

    // Only Alice has order of 200
    try expect(result.rows.items.len == 1);
}

test "subquery: empty result set" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text)");
    _ = try db.execute("CREATE TABLE orders (id int, user_id int)");

    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");

    // No orders exist - subquery returns empty set
    var result = try db.execute(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
    );
    defer result.deinit();

    // Should return no rows
    try expect(result.rows.items.len == 0);
}
```

### 4.3: Edge Case Tests

```zig
test "subquery: nested subqueries" {
    // SELECT * FROM a WHERE id IN (
    //     SELECT b_id FROM b WHERE b_id IN (
    //         SELECT c_id FROM c
    //     )
    // )
    // Complex but should work!
}

test "subquery: NULL handling" {
    // Test NULL in subquery results
    // Test NULL in comparison with subquery
}

test "subquery: multiple rows in scalar" {
    // Should return error when scalar subquery returns > 1 row
}
```

**Acceptance Criteria:**
- ✅ All parser tests pass
- ✅ All integration tests pass
- ✅ Edge cases handled correctly
- ✅ Error messages are clear

---

## Phase 5: Documentation (1 day)

### 5.1: Update SQL_FEATURES.md

Add section on subqueries:

```markdown
## Subqueries

ZVDB supports subqueries in WHERE clauses for powerful filtering.

### IN Operator

Find rows where column value exists in subquery result:

```sql
-- Find users who have placed orders
SELECT * FROM users
WHERE id IN (SELECT user_id FROM orders);

-- Find users who haven't placed orders
SELECT * FROM users
WHERE id NOT IN (SELECT user_id FROM orders);
```

### EXISTS Operator

Check if subquery returns any rows:

```sql
-- Find users with at least one order
SELECT * FROM users
WHERE EXISTS (
    SELECT 1 FROM orders WHERE orders.user_id = users.id
);

-- Find users with no orders
SELECT * FROM users
WHERE NOT EXISTS (
    SELECT 1 FROM orders WHERE orders.user_id = users.id
);
```

### Scalar Subqueries

Use subquery result in comparisons:

```sql
-- Find products more expensive than average
SELECT * FROM products
WHERE price > (SELECT AVG(price) FROM products);

-- Find users with orders above threshold
SELECT * FROM users
WHERE id IN (
    SELECT user_id FROM orders
    WHERE total > (SELECT AVG(total) FROM orders)
);
```

### Limitations

- Subqueries in SELECT list not yet supported
- Subqueries in FROM (derived tables) not yet supported
- Correlated subqueries have limited support
```

### 5.2: Update README.md

Add examples to showcase subquery features.

### 5.3: Add Inline Documentation

Document all new functions with clear comments.

---

## Implementation Timeline

### Day 1: Parser Foundation
- Morning: Add subquery variant to Expr, update BinaryOp
- Afternoon: Implement `isSubqueryStart()` and `parseSubquery()`

### Day 2: Parser Completion
- Morning: Update primary expression parser for subqueries
- Afternoon: Add IN, EXISTS operator parsing

### Day 3: Executor Foundation
- Morning: Refactor evaluateExpr to accept Database
- Afternoon: Update all call sites

### Day 4: Executor Implementation
- Morning: Implement executeSubquery and helper functions
- Afternoon: Update binary expression evaluator for new operators

### Day 5: Testing
- Morning: Write parser unit tests
- Afternoon: Write integration tests

### Day 6-7: Polish & Documentation
- Test edge cases
- Fix bugs
- Write documentation
- Add examples

---

## Success Criteria

### Functional Requirements
- ✅ IN operator works with subqueries
- ✅ NOT IN operator works
- ✅ EXISTS operator works
- ✅ NOT EXISTS operator works
- ✅ Scalar subqueries work in comparisons
- ✅ Subqueries can have WHERE clauses
- ✅ Nested subqueries work (subquery in subquery)

### Code Quality
- ✅ No memory leaks (all subquery allocations freed)
- ✅ Clear error messages for invalid subqueries
- ✅ 15+ comprehensive tests
- ✅ Backward compatible (no breaking changes to existing queries)

### Performance
- ✅ Subqueries execute efficiently
- ✅ No redundant subquery execution
- ✅ Proper cleanup of temporary results

---

## Known Limitations & Future Work

### Out of Scope for This Phase

1. **Subqueries in SELECT**
   ```sql
   -- Not supported yet
   SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users;
   ```

2. **Subqueries in FROM (Derived Tables)**
   ```sql
   -- Not supported yet
   SELECT * FROM (SELECT name, age FROM users WHERE age > 18) AS adults;
   ```

3. **Full Correlated Subquery Support**
   - Basic correlation may work but not thoroughly tested

4. **IN with List Literals**
   ```sql
   -- Not supported yet
   SELECT * FROM users WHERE id IN (1, 2, 3);
   ```

5. **ANY/ALL Operators**
   ```sql
   -- Not supported yet
   SELECT * FROM products WHERE price > ALL (SELECT price FROM products WHERE category = 'Electronics');
   ```

### Future Enhancements

- Query optimization: Cache subquery results when not correlated
- Subquery flattening: Convert simple subqueries to JOINs
- Index usage in subqueries
- LATERAL joins (correlated subqueries in FROM)

---

## Risk Mitigation

### Technical Risks

1. **Circular Dependencies**
   - **Risk:** sql.zig needs Database for execution, Database needs sql.zig for parsing
   - **Mitigation:** Use `?*Database` optional pointer, forward declarations

2. **Memory Management**
   - **Risk:** Subquery results must be freed after use
   - **Mitigation:** Use `defer result.deinit()` religiously, comprehensive leak testing

3. **Performance**
   - **Risk:** Subqueries execute for every row (N+1 problem)
   - **Mitigation:** Document limitation, add caching in future optimization phase

4. **Breaking Changes**
   - **Risk:** Refactoring evaluateExpr signature breaks existing code
   - **Mitigation:** Update all call sites systematically, add compile-time tests

### Testing Risks

1. **Complex Edge Cases**
   - **Mitigation:** Comprehensive test suite, test each operator independently first

2. **Correlated Subqueries**
   - **Mitigation:** Mark as experimental, add warnings, thorough documentation

---

## Conclusion

This plan implements production-ready subquery support for zvdb in 5-7 days. The implementation is:

- **Incremental**: Each phase builds on the previous
- **Testable**: Each phase has clear acceptance criteria
- **Professional**: Proper error handling, memory management, documentation
- **Pragmatic**: Focuses on common use cases (IN, EXISTS), defers complex features

After completion, zvdb will support most common SQL subquery patterns, making it significantly more powerful for complex queries!

**Ready to start implementation!** 🚀
