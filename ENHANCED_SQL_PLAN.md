# Enhanced SQL Features Implementation Plan

**Date:** 2025-11-14
**Goal:** Implement essential SQL features to make zvdb a more complete relational database
**Estimated Total Effort:** 4-5 weeks (1 developer)
**Difficulty:** Low-Medium (Great for learning!)

---

## Executive Summary

This plan adds four core SQL feature categories to zvdb:

1. **Aggregations** (COUNT, SUM, AVG, MIN, MAX) - 1 week
2. **GROUP BY** - 1 week
3. **JOINs** (INNER, LEFT, RIGHT) - 1.5 weeks
4. **Subqueries** - 1 week

**Current SQL Capabilities:**
✅ CREATE TABLE, INSERT, SELECT, DELETE, UPDATE
✅ WHERE clauses with complex expressions (AND, OR, NOT, comparisons)
✅ B-tree indexes with automatic query optimization
✅ Transactions (BEGIN/COMMIT/ROLLBACK)
✅ WAL for crash recovery

**Why This Order?**
- **Aggregations first**: Simplest to implement, immediate value
- **GROUP BY second**: Natural extension of aggregations
- **JOINs third**: More complex, requires understanding of the first two
- **Subqueries last**: Most complex, builds on all previous features

---

## Phase 1: Aggregate Functions (1 week)

### Overview
Add SQL aggregate functions: `COUNT()`, `SUM()`, `AVG()`, `MIN()`, `MAX()`

**Example queries after implementation:**
```sql
SELECT COUNT(*) FROM users;
SELECT COUNT(age) FROM users;
SELECT SUM(balance) FROM accounts;
SELECT AVG(price) FROM products;
SELECT MIN(created_at), MAX(created_at) FROM orders;
```

---

### Phase 1.1: Extend SQL Parser (2 days)

**File:** Modify `src/sql.zig`

**Tasks:**

1. **Add aggregate function enum**
```zig
/// Aggregate function types
pub const AggregateFunc = enum {
    count,
    sum,
    avg,
    min,
    max,

    pub fn fromString(s: []const u8) ?AggregateFunc {
        if (eqlIgnoreCase(s, "COUNT")) return .count;
        if (eqlIgnoreCase(s, "SUM")) return .sum;
        if (eqlIgnoreCase(s, "AVG")) return .avg;
        if (eqlIgnoreCase(s, "MIN")) return .min;
        if (eqlIgnoreCase(s, "MAX")) return .max;
        return null;
    }
};
```

2. **Add aggregate expression type**
```zig
/// Column selection with optional aggregation
pub const SelectColumn = union(enum) {
    regular: []const u8,           // Regular column: "name"
    aggregate: AggregateExpr,      // Aggregate: COUNT(*), SUM(balance)
    star: void,                    // SELECT *

    pub fn deinit(self: *SelectColumn, allocator: Allocator) void {
        switch (self.*) {
            .regular => |col| allocator.free(col),
            .aggregate => |*agg| agg.deinit(allocator),
            .star => {},
        }
    }
};

pub const AggregateExpr = struct {
    func: AggregateFunc,
    column: ?[]const u8,  // null for COUNT(*)

    pub fn deinit(self: *AggregateExpr, allocator: Allocator) void {
        if (self.column) |col| allocator.free(col);
    }
};
```

3. **Update SelectCmd structure**
```zig
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn),  // Changed from ArrayList([]const u8)
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    similar_to_column: ?[]const u8,
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8,
    order_by_vibes: bool,
    limit: ?usize,

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit();
        // ... rest of cleanup
    }
};
```

4. **Update SELECT parser to detect aggregates**
```zig
// In parseSelect function, around line 550:
fn parseSelect(allocator: Allocator, tokens: []const Token) !SelectCmd {
    // ... existing code ...

    var columns = ArrayList(SelectColumn).init(allocator);
    var i: usize = 1;

    // Parse columns
    while (i < tokens.len and !eqlIgnoreCase(tokens[i].text, "FROM")) {
        if (!std.mem.eql(u8, tokens[i].text, ",")) {
            if (std.mem.eql(u8, tokens[i].text, "*")) {
                try columns.append(.star);
            } else {
                // Check if this is an aggregate function call
                if (AggregateFunc.fromString(tokens[i].text)) |func| {
                    // Parse: COUNT(*) or COUNT(column)
                    i += 1;
                    if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, "(")) {
                        return SqlError.InvalidSyntax;
                    }
                    i += 1;

                    var agg_column: ?[]const u8 = null;
                    if (i < tokens.len and !std.mem.eql(u8, tokens[i].text, "*")) {
                        // Named column: COUNT(age)
                        agg_column = try allocator.dupe(u8, tokens[i].text);
                    }
                    // else: COUNT(*) - column remains null

                    i += 1; // Skip column or *
                    if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, ")")) {
                        return SqlError.InvalidSyntax;
                    }

                    try columns.append(.{
                        .aggregate = .{
                            .func = func,
                            .column = agg_column,
                        },
                    });
                } else {
                    // Regular column
                    try columns.append(.{
                        .regular = try allocator.dupe(u8, tokens[i].text),
                    });
                }
            }
        }
        i += 1;
    }

    // ... rest of function
}
```

**Acceptance Criteria:**
- ✅ Parser correctly identifies aggregate functions
- ✅ Handles `COUNT(*)` vs `COUNT(column)`
- ✅ Mixed queries: `SELECT name, COUNT(*) FROM users` (will error for now, GROUP BY needed)

---

### Phase 1.2: Implement Aggregate Execution (2-3 days)

**File:** Modify `src/database/executor.zig`

**Tasks:**

1. **Add aggregate computation helper**
```zig
/// Aggregate accumulator
const AggregateState = struct {
    func: AggregateFunc,
    column: ?[]const u8,

    // Accumulator state
    count: u64,
    sum: f64,
    min: ?ColumnValue,
    max: ?ColumnValue,

    allocator: Allocator,

    pub fn init(allocator: Allocator, func: AggregateFunc, column: ?[]const u8) AggregateState {
        return .{
            .func = func,
            .column = column,
            .count = 0,
            .sum = 0.0,
            .min = null,
            .max = null,
            .allocator = allocator,
        };
    }

    /// Process a single row
    pub fn accumulate(self: *AggregateState, row: *const Row) !void {
        switch (self.func) {
            .count => {
                // COUNT(*) counts all rows
                if (self.column == null) {
                    self.count += 1;
                } else {
                    // COUNT(column) counts non-null values
                    if (row.get(self.column.?)) |val| {
                        if (val != .null_value) {
                            self.count += 1;
                        }
                    }
                }
            },
            .sum, .avg => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        const num = switch (val) {
                            .int => |i| @as(f64, @floatFromInt(i)),
                            .float => |f| f,
                            .null_value => return, // Skip nulls
                            else => return error.TypeMismatch,
                        };
                        self.sum += num;
                        self.count += 1;
                    }
                }
            },
            .min => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        if (val == .null_value) return; // Skip nulls

                        if (self.min == null) {
                            self.min = try val.clone(self.allocator);
                        } else {
                            // Compare and update if smaller
                            if (compareForMinMax(val, self.min.?) == .lt) {
                                var old = self.min.?;
                                old.deinit(self.allocator);
                                self.min = try val.clone(self.allocator);
                            }
                        }
                    }
                }
            },
            .max => {
                if (self.column) |col| {
                    if (row.get(col)) |val| {
                        if (val == .null_value) return; // Skip nulls

                        if (self.max == null) {
                            self.max = try val.clone(self.allocator);
                        } else {
                            // Compare and update if larger
                            if (compareForMinMax(val, self.max.?) == .gt) {
                                var old = self.max.?;
                                old.deinit(self.allocator);
                                self.max = try val.clone(self.allocator);
                            }
                        }
                    }
                }
            },
        }
    }

    /// Get final result
    pub fn finalize(self: *AggregateState) !ColumnValue {
        switch (self.func) {
            .count => return ColumnValue{ .int = @intCast(self.count) },
            .sum => return ColumnValue{ .float = self.sum },
            .avg => {
                if (self.count == 0) return ColumnValue.null_value;
                return ColumnValue{ .float = self.sum / @as(f64, @floatFromInt(self.count)) };
            },
            .min => return self.min orelse ColumnValue.null_value,
            .max => return self.max orelse ColumnValue.null_value,
        }
    }

    pub fn deinit(self: *AggregateState) void {
        if (self.min) |*m| {
            var val = m.*;
            val.deinit(self.allocator);
        }
        if (self.max) |*m| {
            var val = m.*;
            val.deinit(self.allocator);
        }
    }
};

fn compareForMinMax(a: ColumnValue, b: ColumnValue) std.math.Order {
    // Similar to compareValues but returns Order
    switch (a) {
        .int => |ai| {
            const bi = switch (b) {
                .int => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return .eq,
            };
            if (ai < bi) return .lt;
            if (ai > bi) return .gt;
            return .eq;
        },
        .float => |af| {
            const bf = switch (b) {
                .float => |f| f,
                .int => |i| @as(f64, @floatFromInt(i)),
                else => return .eq,
            };
            if (af < bf) return .lt;
            if (af > bf) return .gt;
            return .eq;
        },
        .text => |at| {
            if (b != .text) return .eq;
            return std.mem.order(u8, at, b.text);
        },
        else => return .eq,
    }
}
```

2. **Modify executeSelect to handle aggregates**
```zig
fn executeSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;
    var result = QueryResult.init(db.allocator);

    // Check if this is an aggregate query
    var has_aggregates = false;
    var has_regular_columns = false;

    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => has_aggregates = true,
            .regular => has_regular_columns = true,
            .star => has_regular_columns = true,
        }
    }

    // Error: Cannot mix aggregates with regular columns without GROUP BY
    if (has_aggregates and has_regular_columns) {
        return error.MixedAggregateAndRegular; // Will be fixed with GROUP BY
    }

    if (has_aggregates) {
        return executeAggregateSelect(db, table, cmd);
    } else {
        return executeRegularSelect(db, table, cmd);
    }
}

fn executeAggregateSelect(db: *Database, table: *Table, cmd: sql.SelectCmd) !QueryResult {
    var result = QueryResult.init(db.allocator);

    // Initialize aggregate states
    var agg_states = std.ArrayList(AggregateState).init(db.allocator);
    defer {
        for (agg_states.items) |*state| {
            state.deinit();
        }
        agg_states.deinit();
    }

    // Setup result columns and aggregate states
    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => |agg| {
                // Add column name to result
                const col_name = switch (agg.func) {
                    .count => if (agg.column) |c|
                        try std.fmt.allocPrint(db.allocator, "COUNT({s})", .{c})
                    else
                        try db.allocator.dupe(u8, "COUNT(*)"),
                    .sum => try std.fmt.allocPrint(db.allocator, "SUM({s})", .{agg.column.?}),
                    .avg => try std.fmt.allocPrint(db.allocator, "AVG({s})", .{agg.column.?}),
                    .min => try std.fmt.allocPrint(db.allocator, "MIN({s})", .{agg.column.?}),
                    .max => try std.fmt.allocPrint(db.allocator, "MAX({s})", .{agg.column.?}),
                };
                try result.addColumn(col_name);

                // Initialize aggregate state
                try agg_states.append(AggregateState.init(db.allocator, agg.func, agg.column));
            },
            else => unreachable, // Already validated
        }
    }

    // Scan all rows and accumulate
    const row_ids = try table.getAllRows(db.allocator);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id) orelse continue;

        // Apply WHERE filter
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                const row_val = row.get(where_col) orelse continue;
                if (!valuesEqual(row_val, where_val)) continue;
            }
        }

        // Accumulate in all aggregate states
        for (agg_states.items) |*state| {
            try state.accumulate(row);
        }
    }

    // Finalize and create result row
    var result_row = ArrayList(ColumnValue).init(db.allocator);
    for (agg_states.items) |*state| {
        const final_value = try state.finalize();
        try result_row.append(final_value);
    }
    try result.addRow(result_row);

    return result;
}

fn executeRegularSelect(db: *Database, table: *Table, cmd: sql.SelectCmd) !QueryResult {
    // This is the existing executeSelect logic
    // Move the current implementation here (lines 200-356)
    // ... existing SELECT logic ...
}
```

**Acceptance Criteria:**
- ✅ `SELECT COUNT(*) FROM table` returns total row count
- ✅ `SELECT SUM(balance) FROM accounts` computes sum
- ✅ `SELECT AVG(price), MIN(price), MAX(price) FROM products` works
- ✅ WHERE clauses work with aggregates: `SELECT COUNT(*) FROM users WHERE age > 18`

---

### Phase 1.3: Testing Aggregates (1 day)

**File:** Create `src/test_aggregates.zig`

**Tests:**
```zig
const std = @import("std");
const expect = std.testing.expect;
const Database = @import("database/core.zig").Database;

test "aggregate: COUNT(*)" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    var result = try db.execute("SELECT COUNT(*) FROM users");
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    try expect(result.rows.items[0].items[0].int == 3);
}

test "aggregate: COUNT(column) with nulls" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', NULL)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    var result = try db.execute("SELECT COUNT(age) FROM users");
    defer result.deinit();

    try expect(result.rows.items[0].items[0].int == 2); // NULL not counted
}

test "aggregate: SUM and AVG" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 100.5)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 200.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 150.5)");

    var sum_result = try db.execute("SELECT SUM(amount) FROM sales");
    defer sum_result.deinit();
    try expect(sum_result.rows.items[0].items[0].float == 451.0);

    var avg_result = try db.execute("SELECT AVG(amount) FROM sales");
    defer avg_result.deinit();
    try expect(@abs(avg_result.rows.items[0].items[0].float - 150.33) < 0.01);
}

test "aggregate: MIN and MAX" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE prices (id int, price float)");
    _ = try db.execute("INSERT INTO prices VALUES (1, 9.99)");
    _ = try db.execute("INSERT INTO prices VALUES (2, 19.99)");
    _ = try db.execute("INSERT INTO prices VALUES (3, 4.99)");

    var result = try db.execute("SELECT MIN(price), MAX(price) FROM prices");
    defer result.deinit();

    try expect(result.rows.items[0].items[0].float == 4.99);
    try expect(result.rows.items[0].items[1].float == 19.99);
}

test "aggregate: with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    var result = try db.execute("SELECT COUNT(*) FROM users WHERE age > 25");
    defer result.deinit();

    try expect(result.rows.items[0].items[0].int == 2);
}

test "aggregate: empty table" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE empty (id int, value float)");

    var count_result = try db.execute("SELECT COUNT(*) FROM empty");
    defer count_result.deinit();
    try expect(count_result.rows.items[0].items[0].int == 0);

    var avg_result = try db.execute("SELECT AVG(value) FROM empty");
    defer avg_result.deinit();
    try expect(avg_result.rows.items[0].items[0] == .null_value);
}
```

**Acceptance Criteria:**
- ✅ All aggregate tests pass
- ✅ NULL handling correct
- ✅ Works with empty tables
- ✅ Works with WHERE clauses

---

## Phase 2: GROUP BY (1 week)

### Overview
Add `GROUP BY` to enable aggregation over groups of rows.

**Example queries after implementation:**
```sql
SELECT department, COUNT(*) FROM employees GROUP BY department;
SELECT category, AVG(price) FROM products GROUP BY category;
SELECT city, state, COUNT(*) FROM customers GROUP BY city, state;
```

---

### Phase 2.1: Extend Parser for GROUP BY (2 days)

**File:** Modify `src/sql.zig`

**Tasks:**

1. **Add GROUP BY to SelectCmd**
```zig
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn),
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    similar_to_column: ?[]const u8,
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8,
    order_by_vibes: bool,
    group_by: ArrayList([]const u8),  // NEW: GROUP BY columns
    limit: ?usize,

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit();

        // Free GROUP BY columns
        for (self.group_by.items) |col| {
            allocator.free(col);
        }
        self.group_by.deinit();

        // ... rest of cleanup
    }
};
```

2. **Parse GROUP BY clause**
```zig
// In parseSelect function, after WHERE and before ORDER BY:
fn parseSelect(allocator: Allocator, tokens: []const Token) !SelectCmd {
    // ... existing parsing ...

    var group_by = ArrayList([]const u8).init(allocator);

    // Parse WHERE, GROUP BY, ORDER BY, LIMIT
    while (i < tokens.len) {
        if (eqlIgnoreCase(tokens[i].text, "WHERE")) {
            // ... existing WHERE parsing ...
        } else if (eqlIgnoreCase(tokens[i].text, "GROUP")) {
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "BY")) {
                return SqlError.InvalidSyntax;
            }
            i += 1;

            // Parse comma-separated list of columns
            while (i < tokens.len) {
                if (eqlIgnoreCase(tokens[i].text, "ORDER") or
                    eqlIgnoreCase(tokens[i].text, "LIMIT")) {
                    break;
                }

                if (!std.mem.eql(u8, tokens[i].text, ",")) {
                    try group_by.append(try allocator.dupe(u8, tokens[i].text));
                }
                i += 1;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "ORDER")) {
            // ... existing ORDER BY parsing ...
        }
        // ... rest
    }

    return SelectCmd{
        // ... existing fields ...
        .group_by = group_by,
    };
}
```

**Acceptance Criteria:**
- ✅ Parser handles `GROUP BY col1, col2`
- ✅ Validates GROUP BY comes after WHERE
- ✅ Validates GROUP BY comes before ORDER BY

---

### Phase 2.2: Implement GROUP BY Execution (3-4 days)

**File:** Modify `src/database/executor.zig`

**Concepts:**
- Hash table mapping group keys to aggregate states
- Group key = concatenation of grouping column values

**Tasks:**

1. **Add group key helper**
```zig
/// Create a hash key from group column values
fn makeGroupKey(allocator: Allocator, row: *const Row, group_columns: []const []const u8) ![]u8 {
    var key_parts = std.ArrayList([]const u8).init(allocator);
    defer key_parts.deinit();

    for (group_columns) |col| {
        const val = row.get(col) orelse {
            try key_parts.append("NULL");
            continue;
        };

        const val_str = switch (val) {
            .int => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .text => |t| try allocator.dupe(u8, t),
            .bool => |b| if (b) try allocator.dupe(u8, "true") else try allocator.dupe(u8, "false"),
            .null_value => try allocator.dupe(u8, "NULL"),
            else => try allocator.dupe(u8, ""),
        };
        try key_parts.append(val_str);
    }

    // Join with separator
    return std.mem.join(allocator, "|", key_parts.items);
}
```

2. **Implement GROUP BY execution**
```zig
fn executeGroupBySelect(
    db: *Database,
    table: *Table,
    cmd: sql.SelectCmd
) !QueryResult {
    var result = QueryResult.init(db.allocator);

    // Map: group_key -> (group_values, aggregate_states)
    const GroupData = struct {
        group_values: std.ArrayList(ColumnValue),  // Values of grouping columns
        agg_states: std.ArrayList(AggregateState),

        fn deinit(self: *@This()) void {
            for (self.group_values.items) |*val| {
                var v = val.*;
                v.deinit(self.agg_states.items[0].allocator);
            }
            self.group_values.deinit();

            for (self.agg_states.items) |*state| {
                state.deinit();
            }
            self.agg_states.deinit();
        }
    };

    var groups = std.StringHashMap(GroupData).init(db.allocator);
    defer {
        var it = groups.iterator();
        while (it.next()) |entry| {
            db.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit();
        }
        groups.deinit();
    }

    // Setup result columns
    // 1. Group columns
    for (cmd.group_by.items) |col| {
        try result.addColumn(col);
    }

    // 2. Aggregate columns
    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => |agg| {
                const col_name = switch (agg.func) {
                    .count => if (agg.column) |c|
                        try std.fmt.allocPrint(db.allocator, "COUNT({s})", .{c})
                    else
                        try db.allocator.dupe(u8, "COUNT(*)"),
                    .sum => try std.fmt.allocPrint(db.allocator, "SUM({s})", .{agg.column.?}),
                    .avg => try std.fmt.allocPrint(db.allocator, "AVG({s})", .{agg.column.?}),
                    .min => try std.fmt.allocPrint(db.allocator, "MIN({s})", .{agg.column.?}),
                    .max => try std.fmt.allocPrint(db.allocator, "MAX({s})", .{agg.column.?}),
                };
                try result.addColumn(col_name);
            },
            .regular => |col_name| {
                // Regular column must be in GROUP BY
                var found = false;
                for (cmd.group_by.items) |group_col| {
                    if (std.mem.eql(u8, col_name, group_col)) {
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    return error.ColumnNotInGroupBy;
                }
            },
            .star => return error.CannotUseStarWithGroupBy,
        }
    }

    // Scan rows and build groups
    const row_ids = try table.getAllRows(db.allocator);
    defer db.allocator.free(row_ids);

    for (row_ids) |row_id| {
        const row = table.get(row_id) orelse continue;

        // Apply WHERE filter
        if (cmd.where_column) |where_col| {
            if (cmd.where_value) |where_val| {
                const row_val = row.get(where_col) orelse continue;
                if (!valuesEqual(row_val, where_val)) continue;
            }
        }

        // Create group key
        const group_key = try makeGroupKey(db.allocator, row, cmd.group_by.items);

        // Get or create group
        const gop = try groups.getOrPut(group_key);
        if (!gop.found_existing) {
            // New group - initialize
            var group_values = std.ArrayList(ColumnValue).init(db.allocator);
            for (cmd.group_by.items) |col| {
                const val = row.get(col) orelse ColumnValue.null_value;
                try group_values.append(try val.clone(db.allocator));
            }

            var agg_states = std.ArrayList(AggregateState).init(db.allocator);
            for (cmd.columns.items) |col| {
                if (col == .aggregate) {
                    try agg_states.append(AggregateState.init(
                        db.allocator,
                        col.aggregate.func,
                        col.aggregate.column
                    ));
                }
            }

            gop.value_ptr.* = GroupData{
                .group_values = group_values,
                .agg_states = agg_states,
            };
        } else {
            // Group exists - free the duplicate key
            db.allocator.free(group_key);
        }

        // Accumulate in aggregate states
        for (gop.value_ptr.agg_states.items) |*state| {
            try state.accumulate(row);
        }
    }

    // Finalize all groups and create result rows
    var group_it = groups.iterator();
    while (group_it.next()) |entry| {
        var result_row = ArrayList(ColumnValue).init(db.allocator);

        // Add group column values
        for (entry.value_ptr.group_values.items) |val| {
            try result_row.append(try val.clone(db.allocator));
        }

        // Add aggregate results
        for (entry.value_ptr.agg_states.items) |*state| {
            const final_value = try state.finalize();
            try result_row.append(final_value);
        }

        try result.addRow(result_row);
    }

    return result;
}
```

3. **Update executeSelect to route to GROUP BY handler**
```zig
fn executeSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Check for GROUP BY
    if (cmd.group_by.items.len > 0) {
        return executeGroupBySelect(db, table, cmd);
    }

    // Check for aggregates without GROUP BY
    var has_aggregates = false;
    var has_regular_columns = false;

    for (cmd.columns.items) |col| {
        switch (col) {
            .aggregate => has_aggregates = true,
            .regular => has_regular_columns = true,
            .star => has_regular_columns = true,
        }
    }

    if (has_aggregates) {
        if (has_regular_columns) {
            return error.MixedAggregateAndRegular;
        }
        return executeAggregateSelect(db, table, cmd);
    } else {
        return executeRegularSelect(db, table, cmd);
    }
}
```

**Acceptance Criteria:**
- ✅ `SELECT dept, COUNT(*) FROM emp GROUP BY dept` works
- ✅ Multiple GROUP BY columns work
- ✅ Error if non-aggregate, non-GROUP BY column in SELECT
- ✅ Works with WHERE clause

---

### Phase 2.3: Testing GROUP BY (1 day)

**File:** Add to `src/test_aggregates.zig`

**Tests:**
```zig
test "GROUP BY: single column" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, dept text, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 'electronics', 100.0)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 'electronics', 150.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 'clothing', 75.0)");
    _ = try db.execute("INSERT INTO sales VALUES (4, 'clothing', 50.0)");

    var result = try db.execute("SELECT dept, COUNT(*), SUM(amount) FROM sales GROUP BY dept");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
    // Results might be in any order, check both exist
}

test "GROUP BY: multiple columns" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE orders (id int, city text, state text, total float)");
    _ = try db.execute("INSERT INTO orders VALUES (1, 'NYC', 'NY', 100)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 'NYC', 'NY', 200)");
    _ = try db.execute("INSERT INTO orders VALUES (3, 'Buffalo', 'NY', 50)");

    var result = try db.execute("SELECT city, state, SUM(total) FROM orders GROUP BY city, state");
    defer result.deinit();

    try expect(result.rows.items.len == 2);
}

test "GROUP BY: with WHERE clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE sales (id int, dept text, amount float)");
    _ = try db.execute("INSERT INTO sales VALUES (1, 'electronics', 100.0)");
    _ = try db.execute("INSERT INTO sales VALUES (2, 'electronics', 150.0)");
    _ = try db.execute("INSERT INTO sales VALUES (3, 'electronics', 30.0)");

    var result = try db.execute("SELECT dept, COUNT(*) FROM sales WHERE amount > 50 GROUP BY dept");
    defer result.deinit();

    try expect(result.rows.items[0].items[1].int == 2);
}
```

---

## Phase 3: JOINs (1.5 weeks)

### Overview
Implement INNER JOIN, LEFT JOIN, and RIGHT JOIN.

**Example queries:**
```sql
SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id;
SELECT users.name, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id;
```

---

### Phase 3.1: Extend Parser for JOINs (2-3 days)

**File:** Modify `src/sql.zig`

**Tasks:**

1. **Add JOIN structures**
```zig
pub const JoinType = enum {
    inner,
    left,
    right,
    // cross,  // Future: CROSS JOIN
};

pub const JoinClause = struct {
    join_type: JoinType,
    table_name: []const u8,
    left_column: []const u8,   // e.g., "users.id"
    right_column: []const u8,  // e.g., "orders.user_id"

    pub fn deinit(self: *JoinClause, allocator: Allocator) void {
        allocator.free(self.table_name);
        allocator.free(self.left_column);
        allocator.free(self.right_column);
    }
};
```

2. **Update SelectCmd**
```zig
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn),
    joins: ArrayList(JoinClause),  // NEW
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    // ... rest

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit();

        for (self.joins.items) |*join| {
            join.deinit(allocator);
        }
        self.joins.deinit();

        // ... rest
    }
};
```

3. **Parse JOIN clauses**
```zig
// In parseSelect, after FROM table_name:
fn parseSelect(allocator: Allocator, tokens: []const Token) !SelectCmd {
    // ... parse columns and FROM ...

    const table_name = try allocator.dupe(u8, tokens[i].text);
    i += 1;

    var joins = ArrayList(JoinClause).init(allocator);

    // Parse JOINs
    while (i < tokens.len) {
        // Check for JOIN keywords
        if (eqlIgnoreCase(tokens[i].text, "INNER") or
            eqlIgnoreCase(tokens[i].text, "LEFT") or
            eqlIgnoreCase(tokens[i].text, "RIGHT")) {

            const join_type: JoinType = if (eqlIgnoreCase(tokens[i].text, "INNER"))
                .inner
            else if (eqlIgnoreCase(tokens[i].text, "LEFT"))
                .left
            else
                .right;

            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "JOIN")) {
                return SqlError.InvalidSyntax;
            }
            i += 1;

            // Parse: table_name ON left_col = right_col
            if (i >= tokens.len) return SqlError.InvalidSyntax;
            const join_table = try allocator.dupe(u8, tokens[i].text);
            i += 1;

            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "ON")) {
                return SqlError.InvalidSyntax;
            }
            i += 1;

            // Parse: left_column = right_column
            if (i + 2 >= tokens.len) return SqlError.InvalidSyntax;
            const left_col = try allocator.dupe(u8, tokens[i].text);
            i += 1;

            if (!std.mem.eql(u8, tokens[i].text, "=")) {
                return SqlError.InvalidSyntax;
            }
            i += 1;

            const right_col = try allocator.dupe(u8, tokens[i].text);
            i += 1;

            try joins.append(JoinClause{
                .join_type = join_type,
                .table_name = join_table,
                .left_column = left_col,
                .right_column = right_col,
            });
        } else if (eqlIgnoreCase(tokens[i].text, "JOIN")) {
            // Default to INNER JOIN
            i += 1;
            // ... similar parsing as above ...
        } else {
            break; // No more joins
        }
    }

    // Continue with WHERE, GROUP BY, etc.
    // ...

    return SelectCmd{
        .table_name = table_name,
        .columns = columns,
        .joins = joins,
        // ... rest
    };
}
```

**Acceptance Criteria:**
- ✅ Parser handles INNER JOIN, LEFT JOIN, RIGHT JOIN
- ✅ Handles qualified column names (table.column)
- ✅ Multiple JOINs supported

---

### Phase 3.2: Implement JOIN Execution (4-5 days)

**File:** Modify `src/database/executor.zig`

**Algorithm:**
- Nested loop join (simple, works for small datasets)
- For LEFT/RIGHT JOIN, track unmatched rows

**Tasks:**

1. **Helper to parse qualified column names**
```zig
fn splitQualifiedColumn(col_name: []const u8) struct { table: ?[]const u8, column: []const u8 } {
    if (std.mem.indexOf(u8, col_name, ".")) |dot_idx| {
        return .{
            .table = col_name[0..dot_idx],
            .column = col_name[dot_idx + 1 ..],
        };
    }
    return .{ .table = null, .column = col_name };
}
```

2. **Implement JOIN execution**
```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    var result = QueryResult.init(db.allocator);

    // Get base table
    const base_table = db.tables.get(cmd.table_name) orelse return error.TableNotFound;

    // For simplicity, only support single JOIN for now
    // Multi-join is just repeated application
    if (cmd.joins.items.len == 0) return error.NoJoins;
    const join = cmd.joins.items[0];

    const join_table = db.tables.get(join.table_name) orelse return error.TableNotFound;

    // Parse join columns
    const left_parts = splitQualifiedColumn(join.left_column);
    const right_parts = splitQualifiedColumn(join.right_column);

    // Setup result columns
    // If SELECT *, include all columns from both tables
    const select_all = cmd.columns.items.len == 1 and cmd.columns.items[0] == .star;

    if (select_all) {
        for (base_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                db.allocator,
                "{s}.{s}",
                .{ cmd.table_name, col.name }
            );
            try result.addColumn(qualified);
        }
        for (join_table.columns.items) |col| {
            const qualified = try std.fmt.allocPrint(
                db.allocator,
                "{s}.{s}",
                .{ join.table_name, col.name }
            );
            try result.addColumn(qualified);
        }
    } else {
        for (cmd.columns.items) |col_spec| {
            switch (col_spec) {
                .regular => |col_name| try result.addColumn(col_name),
                .aggregate => return error.AggregateNotSupportedInJoin,
                .star => try result.addColumn("*"),
            }
        }
    }

    // Perform nested loop join
    const base_row_ids = try base_table.getAllRows(db.allocator);
    defer db.allocator.free(base_row_ids);

    const join_row_ids = try join_table.getAllRows(db.allocator);
    defer db.allocator.free(join_row_ids);

    switch (join.join_type) {
        .inner => {
            for (base_row_ids) |base_id| {
                const base_row = base_table.get(base_id) orelse continue;
                const left_val = base_row.get(left_parts.column) orelse continue;

                for (join_row_ids) |join_id| {
                    const join_row = join_table.get(join_id) orelse continue;
                    const right_val = join_row.get(right_parts.column) orelse continue;

                    // Check join condition
                    if (valuesEqual(left_val, right_val)) {
                        // Match! Create result row
                        var result_row = ArrayList(ColumnValue).init(db.allocator);

                        if (select_all) {
                            // Add all columns from base table
                            for (base_table.columns.items) |col| {
                                const val = base_row.get(col.name) orelse ColumnValue.null_value;
                                try result_row.append(try val.clone(db.allocator));
                            }
                            // Add all columns from join table
                            for (join_table.columns.items) |col| {
                                const val = join_row.get(col.name) orelse ColumnValue.null_value;
                                try result_row.append(try val.clone(db.allocator));
                            }
                        } else {
                            // Add only selected columns
                            for (cmd.columns.items) |col_spec| {
                                if (col_spec == .regular) {
                                    const col_name = col_spec.regular;
                                    const parts = splitQualifiedColumn(col_name);

                                    const val = if (parts.table) |tbl| blk: {
                                        if (std.mem.eql(u8, tbl, cmd.table_name)) {
                                            break :blk base_row.get(parts.column);
                                        } else {
                                            break :blk join_row.get(parts.column);
                                        }
                                    } else blk: {
                                        // Try both tables
                                        break :blk base_row.get(col_name) orelse join_row.get(col_name);
                                    };

                                    try result_row.append(try (val orelse ColumnValue.null_value).clone(db.allocator));
                                }
                            }
                        }

                        try result.addRow(result_row);
                    }
                }
            }
        },
        .left => {
            for (base_row_ids) |base_id| {
                const base_row = base_table.get(base_id) orelse continue;
                const left_val = base_row.get(left_parts.column) orelse continue;

                var matched = false;

                for (join_row_ids) |join_id| {
                    const join_row = join_table.get(join_id) orelse continue;
                    const right_val = join_row.get(right_parts.column) orelse continue;

                    if (valuesEqual(left_val, right_val)) {
                        matched = true;
                        // Create result row (same as INNER JOIN)
                        var result_row = ArrayList(ColumnValue).init(db.allocator);

                        // Add base table columns
                        for (base_table.columns.items) |col| {
                            const val = base_row.get(col.name) orelse ColumnValue.null_value;
                            try result_row.append(try val.clone(db.allocator));
                        }

                        // Add join table columns
                        for (join_table.columns.items) |col| {
                            const val = join_row.get(col.name) orelse ColumnValue.null_value;
                            try result_row.append(try val.clone(db.allocator));
                        }

                        try result.addRow(result_row);
                    }
                }

                // LEFT JOIN: If no match, still include base row with NULLs for join table
                if (!matched) {
                    var result_row = ArrayList(ColumnValue).init(db.allocator);

                    // Add base table columns
                    for (base_table.columns.items) |col| {
                        const val = base_row.get(col.name) orelse ColumnValue.null_value;
                        try result_row.append(try val.clone(db.allocator));
                    }

                    // Add NULLs for join table
                    for (join_table.columns.items) |_| {
                        try result_row.append(ColumnValue.null_value);
                    }

                    try result.addRow(result_row);
                }
            }
        },
        .right => {
            // Similar to LEFT JOIN but swap base and join tables
            // Implementation left as exercise
        },
    }

    return result;
}
```

3. **Route JOIN queries**
```zig
fn executeSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    const table = db.tables.get(cmd.table_name) orelse return sql.SqlError.TableNotFound;

    // Check for JOINs
    if (cmd.joins.items.len > 0) {
        return executeJoinSelect(db, cmd);
    }

    // ... rest of existing routing logic ...
}
```

**Acceptance Criteria:**
- ✅ INNER JOIN works
- ✅ LEFT JOIN includes unmatched left rows
- ✅ SELECT * includes all columns from both tables
- ✅ Qualified column names work

---

### Phase 3.3: Testing JOINs (2 days)

**File:** Create `src/test_joins.zig`

**Tests:**
```zig
test "INNER JOIN: basic" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text)");
    _ = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");

    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");

    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 1, 200.0)");
    _ = try db.execute("INSERT INTO orders VALUES (3, 2, 50.0)");

    var result = try db.execute(
        "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id"
    );
    defer result.deinit();

    try expect(result.rows.items.len == 3);
}

test "LEFT JOIN: includes unmatched" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text)");
    _ = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");

    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie')");

    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");

    var result = try db.execute(
        "SELECT * FROM users LEFT JOIN orders ON users.id = orders.user_id"
    );
    defer result.deinit();

    try expect(result.rows.items.len == 3);
    // Bob and Charlie should have NULL orders
}
```

---

## Phase 4: Subqueries (1 week)

### Overview
Enable nested SELECT statements in WHERE clauses and FROM clauses.

**Example queries:**
```sql
SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE total > 100);
SELECT * FROM (SELECT name, age FROM users WHERE age > 18) AS adults;
```

**Note:** Subqueries are complex. For a beginner, I recommend implementing:
1. **Subqueries in WHERE with IN** (most useful)
2. Defer subqueries in FROM (derived tables) to later

---

### Phase 4.1: Parser for Subqueries (2-3 days)

**File:** Modify `src/sql.zig`

**Tasks:**

1. **Add subquery to Expr**
```zig
pub const Expr = union(enum) {
    literal: ColumnValue,
    column: []const u8,
    binary: *BinaryExpr,
    unary: *UnaryExpr,
    subquery: *SelectCmd,  // NEW

    pub fn deinit(self: *Expr, allocator: Allocator) void {
        switch (self.*) {
            // ... existing cases ...
            .subquery => |sq| {
                sq.deinit(allocator);
                allocator.destroy(sq);
            },
        }
    }
};
```

2. **Add IN operator**
```zig
pub const BinaryOp = enum {
    eq, neq, lt, gt, lte, gte,
    and_op, or_op,
    in_op,  // NEW: for WHERE col IN (subquery)
};
```

3. **Parse subqueries**
```zig
// In parsePrimaryExpr:
fn parsePrimaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) !Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    const token_text = tokens[idx.*].text;

    // Parenthesized expression or subquery
    if (std.mem.eql(u8, token_text, "(")) {
        // Peek ahead: is this a subquery?
        if (idx.* + 1 < tokens.len and eqlIgnoreCase(tokens[idx.* + 1].text, "SELECT")) {
            // Parse subquery
            idx.* += 1; // Skip (
            const subquery = try allocator.create(SelectCmd);
            subquery.* = try parseSelectSubquery(allocator, tokens, idx);

            if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, ")")) {
                return SqlError.InvalidExpression;
            }
            idx.* += 1; // Skip )

            return Expr{ .subquery = subquery };
        } else {
            // Regular parenthesized expression
            idx.* += 1;
            const expr = try parseExpr(allocator, tokens, idx);
            if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, ")")) {
                return SqlError.InvalidExpression;
            }
            idx.* += 1;
            return expr;
        }
    }

    // ... rest of function ...
}

fn parseSelectSubquery(allocator: Allocator, tokens: []const Token, idx: *usize) !SelectCmd {
    // Similar to parseSelect but stops at closing parenthesis
    // Implementation details...
}
```

**Acceptance Criteria:**
- ✅ Parser handles subqueries in parentheses
- ✅ Nested SELECT detected
- ✅ IN operator recognized

---

### Phase 4.2: Execute Subqueries (3-4 days)

**File:** Modify `src/database/executor.zig`

**Tasks:**

1. **Evaluate subquery expressions**
```zig
fn evaluateSubqueryExpr(db: *Database, expr: Expr) ![]ColumnValue {
    if (expr != .subquery) return error.NotASubquery;

    // Execute the subquery
    const subquery_result = try executeSelect(db, expr.subquery.*);
    defer subquery_result.deinit();

    // Extract values from first column
    var values = std.ArrayList(ColumnValue).init(db.allocator);
    for (subquery_result.rows.items) |row| {
        if (row.items.len > 0) {
            try values.append(try row.items[0].clone(db.allocator));
        }
    }

    return values.toOwnedSlice();
}
```

2. **Handle IN operator**
```zig
fn compareValues(left: ColumnValue, right: ColumnValue, op: BinaryOp) bool {
    // ... existing comparisons ...

    // Note: For IN, we need access to the database context
    // This becomes tricky - may need to refactor evaluation functions
}

// Better approach: Special case IN during WHERE evaluation
fn evaluateWhereWithSubquery(
    db: *Database,
    expr: Expr,
    row_values: anytype
) !bool {
    switch (expr) {
        .binary => |bin| {
            if (bin.op == .in_op) {
                // Left side should be a column
                const left_val = getExprValue(bin.left, row_values);

                // Right side should be a subquery
                if (bin.right != .subquery) return error.InvalidSubquery;

                // Execute subquery
                const subquery_values = try evaluateSubqueryExpr(db, bin.right);
                defer {
                    for (subquery_values) |*val| {
                        var v = val.*;
                        v.deinit(db.allocator);
                    }
                    db.allocator.free(subquery_values);
                }

                // Check if left_val is in subquery results
                for (subquery_values) |sq_val| {
                    if (valuesEqual(left_val, sq_val)) {
                        return true;
                    }
                }
                return false;
            } else {
                // Regular binary expression
                return evaluateBinaryExpr(bin.*, row_values);
            }
        },
        // ... other cases ...
    }
}
```

**Acceptance Criteria:**
- ✅ Subquery executed correctly
- ✅ IN operator filters rows
- ✅ Works with WHERE clause

---

### Phase 4.3: Testing Subqueries (1 day)

**File:** Create `src/test_subqueries.zig`

**Tests:**
```zig
test "subquery: IN clause" {
    var db = Database.init(std.testing.allocator);
    defer db.deinit();

    _ = try db.execute("CREATE TABLE users (id int, name text)");
    _ = try db.execute("CREATE TABLE orders (id int, user_id int)");

    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie')");

    _ = try db.execute("INSERT INTO orders VALUES (1, 1)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 1)");

    var result = try db.execute(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)"
    );
    defer result.deinit();

    try expect(result.rows.items.len == 1);
    // Only Alice should be returned
}
```

---

## Implementation Timeline

### Week 1: Aggregates
- Days 1-2: Parser extensions
- Days 3-4: Execution logic
- Day 5: Testing

### Week 2: GROUP BY
- Days 1-2: Parser extensions
- Days 3-5: Execution logic
- Day 5: Testing

### Week 3-4: JOINs
- Days 1-3: Parser (INNER, LEFT, RIGHT)
- Days 4-8: Execution logic
- Days 9-10: Testing

### Week 5: Subqueries
- Days 1-3: Parser (IN operator)
- Days 4-5: Execution logic
- Day 5: Testing

**Total: 4-5 weeks**

---

## Success Metrics

### Functional Requirements
- ✅ All aggregate functions work (COUNT, SUM, AVG, MIN, MAX)
- ✅ GROUP BY works with multiple columns
- ✅ INNER JOIN, LEFT JOIN work correctly
- ✅ Subqueries with IN work
- ✅ All features work with WHERE clauses
- ✅ All features work with existing indexes

### Code Quality
- ✅ 30+ new tests covering all features
- ✅ No memory leaks
- ✅ Clear error messages
- ✅ Backward compatible with existing queries

### Performance
- ✅ Aggregates complete in O(n) time
- ✅ GROUP BY completes in O(n) time with hash table
- ✅ JOINs use indexes when available
- ✅ Subqueries avoid redundant execution

---

## Documentation Deliverables

- [ ] Update **SQL_FEATURES.md** with:
  - Aggregate function examples
  - GROUP BY examples
  - JOIN examples
  - Subquery examples
- [ ] Update **README.md** with new SQL capabilities
- [ ] Add inline code documentation
- [ ] Create **QUERY_OPTIMIZATION.md** explaining query execution

---

## Future Enhancements (Post-Plan)

1. **HAVING Clause**
   - Filter groups after aggregation
   - `SELECT dept, AVG(salary) FROM emp GROUP BY dept HAVING AVG(salary) > 50000`

2. **ORDER BY with Aggregates**
   - `SELECT dept, COUNT(*) as cnt FROM emp GROUP BY dept ORDER BY cnt DESC`

3. **More Join Types**
   - FULL OUTER JOIN
   - CROSS JOIN

4. **Correlated Subqueries**
   - Subquery references outer query
   - `SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)`

5. **EXISTS / NOT EXISTS**
   - `SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)`

6. **Hash Joins / Merge Joins**
   - Optimize JOIN performance beyond nested loops

7. **Subqueries in SELECT**
   - `SELECT name, (SELECT COUNT(*) FROM orders WHERE user_id = users.id) FROM users`

8. **DISTINCT**
   - `SELECT DISTINCT city FROM users`

9. **UNION / INTERSECT / EXCEPT**
   - Set operations on query results

---

## Why This Is Better Than MVCC for Learning

### Immediate Applicability
- ✅ Every feature you build is immediately visible and testable
- ✅ You can show off your database with real SQL queries
- ✅ Each milestone is a complete, working feature

### Incremental Learning Curve
- ✅ Start simple (COUNT) → medium (GROUP BY) → complex (JOINs)
- ✅ Each phase builds on the previous
- ✅ Clear acceptance criteria for each step

### Portfolio Value
- ✅ "I built a SQL database with JOINs and aggregations" is impressive
- ✅ Easier to explain and demonstrate than MVCC
- ✅ Shows understanding of query processing and data structures

### Foundation for MVCC
- ✅ Deep understanding of query execution
- ✅ Comfortable with table iteration and row processing
- ✅ Experience with complex state management

**After completing this plan, MVCC will be much easier because you'll understand:**
- How queries access rows
- Where version checks would go
- How to maintain correctness during concurrent modifications

---

## Recommendation

**Start with Phase 1 (Aggregates)!** It's the perfect introduction:
- Small scope (1 week)
- Immediate value (COUNT, SUM, AVG)
- Builds confidence
- Foundation for GROUP BY

Once you've completed aggregates, you'll have momentum and understanding to tackle GROUP BY and JOINs.

**Ready to begin?** 🚀
