# N-Table Join Refactoring Design Document

## Executive Summary

This document provides a detailed design for refactoring ZVDB's `executeJoinSelect()` function to support N-table joins using a pipelined, left-deep join tree approach. The design maintains the existing hash join optimization while extending support from 2-table to N-table joins.

**Current Status**: Only 2-table joins supported (artificial limit at line 455)
**Target**: Support arbitrary N-table joins with pipelined execution
**Approach**: Left-deep join pipeline with intermediate result materialization

---

## 1. Architecture Overview

### 1.1 High-Level Concept

The refactored system will process joins sequentially, building up intermediate results:

```
Query: SELECT * FROM users
       INNER JOIN orders ON users.id = orders.user_id
       LEFT JOIN products ON orders.product_id = products.id

Execution Pipeline:
┌─────────┐
│  users  │ (Base Table)
└────┬────┘
     │ JOIN orders (INNER)
     ▼
┌─────────────────┐
│ Intermediate_1  │ (users ⋈ orders)
│ Schema: [users.id, users.name, orders.id, orders.user_id, orders.product_id]
└────┬────────────┘
     │ JOIN products (LEFT)
     ▼
┌─────────────────┐
│ Final Result    │ (Intermediate_1 ⋈ products)
│ Schema: [users.id, users.name, orders.id, ..., products.id, products.name]
└─────────────────┘
```

### 1.2 Key Design Principles

1. **Materialized Intermediates**: Each join produces a concrete intermediate result
2. **Schema Tracking**: Track column names and table origins through the pipeline
3. **Reuse Hash Join**: Leverage existing hash join optimizations
4. **Memory Conscious**: Clean up intermediate results after use
5. **Backward Compatible**: Existing 2-table joins continue to work

---

## 2. Data Structure Design

### 2.1 New: IntermediateResult Structure

We need a new abstraction to represent intermediate join results that can be used as input to subsequent joins:

```zig
/// Intermediate result from a join operation
/// Can be used as input to subsequent joins
pub const IntermediateResult = struct {
    /// Schema: column names with table qualifications
    /// Example: ["users.id", "users.name", "orders.id", "orders.user_id"]
    schema: ArrayList(ColumnInfo),

    /// Rows: each row is a flat array of values matching schema order
    rows: ArrayList(ArrayList(ColumnValue)),

    /// Allocator for memory management
    allocator: Allocator,

    pub const ColumnInfo = struct {
        /// Full qualified name: "table.column"
        qualified_name: []const u8,

        /// Original table name: "table"
        table_name: []const u8,

        /// Column name without qualification: "column"
        column_name: []const u8,

        /// Position in row array (0-indexed)
        index: usize,
    };

    pub fn init(allocator: Allocator) IntermediateResult {
        return .{
            .schema = ArrayList(ColumnInfo).init(allocator),
            .rows = ArrayList(ArrayList(ColumnValue)).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IntermediateResult) void {
        // Free schema
        for (self.schema.items) |*col_info| {
            self.allocator.free(col_info.qualified_name);
            self.allocator.free(col_info.table_name);
            self.allocator.free(col_info.column_name);
        }
        self.schema.deinit();

        // Free rows
        for (self.rows.items) |*row| {
            for (row.items) |*val| {
                var v = val.*;
                v.deinit(self.allocator);
            }
            row.deinit();
        }
        self.rows.deinit();
    }

    /// Add a column to the schema
    pub fn addColumn(
        self: *IntermediateResult,
        table_name: []const u8,
        column_name: []const u8,
    ) !void {
        const qualified = try std.fmt.allocPrint(
            self.allocator,
            "{s}.{s}",
            .{ table_name, column_name },
        );
        errdefer self.allocator.free(qualified);

        const table_owned = try self.allocator.dupe(u8, table_name);
        errdefer self.allocator.free(table_owned);

        const column_owned = try self.allocator.dupe(u8, column_name);
        errdefer self.allocator.free(column_owned);

        try self.schema.append(.{
            .qualified_name = qualified,
            .table_name = table_owned,
            .column_name = column_owned,
            .index = self.schema.items.len,
        });
    }

    /// Add a row to the result
    pub fn addRow(self: *IntermediateResult, row: ArrayList(ColumnValue)) !void {
        try self.rows.append(row);
    }

    /// Find column index by name (supports both qualified and unqualified)
    /// Returns null if not found
    pub fn findColumn(self: *const IntermediateResult, col_name: []const u8) ?usize {
        // First try exact match on qualified name
        for (self.schema.items) |col_info| {
            if (std.mem.eql(u8, col_info.qualified_name, col_name)) {
                return col_info.index;
            }
        }

        // Then try unqualified match
        for (self.schema.items) |col_info| {
            if (std.mem.eql(u8, col_info.column_name, col_name)) {
                return col_info.index;
            }
        }

        return null;
    }

    /// Get value from a row by column name
    pub fn getValue(
        self: *const IntermediateResult,
        row: []const ColumnValue,
        col_name: []const u8,
    ) ?ColumnValue {
        const idx = self.findColumn(col_name) orelse return null;
        if (idx >= row.len) return null;
        return row[idx];
    }
};
```

### 2.2 New: JoinSource Union Type

Create a unified type that can represent either a base table or an intermediate result:

```zig
/// Represents a source for a join operation
/// Can be either a base table or an intermediate result from a previous join
pub const JoinSource = union(enum) {
    base_table: *Table,
    intermediate: *IntermediateResult,

    /// Get all row data as an iterator-like structure
    pub fn getRowIterator(self: JoinSource, allocator: Allocator) !RowIterator {
        return switch (self) {
            .base_table => |table| RowIterator.fromTable(allocator, table),
            .intermediate => |inter| RowIterator.fromIntermediate(inter),
        };
    }

    /// Get schema information
    pub fn getSchema(self: JoinSource) SchemaInfo {
        return switch (self) {
            .base_table => |table| SchemaInfo.fromTable(table),
            .intermediate => |inter| SchemaInfo.fromIntermediate(inter),
        };
    }
};

/// Iterator-like structure for accessing rows from different sources
pub const RowIterator = struct {
    source_type: enum { table, intermediate },

    // For table source
    table: ?*Table,
    row_ids: ?[]const u64,
    current_idx: usize,

    // For intermediate source
    intermediate: ?*IntermediateResult,

    allocator: Allocator,

    pub fn fromTable(allocator: Allocator, table: *Table) !RowIterator {
        const row_ids = try table.getAllRows(allocator);
        return .{
            .source_type = .table,
            .table = table,
            .row_ids = row_ids,
            .current_idx = 0,
            .intermediate = null,
            .allocator = allocator,
        };
    }

    pub fn fromIntermediate(inter: *IntermediateResult) RowIterator {
        return .{
            .source_type = .intermediate,
            .table = null,
            .row_ids = null,
            .current_idx = 0,
            .intermediate = inter,
            .allocator = inter.allocator,
        };
    }

    pub fn deinit(self: *RowIterator) void {
        if (self.row_ids) |ids| {
            self.allocator.free(ids);
        }
    }

    pub fn next(self: *RowIterator) ?JoinRow {
        return switch (self.source_type) {
            .table => {
                if (self.current_idx >= self.row_ids.?.len) return null;
                const row_id = self.row_ids.?[self.current_idx];
                self.current_idx += 1;
                const row = self.table.?.get(row_id) orelse return null;
                return JoinRow{ .from_table = row };
            },
            .intermediate => {
                if (self.current_idx >= self.intermediate.?.rows.items.len) return null;
                const row = self.intermediate.?.rows.items[self.current_idx];
                self.current_idx += 1;
                return JoinRow{ .from_intermediate = row };
            },
        };
    }

    pub fn reset(self: *RowIterator) void {
        self.current_idx = 0;
    }

    pub fn count(self: *const RowIterator) usize {
        return switch (self.source_type) {
            .table => self.row_ids.?.len,
            .intermediate => self.intermediate.?.rows.items.len,
        };
    }
};

/// Row from either a table or intermediate result
pub const JoinRow = union(enum) {
    from_table: *const Row,
    from_intermediate: []const ColumnValue,

    /// Get a column value by name
    pub fn getValue(
        self: JoinRow,
        col_name: []const u8,
        schema: ?*const IntermediateResult,
    ) ?ColumnValue {
        return switch (self) {
            .from_table => |row| row.get(col_name),
            .from_intermediate => |row| {
                if (schema) |s| {
                    return s.getValue(row, col_name);
                }
                return null;
            },
        };
    }
};

/// Schema information from a join source
pub const SchemaInfo = struct {
    columns: []const ColumnInfo,

    pub const ColumnInfo = struct {
        name: []const u8,
        qualified_name: []const u8,
    };

    pub fn fromTable(table: *Table) SchemaInfo {
        // Implementation would extract column info from Table
    }

    pub fn fromIntermediate(inter: *IntermediateResult) SchemaInfo {
        // Implementation would extract column info from IntermediateResult
    }
};
```

---

## 3. Algorithm Design

### 3.1 Main Pipeline Logic (Pseudocode)

```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // STEP 1: Initialize with base table
    const base_table = db.tables.get(cmd.table_name) orelse return error.TableNotFound;

    // STEP 2: Check if we have any joins
    if (cmd.joins.items.len == 0) return error.NoJoins;

    // STEP 3: Execute joins sequentially (pipeline)
    var current_source: JoinSource = .{ .base_table = base_table };
    var intermediate_results = ArrayList(*IntermediateResult).init(db.allocator);
    defer {
        for (intermediate_results.items) |inter| {
            inter.deinit();
            db.allocator.destroy(inter);
        }
        intermediate_results.deinit();
    }

    // Process each join
    for (cmd.joins.items, 0..) |join, join_idx| {
        const right_table = db.tables.get(join.table_name) orelse return error.TableNotFound;

        // Determine if we should use hash join
        const use_hash_join = shouldUseHashJoinForPipeline(current_source, right_table);

        // Execute this join stage
        var join_result = try executeJoinStage(
            db.allocator,
            current_source,
            right_table,
            join,
            use_hash_join,
        );

        // Save intermediate result
        const intermediate = try db.allocator.create(IntermediateResult);
        intermediate.* = join_result;
        try intermediate_results.append(intermediate);

        // Update current source for next iteration
        current_source = .{ .intermediate = intermediate };
    }

    // STEP 4: Convert final intermediate result to QueryResult
    const final_intermediate = switch (current_source) {
        .intermediate => |inter| inter,
        else => unreachable, // We always have at least one join
    };

    // STEP 5: Apply column projection (SELECT clause)
    var result = try projectColumns(db.allocator, final_intermediate, cmd.columns.items);

    return result;
}
```

### 3.2 Join Stage Execution

```zig
fn executeJoinStage(
    allocator: Allocator,
    left_source: JoinSource,
    right_table: *Table,
    join_clause: JoinClause,
    use_hash_join: bool,
) !IntermediateResult {
    if (use_hash_join) {
        return executeHashJoinStage(
            allocator,
            left_source,
            right_table,
            join_clause,
        );
    } else {
        return executeNestedLoopJoinStage(
            allocator,
            left_source,
            right_table,
            join_clause,
        );
    }
}
```

### 3.3 Hash Join Stage (Detailed)

```zig
fn executeHashJoinStage(
    allocator: Allocator,
    left_source: JoinSource,
    right_table: *Table,
    join_clause: JoinClause,
) !IntermediateResult {
    var result = IntermediateResult.init(allocator);
    errdefer result.deinit();

    // STEP 1: Build schema for result
    // Add all columns from left source
    const left_schema = left_source.getSchema();
    for (left_schema.columns) |col| {
        try result.addColumn(col.table_name, col.column_name);
    }

    // Add all columns from right table
    for (right_table.columns.items) |col| {
        try result.addColumn(join_clause.table_name, col.name);
    }

    // STEP 2: Build hash table on right table
    const right_col_name = extractColumnName(join_clause.right_column);
    var hash_table = try buildHashTable(allocator, right_table, right_col_name);
    defer hash_table.deinit();

    // STEP 3: Probe with left source
    var left_iter = try left_source.getRowIterator(allocator);
    defer left_iter.deinit();

    const left_col_name = extractColumnName(join_clause.left_column);

    switch (join_clause.join_type) {
        .inner => {
            // INNER JOIN: only emit matching rows
            while (left_iter.next()) |left_row| {
                const probe_key = left_row.getValue(left_col_name, null) orelse continue;

                if (probe_key == .null_value) continue; // NULL doesn't match

                const probe_hash = hashColumnValue(probe_key);

                if (hash_table.probe(probe_hash)) |matching_ids| {
                    for (matching_ids) |right_id| {
                        const right_row = right_table.get(right_id) orelse continue;
                        const right_val = right_row.get(right_col_name) orelse continue;

                        if (valuesEqual(probe_key, right_val)) {
                            // Match! Emit combined row
                            try emitCombinedRow(
                                &result,
                                allocator,
                                left_row,
                                left_source,
                                right_row,
                                right_table,
                            );
                        }
                    }
                }
            }
        },
        .left => {
            // LEFT JOIN: emit all left rows, with NULLs if no match
            while (left_iter.next()) |left_row| {
                const probe_key = left_row.getValue(left_col_name, null);

                var matched = false;

                if (probe_key) |key| {
                    if (key != .null_value) {
                        const probe_hash = hashColumnValue(key);

                        if (hash_table.probe(probe_hash)) |matching_ids| {
                            for (matching_ids) |right_id| {
                                const right_row = right_table.get(right_id) orelse continue;
                                const right_val = right_row.get(right_col_name) orelse continue;

                                if (valuesEqual(key, right_val)) {
                                    matched = true;
                                    try emitCombinedRow(
                                        &result,
                                        allocator,
                                        left_row,
                                        left_source,
                                        right_row,
                                        right_table,
                                    );
                                }
                            }
                        }
                    }
                }

                // If no match, emit with NULLs for right side
                if (!matched) {
                    try emitCombinedRowWithNulls(
                        &result,
                        allocator,
                        left_row,
                        left_source,
                        right_table,
                    );
                }
            }
        },
        .right => {
            // RIGHT JOIN: Similar to LEFT but roles reversed
            // Track which right rows were matched
            var matched_right = AutoHashMap(u64, bool).init(allocator);
            defer matched_right.deinit();

            // First pass: emit all matches
            const right_row_ids = try right_table.getAllRows(allocator);
            defer allocator.free(right_row_ids);

            for (right_row_ids) |right_id| {
                const right_row = right_table.get(right_id) orelse continue;
                const right_key = right_row.get(right_col_name);

                var this_right_matched = false;

                if (right_key) |key| {
                    if (key != .null_value) {
                        // Scan left source for matches
                        left_iter.reset();
                        while (left_iter.next()) |left_row| {
                            const left_val = left_row.getValue(left_col_name, null) orelse continue;

                            if (valuesEqual(left_val, key)) {
                                this_right_matched = true;
                                try emitCombinedRow(
                                    &result,
                                    allocator,
                                    left_row,
                                    left_source,
                                    right_row,
                                    right_table,
                                );
                            }
                        }
                    }
                }

                if (this_right_matched) {
                    try matched_right.put(right_id, true);
                }
            }

            // Second pass: emit unmatched right rows with NULLs
            for (right_row_ids) |right_id| {
                if (matched_right.contains(right_id)) continue;

                const right_row = right_table.get(right_id) orelse continue;
                try emitCombinedRowWithNullsLeft(
                    &result,
                    allocator,
                    left_source,
                    right_row,
                    right_table,
                );
            }
        },
    }

    return result;
}
```

### 3.4 Helper: Emit Combined Row

```zig
fn emitCombinedRow(
    result: *IntermediateResult,
    allocator: Allocator,
    left_row: JoinRow,
    left_source: JoinSource,
    right_row: *const Row,
    right_table: *Table,
) !void {
    var combined = ArrayList(ColumnValue).init(allocator);
    errdefer {
        for (combined.items) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        combined.deinit();
    }

    // Add all values from left row
    switch (left_row) {
        .from_table => |row| {
            const left_table = switch (left_source) {
                .base_table => |t| t,
                else => unreachable,
            };
            for (left_table.columns.items) |col| {
                const val = row.get(col.name) orelse ColumnValue.null_value;
                try combined.append(try val.clone(allocator));
            }
        },
        .from_intermediate => |row| {
            for (row) |val| {
                try combined.append(try val.clone(allocator));
            }
        },
    }

    // Add all values from right row
    for (right_table.columns.items) |col| {
        const val = right_row.get(col.name) orelse ColumnValue.null_value;
        try combined.append(try val.clone(allocator));
    }

    try result.addRow(combined);
}

fn emitCombinedRowWithNulls(
    result: *IntermediateResult,
    allocator: Allocator,
    left_row: JoinRow,
    left_source: JoinSource,
    right_table: *Table,
) !void {
    var combined = ArrayList(ColumnValue).init(allocator);
    errdefer {
        for (combined.items) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        combined.deinit();
    }

    // Add all values from left row
    switch (left_row) {
        .from_table => |row| {
            const left_table = switch (left_source) {
                .base_table => |t| t,
                else => unreachable,
            };
            for (left_table.columns.items) |col| {
                const val = row.get(col.name) orelse ColumnValue.null_value;
                try combined.append(try val.clone(allocator));
            }
        },
        .from_intermediate => |row| {
            for (row) |val| {
                try combined.append(try val.clone(allocator));
            }
        },
    }

    // Add NULLs for right table
    for (right_table.columns.items) |_| {
        try combined.append(ColumnValue.null_value);
    }

    try result.addRow(combined);
}
```

### 3.5 Column Projection

```zig
fn projectColumns(
    allocator: Allocator,
    intermediate: *IntermediateResult,
    select_columns: []const SelectColumn,
) !QueryResult {
    var result = QueryResult.init(allocator);
    errdefer result.deinit();

    // Check if SELECT *
    const select_all = select_columns.len == 1 and select_columns[0] == .star;

    if (select_all) {
        // Add all columns from intermediate schema
        for (intermediate.schema.items) |col_info| {
            try result.addColumn(col_info.qualified_name);
        }

        // Add all rows
        for (intermediate.rows.items) |row| {
            var result_row = ArrayList(ColumnValue).init(allocator);
            for (row.items) |val| {
                try result_row.append(try val.clone(allocator));
            }
            try result.addRow(result_row);
        }
    } else {
        // Add only selected columns
        for (select_columns) |col_spec| {
            switch (col_spec) {
                .regular => |col_name| try result.addColumn(col_name),
                .aggregate => return error.AggregateNotSupportedInJoin,
                .star => try result.addColumn("*"),
            }
        }

        // Project rows
        for (intermediate.rows.items) |row| {
            var result_row = ArrayList(ColumnValue).init(allocator);

            for (select_columns) |col_spec| {
                if (col_spec == .regular) {
                    const col_name = col_spec.regular;
                    const idx = intermediate.findColumn(col_name) orelse {
                        return error.ColumnNotFound;
                    };
                    const val = if (idx < row.items.len) row.items[idx] else ColumnValue.null_value;
                    try result_row.append(try val.clone(allocator));
                }
            }

            try result.addRow(result_row);
        }
    }

    return result;
}
```

---

## 4. Integration with Existing Code

### 4.1 Files to Modify

#### **src/database/executor.zig**

**Changes needed:**
- Remove the artificial limit check at line 455: `if (cmd.joins.items.len > 1) return error.MultiJoinNotYetSupported;`
- Replace entire `executeJoinSelect()` function with new pipelined implementation
- Keep backward compatibility by detecting single vs. multiple joins
- Keep existing helper functions: `splitQualifiedColumn()`, `shouldUseHashJoin()`, `estimateTableSize()`

**New additions:**
- Add `IntermediateResult` struct
- Add `JoinSource` union type
- Add `RowIterator` struct
- Add `executeJoinStage()` function
- Add `executeHashJoinStage()` function
- Add `executeNestedLoopJoinStage()` function
- Add `emitCombinedRow()` and helpers
- Add `projectColumns()` function

#### **src/database/hash_join.zig**

**Changes needed:**
- Extract column name resolution logic into helper functions
- Make hash table building more generic (currently works only with Table)

**Potential new functions:**
- `buildHashTableFromSource()` - works with JoinSource instead of just Table
- This may not be necessary if we keep the current approach

**Keep unchanged:**
- `hashColumnValue()` - works fine as-is
- `JoinHashTable` - works fine as-is
- The three main execution functions can be kept as reference implementations

### 4.2 Integration Strategy

**Phase 1: Add new structures (non-breaking)**
1. Add `IntermediateResult` to executor.zig
2. Add `JoinSource` and related types
3. Add new helper functions
4. All existing tests continue to pass

**Phase 2: Implement pipelined execution (new code path)**
1. Implement `executeJoinStage()` and related functions
2. Keep old `executeJoinSelect()` as `executeJoinSelectLegacy()`
3. Add feature flag to switch between implementations

**Phase 3: Migration (breaking change)**
1. Replace old `executeJoinSelect()` with new implementation
2. Remove artificial limit
3. Update tests

### 4.3 Column Resolution Integration

The design anticipates integration with a `ColumnResolver` being built by another team:

```zig
/// Future integration point for ColumnResolver
/// This would replace manual column name resolution
pub const ColumnResolver = struct {
    /// Resolve a column reference in the context of current join pipeline
    pub fn resolve(
        self: *ColumnResolver,
        col_ref: []const u8,
        available_sources: []const SourceInfo,
    ) !ResolvedColumn {
        // Implementation by other team
    }

    pub const SourceInfo = struct {
        table_name: []const u8,
        alias: ?[]const u8,
        columns: []const []const u8,
    };

    pub const ResolvedColumn = struct {
        source_index: usize,
        column_index: usize,
        qualified_name: []const u8,
    };
};
```

**Integration points in our design:**
- `findColumn()` in IntermediateResult could use ColumnResolver
- `getValue()` in JoinRow could use ColumnResolver
- `projectColumns()` would use ColumnResolver for column lookup

---

## 5. Error Handling & Edge Cases

### 5.1 Error Types

```zig
pub const JoinError = error{
    // Existing errors
    TableNotFound,
    ColumnNotFound,
    MultiJoinNotYetSupported, // Will be removed

    // New errors
    EmptyJoinPipeline,
    InvalidJoinColumn,
    AmbiguousColumnReference,
    IncompatibleJoinTypes,
    IntermediateResultTooLarge,
    OutOfMemory,
};
```

### 5.2 Edge Cases

#### **Empty Base Table**
```zig
// If base table is empty, return empty result immediately
if (base_table.row_count == 0 and join_type == .inner) {
    return QueryResult.init(allocator); // Empty result
}
```

#### **NULL Join Keys**
```zig
// Already handled: NULL keys never match in SQL
if (probe_key == .null_value) continue;
```

#### **Duplicate Column Names**
```zig
// Schema tracks qualified names, so "users.id" and "orders.id" are distinct
// Unqualified lookup returns first match (standard SQL behavior)
```

#### **Large Intermediate Results**
```zig
// Consider adding memory limit checking
const MAX_INTERMEDIATE_SIZE = 1_000_000_000; // 1GB
if (intermediate.estimateSize() > MAX_INTERMEDIATE_SIZE) {
    return error.IntermediateResultTooLarge;
}
```

#### **Mixed Join Types**
```zig
// Example: INNER JOIN followed by LEFT JOIN
// This is valid and supported by the pipeline design
// Each stage executes independently with correct semantics
```

### 5.3 Error Handling in Pipeline

```zig
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // ... initialization ...

    // Clean up intermediates on error
    var intermediate_results = ArrayList(*IntermediateResult).init(db.allocator);
    errdefer {
        for (intermediate_results.items) |inter| {
            inter.deinit();
            db.allocator.destroy(inter);
        }
        intermediate_results.deinit();
    }

    for (cmd.joins.items, 0..) |join, join_idx| {
        const right_table = db.tables.get(join.table_name) orelse {
            // Error will trigger errdefer cleanup
            return error.TableNotFound;
        };

        var join_result = try executeJoinStage(
            db.allocator,
            current_source,
            right_table,
            join,
            use_hash_join,
        );

        // If join fails, join_result is not created, so no leak
        // If allocation fails, errdefer cleans up previous intermediates
    }

    // ... rest of function ...
}
```

---

## 6. Performance Considerations

### 6.1 Memory Usage

**Current (2-table joins):**
- Memory = O(rows_table1 + rows_table2 + rows_result)

**New (N-table joins):**
- Memory = O(rows_table1 + rows_table2 + ... + rows_tableN + Σ rows_intermediate_i + rows_result)
- Worst case: each intermediate can be as large as the Cartesian product

**Optimization strategies:**
1. **Streaming intermediate results**: Don't materialize if not needed (future work)
2. **Memory limits**: Fail gracefully if intermediate grows too large
3. **Join reordering**: Use cost-based optimizer to choose optimal order (future work)
4. **Lazy evaluation**: Only compute rows that pass WHERE clause (future work)

### 6.2 Time Complexity

For N tables with average size R:

**Hash Join (current approach):**
- Single 2-table join: O(R₁ + R₂)
- N-table pipeline: O(R₁ + R₂ + I₁ + R₃ + I₂ + ... + Rₙ)
  where Iᵢ is the size of intermediate result i

**Nested Loop Join:**
- Single 2-table join: O(R₁ × R₂)
- N-table pipeline: O(R₁ × R₂ × ... × Rₙ) worst case

**Join order matters!**
- Good order: users(100) ⋈ orders(1000) ⋈ products(50)
  - Intermediate 1: ~1000 rows → Final: ~1000 rows
- Bad order: products(50) ⋈ orders(1000) ⋈ users(100)
  - Intermediate 1: ~1000 rows → Final: ~1000 rows (same result, similar cost in this case)

### 6.3 Cost-Based Decisions

Extend existing `shouldUseHashJoin()`:

```zig
fn shouldUseHashJoinForPipeline(
    left_source: JoinSource,
    right_table: *Table,
) bool {
    const left_size = switch (left_source) {
        .base_table => |t| estimateTableSize(t),
        .intermediate => |i| i.rows.items.len,
    };
    const right_size = estimateTableSize(right_table);

    // Use hash join if both are large enough
    // Threshold: 100 rows
    return left_size > 100 or right_size > 100;
}
```

---

## 7. Testing Strategy

### 7.1 Unit Tests

```zig
test "IntermediateResult basic operations" {
    var inter = IntermediateResult.init(std.testing.allocator);
    defer inter.deinit();

    try inter.addColumn("users", "id");
    try inter.addColumn("users", "name");

    try std.testing.expectEqual(@as(usize, 2), inter.schema.items.len);
    try std.testing.expectEqualStrings("users.id", inter.schema.items[0].qualified_name);
}

test "JoinSource with table" {
    // Test RowIterator with base table
}

test "JoinSource with intermediate" {
    // Test RowIterator with intermediate result
}
```

### 7.2 Integration Tests

```zig
test "3-table INNER JOIN" {
    // Setup
    var db = Database.init(allocator);
    defer db.deinit();

    // Create tables: users, orders, products
    _ = try db.execute("CREATE TABLE users (id INT, name TEXT)");
    _ = try db.execute("CREATE TABLE orders (id INT, user_id INT, product_id INT)");
    _ = try db.execute("CREATE TABLE products (id INT, name TEXT)");

    // Insert data
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice')");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob')");
    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 10)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 1, 20)");
    _ = try db.execute("INSERT INTO products VALUES (10, 'Widget')");
    _ = try db.execute("INSERT INTO products VALUES (20, 'Gadget')");

    // Query with 3-table join
    var result = try db.execute(
        \\SELECT users.name, orders.id, products.name
        \\FROM users
        \\INNER JOIN orders ON users.id = orders.user_id
        \\INNER JOIN products ON orders.product_id = products.id
    );
    defer result.deinit();

    // Verify
    try std.testing.expectEqual(@as(usize, 2), result.rows.items.len);
    // First row: Alice, 1, Widget
    // Second row: Alice, 2, Gadget
}

test "Mixed JOIN types" {
    // Test: INNER JOIN followed by LEFT JOIN
}

test "NULL handling in multi-join" {
    // Test: NULL values in join columns
}

test "Large intermediate results" {
    // Test: Join producing >10000 intermediate rows
}
```

### 7.3 Performance Tests

```zig
test "benchmark: 2-table join (baseline)" {
    // Measure old implementation
}

test "benchmark: 2-table join (new)" {
    // Ensure new implementation has similar performance
}

test "benchmark: 5-table join" {
    // Measure multi-table join performance
}
```

---

## 8. Migration Path

### 8.1 Backward Compatibility

The new implementation maintains 100% backward compatibility with existing 2-table joins:

```zig
// Old query (still works)
SELECT * FROM users JOIN orders ON users.id = orders.user_id

// New query (now supported)
SELECT * FROM users
  JOIN orders ON users.id = orders.user_id
  JOIN products ON orders.product_id = products.id
```

### 8.2 Deprecation Plan

1. **Version 1.x**: Current implementation (2-table limit)
2. **Version 2.0**: Add new implementation behind feature flag
3. **Version 2.1**: Enable by default, keep old as fallback
4. **Version 3.0**: Remove old implementation

### 8.3 Documentation Updates

Files to update:
- `/docs/SQL.md` - Add N-table join examples
- `/docs/ARCHITECTURE.md` - Document join pipeline
- `/README.md` - Update feature list

---

## 9. Future Enhancements

### 9.1 Short-term (Within this refactor)
- ✅ Support N-table joins
- ✅ Maintain hash join optimization
- ✅ Proper error handling

### 9.2 Medium-term (Next iteration)
- Join reordering based on cost estimation
- Parallel hash table building for multi-table joins
- Index-based joins (if indexes exist on join columns)
- Streaming evaluation to reduce memory

### 9.3 Long-term (Future work)
- Support for CROSS JOIN, FULL OUTER JOIN
- Subquery support in JOIN clauses
- Common Table Expressions (CTEs)
- Lateral joins

---

## 10. Implementation Checklist

### Phase 1: Foundation (Week 1)
- [ ] Add `IntermediateResult` struct to executor.zig
- [ ] Add `JoinSource` union type
- [ ] Add `RowIterator` and `JoinRow`
- [ ] Write unit tests for new structures
- [ ] Ensure all existing tests still pass

### Phase 2: Core Pipeline (Week 2)
- [ ] Implement `executeJoinStage()`
- [ ] Implement `executeHashJoinStage()`
- [ ] Implement `emitCombinedRow()` helpers
- [ ] Implement `projectColumns()`
- [ ] Write integration tests for 3-table joins

### Phase 3: Refactor executeJoinSelect (Week 3)
- [ ] Rewrite main `executeJoinSelect()` function
- [ ] Remove artificial limit
- [ ] Add error handling and edge cases
- [ ] Test with 4+ table joins
- [ ] Performance benchmarking

### Phase 4: Polish & Documentation (Week 4)
- [ ] Code review and cleanup
- [ ] Update documentation
- [ ] Add comprehensive tests
- [ ] Performance optimization
- [ ] Final integration testing

---

## 11. Code Snippets for Key Changes

### 11.1 New executeJoinSelect() Header

```zig
/// Execute a SELECT query with one or more JOINs
/// Supports N-table joins using a pipelined approach
///
/// Example:
///   SELECT * FROM users
///     INNER JOIN orders ON users.id = orders.user_id
///     LEFT JOIN products ON orders.product_id = products.id
///
/// Execution plan:
///   1. users ⋈ orders → intermediate_1
///   2. intermediate_1 ⋈ products → final_result
///
fn executeJoinSelect(db: *Database, cmd: sql.SelectCmd) !QueryResult {
    // Implementation from Section 3.1
}
```

### 11.2 Modified Error Check

**Before (executor.zig:455):**
```zig
if (cmd.joins.items.len > 1) return error.MultiJoinNotYetSupported;
```

**After:**
```zig
// No limit check - support arbitrary number of joins
// Validation only checks for at least one join
if (cmd.joins.items.len == 0) return error.NoJoins;
```

### 11.3 Helper: Extract Column Name

```zig
/// Extract the column name from a potentially qualified reference
/// Examples:
///   "users.id" -> "id"
///   "id" -> "id"
fn extractColumnName(qualified: []const u8) []const u8 {
    if (std.mem.lastIndexOf(u8, qualified, ".")) |idx| {
        return qualified[idx + 1..];
    }
    return qualified;
}
```

---

## 12. ASCII Art Diagrams

### 12.1 Current Architecture (2-table limit)

```
┌─────────────────────────────────────────────────┐
│          executeJoinSelect()                    │
│                                                 │
│  ┌──────────┐         ┌──────────┐             │
│  │ Table A  │  JOIN   │ Table B  │             │
│  └────┬─────┘         └─────┬────┘             │
│       │                     │                  │
│       └──────────┬──────────┘                  │
│                  ▼                              │
│          ┌───────────────┐                      │
│          │ Hash Join OR  │                      │
│          │ Nested Loop   │                      │
│          └───────┬───────┘                      │
│                  ▼                              │
│          ┌───────────────┐                      │
│          │ QueryResult   │                      │
│          └───────────────┘                      │
└─────────────────────────────────────────────────┘
```

### 12.2 New Architecture (N-table pipeline)

```
┌─────────────────────────────────────────────────────────────┐
│              executeJoinSelect() - Pipeline                 │
│                                                             │
│  ┌──────────┐                                               │
│  │ Table A  │ (Base)                                        │
│  └────┬─────┘                                               │
│       │                                                     │
│       │ JOIN Table B                                        │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │ executeJoinStage│ (Stage 1)                              │
│  └────┬────────────┘                                        │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │ Intermediate_1  │ (A ⋈ B)                                │
│  └────┬────────────┘                                        │
│       │                                                     │
│       │ JOIN Table C                                        │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │ executeJoinStage│ (Stage 2)                              │
│  └────┬────────────┘                                        │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │ Intermediate_2  │ (A ⋈ B ⋈ C)                            │
│  └────┬────────────┘                                        │
│       │                                                     │
│       │ JOIN Table D (if exists)                            │
│       ▼                                                     │
│      ...                                                    │
│       │                                                     │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │projectColumns() │                                        │
│  └────┬────────────┘                                        │
│       ▼                                                     │
│  ┌─────────────────┐                                        │
│  │  QueryResult    │                                        │
│  └─────────────────┘                                        │
└─────────────────────────────────────────────────────────────┘
```

### 12.3 Data Flow for 3-Table Join

```
Query: SELECT * FROM users JOIN orders ON users.id = orders.user_id
                          JOIN products ON orders.product_id = products.id

Step 1: Base Table
┌──────────┐
│  users   │
│ id | name│
├──────────┤
│ 1  | Alice│
│ 2  | Bob  │
└──────────┘

Step 2: First Join (users ⋈ orders)
┌──────────┐       ┌─────────────────────────┐
│  users   │  ⋈    │        orders           │
└────┬─────┘       │ id | user_id | product_id│
     │             ├─────────────────────────┤
     │             │ 1  |    1    |    10     │
     │             │ 2  |    1    |    20     │
     │             │ 3  |    2    |    10     │
     └─────────────┴─────────────────────────┘
                   ▼
        ┌───────────────────────────────────────────┐
        │      Intermediate_1                        │
        │ users.id|users.name|orders.id|user_id|product_id│
        ├───────────────────────────────────────────┤
        │    1    |  Alice   |    1    |   1   |   10  │
        │    1    |  Alice   |    2    |   1   |   20  │
        │    2    |  Bob     |    3    |   2   |   10  │
        └───────────────────────────────────────────┘

Step 3: Second Join (Intermediate_1 ⋈ products)
┌───────────────┐       ┌──────────────┐
│Intermediate_1 │  ⋈    │   products   │
└───────┬───────┘       │ id  |  name  │
        │               ├──────────────┤
        │               │ 10  | Widget │
        │               │ 20  | Gadget │
        └───────────────┴──────────────┘
                        ▼
          ┌──────────────────────────────────────────────────────┐
          │              Final Result                             │
          │users.id|users.name|orders.id|...|products.id|products.name│
          ├──────────────────────────────────────────────────────┤
          │   1    |  Alice   |    1    |...|    10     |  Widget │
          │   1    |  Alice   |    2    |...|    20     |  Gadget │
          │   2    |  Bob     |    3    |...|    10     |  Widget │
          └──────────────────────────────────────────────────────┘
```

---

## 13. Summary

This design provides a complete roadmap for extending ZVDB's join capabilities from 2-table to N-table joins while:

1. **Maintaining Performance**: Hash join optimization preserved
2. **Ensuring Correctness**: Proper SQL semantics for INNER/LEFT/RIGHT joins
3. **Managing Memory**: Intermediate results properly tracked and cleaned up
4. **Enabling Growth**: Foundation for future optimizations (join reordering, streaming, etc.)
5. **Backward Compatible**: Existing queries continue to work

The pipelined approach is a standard technique used by production databases and provides a solid foundation for future query optimization work.

**Key Innovation**: The `IntermediateResult` and `JoinSource` abstractions allow the join pipeline to treat intermediate results and base tables uniformly, enabling clean, composable join execution.

**Next Steps**:
1. Review this design with the team
2. Get approval for the approach
3. Begin Phase 1 implementation (foundation structures)
4. Coordinate with ColumnResolver team for integration points
