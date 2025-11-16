// validator.zig
// ============================================================================
// SQL Query Semantic Validator
// ============================================================================
//
// Implements deferred validation that runs after parsing but before execution.
// Based on PostgreSQL and SQLite validation strategies.
//
// See docs/column_validation_research.md for design rationale.
// ============================================================================

const std = @import("std");
const sql = @import("../sql.zig");
const Table = @import("../table.zig").Table;
const ColumnType = @import("../table.zig").ColumnType;
const column_resolver = @import("column_resolver.zig");
const ColumnResolver = column_resolver.ColumnResolver;
const StringHashMap = std.StringHashMap;
const Allocator = std.mem.Allocator;

// ============================================================================
// Error Types
// ============================================================================

pub const ValidationError = error{
    /// Column does not exist in any available table
    ColumnNotFound,

    /// Column exists in multiple tables (ambiguous reference)
    AmbiguousColumn,

    /// Table does not exist
    TableNotFound,

    /// Invalid table alias
    InvalidAlias,

    /// Aggregate function used in invalid context (e.g., WHERE clause)
    AggregateInWhere,

    /// Invalid expression structure
    InvalidExpression,

    /// Memory allocation failure
    OutOfMemory,
};

// ============================================================================
// Validation Result Types
// ============================================================================

/// Detailed information about a validation issue
pub const ValidationIssue = struct {
    /// Type of error/warning
    error_type: ValidationError,

    /// Column or expression that caused the issue (if applicable)
    context: ?[]const u8,

    /// Human-readable message
    message: []const u8,

    /// Optional hint for fixing the issue
    hint: ?[]const u8,

    /// Line number in query (future enhancement)
    line: ?usize,

    /// Column position in line (future enhancement)
    column: ?usize,

    allocator: Allocator,

    pub fn init(
        allocator: Allocator,
        error_type: ValidationError,
        context: ?[]const u8,
        message: []const u8,
        hint: ?[]const u8,
    ) !ValidationIssue {
        return ValidationIssue{
            .error_type = error_type,
            .context = if (context) |c| try allocator.dupe(u8, c) else null,
            .message = try allocator.dupe(u8, message),
            .hint = if (hint) |h| try allocator.dupe(u8, h) else null,
            .line = null,
            .column = null,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ValidationIssue) void {
        if (self.context) |c| self.allocator.free(c);
        self.allocator.free(self.message);
        if (self.hint) |h| self.allocator.free(h);
    }

    /// Format issue as PostgreSQL-style error message
    pub fn format(self: *const ValidationIssue, writer: anytype) !void {
        try writer.print("ERROR: {s}\n", .{self.message});
        if (self.line) |line| {
            try writer.print("LINE {d}", .{line});
            if (self.column) |col| {
                try writer.print(":{d}", .{col});
            }
            try writer.print("\n", .{});
        }
        if (self.hint) |hint| {
            try writer.print("HINT: {s}\n", .{hint});
        }
    }
};

/// Result of validation - can contain errors and warnings
pub const ValidationResult = struct {
    /// Whether validation passed
    valid: bool,

    /// Validation errors (block execution in strict mode)
    errors: std.ArrayList(ValidationIssue),

    /// Validation warnings (logged but don't block execution)
    warnings: std.ArrayList(ValidationIssue),

    allocator: Allocator,

    pub fn init(allocator: Allocator) ValidationResult {
        return ValidationResult{
            .valid = true,
            .errors = std.ArrayList(ValidationIssue).init(allocator),
            .warnings = std.ArrayList(ValidationIssue).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ValidationResult) void {
        for (self.errors.items) |*err| {
            err.deinit();
        }
        self.errors.deinit();

        for (self.warnings.items) |*warn| {
            warn.deinit();
        }
        self.warnings.deinit();
    }

    /// Add an error to the result
    pub fn addError(
        self: *ValidationResult,
        error_type: ValidationError,
        context: ?[]const u8,
        message: []const u8,
        hint: ?[]const u8,
    ) !void {
        const issue = try ValidationIssue.init(
            self.allocator,
            error_type,
            context,
            message,
            hint,
        );
        try self.errors.append(issue);
        self.valid = false;
    }

    /// Add a warning to the result
    pub fn addWarning(
        self: *ValidationResult,
        error_type: ValidationError,
        context: ?[]const u8,
        message: []const u8,
        hint: ?[]const u8,
    ) !void {
        const issue = try ValidationIssue.init(
            self.allocator,
            error_type,
            context,
            message,
            hint,
        );
        try self.warnings.append(issue);
    }

    /// Check if there are any errors
    pub fn hasErrors(self: *const ValidationResult) bool {
        return self.errors.items.len > 0;
    }

    /// Check if there are any warnings
    pub fn hasWarnings(self: *const ValidationResult) bool {
        return self.warnings.items.len > 0;
    }

    /// Get total issue count
    pub fn getTotalIssues(self: *const ValidationResult) usize {
        return self.errors.items.len + self.warnings.items.len;
    }

    /// Format all issues to a writer
    pub fn formatAll(self: *const ValidationResult, writer: anytype) !void {
        for (self.errors.items) |*err| {
            try err.format(writer);
        }
        for (self.warnings.items) |*warn| {
            try writer.print("WARNING: ", .{});
            try warn.format(writer);
        }
    }
};

// ============================================================================
// Validation Context
// ============================================================================

/// Context for validation - tracks available tables, aliases, and scope
pub const ValidationContext = struct {
    /// Allocator for temporary structures
    allocator: Allocator,

    /// Column resolver for multi-table validation
    resolver: ?*ColumnResolver,

    /// Map of table aliases to actual table names
    aliases: StringHashMap([]const u8),

    /// Current scope (determines what's allowed)
    scope: ValidationScope,

    /// Whether we're in an aggregate context (GROUP BY present)
    has_group_by: bool,

    pub fn init(allocator: Allocator, scope: ValidationScope) ValidationContext {
        return .{
            .allocator = allocator,
            .resolver = null,
            .aliases = StringHashMap([]const u8).init(allocator),
            .scope = scope,
            .has_group_by = false,
        };
    }

    pub fn deinit(self: *ValidationContext) void {
        self.aliases.deinit();
    }

    /// Add a table alias mapping
    pub fn addAlias(self: *ValidationContext, alias: []const u8, table_name: []const u8) !void {
        try self.aliases.put(alias, table_name);
    }

    /// Resolve an alias to its actual table name
    pub fn resolveAlias(self: *const ValidationContext, alias: []const u8) ?[]const u8 {
        return self.aliases.get(alias);
    }
};

/// Validation scope determines what expressions are allowed
pub const ValidationScope = enum {
    /// SELECT clause - allows literals, aggregates, expressions
    select,

    /// WHERE clause - allows expressions, but NOT aggregates
    where,

    /// HAVING clause - allows aggregates and expressions
    having,

    /// GROUP BY clause - allows expressions, but NOT aggregates
    group_by,

    /// ORDER BY clause - allows expressions, may allow aggregates if GROUP BY present
    order_by,
};

// ============================================================================
// Expression Type Detection
// ============================================================================

/// Check if a string represents a numeric literal
pub fn isNumericLiteral(str: []const u8) bool {
    if (str.len == 0) return false;

    var has_digit = false;
    var has_dot = false;

    for (str, 0..) |c, i| {
        if (c >= '0' and c <= '9') {
            has_digit = true;
        } else if (c == '.') {
            // Only one dot allowed, and not at the start or end
            if (has_dot or i == 0 or i == str.len - 1) return false;
            has_dot = true;
        } else if (c == '-' or c == '+') {
            // Sign only allowed at the start
            if (i != 0) return false;
        } else {
            return false;
        }
    }

    return has_digit;
}

/// Check if an expression is an aggregate function
pub fn isAggregate(expr: sql.Expr) bool {
    return switch (expr) {
        .aggregate => true,
        else => false,
    };
}

/// Check if an expression contains any aggregates (recursive)
pub fn containsAggregate(expr: sql.Expr) bool {
    return switch (expr) {
        .aggregate => true,
        .binary => |bin| containsAggregate(bin.left.*) or containsAggregate(bin.right.*),
        .subquery => false, // Subqueries are independent scopes
        else => false,
    };
}

// ============================================================================
// Column Validation
// ============================================================================

/// Validate a column reference in the current context
/// Returns true if valid, error otherwise with helpful message
pub fn validateColumnReference(
    ctx: *const ValidationContext,
    col_name: []const u8,
) ValidationError!void {
    // Skip validation for numeric literals
    if (isNumericLiteral(col_name)) {
        return;
    }

    // If we have a resolver (multi-table context), use it
    if (ctx.resolver) |resolver| {
        // Try to resolve the column
        const result = resolver.resolveColumn(col_name);

        if (result) |_| {
            // Column found - valid!
            return;
        } else |err| {
            // Map resolver errors to validation errors
            return switch (err) {
                column_resolver.ColumnResolverError.ColumnNotFound => ValidationError.ColumnNotFound,
                column_resolver.ColumnResolverError.AmbiguousColumn => ValidationError.AmbiguousColumn,
                column_resolver.ColumnResolverError.InvalidQualifiedName => ValidationError.InvalidExpression,
                else => ValidationError.ColumnNotFound,
            };
        }
    }

    // No resolver - can't validate columns
    // This is OK for simple cases (will be caught at execution time)
    return;
}

/// Validate an expression recursively
pub fn validateExpression(
    ctx: *const ValidationContext,
    expr: sql.Expr,
) ValidationError!void {
    switch (expr) {
        .literal => {
            // Literals are always valid
            return;
        },

        .column => |col| {
            // Validate column exists
            try validateColumnReference(ctx, col);
        },

        .aggregate => |agg| {
            // Check if aggregates are allowed in this scope
            switch (ctx.scope) {
                .where, .group_by => {
                    // Aggregates not allowed in WHERE or GROUP BY
                    return ValidationError.AggregateInWhere;
                },
                .select, .having, .order_by => {
                    // Aggregates are valid in these scopes
                    // Validate the column reference if not COUNT(*)
                    if (agg.column) |col| {
                        try validateColumnReference(ctx, col);
                    }
                    return;
                },
            }
        },

        .binary => |bin| {
            // Recursively validate both sides
            // bin is a pointer to BinaryExpr, which contains Expr values (not pointers)
            try validateExpression(ctx, bin.left);
            try validateExpression(ctx, bin.right);
        },

        .unary => |un| {
            // Recursively validate the operand
            // un is a pointer to UnaryExpr, which contains an Expr value (not pointer)
            try validateExpression(ctx, un.expr);
        },

        .subquery => {
            // Subqueries have their own independent scope
            // Validation would happen recursively, but that's complex
            // For now, we trust subqueries (validated at execution time)
            return;
        },
    }
}

/// Validate an expression and collect errors into a ValidationResult
/// This is a helper that converts ValidationError into ValidationResult
pub fn validateExpressionWithResult(
    ctx: *const ValidationContext,
    expr: sql.Expr,
    result: *ValidationResult,
) !void {
    validateExpression(ctx, expr) catch |err| {
        const msg = try formatErrorMessage(result.allocator, err, null);
        defer result.allocator.free(msg);

        const hint = switch (err) {
            ValidationError.AggregateInWhere => "Use HAVING clause instead of WHERE for aggregate filtering",
            ValidationError.ColumnNotFound => "Check column name spelling and table schema",
            ValidationError.AmbiguousColumn => "Use qualified column names (table.column)",
            else => null,
        };

        try result.addError(err, null, msg, hint);
    };
}

// ============================================================================
// SELECT Command Validation
// ============================================================================

/// Validate a SELECT command's column references
pub fn validateSelectColumns(
    allocator: Allocator,
    cmd: *const sql.SelectCmd,
    base_table: *Table,
    joined_tables: ?[]const *Table,
) ValidationError!void {
    // Build column resolver - always needed for validation
    var resolver = try ColumnResolver.init(allocator, base_table);
    defer resolver.deinit();

    // Add joined tables if present
    if (joined_tables) |tables| {
        for (tables) |table| {
            try resolver.addJoinedTable(table);
        }
    }

    // Create validation context
    var ctx = ValidationContext.init(allocator, .select);
    defer ctx.deinit();
    ctx.resolver = &resolver;
    ctx.has_group_by = cmd.group_by.items.len > 0;

    // Validate each selected column
    for (cmd.columns.items) |col_spec| {
        switch (col_spec) {
            .regular => |col_name| {
                // Skip numeric literals
                if (!isNumericLiteral(col_name)) {
                    try validateColumnReference(&ctx, col_name);
                }
            },
            .aggregate => {
                // Aggregates are valid in SELECT
                // (validated separately if needed)
            },
            .star => {
                // SELECT * is always valid
            },
        }
    }

    // Validate WHERE clause if present
    if (cmd.where_expr) |expr| {
        var where_ctx = ValidationContext.init(allocator, .where);
        defer where_ctx.deinit();
        where_ctx.resolver = &resolver;

        try validateExpression(&where_ctx, expr);
    }

    // Validate HAVING clause if present
    if (cmd.having_expr) |expr| {
        var having_ctx = ValidationContext.init(allocator, .having);
        defer having_ctx.deinit();
        having_ctx.resolver = &resolver;

        try validateExpression(&having_ctx, expr);
    }

    // Validate ORDER BY clause if present
    if (cmd.order_by) |order_by_clause| {
        var order_ctx = ValidationContext.init(allocator, .order_by);
        defer order_ctx.deinit();
        order_ctx.resolver = &resolver;
        order_ctx.has_group_by = ctx.has_group_by;

        for (order_by_clause.items.items) |order_item| {
            // Skip validation for aggregate functions (e.g., "COUNT(*)", "SUM(amount)")
            // These are validated separately against the SELECT clause in GROUP BY queries
            const is_aggregate_expr = std.mem.indexOf(u8, order_item.column, "(") != null;
            if (!is_aggregate_expr) {
                try validateColumnReference(&order_ctx, order_item.column);
            }
        }
    }
}

// ============================================================================
// INSERT Command Validation
// ============================================================================

/// Validate an INSERT command's columns and values
pub fn validateInsert(
    allocator: Allocator,
    cmd: *const sql.InsertCmd,
    table: *Table,
) !ValidationResult {
    var result = ValidationResult.init(allocator);
    errdefer result.deinit();

    // Check for empty column list
    if (cmd.columns.items.len == 0) {
        try result.addError(
            ValidationError.InvalidExpression,
            null,
            "INSERT command must specify at least one column",
            "Try: INSERT INTO table_name (col1, col2) VALUES (val1, val2)",
        );
        return result;
    }

    // Check that value count matches column count
    if (cmd.columns.items.len != cmd.values.items.len) {
        const msg = try std.fmt.allocPrint(
            allocator,
            "INSERT column count ({d}) does not match value count ({d})",
            .{ cmd.columns.items.len, cmd.values.items.len },
        );
        defer allocator.free(msg);

        try result.addError(
            ValidationError.InvalidExpression,
            null,
            msg,
            "Ensure the number of columns matches the number of values",
        );
        return result;
    }

    // Track seen columns to detect duplicates
    var seen_columns = StringHashMap(void).init(allocator);
    defer seen_columns.deinit();

    // Validate each column exists in the table
    for (cmd.columns.items) |col_name| {
        // Check for duplicate columns in INSERT
        if (seen_columns.contains(col_name)) {
            const msg = try std.fmt.allocPrint(
                allocator,
                "column \"{s}\" specified more than once",
                .{col_name},
            );
            defer allocator.free(msg);

            try result.addError(
                ValidationError.InvalidExpression,
                col_name,
                msg,
                "Remove duplicate column references",
            );
            continue;
        }
        try seen_columns.put(col_name, {});

        // Check if column exists in table schema
        var column_exists = false;
        for (table.schema.columns.items) |schema_col| {
            if (std.mem.eql(u8, col_name, schema_col.name)) {
                column_exists = true;
                break;
            }
        }

        if (!column_exists) {
            const msg = try std.fmt.allocPrint(
                allocator,
                "column \"{s}\" does not exist in table \"{s}\"",
                .{ col_name, table.name },
            );
            defer allocator.free(msg);

            // Try to find a similar column name for suggestion
            const hint = if (findSimilarColumnInTable(col_name, table, 2)) |similar|
                try std.fmt.allocPrint(
                    allocator,
                    "Did you mean \"{s}\"?",
                    .{similar},
                )
            else
                null;

            try result.addError(
                ValidationError.ColumnNotFound,
                col_name,
                msg,
                hint,
            );

            // Free hint if allocated
            if (hint) |h| allocator.free(h);
        }
    }

    return result;
}

// ============================================================================
// UPDATE Command Validation
// ============================================================================

/// Validate an UPDATE command's assignments and WHERE clause
pub fn validateUpdate(
    allocator: Allocator,
    cmd: *const sql.UpdateCmd,
    table: *Table,
) !ValidationResult {
    var result = ValidationResult.init(allocator);
    errdefer result.deinit();

    // Check for empty assignments
    if (cmd.assignments.items.len == 0) {
        try result.addError(
            ValidationError.InvalidExpression,
            null,
            "UPDATE command must specify at least one column assignment",
            "Try: UPDATE table_name SET col1 = value1 WHERE condition",
        );
        return result;
    }

    // Track seen columns to detect duplicate assignments
    var seen_columns = StringHashMap(void).init(allocator);
    defer seen_columns.deinit();

    // Validate each assignment column exists
    for (cmd.assignments.items) |assignment| {
        const col_name = assignment.column;

        // Check for duplicate assignments
        if (seen_columns.contains(col_name)) {
            const msg = try std.fmt.allocPrint(
                allocator,
                "column \"{s}\" assigned more than once",
                .{col_name},
            );
            defer allocator.free(msg);

            try result.addError(
                ValidationError.InvalidExpression,
                col_name,
                msg,
                "Remove duplicate assignments",
            );
            continue;
        }
        try seen_columns.put(col_name, {});

        // Check if column exists in table schema
        var column_exists = false;
        for (table.schema.columns.items) |schema_col| {
            if (std.mem.eql(u8, col_name, schema_col.name)) {
                column_exists = true;
                break;
            }
        }

        if (!column_exists) {
            const msg = try std.fmt.allocPrint(
                allocator,
                "column \"{s}\" does not exist in table \"{s}\"",
                .{ col_name, table.name },
            );
            defer allocator.free(msg);

            // Try to find a similar column name for suggestion
            const hint = if (findSimilarColumnInTable(col_name, table, 2)) |similar|
                try std.fmt.allocPrint(
                    allocator,
                    "Did you mean \"{s}\"?",
                    .{similar},
                )
            else
                null;

            try result.addError(
                ValidationError.ColumnNotFound,
                col_name,
                msg,
                hint,
            );

            // Free hint if allocated
            if (hint) |h| allocator.free(h);
        }
    }

    // Validate WHERE clause if present
    if (cmd.where_expr) |expr| {
        // Build column resolver for WHERE validation
        var resolver = try ColumnResolver.init(allocator, table);
        defer resolver.deinit();

        var where_ctx = ValidationContext.init(allocator, .where);
        defer where_ctx.deinit();
        where_ctx.resolver = &resolver;

        // Validate WHERE expression
        validateExpression(&where_ctx, expr) catch |err| {
            const msg = try formatErrorMessage(allocator, err, null);
            defer allocator.free(msg);

            try result.addError(
                err,
                null,
                msg,
                "Check that all columns in WHERE clause exist in the table",
            );
        };
    }

    return result;
}

// ============================================================================
// DELETE Command Validation
// ============================================================================

/// Validate a DELETE command's WHERE clause
pub fn validateDelete(
    allocator: Allocator,
    cmd: *const sql.DeleteCmd,
    table: *Table,
) !ValidationResult {
    var result = ValidationResult.init(allocator);
    errdefer result.deinit();

    // Validate WHERE column if present (simple WHERE clause)
    if (cmd.where_column) |col_name| {
        // Check if column exists in table schema
        var column_exists = false;
        for (table.schema.columns.items) |schema_col| {
            if (std.mem.eql(u8, col_name, schema_col.name)) {
                column_exists = true;
                break;
            }
        }

        if (!column_exists) {
            const msg = try std.fmt.allocPrint(
                allocator,
                "column \"{s}\" does not exist in table \"{s}\"",
                .{ col_name, table.name },
            );
            defer allocator.free(msg);

            // Try to find a similar column name for suggestion
            const hint = if (findSimilarColumnInTable(col_name, table, 2)) |similar|
                try std.fmt.allocPrint(
                    allocator,
                    "Did you mean \"{s}\"?",
                    .{similar},
                )
            else
                null;

            try result.addError(
                ValidationError.ColumnNotFound,
                col_name,
                msg,
                hint,
            );

            // Free hint if allocated
            if (hint) |h| allocator.free(h);
        }
    }

    return result;
}

// ============================================================================
// Fuzzy Matching for Smart Suggestions
// ============================================================================

/// Calculate Levenshtein distance between two strings
/// Returns the minimum number of single-character edits (insertions, deletions, substitutions)
/// needed to transform string `a` into string `b`
pub fn levenshteinDistance(a: []const u8, b: []const u8) usize {
    if (a.len == 0) return b.len;
    if (b.len == 0) return a.len;

    // Create distance matrix
    const rows = a.len + 1;
    const cols = b.len + 1;

    // Use stack allocation for small strings, heap for large ones
    var matrix_buffer: [256]usize = undefined;
    const use_stack = rows * cols <= 256;

    var matrix_slice = if (use_stack)
        matrix_buffer[0..rows * cols]
    else
        // For large strings, we'd need heap allocation, but for now just use truncated stack
        matrix_buffer[0..256];

    // Initialize first row and column
    for (0..rows) |i| {
        matrix_slice[i * cols] = i;
    }
    for (0..cols) |j| {
        matrix_slice[j] = j;
    }

    // Fill in the rest of the matrix
    for (1..rows) |i| {
        for (1..cols) |j| {
            const cost: usize = if (a[i - 1] == b[j - 1]) 0 else 1;

            const deletion = matrix_slice[(i - 1) * cols + j] + 1;
            const insertion = matrix_slice[i * cols + (j - 1)] + 1;
            const substitution = matrix_slice[(i - 1) * cols + (j - 1)] + cost;

            matrix_slice[i * cols + j] = @min(@min(deletion, insertion), substitution);
        }
    }

    return matrix_slice[(rows - 1) * cols + (cols - 1)];
}

/// Find the most similar column name from a list of candidates
/// Returns the best match if within the distance threshold, null otherwise
pub fn findSimilarColumn(
    needle: []const u8,
    haystack: []const []const u8,
    max_distance: usize,
) ?[]const u8 {
    if (haystack.len == 0) return null;

    var best_match: ?[]const u8 = null;
    var best_distance: usize = max_distance + 1;

    for (haystack) |candidate| {
        const distance = levenshteinDistance(needle, candidate);

        // Update best match if this is closer
        if (distance < best_distance) {
            best_distance = distance;
            best_match = candidate;
        }
    }

    // Only return match if within threshold
    if (best_distance <= max_distance) {
        return best_match;
    }

    return null;
}

/// Find similar column in a table's schema
pub fn findSimilarColumnInTable(
    needle: []const u8,
    table: *Table,
    max_distance: usize,
) ?[]const u8 {
    if (table.schema.columns.items.len == 0) return null;

    var best_match: ?[]const u8 = null;
    var best_distance: usize = max_distance + 1;

    for (table.schema.columns.items) |col_def| {
        const distance = levenshteinDistance(needle, col_def.name);

        if (distance < best_distance) {
            best_distance = distance;
            best_match = col_def.name;
        }
    }

    if (best_distance <= max_distance) {
        return best_match;
    }

    return null;
}

// ============================================================================
// Error Message Formatting
// ============================================================================

/// Format a helpful error message for a validation error
pub fn formatErrorMessage(
    allocator: Allocator,
    err: ValidationError,
    col_name: ?[]const u8,
) ![]const u8 {
    return switch (err) {
        ValidationError.ColumnNotFound => blk: {
            if (col_name) |name| {
                break :blk try std.fmt.allocPrint(
                    allocator,
                    "column \"{s}\" does not exist",
                    .{name},
                );
            }
            break :blk try allocator.dupe(u8, "column does not exist");
        },

        ValidationError.AmbiguousColumn => blk: {
            if (col_name) |name| {
                break :blk try std.fmt.allocPrint(
                    allocator,
                    "column \"{s}\" is ambiguous (exists in multiple tables)",
                    .{name},
                );
            }
            break :blk try allocator.dupe(u8, "column reference is ambiguous");
        },

        ValidationError.TableNotFound => try allocator.dupe(u8, "table does not exist"),
        ValidationError.InvalidAlias => try allocator.dupe(u8, "invalid table alias"),
        ValidationError.AggregateInWhere => try allocator.dupe(u8, "aggregate functions are not allowed in WHERE clause"),
        ValidationError.InvalidExpression => try allocator.dupe(u8, "invalid expression"),
        ValidationError.OutOfMemory => try allocator.dupe(u8, "out of memory"),
    };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "isNumericLiteral: valid integers" {
    try testing.expect(isNumericLiteral("0"));
    try testing.expect(isNumericLiteral("1"));
    try testing.expect(isNumericLiteral("42"));
    try testing.expect(isNumericLiteral("12345"));
}

test "isNumericLiteral: valid floats" {
    try testing.expect(isNumericLiteral("1.0"));
    try testing.expect(isNumericLiteral("3.14"));
    try testing.expect(isNumericLiteral("0.5"));
}

test "isNumericLiteral: with signs" {
    try testing.expect(isNumericLiteral("+42"));
    try testing.expect(isNumericLiteral("-42"));
    try testing.expect(isNumericLiteral("+3.14"));
    try testing.expect(isNumericLiteral("-3.14"));
}

test "isNumericLiteral: invalid inputs" {
    try testing.expect(!isNumericLiteral(""));
    try testing.expect(!isNumericLiteral("abc"));
    try testing.expect(!isNumericLiteral("12abc"));
    try testing.expect(!isNumericLiteral("1.2.3"));
    try testing.expect(!isNumericLiteral(".5"));
    try testing.expect(!isNumericLiteral("5."));
    try testing.expect(!isNumericLiteral("1+2"));
}

test "ValidationContext: alias management" {
    var ctx = ValidationContext.init(testing.allocator, .select);
    defer ctx.deinit();

    // Add aliases
    try ctx.addAlias("u", "users");
    try ctx.addAlias("o", "orders");

    // Resolve aliases
    try testing.expectEqualStrings("users", ctx.resolveAlias("u").?);
    try testing.expectEqualStrings("orders", ctx.resolveAlias("o").?);

    // Non-existent alias
    try testing.expect(ctx.resolveAlias("nonexistent") == null);
}

test "formatErrorMessage: column not found" {
    const msg = try formatErrorMessage(testing.allocator, ValidationError.ColumnNotFound, "invalid_col");
    defer testing.allocator.free(msg);

    try testing.expect(std.mem.indexOf(u8, msg, "invalid_col") != null);
    try testing.expect(std.mem.indexOf(u8, msg, "does not exist") != null);
}

test "formatErrorMessage: ambiguous column" {
    const msg = try formatErrorMessage(testing.allocator, ValidationError.AmbiguousColumn, "id");
    defer testing.allocator.free(msg);

    try testing.expect(std.mem.indexOf(u8, msg, "id") != null);
    try testing.expect(std.mem.indexOf(u8, msg, "ambiguous") != null);
}

// ============================================================================
// INSERT Validation Tests
// ============================================================================

test "validateInsert: valid insert" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create a valid INSERT command
    var columns = std.ArrayList([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");

    var values = std.ArrayList(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(result.valid);
    try testing.expectEqual(@as(usize, 0), result.errors.items.len);
}

test "validateInsert: column not found" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create INSERT with invalid column
    var columns = std.ArrayList([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("invalid_col");

    var values = std.ArrayList(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateInsert: duplicate columns" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create INSERT with duplicate column
    var columns = std.ArrayList([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("id"); // Duplicate!

    var values = std.ArrayList(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .int = 2 });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
}

test "validateInsert: column count mismatch" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create INSERT with mismatched counts
    var columns = std.ArrayList([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("name");

    var values = std.ArrayList(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 }); // Only one value!

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
}

// ============================================================================
// UPDATE Validation Tests
// ============================================================================

test "validateUpdate: valid update" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create a valid UPDATE command
    var assignments = std.ArrayList(sql.Assignment).init(testing.allocator);
    defer assignments.deinit();
    try assignments.append(sql.Assignment{
        .column = "name",
        .value = ColumnValue{ .text = "Bob" },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(result.valid);
    try testing.expectEqual(@as(usize, 0), result.errors.items.len);
}

test "validateUpdate: column not found" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create UPDATE with invalid column
    var assignments = std.ArrayList(sql.Assignment).init(testing.allocator);
    defer assignments.deinit();
    try assignments.append(sql.Assignment{
        .column = "invalid_col",
        .value = ColumnValue{ .text = "Bob" },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

test "validateUpdate: duplicate assignments" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create UPDATE with duplicate assignments
    var assignments = std.ArrayList(sql.Assignment).init(testing.allocator);
    defer assignments.deinit();
    try assignments.append(sql.Assignment{
        .column = "name",
        .value = ColumnValue{ .text = "Bob" },
    });
    try assignments.append(sql.Assignment{
        .column = "name", // Duplicate!
        .value = ColumnValue{ .text = "Charlie" },
    });

    const update_cmd = sql.UpdateCmd{
        .table_name = "users",
        .assignments = assignments,
        .where_expr = null,
    };

    var result = try validateUpdate(testing.allocator, &update_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
}

// ============================================================================
// DELETE Validation Tests
// ============================================================================

test "validateDelete: valid delete" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create a valid DELETE command
    const delete_cmd = sql.DeleteCmd{
        .table_name = "users",
        .where_column = "id",
        .where_value = ColumnValue{ .int = 1 },
    };

    var result = try validateDelete(testing.allocator, &delete_cmd, &table);
    defer result.deinit();

    try testing.expect(result.valid);
    try testing.expectEqual(@as(usize, 0), result.errors.items.len);
}

test "validateDelete: column not found" {
    const ColumnDef = @import("../table.zig").ColumnDef;
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create DELETE with invalid column
    const delete_cmd = sql.DeleteCmd{
        .table_name = "users",
        .where_column = "invalid_col",
        .where_value = ColumnValue{ .int = 1 },
    };

    var result = try validateDelete(testing.allocator, &delete_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);
    try testing.expectEqual(ValidationError.ColumnNotFound, result.errors.items[0].error_type);
}

// ============================================================================
// Fuzzy Matching Tests
// ============================================================================

test "levenshteinDistance: identical strings" {
    try testing.expectEqual(@as(usize, 0), levenshteinDistance("hello", "hello"));
    try testing.expectEqual(@as(usize, 0), levenshteinDistance("test", "test"));
}

test "levenshteinDistance: empty strings" {
    try testing.expectEqual(@as(usize, 5), levenshteinDistance("hello", ""));
    try testing.expectEqual(@as(usize, 4), levenshteinDistance("", "test"));
    try testing.expectEqual(@as(usize, 0), levenshteinDistance("", ""));
}

test "levenshteinDistance: single character difference" {
    try testing.expectEqual(@as(usize, 1), levenshteinDistance("hello", "hallo")); // substitution
    try testing.expectEqual(@as(usize, 1), levenshteinDistance("test", "tests")); // insertion
    try testing.expectEqual(@as(usize, 1), levenshteinDistance("hello", "hell")); // deletion
}

test "levenshteinDistance: multiple differences" {
    try testing.expectEqual(@as(usize, 3), levenshteinDistance("kitten", "sitting")); // classic example
    try testing.expectEqual(@as(usize, 2), levenshteinDistance("user_name", "username")); // underscore removed
}

test "levenshteinDistance: typos" {
    try testing.expectEqual(@as(usize, 1), levenshteinDistance("name", "nmae")); // transposition (counted as 2 in basic LD)
    try testing.expectEqual(@as(usize, 2), levenshteinDistance("email", "emial")); // transposition
}

test "findSimilarColumn: exact match within threshold" {
    const columns = [_][]const u8{ "id", "name", "email" };

    const result = findSimilarColumn("id", &columns, 2);
    try testing.expect(result != null);
    try testing.expectEqualStrings("id", result.?);
}

test "findSimilarColumn: close match" {
    const columns = [_][]const u8{ "id", "user_name", "email" };

    // "usr_name" is distance 2 from "user_name"
    const result = findSimilarColumn("usr_name", &columns, 2);
    try testing.expect(result != null);
    try testing.expectEqualStrings("user_name", result.?);
}

test "findSimilarColumn: no match beyond threshold" {
    const columns = [_][]const u8{ "id", "name", "email" };

    // "completely_different" is far from any column
    const result = findSimilarColumn("completely_different", &columns, 2);
    try testing.expect(result == null);
}

test "findSimilarColumn: empty haystack" {
    const columns = [_][]const u8{};

    const result = findSimilarColumn("test", &columns, 2);
    try testing.expect(result == null);
}

test "findSimilarColumn: picks closest match" {
    const columns = [_][]const u8{ "id", "name", "user_name", "username" };

    // "usrname" is closest to "username" (distance 1 vs 2 for "user_name")
    const result = findSimilarColumn("usrname", &columns, 2);
    try testing.expect(result != null);
    try testing.expectEqualStrings("username", result.?);
}

test "findSimilarColumnInTable: finds similar column" {
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("user_name", ColumnType.text);
    try schema.addColumn("email", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // "usr_name" should match "user_name"
    const result = findSimilarColumnInTable("usr_name", &table, 2);
    try testing.expect(result != null);
    try testing.expectEqualStrings("user_name", result.?);
}

test "validateInsert: fuzzy matching hint" {
    const ColumnType = @import("../table.zig").ColumnType;
    const TableSchema = @import("../table.zig").TableSchema;
    const ColumnValue = @import("../table.zig").ColumnValue;

    var schema = TableSchema.init(testing.allocator);
    defer schema.deinit();
    try schema.addColumn("id", ColumnType.int);
    try schema.addColumn("user_name", ColumnType.text);

    var table = Table{
        .name = "users",
        .schema = schema,
        .rows = std.ArrayList(std.ArrayList(ColumnValue)).init(testing.allocator),
        .allocator = testing.allocator,
    };
    defer table.rows.deinit();

    // Create INSERT with typo in column name
    var columns = std.ArrayList([]const u8).init(testing.allocator);
    defer columns.deinit();
    try columns.append("id");
    try columns.append("usr_name"); // Typo: missing "e"

    var values = std.ArrayList(ColumnValue).init(testing.allocator);
    defer values.deinit();
    try values.append(ColumnValue{ .int = 1 });
    try values.append(ColumnValue{ .text = "Alice" });

    const insert_cmd = sql.InsertCmd{
        .table_name = "users",
        .columns = columns,
        .values = values,
    };

    var result = try validateInsert(testing.allocator, &insert_cmd, &table);
    defer result.deinit();

    try testing.expect(!result.valid);
    try testing.expect(result.errors.items.len > 0);

    // Check that hint contains suggestion
    const error_item = result.errors.items[0];
    try testing.expect(error_item.hint != null);

    const hint = error_item.hint.?;
    try testing.expect(std.mem.indexOf(u8, hint, "Did you mean") != null);
    try testing.expect(std.mem.indexOf(u8, hint, "user_name") != null);
}
