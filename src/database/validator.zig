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

        .aggregate => {
            // Check if aggregates are allowed in this scope
            switch (ctx.scope) {
                .where, .group_by => {
                    // Aggregates not allowed in WHERE or GROUP BY
                    return ValidationError.AggregateInWhere;
                },
                .select, .having, .order_by => {
                    // Aggregates are valid in these scopes
                    return;
                },
            }
        },

        .binary => |bin| {
            // Recursively validate both sides
            try validateExpression(ctx, bin.left);
            try validateExpression(ctx, bin.right);
        },

        .unary => |un| {
            // Recursively validate the operand
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
