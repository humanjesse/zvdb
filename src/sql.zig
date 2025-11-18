const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const StringHashMap = std.StringHashMap;
const Table = @import("table.zig").Table;
const ColumnValue = @import("table.zig").ColumnValue;
const ColumnType = @import("table.zig").ColumnType;

/// SQL parsing errors
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
    HavingWithoutGroupBy, // HAVING used without GROUP BY
    ValidationFailed, // Query validation failed (semantic errors)
    DuplicateEmbeddingDimension, // Multiple embedding columns with same dimension in one table
    TooManyEmbeddings, // Too many embedding columns per row (resource limit)
};

/// ORDER BY direction
pub const OrderDirection = enum {
    asc,
    desc,
};

/// Single ORDER BY clause item
pub const OrderByItem = struct {
    column: []const u8, // Column name or aggregate function name
    direction: OrderDirection,

    pub fn deinit(self: *OrderByItem, allocator: Allocator) void {
        allocator.free(self.column);
    }
};

/// Full ORDER BY clause
pub const OrderByClause = struct {
    items: ArrayList(OrderByItem),

    pub fn deinit(self: *OrderByClause, allocator: Allocator) void {
        for (self.items.items) |*item| {
            item.deinit(allocator);
        }
        self.items.deinit();
    }
};

/// SQL command types
pub const SqlCommand = union(enum) {
    create_table: CreateTableCmd,
    create_index: CreateIndexCmd,
    drop_index: DropIndexCmd,
    insert: InsertCmd,
    select: SelectCmd,
    delete: DeleteCmd,
    update: UpdateCmd,
    begin: void,
    commit: void,
    rollback: void,
    vacuum: VacuumCmd,
    alter_table: AlterTableCmd,

    pub fn deinit(self: *SqlCommand, allocator: Allocator) void {
        switch (self.*) {
            .create_table => |*cmd| cmd.deinit(allocator),
            .create_index => |*cmd| cmd.deinit(allocator),
            .drop_index => |*cmd| cmd.deinit(allocator),
            .insert => |*cmd| cmd.deinit(allocator),
            .select => |*cmd| cmd.deinit(allocator),
            .delete => |*cmd| cmd.deinit(allocator),
            .update => |*cmd| cmd.deinit(allocator),
            .begin => {},
            .commit => {},
            .rollback => {},
            .vacuum => |*cmd| cmd.deinit(allocator),
            .alter_table => |*cmd| cmd.deinit(allocator),
        }
    }
};

/// CREATE TABLE command
pub const CreateTableCmd = struct {
    table_name: []const u8,
    columns: ArrayList(ColumnDef),

    pub fn deinit(self: *CreateTableCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            allocator.free(col.name);
        }
        self.columns.deinit();
    }
};

pub const ColumnDef = struct {
    name: []const u8,
    col_type: ColumnType,
    embedding_dim: ?usize, // Dimension for embedding type (null for non-embedding types)
};

/// CREATE INDEX command
pub const CreateIndexCmd = struct {
    index_name: []const u8,
    table_name: []const u8,
    column_name: []const u8,

    pub fn deinit(self: *CreateIndexCmd, allocator: Allocator) void {
        allocator.free(self.index_name);
        allocator.free(self.table_name);
        allocator.free(self.column_name);
    }
};

/// DROP INDEX command
pub const DropIndexCmd = struct {
    index_name: []const u8,

    pub fn deinit(self: *DropIndexCmd, allocator: Allocator) void {
        allocator.free(self.index_name);
    }
};

/// VACUUM command
/// Cleans up old row versions to reclaim memory
pub const VacuumCmd = struct {
    /// Table name to vacuum (null = vacuum all tables)
    table_name: ?[]const u8,

    pub fn deinit(self: *VacuumCmd, allocator: Allocator) void {
        if (self.table_name) |name| {
            allocator.free(name);
        }
    }
};

/// ALTER TABLE command
pub const AlterTableCmd = struct {
    table_name: []const u8,
    operation: AlterOperation,

    pub fn deinit(self: *AlterTableCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        self.operation.deinit(allocator);
    }
};

/// ALTER TABLE operations
pub const AlterOperation = union(enum) {
    add_column: AddColumnOp,
    drop_column: DropColumnOp,
    rename_column: RenameColumnOp,

    pub fn deinit(self: *AlterOperation, allocator: Allocator) void {
        switch (self.*) {
            .add_column => |*op| op.deinit(allocator),
            .drop_column => |*op| op.deinit(allocator),
            .rename_column => |*op| op.deinit(allocator),
        }
    }
};

/// ADD COLUMN operation
pub const AddColumnOp = struct {
    column: ColumnDef,

    pub fn deinit(self: *AddColumnOp, allocator: Allocator) void {
        allocator.free(self.column.name);
    }
};

/// DROP COLUMN operation
pub const DropColumnOp = struct {
    column_name: []const u8,

    pub fn deinit(self: *DropColumnOp, allocator: Allocator) void {
        allocator.free(self.column_name);
    }
};

/// RENAME COLUMN operation
pub const RenameColumnOp = struct {
    old_name: []const u8,
    new_name: []const u8,

    pub fn deinit(self: *RenameColumnOp, allocator: Allocator) void {
        allocator.free(self.old_name);
        allocator.free(self.new_name);
    }
};

/// INSERT command
pub const InsertCmd = struct {
    table_name: []const u8,
    columns: ArrayList([]const u8),
    values: ArrayList(ColumnValue),

    pub fn deinit(self: *InsertCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |col| {
            allocator.free(col);
        }
        self.columns.deinit();
        for (self.values.items) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        self.values.deinit();
    }
};

/// SELECT command with semantic search support
pub const SelectCmd = struct {
    table_name: []const u8,
    columns: ArrayList(SelectColumn), // Changed to support aggregates
    joins: ArrayList(JoinClause), // JOIN clauses
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    where_expr: ?Expr, // Complex WHERE expressions (for JOINs and advanced filtering)
    similar_to_column: ?[]const u8, // For SIMILAR TO queries
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8, // ORDER BY SIMILARITY TO "text"
    order_by_vibes: bool, // Fun parody feature!
    order_by: ?OrderByClause, // Generic ORDER BY clause
    group_by: ArrayList([]const u8), // GROUP BY columns
    having_expr: ?Expr, // HAVING clause for filtering grouped results
    limit: ?usize,

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit();

        // Free JOIN clauses
        for (self.joins.items) |*join| {
            join.deinit(allocator);
        }
        self.joins.deinit();

        if (self.where_column) |col| allocator.free(col);
        if (self.where_value) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        if (self.where_expr) |*expr| {
            var e = expr.*;
            e.deinit(allocator);
        }
        if (self.similar_to_column) |col| allocator.free(col);
        if (self.similar_to_text) |text| allocator.free(text);
        if (self.order_by_similarity) |text| allocator.free(text);

        // Free generic ORDER BY clause
        if (self.order_by) |*ob| {
            ob.deinit(allocator);
        }

        // Free GROUP BY columns
        for (self.group_by.items) |col| {
            allocator.free(col);
        }
        self.group_by.deinit();

        // Free HAVING expression
        if (self.having_expr) |*expr| {
            var e = expr.*;
            e.deinit(allocator);
        }
    }
};

/// DELETE command
pub const DeleteCmd = struct {
    table_name: []const u8,
    where_expr: ?Expr,

    pub fn deinit(self: *DeleteCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        if (self.where_expr) |*expr| {
            var e = expr.*;
            e.deinit(allocator);
        }
    }
};

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

/// Aggregate expression (e.g., COUNT(*), SUM(balance))
pub const AggregateExpr = struct {
    func: AggregateFunc,
    column: ?[]const u8, // null for COUNT(*)

    pub fn deinit(self: *AggregateExpr, allocator: Allocator) void {
        if (self.column) |col| allocator.free(col);
    }
};

/// Column selection with optional aggregation
pub const SelectColumn = union(enum) {
    regular: []const u8, // Regular column: "name"
    aggregate: AggregateExpr, // Aggregate: COUNT(*), SUM(balance)
    star: void, // SELECT *

    pub fn deinit(self: *SelectColumn, allocator: Allocator) void {
        switch (self.*) {
            .regular => |col| allocator.free(col),
            .aggregate => |*agg| agg.deinit(allocator),
            .star => {},
        }
    }
};

/// Binary operators for WHERE expressions
pub const BinaryOp = enum {
    eq, // =
    neq, // !=
    lt, // <
    gt, // >
    lte, // <=
    gte, // >=
    and_op, // AND
    or_op, // OR
    in_op, // IN (for subqueries and lists)
    not_in_op, // NOT IN
    exists_op, // EXISTS
    not_exists_op, // NOT EXISTS
};

/// Unary operators for WHERE expressions
pub const UnaryOp = enum {
    not, // NOT
    is_null, // IS NULL
    is_not_null, // IS NOT NULL
};

/// Binary expression node
pub const BinaryExpr = struct {
    op: BinaryOp,
    left: Expr,
    right: Expr,

    pub fn deinit(self: *BinaryExpr, allocator: Allocator) void {
        self.left.deinit(allocator);
        self.right.deinit(allocator);
    }
};

/// Unary expression node
pub const UnaryExpr = struct {
    op: UnaryOp,
    expr: Expr,

    pub fn deinit(self: *UnaryExpr, allocator: Allocator) void {
        self.expr.deinit(allocator);
    }
};

/// Expression tree for WHERE clauses
pub const Expr = union(enum) {
    literal: ColumnValue,
    column: []const u8,
    binary: *BinaryExpr,
    unary: *UnaryExpr,
    subquery: *SelectCmd, // Nested SELECT statement for subqueries
    aggregate: AggregateExpr, // Aggregate function for HAVING clauses

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
            .aggregate => |*agg| {
                var a = agg.*;
                a.deinit(allocator);
            },
        }
    }
};

/// Assignment for UPDATE SET clause
pub const Assignment = struct {
    column: []const u8,
    value: ColumnValue,

    pub fn deinit(self: *Assignment, allocator: Allocator) void {
        allocator.free(self.column);
        var val = self.value;
        val.deinit(allocator);
    }
};

/// UPDATE command
pub const UpdateCmd = struct {
    table_name: []const u8,
    assignments: ArrayList(Assignment),
    where_expr: ?Expr,

    pub fn deinit(self: *UpdateCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.assignments.items) |*assign| {
            assign.deinit(allocator);
        }
        self.assignments.deinit();
        if (self.where_expr) |*expr| {
            var e = expr.*;
            e.deinit(allocator);
        }
    }
};

/// JOIN types
pub const JoinType = enum {
    inner,
    left,
    right,
    // cross,  // Future: CROSS JOIN
};

/// JOIN clause for SELECT queries
pub const JoinClause = struct {
    join_type: JoinType,
    table_name: []const u8,
    left_column: []const u8, // e.g., "users.id" or "id"
    right_column: []const u8, // e.g., "orders.user_id" or "user_id"

    pub fn deinit(self: *JoinClause, allocator: Allocator) void {
        allocator.free(self.table_name);
        allocator.free(self.left_column);
        allocator.free(self.right_column);
    }
};

/// Simple SQL tokenizer
const Token = struct {
    text: []const u8,
    start: usize,
};

fn tokenize(allocator: Allocator, sql: []const u8) !ArrayList(Token) {
    var tokens = ArrayList(Token).init(allocator);
    var i: usize = 0;

    while (i < sql.len) {
        // Skip whitespace
        while (i < sql.len and std.ascii.isWhitespace(sql[i])) : (i += 1) {}
        if (i >= sql.len) break;

        const start = i;

        // String literal
        if (sql[i] == '"' or sql[i] == '\'') {
            const quote = sql[i];
            i += 1;
            while (i < sql.len and sql[i] != quote) : (i += 1) {}
            if (i < sql.len) i += 1; // Skip closing quote
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Number
        else if (std.ascii.isDigit(sql[i]) or (sql[i] == '-' and i + 1 < sql.len and std.ascii.isDigit(sql[i + 1]))) {
            if (sql[i] == '-') i += 1; // Skip the negative sign
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) : (i += 1) {}
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Identifier or keyword
        else if (std.ascii.isAlphabetic(sql[i]) or sql[i] == '_') {
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_' or sql[i] == '.')) : (i += 1) {}
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Special characters and operators
        else if (sql[i] == '(' or sql[i] == ')' or sql[i] == ',' or sql[i] == '*') {
            i += 1;
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Square brackets for array literals (embeddings)
        else if (sql[i] == '[' or sql[i] == ']') {
            i += 1;
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Multi-character operators: <=, >=, !=
        else if (sql[i] == '<' or sql[i] == '>' or sql[i] == '!' or sql[i] == '=') {
            i += 1;
            if (i < sql.len and sql[i] == '=') {
                i += 1; // Include the = for <=, >=, !=, ==
            }
            try tokens.append(.{ .text = sql[start..i], .start = start });
        } else {
            i += 1; // Skip unknown characters
        }
    }

    return tokens;
}

fn parseString(text: []const u8) []const u8 {
    if (text.len >= 2 and (text[0] == '"' or text[0] == '\'')) {
        return text[1 .. text.len - 1];
    }
    return text;
}

fn parseColumnType(type_str: []const u8) !ColumnType {
    const lower = type_str; // We'll do case-insensitive later
    if (std.mem.eql(u8, lower, "int") or std.mem.eql(u8, lower, "INT")) {
        return .int;
    } else if (std.mem.eql(u8, lower, "float") or std.mem.eql(u8, lower, "FLOAT")) {
        return .float;
    } else if (std.mem.eql(u8, lower, "text") or std.mem.eql(u8, lower, "TEXT")) {
        return .text;
    } else if (std.mem.eql(u8, lower, "bool") or std.mem.eql(u8, lower, "BOOL")) {
        return .bool;
    } else if (std.mem.eql(u8, lower, "embedding") or std.mem.eql(u8, lower, "EMBEDDING")) {
        return .embedding;
    }
    return SqlError.InvalidColumnType;
}

fn eqlIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |ca, cb| {
        if (std.ascii.toLower(ca) != std.ascii.toLower(cb)) return false;
    }
    return true;
}

/// Check if a token is a SELECT clause keyword
/// Used to detect table aliases (non-keywords after table names)
fn isSelectKeyword(token: []const u8) bool {
    return eqlIgnoreCase(token, "JOIN") or
        eqlIgnoreCase(token, "INNER") or
        eqlIgnoreCase(token, "LEFT") or
        eqlIgnoreCase(token, "RIGHT") or
        eqlIgnoreCase(token, "OUTER") or
        eqlIgnoreCase(token, "FULL") or
        eqlIgnoreCase(token, "CROSS") or
        eqlIgnoreCase(token, "ON") or
        eqlIgnoreCase(token, "WHERE") or
        eqlIgnoreCase(token, "GROUP") or
        eqlIgnoreCase(token, "HAVING") or
        eqlIgnoreCase(token, "ORDER") or
        eqlIgnoreCase(token, "LIMIT") or
        eqlIgnoreCase(token, "SIMILAR") or
        eqlIgnoreCase(token, "AND") or
        eqlIgnoreCase(token, "OR");
}

/// Parse a SQL command
pub fn parse(allocator: Allocator, sql: []const u8) !SqlCommand {
    var tokens = try tokenize(allocator, sql);
    defer tokens.deinit();

    if (tokens.items.len == 0) return SqlError.InvalidSyntax;

    const first = tokens.items[0].text;

    if (eqlIgnoreCase(first, "CREATE")) {
        // Distinguish between CREATE TABLE and CREATE INDEX
        if (tokens.items.len < 2) return SqlError.InvalidSyntax;
        if (eqlIgnoreCase(tokens.items[1].text, "TABLE")) {
            return SqlCommand{ .create_table = try parseCreateTable(allocator, tokens.items) };
        } else if (eqlIgnoreCase(tokens.items[1].text, "INDEX")) {
            return SqlCommand{ .create_index = try parseCreateIndex(allocator, tokens.items) };
        }
        return SqlError.InvalidSyntax;
    } else if (eqlIgnoreCase(first, "ALTER")) {
        // ALTER TABLE
        if (tokens.items.len < 2) return SqlError.InvalidSyntax;
        if (eqlIgnoreCase(tokens.items[1].text, "TABLE")) {
            return SqlCommand{ .alter_table = try parseAlterTable(allocator, tokens.items) };
        }
        return SqlError.InvalidSyntax;
    } else if (eqlIgnoreCase(first, "DROP")) {
        // DROP INDEX
        if (tokens.items.len < 2) return SqlError.InvalidSyntax;
        if (eqlIgnoreCase(tokens.items[1].text, "INDEX")) {
            return SqlCommand{ .drop_index = try parseDropIndex(allocator, tokens.items) };
        }
        return SqlError.InvalidSyntax;
    } else if (eqlIgnoreCase(first, "INSERT")) {
        return SqlCommand{ .insert = try parseInsert(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "SELECT")) {
        return SqlCommand{ .select = try parseSelect(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "DELETE")) {
        return SqlCommand{ .delete = try parseDelete(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "UPDATE")) {
        return SqlCommand{ .update = try parseUpdate(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "BEGIN")) {
        // Support both "BEGIN" and "BEGIN TRANSACTION"
        return SqlCommand{ .begin = {} };
    } else if (eqlIgnoreCase(first, "COMMIT")) {
        // Support both "COMMIT" and "COMMIT TRANSACTION"
        return SqlCommand{ .commit = {} };
    } else if (eqlIgnoreCase(first, "ROLLBACK")) {
        // Support both "ROLLBACK" and "ROLLBACK TRANSACTION"
        return SqlCommand{ .rollback = {} };
    } else if (eqlIgnoreCase(first, "VACUUM")) {
        return SqlCommand{ .vacuum = try parseVacuum(allocator, tokens.items) };
    }

    return SqlError.UnknownCommand;
}

fn parseCreateTable(allocator: Allocator, tokens: []const Token) !CreateTableCmd {
    // CREATE TABLE name (col1 type1, col2 type2, ...)
    if (tokens.len < 4) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "TABLE")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    var columns = ArrayList(ColumnDef).init(allocator);

    if (tokens.len < 5 or !std.mem.eql(u8, tokens[3].text, "(")) {
        return SqlError.InvalidSyntax;
    }

    var i: usize = 4;
    while (i < tokens.len) : (i += 1) {
        if (std.mem.eql(u8, tokens[i].text, ")")) break;
        if (std.mem.eql(u8, tokens[i].text, ",")) continue;

        if (i + 1 >= tokens.len) return SqlError.InvalidSyntax;

        const col_name = try allocator.dupe(u8, tokens[i].text);
        const col_type = try parseColumnType(tokens[i + 1].text);

        var embedding_dim: ?usize = null;
        var type_token_count: usize = 1; // How many tokens the type consumed

        // For embedding type, dimension is required: embedding(N)
        if (col_type == .embedding) {
            // Expect: embedding ( number )
            if (i + 4 >= tokens.len) return SqlError.InvalidSyntax;
            if (!std.mem.eql(u8, tokens[i + 2].text, "(")) {
                return SqlError.InvalidSyntax; // embedding must be followed by (N)
            }

            const dim_value = std.fmt.parseInt(usize, tokens[i + 3].text, 10) catch {
                return SqlError.InvalidSyntax;
            };

            if (!std.mem.eql(u8, tokens[i + 4].text, ")")) {
                return SqlError.InvalidSyntax;
            }

            embedding_dim = dim_value;
            type_token_count = 4; // embedding ( N )
        }

        try columns.append(.{
            .name = col_name,
            .col_type = col_type,
            .embedding_dim = embedding_dim,
        });
        i += type_token_count; // Skip type tokens
    }

    return CreateTableCmd{
        .table_name = table_name,
        .columns = columns,
    };
}

fn parseCreateIndex(allocator: Allocator, tokens: []const Token) !CreateIndexCmd {
    // CREATE INDEX idx_name ON table_name(column_name)
    if (tokens.len < 6) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "INDEX")) return SqlError.InvalidSyntax;

    const index_name = try allocator.dupe(u8, tokens[2].text);
    errdefer allocator.free(index_name);

    if (!eqlIgnoreCase(tokens[3].text, "ON")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[4].text);
    errdefer allocator.free(table_name);

    // Parse column name - may be with parentheses: (column_name)
    var column_name: []const u8 = undefined;
    if (std.mem.eql(u8, tokens[5].text, "(")) {
        // Format: ON table (column)
        if (tokens.len < 7) return SqlError.InvalidSyntax;
        column_name = try allocator.dupe(u8, tokens[6].text);
    } else {
        // Format: ON table column (no parentheses)
        column_name = try allocator.dupe(u8, tokens[5].text);
    }

    return CreateIndexCmd{
        .index_name = index_name,
        .table_name = table_name,
        .column_name = column_name,
    };
}

fn parseDropIndex(allocator: Allocator, tokens: []const Token) !DropIndexCmd {
    // DROP INDEX idx_name
    if (tokens.len < 3) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "INDEX")) return SqlError.InvalidSyntax;

    const index_name = try allocator.dupe(u8, tokens[2].text);

    return DropIndexCmd{
        .index_name = index_name,
    };
}

fn parseVacuum(allocator: Allocator, tokens: []const Token) !VacuumCmd {
    // VACUUM;              -> vacuum all tables
    // VACUUM table_name;   -> vacuum specific table

    if (tokens.len == 1) {
        // VACUUM (all tables)
        return VacuumCmd{
            .table_name = null,
        };
    } else if (tokens.len == 2) {
        // VACUUM table_name
        const table_name = try allocator.dupe(u8, tokens[1].text);
        return VacuumCmd{
            .table_name = table_name,
        };
    }

    return SqlError.InvalidSyntax;
}

fn parseAlterTable(allocator: Allocator, tokens: []const Token) !AlterTableCmd {
    // ALTER TABLE table_name ADD COLUMN col_name col_type
    // ALTER TABLE table_name DROP COLUMN col_name
    // ALTER TABLE table_name RENAME COLUMN old_name TO new_name
    if (tokens.len < 5) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "TABLE")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    errdefer allocator.free(table_name);

    // Parse operation type
    if (eqlIgnoreCase(tokens[3].text, "ADD")) {
        // ADD COLUMN col_name col_type [embedding(N)]
        if (tokens.len < 6) return SqlError.InvalidSyntax;
        if (!eqlIgnoreCase(tokens[4].text, "COLUMN")) return SqlError.InvalidSyntax;

        const col_name = try allocator.dupe(u8, tokens[5].text);
        errdefer allocator.free(col_name);

        if (tokens.len < 7) {
            allocator.free(col_name);
            return SqlError.InvalidSyntax;
        }

        const col_type = try parseColumnType(tokens[6].text);

        var embedding_dim: ?usize = null;

        // For embedding type, dimension is required: embedding(N)
        if (col_type == .embedding) {
            // Expect: embedding ( number )
            if (tokens.len < 10) {
                allocator.free(col_name);
                return SqlError.InvalidSyntax;
            }
            if (!std.mem.eql(u8, tokens[7].text, "(")) {
                allocator.free(col_name);
                return SqlError.InvalidSyntax;
            }

            const dim_value = std.fmt.parseInt(usize, tokens[8].text, 10) catch {
                allocator.free(col_name);
                return SqlError.InvalidSyntax;
            };

            if (!std.mem.eql(u8, tokens[9].text, ")")) {
                allocator.free(col_name);
                return SqlError.InvalidSyntax;
            }

            embedding_dim = dim_value;
        }

        return AlterTableCmd{
            .table_name = table_name,
            .operation = .{
                .add_column = .{
                    .column = .{
                        .name = col_name,
                        .col_type = col_type,
                        .embedding_dim = embedding_dim,
                    },
                },
            },
        };
    } else if (eqlIgnoreCase(tokens[3].text, "DROP")) {
        // DROP COLUMN col_name
        if (tokens.len < 6) return SqlError.InvalidSyntax;
        if (!eqlIgnoreCase(tokens[4].text, "COLUMN")) return SqlError.InvalidSyntax;

        const col_name = try allocator.dupe(u8, tokens[5].text);

        return AlterTableCmd{
            .table_name = table_name,
            .operation = .{
                .drop_column = .{
                    .column_name = col_name,
                },
            },
        };
    } else if (eqlIgnoreCase(tokens[3].text, "RENAME")) {
        // RENAME COLUMN old_name TO new_name
        if (tokens.len < 8) return SqlError.InvalidSyntax;
        if (!eqlIgnoreCase(tokens[4].text, "COLUMN")) return SqlError.InvalidSyntax;

        const old_name = try allocator.dupe(u8, tokens[5].text);
        errdefer allocator.free(old_name);

        if (!eqlIgnoreCase(tokens[6].text, "TO")) {
            allocator.free(old_name);
            return SqlError.InvalidSyntax;
        }

        const new_name = try allocator.dupe(u8, tokens[7].text);

        return AlterTableCmd{
            .table_name = table_name,
            .operation = .{
                .rename_column = .{
                    .old_name = old_name,
                    .new_name = new_name,
                },
            },
        };
    }

    return SqlError.InvalidSyntax;
}

fn parseInsert(allocator: Allocator, tokens: []const Token) !InsertCmd {
    // INSERT INTO table (col1, col2) VALUES (val1, val2)
    // or INSERT INTO table VALUES (val1, val2)
    if (tokens.len < 4) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "INTO")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    errdefer allocator.free(table_name);

    var columns = ArrayList([]const u8).init(allocator);
    errdefer {
        for (columns.items) |col| {
            allocator.free(col);
        }
        columns.deinit();
    }

    var values = ArrayList(ColumnValue).init(allocator);
    errdefer {
        for (values.items) |*val| {
            val.deinit(allocator);
        }
        values.deinit();
    }

    var i: usize = 3;

    // Parse columns if specified
    if (std.mem.eql(u8, tokens[i].text, "(")) {
        i += 1;
        while (i < tokens.len and !std.mem.eql(u8, tokens[i].text, ")")) {
            if (!std.mem.eql(u8, tokens[i].text, ",")) {
                try columns.append(try allocator.dupe(u8, tokens[i].text));
            }
            i += 1;
        }
        i += 1; // Skip )
    }

    // Find VALUES keyword
    while (i < tokens.len and !eqlIgnoreCase(tokens[i].text, "VALUES")) : (i += 1) {}
    if (i >= tokens.len) return SqlError.InvalidSyntax;
    i += 1;

    // Parse values
    if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, "(")) return SqlError.InvalidSyntax;
    i += 1;

    while (i < tokens.len and !std.mem.eql(u8, tokens[i].text, ")")) {
        if (std.mem.eql(u8, tokens[i].text, ",")) {
            i += 1;
            continue;
        }

        // Check for array literal first: [0.1, 0.2, 0.3]
        if (std.mem.eql(u8, tokens[i].text, "[")) {
            const embedding_value = try parseArrayValue(allocator, tokens, &i);
            try values.append(embedding_value);
            continue; // i already advanced by parseArrayValue
        }

        const token_text = tokens[i].text;

        // Parse other value types
        if (token_text[0] == '"' or token_text[0] == '\'') {
            const str = parseString(token_text);
            const owned = try allocator.dupe(u8, str);
            try values.append(ColumnValue{ .text = owned });
        } else if (std.mem.indexOf(u8, token_text, ".")) |_| {
            const f = try std.fmt.parseFloat(f64, token_text);
            try values.append(ColumnValue{ .float = f });
        } else if (eqlIgnoreCase(token_text, "true")) {
            try values.append(ColumnValue{ .bool = true });
        } else if (eqlIgnoreCase(token_text, "false")) {
            try values.append(ColumnValue{ .bool = false });
        } else if (eqlIgnoreCase(token_text, "NULL")) {
            try values.append(ColumnValue.null_value);
        } else {
            const num = try std.fmt.parseInt(i64, token_text, 10);
            try values.append(ColumnValue{ .int = num });
        }

        i += 1;
    }

    return InsertCmd{
        .table_name = table_name,
        .columns = columns,
        .values = values,
    };
}

fn parseSelect(allocator: Allocator, tokens: []const Token) !SelectCmd {
    // SELECT * FROM table
    // SELECT col1, col2 FROM table WHERE col = val
    // SELECT COUNT(*), SUM(amount) FROM table
    // SELECT * FROM table WHERE col SIMILAR TO "text"
    // SELECT * FROM table ORDER BY SIMILARITY TO "text" LIMIT 5
    // SELECT * FROM table ORDER BY VIBES
    if (tokens.len < 4) return SqlError.InvalidSyntax;

    var columns = ArrayList(SelectColumn).init(allocator);
    errdefer {
        for (columns.items) |*col| {
            col.deinit(allocator);
        }
        columns.deinit();
    }
    var i: usize = 1;

    // Parse columns
    while (i < tokens.len and !eqlIgnoreCase(tokens[i].text, "FROM")) {
        if (!std.mem.eql(u8, tokens[i].text, ",")) {
            if (std.mem.eql(u8, tokens[i].text, "*")) {
                // SELECT * means all columns
                try columns.append(.star);
            } else if (AggregateFunc.fromString(tokens[i].text)) |func| {
                // Parse aggregate function: COUNT(*) or COUNT(column)
                i += 1;
                if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, "(")) {
                    return SqlError.InvalidSyntax;
                }
                i += 1;

                var agg_column: ?[]const u8 = null;
                if (i < tokens.len and !std.mem.eql(u8, tokens[i].text, "*") and !std.mem.eql(u8, tokens[i].text, ")")) {
                    // Named column: COUNT(age)
                    agg_column = try allocator.dupe(u8, tokens[i].text);
                    i += 1;
                } else if (i < tokens.len and std.mem.eql(u8, tokens[i].text, "*")) {
                    // COUNT(*) - column remains null
                    i += 1;
                }

                if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, ")")) {
                    if (agg_column) |col| allocator.free(col);
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
        i += 1;
    }

    if (i >= tokens.len) return SqlError.InvalidSyntax;
    i += 1; // Skip FROM

    const table_name = try allocator.dupe(u8, tokens[i].text);
    errdefer allocator.free(table_name);
    i += 1;

    // Skip table alias if present (e.g., "FROM users u" - skip the "u")
    // An alias is any token that's not a SQL keyword
    if (i < tokens.len and !isSelectKeyword(tokens[i].text)) {
        i += 1; // Skip alias
    }

    var joins = ArrayList(JoinClause).init(allocator);
    errdefer {
        for (joins.items) |*join| {
            join.deinit(allocator);
        }
        joins.deinit();
    }
    const where_column: ?[]const u8 = null;
    const where_value: ?ColumnValue = null;
    var where_expr: ?Expr = null;
    errdefer if (where_expr) |*expr| {
        var e = expr.*;
        e.deinit(allocator);
    };
    var similar_to_column: ?[]const u8 = null;
    errdefer if (similar_to_column) |col| allocator.free(col);
    var similar_to_text: ?[]const u8 = null;
    errdefer if (similar_to_text) |text| allocator.free(text);
    var order_by_similarity: ?[]const u8 = null;
    errdefer if (order_by_similarity) |text| allocator.free(text);
    var order_by_vibes = false;
    var order_by: ?OrderByClause = null;
    errdefer if (order_by) |*ob| {
        ob.deinit(allocator);
    };
    var group_by = ArrayList([]const u8).init(allocator);
    errdefer {
        for (group_by.items) |col| {
            allocator.free(col);
        }
        group_by.deinit();
    }
    var having_expr: ?Expr = null;
    errdefer if (having_expr) |*expr| {
        var e = expr.*;
        e.deinit(allocator);
    };
    var limit: ?usize = null;

    // Parse JOINs (before WHERE, GROUP BY, ORDER BY, LIMIT)
    while (i < tokens.len) {
        // Check for JOIN keywords
        var join_type: ?JoinType = null;

        if (eqlIgnoreCase(tokens[i].text, "INNER")) {
            join_type = .inner;
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "JOIN")) {
                return SqlError.InvalidSyntax;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "LEFT")) {
            join_type = .left;
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "JOIN")) {
                return SqlError.InvalidSyntax;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "RIGHT")) {
            join_type = .right;
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "JOIN")) {
                return SqlError.InvalidSyntax;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "JOIN")) {
            // Default to INNER JOIN
            join_type = .inner;
        } else {
            // No more JOINs, break and continue to WHERE/GROUP BY/etc
            break;
        }

        i += 1; // Skip JOIN keyword

        // Parse: table_name ON left_col = right_col
        if (i >= tokens.len) return SqlError.InvalidSyntax;
        const join_table = try allocator.dupe(u8, tokens[i].text);
        i += 1;

        // Skip table alias if present (e.g., "JOIN orders o" - skip the "o")
        if (i < tokens.len and !eqlIgnoreCase(tokens[i].text, "ON")) {
            i += 1; // Skip alias
        }

        if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "ON")) {
            allocator.free(join_table);
            return SqlError.InvalidSyntax;
        }
        i += 1;

        // Parse: left_column = right_column
        if (i >= tokens.len) {
            allocator.free(join_table);
            return SqlError.InvalidSyntax;
        }
        const left_col = try allocator.dupe(u8, tokens[i].text);
        i += 1;

        if (i >= tokens.len or !std.mem.eql(u8, tokens[i].text, "=")) {
            allocator.free(join_table);
            allocator.free(left_col);
            return SqlError.InvalidSyntax;
        }
        i += 1;

        if (i >= tokens.len) {
            allocator.free(join_table);
            allocator.free(left_col);
            return SqlError.InvalidSyntax;
        }
        const right_col = try allocator.dupe(u8, tokens[i].text);
        i += 1;

        try joins.append(JoinClause{
            .join_type = join_type.?,
            .table_name = join_table,
            .left_column = left_col,
            .right_column = right_col,
        });
    }

    // Parse WHERE, GROUP BY, ORDER BY, LIMIT
    while (i < tokens.len) {
        if (eqlIgnoreCase(tokens[i].text, "WHERE")) {
            i += 1;
            if (i >= tokens.len) return SqlError.InvalidSyntax;

            // Check for SIMILAR TO special case
            const where_start = i;
            const first_token = tokens[i].text;
            i += 1;

            if (i + 1 < tokens.len and eqlIgnoreCase(tokens[i].text, "SIMILAR") and eqlIgnoreCase(tokens[i + 1].text, "TO")) {
                similar_to_column = try allocator.dupe(u8, first_token);
                i += 2;
                if (i >= tokens.len) return SqlError.InvalidSyntax;
                const text = parseString(tokens[i].text);
                similar_to_text = try allocator.dupe(u8, text);
                i += 1;
            } else {
                // Parse as expression - find end of WHERE clause
                // Need to track parentheses depth to avoid breaking on keywords inside subqueries
                var where_end = i - 1; // Start from the first token after WHERE
                var paren_depth: usize = 0;
                while (where_end < tokens.len) {
                    if (std.mem.eql(u8, tokens[where_end].text, "(")) {
                        paren_depth += 1;
                    } else if (std.mem.eql(u8, tokens[where_end].text, ")")) {
                        paren_depth -= 1;
                    } else if (paren_depth == 0) {
                        // Only check for clause-ending keywords when not inside parentheses
                        if (eqlIgnoreCase(tokens[where_end].text, "GROUP") or
                            eqlIgnoreCase(tokens[where_end].text, "ORDER") or
                            eqlIgnoreCase(tokens[where_end].text, "LIMIT"))
                        {
                            break;
                        }
                    }
                    where_end += 1;
                }

                // Parse the WHERE expression
                var expr_idx = where_start;
                where_expr = try parseExpr(allocator, tokens[0..where_end], &expr_idx);
                i = where_end;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "GROUP")) {
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "BY")) {
                return SqlError.InvalidSyntax;
            }
            i += 1;

            // Parse comma-separated list of columns
            while (i < tokens.len) {
                if (eqlIgnoreCase(tokens[i].text, "HAVING") or
                    eqlIgnoreCase(tokens[i].text, "ORDER") or
                    eqlIgnoreCase(tokens[i].text, "LIMIT"))
                {
                    break;
                }

                if (!std.mem.eql(u8, tokens[i].text, ",")) {
                    try group_by.append(try allocator.dupe(u8, tokens[i].text));
                }
                i += 1;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "HAVING")) {
            const having_result = try parseHaving(allocator, tokens, i);
            having_expr = having_result.expr;
            i = having_result.next_idx;
        } else if (eqlIgnoreCase(tokens[i].text, "ORDER")) {
            const order_start = i; // Save starting position
            i += 1;
            if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "BY")) return SqlError.InvalidSyntax;
            i += 1;
            if (i >= tokens.len) return SqlError.InvalidSyntax;

            // Check for VIBES (fun parody!)
            if (eqlIgnoreCase(tokens[i].text, "VIBES")) {
                order_by_vibes = true;
                i += 1;
            }
            // Check for SIMILARITY TO "text"
            else if (eqlIgnoreCase(tokens[i].text, "SIMILARITY")) {
                i += 1;
                if (i >= tokens.len or !eqlIgnoreCase(tokens[i].text, "TO")) return SqlError.InvalidSyntax;
                i += 1;
                if (i >= tokens.len) return SqlError.InvalidSyntax;
                const text = parseString(tokens[i].text);
                order_by_similarity = try allocator.dupe(u8, text);
                i += 1;
            } else {
                // Generic ORDER BY (column name with optional ASC/DESC)
                const result = try parseOrderBy(allocator, tokens, order_start);
                order_by = result.clause;
                i = result.next_idx;
            }
        } else if (eqlIgnoreCase(tokens[i].text, "LIMIT")) {
            i += 1;
            if (i >= tokens.len) return SqlError.InvalidSyntax;
            limit = try std.fmt.parseInt(usize, tokens[i].text, 10);
            i += 1;
        } else {
            i += 1;
        }
    }

    // Build the SelectCmd
    // Validate HAVING only used with GROUP BY (before creating cmd)
    if (having_expr != null and group_by.items.len == 0) {
        // Return error and let errdefers handle cleanup
        return error.HavingWithoutGroupBy;
    }

    const cmd = SelectCmd{
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
        .having_expr = having_expr,
        .limit = limit,
    };

    return cmd;
}

fn parseDelete(allocator: Allocator, tokens: []const Token) !DeleteCmd {
    // DELETE FROM table [WHERE expr]
    if (tokens.len < 3) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "FROM")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    errdefer allocator.free(table_name);

    var i: usize = 3;

    // Parse optional WHERE clause (using full expression parser like UPDATE)
    var where_expr: ?Expr = null;
    if (i < tokens.len and eqlIgnoreCase(tokens[i].text, "WHERE")) {
        i += 1;
        where_expr = try parseExpr(allocator, tokens, &i);
    }

    return DeleteCmd{
        .table_name = table_name,
        .where_expr = where_expr,
    };
}

/// Helper function to parse a value token into a ColumnValue
fn parseValue(allocator: Allocator, token_text: []const u8) !ColumnValue {
    if (token_text.len == 0) return SqlError.InvalidSyntax;

    if (token_text[0] == '"' or token_text[0] == '\'') {
        const str = parseString(token_text);
        const owned = try allocator.dupe(u8, str);
        return ColumnValue{ .text = owned };
    } else if (std.mem.indexOf(u8, token_text, ".")) |_| {
        const f = try std.fmt.parseFloat(f64, token_text);
        return ColumnValue{ .float = f };
    } else if (eqlIgnoreCase(token_text, "true")) {
        return ColumnValue{ .bool = true };
    } else if (eqlIgnoreCase(token_text, "false")) {
        return ColumnValue{ .bool = false };
    } else if (eqlIgnoreCase(token_text, "NULL")) {
        return ColumnValue.null_value;
    } else {
        const num = try std.fmt.parseInt(i64, token_text, 10);
        return ColumnValue{ .int = num };
    }
}

/// Parse array literal for embeddings: [0.1, 0.2, 0.3]
/// Returns ColumnValue.embedding with owned f32 slice
/// Advances idx past the closing ]
fn parseArrayValue(allocator: Allocator, tokens: []const Token, idx: *usize) !ColumnValue {
    // Expect opening [
    if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, "[")) {
        return SqlError.InvalidSyntax;
    }
    idx.* += 1;

    var values = ArrayList(f32).init(allocator);
    errdefer values.deinit();

    // Parse comma-separated float values
    while (idx.* < tokens.len) {
        const token = tokens[idx.*];

        // Check for closing ]
        if (std.mem.eql(u8, token.text, "]")) {
            idx.* += 1;
            break;
        }

        // Skip commas
        if (std.mem.eql(u8, token.text, ",")) {
            idx.* += 1;
            continue;
        }

        // Parse float value (try float first, then int)
        const value = std.fmt.parseFloat(f64, token.text) catch |err| blk: {
            // If float parsing fails, try int and convert to float
            const int_val = std.fmt.parseInt(i64, token.text, 10) catch {
                return err; // Return original float parse error
            };
            break :blk @as(f64, @floatFromInt(int_val));
        };
        try values.append(@floatCast(value));
        idx.* += 1;
    }

    // Validate non-empty array
    if (values.items.len == 0) {
        values.deinit();
        return SqlError.InvalidSyntax;
    }

    // Convert to owned slice
    const owned_slice = try values.toOwnedSlice();
    return ColumnValue{ .embedding = owned_slice };
}

/// Parse an expression (recursive descent parser)
fn parseExpr(allocator: Allocator, tokens: []const Token, start_idx: *usize) (Allocator.Error || SqlError)!Expr {
    return try parseOrExpr(allocator, tokens, start_idx);
}

/// Parse OR expressions (lowest precedence)
fn parseOrExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    var left = try parseAndExpr(allocator, tokens, idx);
    errdefer left.deinit(allocator);

    while (idx.* < tokens.len and eqlIgnoreCase(tokens[idx.*].text, "OR")) {
        idx.* += 1;
        var right = try parseAndExpr(allocator, tokens, idx);
        errdefer right.deinit(allocator);

        const binary = try allocator.create(BinaryExpr);
        binary.* = BinaryExpr{
            .op = .or_op,
            .left = left,
            .right = right,
        };
        left = Expr{ .binary = binary };
    }

    return left;
}

/// Parse AND expressions
fn parseAndExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    var left = try parseComparisonExpr(allocator, tokens, idx);
    errdefer left.deinit(allocator);

    while (idx.* < tokens.len and eqlIgnoreCase(tokens[idx.*].text, "AND")) {
        idx.* += 1;
        var right = try parseComparisonExpr(allocator, tokens, idx);
        errdefer right.deinit(allocator);

        const binary = try allocator.create(BinaryExpr);
        binary.* = BinaryExpr{
            .op = .and_op,
            .left = left,
            .right = right,
        };
        left = Expr{ .binary = binary };
    }

    return left;
}

/// Parse comparison expressions (=, !=, <, >, <=, >=, IN, NOT IN)
fn parseComparisonExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    var left = try parseUnaryExpr(allocator, tokens, idx);
    errdefer left.deinit(allocator);

    if (idx.* < tokens.len) {
        const op_text = tokens[idx.*].text;

        // Check for IN operator
        if (eqlIgnoreCase(op_text, "IN")) {
            idx.* += 1;
            var right = try parseUnaryExpr(allocator, tokens, idx);
            errdefer right.deinit(allocator);

            const binary = try allocator.create(BinaryExpr);
            binary.* = BinaryExpr{
                .op = .in_op,
                .left = left,
                .right = right,
            };
            return Expr{ .binary = binary };
        }

        // Check for NOT IN operator
        if (eqlIgnoreCase(op_text, "NOT")) {
            if (idx.* + 1 < tokens.len and eqlIgnoreCase(tokens[idx.* + 1].text, "IN")) {
                idx.* += 2; // Skip NOT IN
                var right = try parseUnaryExpr(allocator, tokens, idx);
                errdefer right.deinit(allocator);

                const binary = try allocator.create(BinaryExpr);
                binary.* = BinaryExpr{
                    .op = .not_in_op,
                    .left = left,
                    .right = right,
                };
                return Expr{ .binary = binary };
            }
        }

        // Regular comparison operators
        const op: ?BinaryOp = if (std.mem.eql(u8, op_text, "="))
            .eq
        else if (std.mem.eql(u8, op_text, "!="))
            .neq
        else if (std.mem.eql(u8, op_text, "<"))
            .lt
        else if (std.mem.eql(u8, op_text, ">"))
            .gt
        else if (std.mem.eql(u8, op_text, "<="))
            .lte
        else if (std.mem.eql(u8, op_text, ">="))
            .gte
        else
            null;

        if (op) |o| {
            idx.* += 1;
            var right = try parseUnaryExpr(allocator, tokens, idx);
            errdefer right.deinit(allocator);

            const binary = try allocator.create(BinaryExpr);
            binary.* = BinaryExpr{
                .op = o,
                .left = left,
                .right = right,
            };
            return Expr{ .binary = binary };
        }
    }

    return left;
}

/// Parse unary expressions (NOT, IS NULL, IS NOT NULL, EXISTS, NOT EXISTS)
fn parseUnaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    // EXISTS expression
    if (eqlIgnoreCase(tokens[idx.*].text, "EXISTS")) {
        idx.* += 1;

        // EXISTS must be followed by a subquery
        if (!isSubqueryStart(tokens, idx.*)) {
            return SqlError.InvalidSyntax;
        }

        const subquery = try parseSubquery(allocator, tokens, idx);
        errdefer {
            subquery.deinit(allocator);
            allocator.destroy(subquery);
        }

        // Represent EXISTS as a binary expression with a literal true on the left
        const binary = try allocator.create(BinaryExpr);
        binary.* = BinaryExpr{
            .op = .exists_op,
            .left = Expr{ .literal = ColumnValue{ .bool = true } },
            .right = Expr{ .subquery = subquery },
        };
        return Expr{ .binary = binary };
    }

    // NOT expression (could be NOT EXISTS or regular NOT)
    if (eqlIgnoreCase(tokens[idx.*].text, "NOT")) {
        // Look ahead for EXISTS
        if (idx.* + 1 < tokens.len and eqlIgnoreCase(tokens[idx.* + 1].text, "EXISTS")) {
            idx.* += 2; // Skip NOT EXISTS

            // NOT EXISTS must be followed by a subquery
            if (!isSubqueryStart(tokens, idx.*)) {
                return SqlError.InvalidSyntax;
            }

            const subquery = try parseSubquery(allocator, tokens, idx);
            errdefer {
                subquery.deinit(allocator);
                allocator.destroy(subquery);
            }

            // Represent NOT EXISTS as a binary expression
            const binary = try allocator.create(BinaryExpr);
            binary.* = BinaryExpr{
                .op = .not_exists_op,
                .left = Expr{ .literal = ColumnValue{ .bool = true } },
                .right = Expr{ .subquery = subquery },
            };
            return Expr{ .binary = binary };
        }

        // Regular NOT expression
        idx.* += 1;
        var expr = try parseUnaryExpr(allocator, tokens, idx);
        errdefer expr.deinit(allocator);

        const unary = try allocator.create(UnaryExpr);
        unary.* = UnaryExpr{
            .op = .not,
            .expr = expr,
        };
        return Expr{ .unary = unary };
    }

    var expr = try parsePrimaryExpr(allocator, tokens, idx);
    errdefer expr.deinit(allocator);

    // IS NULL / IS NOT NULL
    if (idx.* < tokens.len and eqlIgnoreCase(tokens[idx.*].text, "IS")) {
        idx.* += 1;
        if (idx.* >= tokens.len) return SqlError.InvalidExpression;

        const is_not = eqlIgnoreCase(tokens[idx.*].text, "NOT");
        if (is_not) {
            idx.* += 1;
            if (idx.* >= tokens.len) return SqlError.InvalidExpression;
        }

        if (!eqlIgnoreCase(tokens[idx.*].text, "NULL")) return SqlError.InvalidExpression;
        idx.* += 1;

        const unary = try allocator.create(UnaryExpr);
        unary.* = UnaryExpr{
            .op = if (is_not) .is_not_null else .is_null,
            .expr = expr,
        };
        return Expr{ .unary = unary };
    }

    return expr;
}

/// Check if token sequence starts a subquery
fn isSubqueryStart(tokens: []const Token, idx: usize) bool {
    // Subquery starts with ( SELECT
    if (idx >= tokens.len) return false;
    if (!std.mem.eql(u8, tokens[idx].text, "(")) return false;
    if (idx + 1 >= tokens.len) return false;
    return eqlIgnoreCase(tokens[idx + 1].text, "SELECT");
}

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
    const subquery_tokens = tokens[idx.* .. end_idx - 1];
    const subquery = try allocator.create(SelectCmd);
    errdefer allocator.destroy(subquery);

    subquery.* = try parseSelect(allocator, subquery_tokens);

    idx.* = end_idx; // Move past closing )
    return subquery;
}

/// Parse primary expressions (literals, columns, parentheses)
fn parsePrimaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    const token_text = tokens[idx.*].text;

    // Check for subquery: ( SELECT ...
    if (isSubqueryStart(tokens, idx.*)) {
        const subquery = try parseSubquery(allocator, tokens, idx);
        return Expr{ .subquery = subquery };
    }

    // Regular parenthesized expression
    if (std.mem.eql(u8, token_text, "(")) {
        idx.* += 1;
        var expr = try parseExpr(allocator, tokens, idx);
        errdefer expr.deinit(allocator);
        if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, ")")) {
            return SqlError.InvalidExpression;
        }
        idx.* += 1;
        return expr;
    }

    // Try to parse as a value literal
    if (token_text[0] == '"' or token_text[0] == '\'' or
        std.ascii.isDigit(token_text[0]) or token_text[0] == '-' or
        eqlIgnoreCase(token_text, "true") or eqlIgnoreCase(token_text, "false") or
        eqlIgnoreCase(token_text, "NULL"))
    {
        const value = try parseValue(allocator, token_text);
        idx.* += 1;
        return Expr{ .literal = value };
    }

    // Check for aggregate function (COUNT, SUM, AVG, MIN, MAX)
    if (AggregateFunc.fromString(token_text)) |func| {
        idx.* += 1;

        // Expect opening parenthesis
        if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, "(")) {
            return SqlError.InvalidSyntax;
        }
        idx.* += 1;

        var agg_column: ?[]const u8 = null;
        if (idx.* < tokens.len and !std.mem.eql(u8, tokens[idx.*].text, "*") and !std.mem.eql(u8, tokens[idx.*].text, ")")) {
            // Named column: COUNT(age), SUM(amount)
            agg_column = try allocator.dupe(u8, tokens[idx.*].text);
            idx.* += 1;
        } else if (idx.* < tokens.len and std.mem.eql(u8, tokens[idx.*].text, "*")) {
            // COUNT(*) - no column
            idx.* += 1;
        }

        // Expect closing parenthesis
        if (idx.* >= tokens.len or !std.mem.eql(u8, tokens[idx.*].text, ")")) {
            if (agg_column) |col| allocator.free(col);
            return SqlError.InvalidSyntax;
        }
        idx.* += 1;

        return Expr{ .aggregate = .{
            .func = func,
            .column = agg_column,
        } };
    }

    // Otherwise, it's a column reference
    const col_name = try allocator.dupe(u8, token_text);
    idx.* += 1;
    return Expr{ .column = col_name };
}

/// Evaluate an expression against a row's values
/// db parameter is optional and required for subquery execution
pub fn evaluateExpr(expr: Expr, row_values: anytype, db: ?*anyopaque) bool {
    switch (expr) {
        .literal => |val| {
            // A standalone literal is truthy if not null/false
            return switch (val) {
                .null_value => false,
                .bool => |b| b,
                .int => |i| i != 0,
                .float => |f| f != 0.0,
                .text => |t| t.len > 0,
                .embedding => true,
            };
        },
        .column => {
            // A column reference is truthy if it exists and is truthy
            var it = row_values.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, expr.column)) {
                    return switch (entry.value_ptr.*) {
                        .null_value => false,
                        .bool => |b| b,
                        .int => |i| i != 0,
                        .float => |f| f != 0.0,
                        .text => |t| t.len > 0,
                        .embedding => true,
                    };
                }
            }
            return false; // Column not found
        },
        .aggregate => {
            // Get the aggregate value and check if it's truthy
            const val = getExprValue(expr, row_values, db);
            return switch (val) {
                .null_value => false,
                .bool => |b| b,
                .int => |i| i != 0,
                .float => |f| f != 0.0,
                .text => |t| t.len > 0,
                .embedding => true,
            };
        },
        .binary => |bin| {
            return evaluateBinaryExpr(bin.*, row_values, db);
        },
        .unary => |un| {
            return evaluateUnaryExpr(un.*, row_values, db);
        },
        .subquery => {
            // Subqueries should not be evaluated standalone
            // They must be part of a binary expression (IN, EXISTS, etc.)
            // This will be properly handled in Phase 2
            return false;
        },
    }
}

fn evaluateBinaryExpr(expr: BinaryExpr, row_values: anytype, db: ?*anyopaque) bool {
    switch (expr.op) {
        .and_op => {
            return evaluateExpr(expr.left, row_values, db) and evaluateExpr(expr.right, row_values, db);
        },
        .or_op => {
            return evaluateExpr(expr.left, row_values, db) or evaluateExpr(expr.right, row_values, db);
        },
        .eq, .neq, .lt, .gt, .lte, .gte => {
            const left_val = getExprValue(expr.left, row_values, db);
            const right_val = getExprValue(expr.right, row_values, db);
            return compareValues(left_val, right_val, expr.op);
        },
        .in_op, .not_in_op, .exists_op, .not_exists_op => {
            // Subquery operators require database context
            // Will be fully implemented in Phase 2.2 and 2.3
            // For now, return false (fail closed)
            return false;
        },
    }
}

fn evaluateUnaryExpr(expr: UnaryExpr, row_values: anytype, db: ?*anyopaque) bool {
    switch (expr.op) {
        .not => {
            return !evaluateExpr(expr.expr, row_values, db);
        },
        .is_null => {
            const val = getExprValue(expr.expr, row_values, db);
            return val == .null_value;
        },
        .is_not_null => {
            const val = getExprValue(expr.expr, row_values, db);
            return val != .null_value;
        },
    }
}

fn getExprValue(expr: Expr, row_values: anytype, db: ?*anyopaque) ColumnValue {
    switch (expr) {
        .literal => |val| return val,
        .column => |col| {
            var it = row_values.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, col)) {
                    return entry.value_ptr.*;
                }
            }
            return ColumnValue.null_value;
        },
        .aggregate => |agg| {
            // Build the aggregate column name to match what's stored in grouped results
            // We need a temporary allocator for this string - use a stack buffer
            var buf: [256]u8 = undefined;
            const col_name = switch (agg.func) {
                .count => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "COUNT({s})", .{col}) catch "COUNT(*)"
                else
                    "COUNT(*)",
                .sum => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "SUM({s})", .{col}) catch ""
                else
                    "",
                .avg => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "AVG({s})", .{col}) catch ""
                else
                    "",
                .min => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "MIN({s})", .{col}) catch ""
                else
                    "",
                .max => if (agg.column) |col|
                    std.fmt.bufPrint(&buf, "MAX({s})", .{col}) catch ""
                else
                    "",
            };

            // Look up the aggregate column in row_values
            var it = row_values.iterator();
            while (it.next()) |entry| {
                if (std.mem.eql(u8, entry.key_ptr.*, col_name)) {
                    return entry.value_ptr.*;
                }
            }
            return ColumnValue.null_value;
        },
        .binary, .unary, .subquery => {
            // For complex expressions in comparison context, treat as bool
            // Subquery evaluation will be fully implemented in Phase 2.2 and 2.3
            const result = evaluateExpr(expr, row_values, db);
            return ColumnValue{ .bool = result };
        },
    }
}

fn compareValues(left: ColumnValue, right: ColumnValue, op: BinaryOp) bool {
    // Handle NULL comparisons
    if (left == .null_value or right == .null_value) {
        return switch (op) {
            .eq => left == .null_value and right == .null_value,
            .neq => !(left == .null_value and right == .null_value),
            else => false,
        };
    }

    // Type-specific comparisons
    switch (left) {
        .int => |l| {
            const r = switch (right) {
                .int => |i| i,
                .float => |f| @as(i64, @intFromFloat(f)),
                else => return false,
            };
            return switch (op) {
                .eq => l == r,
                .neq => l != r,
                .lt => l < r,
                .gt => l > r,
                .lte => l <= r,
                .gte => l >= r,
                else => false,
            };
        },
        .float => |l| {
            const r = switch (right) {
                .float => |f| f,
                .int => |i| @as(f64, @floatFromInt(i)),
                else => return false,
            };
            return switch (op) {
                .eq => l == r,
                .neq => l != r,
                .lt => l < r,
                .gt => l > r,
                .lte => l <= r,
                .gte => l >= r,
                else => false,
            };
        },
        .text => |l| {
            if (right != .text) return false;
            const cmp = std.mem.order(u8, l, right.text);
            return switch (op) {
                .eq => cmp == .eq,
                .neq => cmp != .eq,
                .lt => cmp == .lt,
                .gt => cmp == .gt,
                .lte => cmp == .lt or cmp == .eq,
                .gte => cmp == .gt or cmp == .eq,
                else => false,
            };
        },
        .bool => |l| {
            if (right != .bool) return false;
            return switch (op) {
                .eq => l == right.bool,
                .neq => l != right.bool,
                else => false,
            };
        },
        .embedding, .null_value => return false,
    }
}

/// Parse UPDATE statement
fn parseUpdate(allocator: Allocator, tokens: []const Token) !UpdateCmd {
    // UPDATE table SET col1 = val1, col2 = val2 WHERE expr
    if (tokens.len < 6) return SqlError.InvalidSyntax; // UPDATE table SET col = val

    const table_name = try allocator.dupe(u8, tokens[1].text);
    errdefer allocator.free(table_name);

    var i: usize = 2;
    if (!eqlIgnoreCase(tokens[i].text, "SET")) {
        allocator.free(table_name);
        return SqlError.InvalidSyntax;
    }
    i += 1;

    // Parse SET assignments
    var assignments = ArrayList(Assignment).init(allocator);
    errdefer {
        for (assignments.items) |*assign| {
            assign.deinit(allocator);
        }
        assignments.deinit();
    }

    while (i < tokens.len) {
        if (eqlIgnoreCase(tokens[i].text, "WHERE")) break;

        // Skip commas
        if (std.mem.eql(u8, tokens[i].text, ",")) {
            i += 1;
            continue;
        }

        // Parse: column = value
        if (i + 2 >= tokens.len) {
            allocator.free(table_name);
            return SqlError.InvalidSyntax;
        }

        const col_name = try allocator.dupe(u8, tokens[i].text);
        errdefer allocator.free(col_name);
        i += 1;

        if (!std.mem.eql(u8, tokens[i].text, "=")) {
            allocator.free(col_name);
            allocator.free(table_name);
            return SqlError.InvalidSyntax;
        }
        i += 1;

        const value = try parseValue(allocator, tokens[i].text);
        errdefer {
            var v = value;
            v.deinit(allocator);
        }
        i += 1;

        try assignments.append(Assignment{
            .column = col_name,
            .value = value,
        });
    }

    if (assignments.items.len == 0) {
        allocator.free(table_name);
        return SqlError.InvalidSyntax;
    }

    // Parse optional WHERE clause
    var where_expr: ?Expr = null;
    if (i < tokens.len and eqlIgnoreCase(tokens[i].text, "WHERE")) {
        i += 1;
        where_expr = try parseExpr(allocator, tokens, &i);
    }

    return UpdateCmd{
        .table_name = table_name,
        .assignments = assignments,
        .where_expr = where_expr,
    };
}

/// Parse ORDER BY clause
fn parseOrderBy(allocator: Allocator, tokens: []const Token, start_idx: usize) !struct { clause: OrderByClause, next_idx: usize } {
    var idx = start_idx;

    // Expect "ORDER"
    if (idx >= tokens.len or !eqlIgnoreCase(tokens[idx].text, "ORDER")) {
        return error.InvalidSyntax;
    }
    idx += 1;

    // Expect "BY"
    if (idx >= tokens.len or !eqlIgnoreCase(tokens[idx].text, "BY")) {
        return error.InvalidSyntax;
    }
    idx += 1;

    var items = ArrayList(OrderByItem).init(allocator);
    errdefer {
        for (items.items) |*item| {
            item.deinit(allocator);
        }
        items.deinit();
    }

    while (idx < tokens.len) {
        // Parse column name (could be aggregate like "COUNT(*)")
        if (idx >= tokens.len) break;

        var col_name: []const u8 = undefined;
        var needs_free = false;

        // Check if this is an aggregate function
        if (AggregateFunc.fromString(tokens[idx].text)) |func| {
            // Parse aggregate function: COUNT(*), SUM(column), etc.
            idx += 1;

            if (idx >= tokens.len or !std.mem.eql(u8, tokens[idx].text, "(")) {
                return SqlError.InvalidSyntax;
            }
            idx += 1;

            var agg_column: ?[]const u8 = null;
            if (idx < tokens.len and !std.mem.eql(u8, tokens[idx].text, "*") and !std.mem.eql(u8, tokens[idx].text, ")")) {
                // Named column: COUNT(age), SUM(amount)
                agg_column = tokens[idx].text;
                idx += 1;
            } else if (idx < tokens.len and std.mem.eql(u8, tokens[idx].text, "*")) {
                // COUNT(*) - no column
                idx += 1;
            }

            if (idx >= tokens.len or !std.mem.eql(u8, tokens[idx].text, ")")) {
                return SqlError.InvalidSyntax;
            }
            idx += 1;

            // Build the aggregate column name to match what's in the result
            col_name = switch (func) {
                .count => if (agg_column) |col|
                    try std.fmt.allocPrint(allocator, "COUNT({s})", .{col})
                else
                    try allocator.dupe(u8, "COUNT(*)"),
                .sum => try std.fmt.allocPrint(allocator, "SUM({s})", .{agg_column.?}),
                .avg => try std.fmt.allocPrint(allocator, "AVG({s})", .{agg_column.?}),
                .min => try std.fmt.allocPrint(allocator, "MIN({s})", .{agg_column.?}),
                .max => try std.fmt.allocPrint(allocator, "MAX({s})", .{agg_column.?}),
            };
            needs_free = true;
        } else {
            // Regular column name
            col_name = tokens[idx].text;
            idx += 1;
        }

        // Check for direction (ASC/DESC)
        var direction = OrderDirection.asc; // Default to ASC
        if (idx < tokens.len) {
            if (eqlIgnoreCase(tokens[idx].text, "ASC")) {
                direction = .asc;
                idx += 1;
            } else if (eqlIgnoreCase(tokens[idx].text, "DESC")) {
                direction = .desc;
                idx += 1;
            }
        }

        // Create OrderByItem
        const owned_col = if (needs_free) col_name else try allocator.dupe(u8, col_name);
        try items.append(OrderByItem{
            .column = owned_col,
            .direction = direction,
        });

        // Check for comma (multiple ORDER BY columns)
        if (idx < tokens.len and std.mem.eql(u8, tokens[idx].text, ",")) {
            idx += 1;
            continue;
        } else {
            // No comma, done with ORDER BY
            break;
        }
    }

    return .{
        .clause = OrderByClause{ .items = items },
        .next_idx = idx,
    };
}

/// Parse HAVING clause (similar to WHERE but for GROUP BY results)
fn parseHaving(allocator: Allocator, tokens: []const Token, start_idx: usize) !struct { expr: Expr, next_idx: usize } {
    var idx = start_idx;

    // Expect "HAVING"
    if (idx >= tokens.len or !eqlIgnoreCase(tokens[idx].text, "HAVING")) {
        return error.InvalidSyntax;
    }
    idx += 1;

    // Find end of HAVING clause (before ORDER BY or LIMIT)
    // Need to track parentheses depth to avoid breaking on keywords inside subqueries
    var having_end = idx;
    var paren_depth: usize = 0;
    while (having_end < tokens.len) {
        if (std.mem.eql(u8, tokens[having_end].text, "(")) {
            paren_depth += 1;
        } else if (std.mem.eql(u8, tokens[having_end].text, ")")) {
            paren_depth -= 1;
        } else if (paren_depth == 0) {
            // Only check for clause-ending keywords when not inside parentheses
            if (eqlIgnoreCase(tokens[having_end].text, "ORDER") or
                eqlIgnoreCase(tokens[having_end].text, "LIMIT"))
            {
                break;
            }
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
