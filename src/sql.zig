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
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    similar_to_column: ?[]const u8, // For SIMILAR TO queries
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8, // ORDER BY SIMILARITY TO "text"
    order_by_vibes: bool, // Fun parody feature!
    group_by: ArrayList([]const u8), // GROUP BY columns
    limit: ?usize,

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |*col| {
            col.deinit(allocator);
        }
        self.columns.deinit();
        if (self.where_column) |col| allocator.free(col);
        if (self.where_value) |*val| {
            var v = val.*;
            v.deinit(allocator);
        }
        if (self.similar_to_column) |col| allocator.free(col);
        if (self.similar_to_text) |text| allocator.free(text);
        if (self.order_by_similarity) |text| allocator.free(text);

        // Free GROUP BY columns
        for (self.group_by.items) |col| {
            allocator.free(col);
        }
        self.group_by.deinit();
    }
};

/// DELETE command
pub const DeleteCmd = struct {
    table_name: []const u8,
    where_column: ?[]const u8,
    where_value: ?ColumnValue,

    pub fn deinit(self: *DeleteCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        if (self.where_column) |col| allocator.free(col);
        if (self.where_value) |*val| {
            var v = val.*;
            v.deinit(allocator);
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
    regular: []const u8,      // Regular column: "name"
    aggregate: AggregateExpr, // Aggregate: COUNT(*), SUM(balance)
    star: void,               // SELECT *

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
    eq,     // =
    neq,    // !=
    lt,     // <
    gt,     // >
    lte,    // <=
    gte,    // >=
    and_op, // AND
    or_op,  // OR
};

/// Unary operators for WHERE expressions
pub const UnaryOp = enum {
    not,         // NOT
    is_null,     // IS NULL
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
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_')) : (i += 1) {}
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Special characters and operators
        else if (sql[i] == '(' or sql[i] == ')' or sql[i] == ',' or sql[i] == '*') {
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

        try columns.append(.{ .name = col_name, .col_type = col_type });
        i += 1; // Skip type token
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

fn parseInsert(allocator: Allocator, tokens: []const Token) !InsertCmd {
    // INSERT INTO table (col1, col2) VALUES (val1, val2)
    // or INSERT INTO table VALUES (val1, val2)
    if (tokens.len < 4) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "INTO")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    var columns = ArrayList([]const u8).init(allocator);
    var values = ArrayList(ColumnValue).init(allocator);

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

        const token_text = tokens[i].text;

        // Parse value
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
    i += 1;

    var where_column: ?[]const u8 = null;
    var where_value: ?ColumnValue = null;
    var similar_to_column: ?[]const u8 = null;
    var similar_to_text: ?[]const u8 = null;
    var order_by_similarity: ?[]const u8 = null;
    var order_by_vibes = false;
    var group_by = ArrayList([]const u8).init(allocator);
    var limit: ?usize = null;

    // Parse WHERE, GROUP BY, ORDER BY, LIMIT
    while (i < tokens.len) {
        if (eqlIgnoreCase(tokens[i].text, "WHERE")) {
            i += 1;
            if (i >= tokens.len) return SqlError.InvalidSyntax;
            where_column = try allocator.dupe(u8, tokens[i].text);
            i += 1;

            // Check for SIMILAR TO
            if (i + 1 < tokens.len and eqlIgnoreCase(tokens[i].text, "SIMILAR") and eqlIgnoreCase(tokens[i + 1].text, "TO")) {
                similar_to_column = where_column;
                where_column = null;
                i += 2;
                if (i >= tokens.len) return SqlError.InvalidSyntax;
                const text = parseString(tokens[i].text);
                similar_to_text = try allocator.dupe(u8, text);
                i += 1;
            } else if (i + 1 < tokens.len and std.mem.eql(u8, tokens[i].text, "=")) {
                i += 1;
                const token_text = tokens[i].text;
                if (token_text[0] == '"' or token_text[0] == '\'') {
                    const str = parseString(token_text);
                    const owned = try allocator.dupe(u8, str);
                    where_value = ColumnValue{ .text = owned };
                } else if (std.mem.indexOf(u8, token_text, ".")) |_| {
                    const f = try std.fmt.parseFloat(f64, token_text);
                    where_value = ColumnValue{ .float = f };
                } else if (eqlIgnoreCase(token_text, "true")) {
                    where_value = ColumnValue{ .bool = true };
                } else if (eqlIgnoreCase(token_text, "false")) {
                    where_value = ColumnValue{ .bool = false };
                } else if (eqlIgnoreCase(token_text, "NULL")) {
                    where_value = ColumnValue.null_value;
                } else {
                    const num = try std.fmt.parseInt(i64, token_text, 10);
                    where_value = ColumnValue{ .int = num };
                }
                i += 1;
            }
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
                i += 1;
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

    return SelectCmd{
        .table_name = table_name,
        .columns = columns,
        .where_column = where_column,
        .where_value = where_value,
        .similar_to_column = similar_to_column,
        .similar_to_text = similar_to_text,
        .order_by_similarity = order_by_similarity,
        .order_by_vibes = order_by_vibes,
        .group_by = group_by,
        .limit = limit,
    };
}

fn parseDelete(allocator: Allocator, tokens: []const Token) !DeleteCmd {
    // DELETE FROM table WHERE col = val
    if (tokens.len < 3) return SqlError.InvalidSyntax;
    if (!eqlIgnoreCase(tokens[1].text, "FROM")) return SqlError.InvalidSyntax;

    const table_name = try allocator.dupe(u8, tokens[2].text);
    var where_column: ?[]const u8 = null;
    var where_value: ?ColumnValue = null;

    var i: usize = 3;
    if (i < tokens.len and eqlIgnoreCase(tokens[i].text, "WHERE")) {
        i += 1;
        if (i >= tokens.len) return SqlError.InvalidSyntax;
        where_column = try allocator.dupe(u8, tokens[i].text);
        i += 1;

        if (i + 1 < tokens.len and std.mem.eql(u8, tokens[i].text, "=")) {
            i += 1;
            const token_text = tokens[i].text;
            if (token_text[0] == '"' or token_text[0] == '\'') {
                const str = parseString(token_text);
                const owned = try allocator.dupe(u8, str);
                where_value = ColumnValue{ .text = owned };
            } else if (std.mem.indexOf(u8, token_text, ".")) |_| {
                const f = try std.fmt.parseFloat(f64, token_text);
                where_value = ColumnValue{ .float = f };
            } else if (eqlIgnoreCase(token_text, "true")) {
                where_value = ColumnValue{ .bool = true };
            } else if (eqlIgnoreCase(token_text, "false")) {
                where_value = ColumnValue{ .bool = false };
            } else if (eqlIgnoreCase(token_text, "NULL")) {
                where_value = ColumnValue.null_value;
            } else {
                const num = try std.fmt.parseInt(i64, token_text, 10);
                where_value = ColumnValue{ .int = num };
            }
        }
    }

    return DeleteCmd{
        .table_name = table_name,
        .where_column = where_column,
        .where_value = where_value,
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

/// Parse an expression (recursive descent parser)
fn parseExpr(allocator: Allocator, tokens: []const Token, start_idx: *usize) (Allocator.Error || SqlError)!Expr {
    return try parseOrExpr(allocator, tokens, start_idx);
}

/// Parse OR expressions (lowest precedence)
fn parseOrExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    var left = try parseAndExpr(allocator, tokens, idx);

    while (idx.* < tokens.len and eqlIgnoreCase(tokens[idx.*].text, "OR")) {
        idx.* += 1;
        const right = try parseAndExpr(allocator, tokens, idx);

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

    while (idx.* < tokens.len and eqlIgnoreCase(tokens[idx.*].text, "AND")) {
        idx.* += 1;
        const right = try parseComparisonExpr(allocator, tokens, idx);

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

/// Parse comparison expressions (=, !=, <, >, <=, >=)
fn parseComparisonExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    const left = try parseUnaryExpr(allocator, tokens, idx);

    if (idx.* < tokens.len) {
        const op_text = tokens[idx.*].text;
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
            const right = try parseUnaryExpr(allocator, tokens, idx);

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

/// Parse unary expressions (NOT, IS NULL, IS NOT NULL)
fn parseUnaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    // NOT expression
    if (eqlIgnoreCase(tokens[idx.*].text, "NOT")) {
        idx.* += 1;
        const expr = try parseUnaryExpr(allocator, tokens, idx);
        const unary = try allocator.create(UnaryExpr);
        unary.* = UnaryExpr{
            .op = .not,
            .expr = expr,
        };
        return Expr{ .unary = unary };
    }

    const expr = try parsePrimaryExpr(allocator, tokens, idx);

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

/// Parse primary expressions (literals, columns, parentheses)
fn parsePrimaryExpr(allocator: Allocator, tokens: []const Token, idx: *usize) (Allocator.Error || SqlError)!Expr {
    if (idx.* >= tokens.len) return SqlError.InvalidExpression;

    const token_text = tokens[idx.*].text;

    // Parenthesized expression
    if (std.mem.eql(u8, token_text, "(")) {
        idx.* += 1;
        const expr = try parseExpr(allocator, tokens, idx);
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
        eqlIgnoreCase(token_text, "NULL")) {
        const value = try parseValue(allocator, token_text);
        idx.* += 1;
        return Expr{ .literal = value };
    }

    // Otherwise, it's a column reference
    const col_name = try allocator.dupe(u8, token_text);
    idx.* += 1;
    return Expr{ .column = col_name };
}

/// Evaluate an expression against a row's values
pub fn evaluateExpr(expr: Expr, row_values: anytype) bool {
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
        .binary => |bin| {
            return evaluateBinaryExpr(bin.*, row_values);
        },
        .unary => |un| {
            return evaluateUnaryExpr(un.*, row_values);
        },
    }
}

fn evaluateBinaryExpr(expr: BinaryExpr, row_values: anytype) bool {
    switch (expr.op) {
        .and_op => {
            return evaluateExpr(expr.left, row_values) and evaluateExpr(expr.right, row_values);
        },
        .or_op => {
            return evaluateExpr(expr.left, row_values) or evaluateExpr(expr.right, row_values);
        },
        .eq, .neq, .lt, .gt, .lte, .gte => {
            const left_val = getExprValue(expr.left, row_values);
            const right_val = getExprValue(expr.right, row_values);
            return compareValues(left_val, right_val, expr.op);
        },
    }
}

fn evaluateUnaryExpr(expr: UnaryExpr, row_values: anytype) bool {
    switch (expr.op) {
        .not => {
            return !evaluateExpr(expr.expr, row_values);
        },
        .is_null => {
            const val = getExprValue(expr.expr, row_values);
            return val == .null_value;
        },
        .is_not_null => {
            const val = getExprValue(expr.expr, row_values);
            return val != .null_value;
        },
    }
}

fn getExprValue(expr: Expr, row_values: anytype) ColumnValue {
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
        .binary, .unary => {
            // For complex expressions in comparison context, treat as bool
            const result = evaluateExpr(expr, row_values);
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
