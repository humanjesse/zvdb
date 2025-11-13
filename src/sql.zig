const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.ArrayList;
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
};

/// SQL command types
pub const SqlCommand = union(enum) {
    create_table: CreateTableCmd,
    insert: InsertCmd,
    select: SelectCmd,
    delete: DeleteCmd,

    pub fn deinit(self: *SqlCommand, allocator: Allocator) void {
        switch (self.*) {
            .create_table => |*cmd| cmd.deinit(allocator),
            .insert => |*cmd| cmd.deinit(allocator),
            .select => |*cmd| cmd.deinit(allocator),
            .delete => |*cmd| cmd.deinit(allocator),
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
    columns: ArrayList([]const u8), // empty = SELECT *
    where_column: ?[]const u8,
    where_value: ?ColumnValue,
    similar_to_column: ?[]const u8, // For SIMILAR TO queries
    similar_to_text: ?[]const u8,
    order_by_similarity: ?[]const u8, // ORDER BY SIMILARITY TO "text"
    order_by_vibes: bool, // Fun parody feature!
    limit: ?usize,

    pub fn deinit(self: *SelectCmd, allocator: Allocator) void {
        allocator.free(self.table_name);
        for (self.columns.items) |col| {
            allocator.free(col);
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
            while (i < sql.len and (std.ascii.isDigit(sql[i]) or sql[i] == '.')) : (i += 1) {}
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Identifier or keyword
        else if (std.ascii.isAlphabetic(sql[i]) or sql[i] == '_') {
            while (i < sql.len and (std.ascii.isAlphanumeric(sql[i]) or sql[i] == '_')) : (i += 1) {}
            try tokens.append(.{ .text = sql[start..i], .start = start });
        }
        // Special characters
        else if (sql[i] == '(' or sql[i] == ')' or sql[i] == ',' or sql[i] == '*' or sql[i] == '=') {
            i += 1;
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
        return SqlCommand{ .create_table = try parseCreateTable(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "INSERT")) {
        return SqlCommand{ .insert = try parseInsert(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "SELECT")) {
        return SqlCommand{ .select = try parseSelect(allocator, tokens.items) };
    } else if (eqlIgnoreCase(first, "DELETE")) {
        return SqlCommand{ .delete = try parseDelete(allocator, tokens.items) };
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
    // SELECT * FROM table WHERE col SIMILAR TO "text"
    // SELECT * FROM table ORDER BY SIMILARITY TO "text" LIMIT 5
    // SELECT * FROM table ORDER BY VIBES
    if (tokens.len < 4) return SqlError.InvalidSyntax;

    var columns = ArrayList([]const u8).init(allocator);
    var i: usize = 1;

    // Parse columns
    while (i < tokens.len and !eqlIgnoreCase(tokens[i].text, "FROM")) {
        if (!std.mem.eql(u8, tokens[i].text, ",")) {
            if (std.mem.eql(u8, tokens[i].text, "*")) {
                // SELECT * means all columns (empty list)
            } else {
                try columns.append(try allocator.dupe(u8, tokens[i].text));
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
    var limit: ?usize = null;

    // Parse WHERE, ORDER BY, LIMIT
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
                } else {
                    const num = try std.fmt.parseInt(i64, token_text, 10);
                    where_value = ColumnValue{ .int = num };
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
