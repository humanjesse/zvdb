# Column Validation Research: PostgreSQL & SQLite

## Executive Summary

This document summarizes research on how production databases (PostgreSQL and SQLite) handle column validation, with key findings that inform our implementation of deferred validation in ZVDB.

**Key Takeaway:** Both PostgreSQL and SQLite perform column validation during the **semantic analysis phase** (after parsing, before execution), not at parse time or execution time. This is known as **deferred validation**.

---

## PostgreSQL Validation Strategy

### Query Processing Phases

PostgreSQL processes queries through five distinct phases:

1. **Parser**: Checks syntactic validity (grammar, keywords)
2. **Analyzer/Rewriter**: Validates semantics (column/table existence, type inference)
3. **Rewriter**: Expands views and applies rules
4. **Planner**: Generates optimal execution plan
5. **Executor**: Executes the plan

### When Column Validation Occurs

**Validation happens in Phase 2 (Analyzer), not during execution.**

From PostgreSQL source code (`src/backend/parser/parse_relation.c`):
```c
ereport(ERROR,
    (errcode(ERRCODE_UNDEFINED_COLUMN),
     errmsg("column \"%s\" of relation \"%s\" does not exist",
            NameStr(att_tup->attname), rte->eref->aliasname)));
```

**Error Code:** `42703` (UNDEFINED_COLUMN)

### Key Characteristics

- **Semantic Analysis:** After parsing, PostgreSQL identifies column types, maps table names to schema, and validates all references exist
- **Pre-execution:** Column errors are caught before the planner creates an execution plan
- **Type Inference:** The analyzer also infers data types of all expressions during this phase

### PostgreSQL Error Messages

PostgreSQL provides helpful, specific error messages:

```
ERROR: column "invalid_col" does not exist
LINE 1: SELECT invalid_col FROM users;
                ^
```

For case sensitivity issues:
```
ERROR: column "Name" does not exist
HINT: Perhaps you meant to reference the column "users.name".
```

### Important Notes

- **Case Sensitivity:** PostgreSQL converts unquoted identifiers to lowercase. `SELECT Name` becomes `select name`. Use double quotes for case-sensitive names: `SELECT "Name"`
- **Double Quotes:** Used for identifiers, not strings (unlike MySQL). Strings use single quotes.

---

## SQLite Validation Strategy

### Column Naming Rules

SQLite follows similar rules to PostgreSQL:
- Valid names: alphanumeric characters and underscores
- Best practice: Begin with alpha character or underscore
- Special characters: Allowed if enclosed in double quotes

### Validation Timing

Like PostgreSQL, SQLite validates columns during the **parse analysis phase** before execution.

### Error Messages

**Column Not Found:**
```
Parse error: no such column: unknowncol
```

**Syntax Errors (reserved keywords):**
```
SQL error: near 'order': syntax error
```

To use reserved keywords as column names, wrap in quotes:
```sql
SELECT "order" FROM orders;  -- Correct
SELECT order FROM orders;    -- Error
```

### Unique SQLite Behavior

**Quoted Identifier Fallback:** When SQLite's parser sees a quoted identifier like `"FirstName"` and the column doesn't exist, it treats it as a string literal instead of throwing an error. This can **mask validation errors**.

**Recommendation:** This behavior makes early error detection harder. ZVDB should not adopt this behavior.

---

## Best Practices for SQL Validation

### 1. Literals in SELECT

Literals (numeric, string) are valid expressions in SELECT clauses:
```sql
SELECT 1 FROM users;                    -- Valid: numeric literal
SELECT 'active' FROM users;             -- Valid: string literal
SELECT 42, 'test', name FROM employees; -- Valid: mixed
```

**Validation Rule:** Literals should pass validation without needing to exist as columns.

### 2. Aggregate Functions

Aggregates cannot be used in WHERE or GROUP BY clauses without a subquery:

```sql
-- Valid:
SELECT COUNT(*) FROM users;
SELECT AVG(price) FROM products;

-- Invalid:
SELECT * FROM users WHERE COUNT(*) > 5;  -- Error: aggregate in WHERE

-- Valid workaround:
SELECT * FROM users GROUP BY status HAVING COUNT(*) > 5;
```

**Validation Rule:**
- Aggregates are valid in SELECT and HAVING
- Aggregates create synthetic column names like `COUNT(*)`, `AVG(price)`
- These don't exist in table schema and should bypass column existence checks

### 3. Expressions

Expressions are valid anywhere: SELECT, WHERE, GROUP BY, HAVING, ORDER BY

```sql
SELECT price * 1.1 AS discounted FROM products;
SELECT * FROM users WHERE LENGTH(name) > 10;
```

**Validation Rule:** Expressions should be validated recursively - validate column references within the expression, not the expression result.

### 4. WHERE vs HAVING

- **WHERE:** Filters rows before aggregation (cannot use aggregates)
- **HAVING:** Filters groups after aggregation (can use aggregates)

**Performance:** Use WHERE to filter early, then HAVING for aggregate filters.

---

## Semantic Analysis: The "Hard Cases"

From research on semantic analyzers, name resolution is complex because names can refer to:

1. Global variables
2. Local variables
3. Function arguments
4. Table columns
5. Cursor fields
6. Aliases

**Multi-Phase Resolution:** Semantic analyzers go through several phases:
1. Try exact match
2. Try qualified name resolution (table.column)
3. Try unqualified name resolution (column)
4. Try alias resolution
5. Report error if none succeed

This matches our three-layer fallback logic in executor.zig!

---

## Deferred Name Resolution (SQL Server)

Microsoft SQL Server has a concept called "deferred name resolution" for stored procedures:
- Table objects can be referenced even if they don't exist at procedure creation time
- Validation happens at execution time
- Allows creating procedures before the tables exist

**Note:** This is different from our use case. We want to validate before execution for better error messages.

---

## Static Analysis Tools

Modern databases and tools use **semantic trees** for advanced analysis:

- **Meta's UPM:** Represents SQL queries as hierarchical semantic trees
- Enables static analysis beyond syntax checking
- Verifies tables and columns exist in schema
- Detects semantic issues without executing queries

**Application to ZVDB:** We can build a lightweight semantic validator that:
1. Parses the query into an AST (already done by our SQL parser)
2. Walks the AST to collect all column references
3. Validates each reference against available tables/aliases
4. Provides helpful error messages before execution

---

## Comparison: PostgreSQL vs SQLite vs ZVDB

| Feature | PostgreSQL | SQLite | ZVDB (Current) | ZVDB (Proposed) |
|---------|-----------|--------|----------------|-----------------|
| Validation Phase | Analyzer (Phase 2) | Parse Analysis | Execution | Semantic Analysis |
| Timing | After parse, before planning | After parse, before execution | During execution | After parse, before execution |
| Literals | Allowed | Allowed | Allowed | Allowed |
| Aggregates | Allowed in SELECT/HAVING | Allowed | Allowed | Allowed |
| Error Messages | Detailed with hints | Basic | Runtime errors | Detailed with suggestions |
| Column Resolution | Multi-phase fallback | Basic matching | Three-layer fallback | Dedicated resolver module |
| Case Sensitivity | Lowercase conversion | Case-sensitive | Case-sensitive | Case-sensitive |

---

## Recommendations for ZVDB

### 1. Adopt Deferred Validation

**Implement validation after parsing but before execution:**
- Parse query → Build AST ✓ (already done)
- **Semantic Analysis → Validate columns** ⬅️ ADD THIS
- Plan execution
- Execute query

### 2. Smart Expression Detection

Validator should recognize and allow:
- **Numeric literals:** `\d+` or `\d+\.\d+`
- **String literals:** Already handled by parser as `.literal`
- **Aggregates:** `COUNT(*)`, `SUM(...)`, `AVG(...)`, `MIN(...)`, `MAX(...)`
- **Expressions:** Validate recursively

### 3. Multi-Phase Column Resolution

Extract the existing three-layer fallback logic into a dedicated `column_resolver.zig` module:
```zig
pub fn resolveColumn(
    name: []const u8,
    available_columns: StringHashMap(ColumnValue),
    aliases: ?StringHashMap([]const u8)
) !ColumnValue {
    // Phase 1: Exact match
    // Phase 2: Qualified → unqualified
    // Phase 3: Alias resolution
}
```

### 4. Helpful Error Messages

Model after PostgreSQL:
```
ERROR: column "u.name" does not exist
HINT: Table alias "u" not found. Available tables: users, orders
      Did you mean "users.name"?
```

### 5. Validation Context

Create a context structure with all information needed for validation:
```zig
pub const ValidationContext = struct {
    tables: []TableInfo,           // Available tables
    aliases: StringHashMap([]const u8),  // alias → table name
    allow_aggregates: bool,        // true for SELECT/HAVING
    allow_literals: bool,          // true for SELECT
    current_scope: ScopeType,      // WHERE, HAVING, SELECT, etc.
};
```

---

## Learning Points

### Why Not Validate at Parse Time?

Parsing checks **syntax** (grammar rules), not **semantics** (meaning):
- Parser: "Is this valid SQL grammar?" → `SELECT invalid_col FROM users` ✓ syntactically valid
- Semantic Analyzer: "Do these columns exist?" → `invalid_col` ✗ does not exist

### Why Not Validate at Execution Time?

Execution-time validation:
- ❌ Fails late (wasted planning time)
- ❌ Poor error messages (less context available)
- ❌ Harder to debug (errors mixed with data issues)

Semantic analysis validation:
- ✅ Fails early (before planning/execution)
- ✅ Better error messages (full query context available)
- ✅ Easier to debug (query structure still in memory)

### The Three-Layer Fallback is Correct

Our current implementation has the right idea:
1. Try exact match: `name` → `name`
2. Try partial match: `u.name` → `users.name` (extract column part)
3. Try unqualified: `id` matches `users.id` or `orders.id`

**Problem:** It's duplicated in three places. **Solution:** Extract to shared module.

---

## References

1. PostgreSQL Documentation: Query Processing
   - https://www.postgresql.org/docs/current/query-path.html
2. PostgreSQL Source Code: `parse_relation.c`
   - https://doxygen.postgresql.org/parse__relation_8c.html
3. SQLite Documentation: Expression Syntax
   - https://www.sqlite.org/lang_expr.html
4. Meta Engineering: Static Analysis of SQL Queries
   - https://engineering.fb.com/2022/11/30/data-infrastructure/static-analysis-sql-queries/
5. A Formal Semantics of SQL Queries (VLDB)
   - http://www.vldb.org/pvldb/vol11/p27-guagliardo.pdf

---

## Conclusion

Production databases validate columns during **semantic analysis** (after parsing, before execution) using multi-phase name resolution strategies. ZVDB should adopt this approach with:

1. Dedicated column resolver module (eliminate duplication)
2. Semantic validator that runs after parsing
3. Smart detection of literals, aggregates, and expressions
4. Helpful, PostgreSQL-style error messages
5. Validation context with full query scope

This will provide a **professional, production-quality implementation** while maintaining the 100% test pass rate.
