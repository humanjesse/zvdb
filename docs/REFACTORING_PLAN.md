# ZVDB Refactoring Plan: File Splitting for Better Coherency

## Executive Summary

This document outlines a phased approach to restructure zvdb's codebase for improved maintainability, testability, and adherence to single-responsibility principles.

**Current Status:** 25,000+ lines across 43 files
**Key Issue:** `executor.zig` (3,085 lines) and `sql.zig` (1,859 lines) handle multiple responsibilities

---

## Phase 1: Split Query Executor (HIGH PRIORITY)

### Problem
`src/database/executor.zig` currently handles 10+ distinct responsibilities:
- Command execution (CREATE, INSERT, DELETE, UPDATE)
- SELECT query execution
- Join strategies (hash join, nested loop)
- Aggregate computation (SUM, AVG, COUNT, MIN, MAX)
- GROUP BY processing
- ORDER BY sorting
- HAVING filters
- Subquery execution
- WHERE clause evaluation
- Column projection
- Transaction commands (BEGIN, COMMIT, ROLLBACK)

### Solution: Create `src/database/executor/` subdirectory

```
src/database/executor/
├── executor.zig           (200-300 lines)
│   └─ Main query executor coordinator
│      - Delegates to specialized executors
│      - MVCC snapshot integration
│      - Transaction context management
│
├── command_executor.zig   (300-400 lines)
│   └─ Simple SQL commands
│      - CREATE TABLE/INDEX
│      - INSERT INTO
│      - DELETE FROM (simple, non-joined)
│      - UPDATE (simple, non-joined)
│      - BEGIN/COMMIT/ROLLBACK
│
├── select_executor.zig    (400-500 lines)
│   └─ SELECT query execution
│      - Column projection logic
│      - Subquery execution
│      - Result set construction
│      - Simple (single-table) SELECT optimization
│
├── join_executor.zig      (600-700 lines)
│   └─ All join algorithms
│      - Hash join vs nested loop decision
│      - INNER JOIN
│      - LEFT/RIGHT/FULL OUTER JOIN
│      - Multi-table (N-way) joins
│      - Join cardinality estimation
│
├── aggregate_executor.zig (500-600 lines)
│   └─ Aggregation & grouping
│      - Aggregate functions (SUM, AVG, COUNT, MIN, MAX)
│      - GROUP BY processing
│      - HAVING clause evaluation
│      - Aggregate state management
│
├── sort_executor.zig      (200-300 lines)
│   └─ Sorting operations
│      - ORDER BY implementation
│      - Multi-column sorting
│      - ASC/DESC handling
│      - Null handling in sorts
│
└── where_evaluator.zig    (300-400 lines)
    └─ Filter evaluation
       - WHERE clause evaluation
       - Predicate pushdown optimization
       - Expression evaluation for filters
```

### Migration Strategy

1. **Create directory structure**
   ```bash
   mkdir -p src/database/executor
   ```

2. **Extract in order** (to minimize breakage):
   - `where_evaluator.zig` - Least coupled
   - `sort_executor.zig` - Depends only on Table types
   - `aggregate_executor.zig` - Depends on where_evaluator
   - `join_executor.zig` - Depends on where_evaluator
   - `select_executor.zig` - Depends on join, aggregate, sort
   - `command_executor.zig` - Depends on select for subqueries
   - Refactor `executor.zig` to coordinate

3. **Update imports**
   - `database.zig` imports `executor/executor.zig`
   - Internal executor files import each other as needed

4. **Test suite updates**
   - `test_sql.zig` - Update imports
   - `test_joins.zig`, `test_hash_join.zig` - Test join_executor directly
   - `test_aggregates.zig`, `test_group_by.zig` - Test aggregate_executor
   - `test_order_by.zig` - Test sort_executor

### Benefits
- **Single Responsibility:** Each file has one clear purpose
- **Independent Testing:** Test join strategies without loading aggregate logic
- **Easier Optimization:** Focus performance work on specific executors
- **Better Navigation:** Developers can find code intuitively
- **Reduced Compilation Times:** Changes to joins don't recompile aggregates
- **Team Scalability:** Multiple developers can work on different executors

---

## Phase 2: Split SQL Parser (MEDIUM PRIORITY)

### Problem
`src/sql.zig` mixes three concerns:
- AST type definitions (structs for SqlCommand, SelectCmd, etc.)
- SQL parsing logic (parse functions)
- Expression evaluation (evaluateExpr)

### Solution: Create `src/sql/` subdirectory

```
src/sql/
├── sql.zig            (50-100 lines)
│   └─ Public API exports
│      - Re-exports from ast, parser, expr_evaluator
│      - Backward compatibility layer
│
├── ast.zig            (400-500 lines)
│   └─ Abstract Syntax Tree definitions
│      - SqlCommand (union type)
│      - SelectCmd, InsertCmd, DeleteCmd, UpdateCmd
│      - CreateTableCmd, CreateIndexCmd
│      - Expression types
│      - JoinClause, WhereClause, etc.
│
├── parser.zig         (800-900 lines)
│   └─ SQL parsing logic
│      - parse() function (main entry point)
│      - parseSelectCommand()
│      - parseInsertCommand()
│      - parseCreateTable()
│      - parseExpression()
│      - Token handling
│
└── expr_evaluator.zig (500-600 lines)
    └─ Expression evaluation
       - evaluateExpr() function
       - Operator handling (+, -, *, /, =, <, >, etc.)
       - Function calls (LOWER, UPPER, etc.)
       - Type coercion
       - Null handling
```

### Migration Strategy

1. **Create `ast.zig`**
   - Move all type definitions
   - Keep public to avoid namespace pollution

2. **Create `expr_evaluator.zig`**
   - Move evaluateExpr and helpers
   - Import ast.zig for types
   - Import table.zig for ColumnValue types

3. **Create `parser.zig`**
   - Move all parsing logic
   - Import ast.zig for return types
   - Keep parse() as main entry point

4. **Refactor `sql.zig`**
   - Re-export public APIs
   - Maintain backward compatibility

5. **Update imports throughout codebase**
   - `executor.zig` imports `sql/ast.zig` for types
   - `validator.zig` imports `sql/ast.zig`
   - Test files import through `sql.zig`

### Benefits
- **Clearer Dependencies:** AST types can be imported without parser
- **Easier Grammar Changes:** Modify parser without affecting AST
- **Independent Testing:** Test expression evaluation separately
- **Future Extensions:** Easy to add new parsers (e.g., JSON query syntax)

---

## Phase 3: Extract Buffer Management (FUTURE)

### Current State
Buffer/memory management is implicit:
- B-tree nodes allocated on-demand
- No explicit LRU caching
- No buffer pool for table pages

### Opportunity
Create explicit buffer management layer (following Postgres/MySQL patterns):

```
src/database/buffer/
├── buffer_pool.zig
│   └─ LRU page cache
│      - Fixed-size buffer pool (configurable)
│      - Eviction policy (LRU, Clock, etc.)
│      - Pin/unpin semantics
│      - Dirty page tracking
│
└── memory_manager.zig
    └─ Memory allocation tracking
       - Query memory limits
       - Temp space for sorts/joins
       - Memory pressure detection
```

### Benefits (Future)
- **Performance:** Reduce disk I/O via caching
- **Predictability:** Control memory usage
- **Scalability:** Support larger-than-memory databases

**Note:** This is lower priority as zvdb is currently an in-memory database with persistence. Implement when transitioning to disk-backed storage.

---

## Phase 4: Optional WAL Splitting (LOW PRIORITY)

### Current State
`src/database/wal.zig` (1,848 lines) is large but well-organized internally.

### Potential Split
```
src/database/wal/
├── wal.zig        (300-400 lines)
│   └─ Main coordinator
│      - WAL types and constants
│      - Public API
│
├── wal_writer.zig (700-800 lines)
│   └─ Write operations
│      - Record writing
│      - Checkpointing
│      - Fsync management
│
└── wal_reader.zig (800-900 lines)
    └─ Recovery operations
       - WAL replay
       - Crash recovery
       - Record deserialization
```

### Consideration
Current `wal.zig` is cohesive and focused on a single subsystem. Only split if:
- Adding significant features (e.g., replication)
- Team members need to work on write/recovery independently
- File becomes significantly larger (>2,500 lines)

---

## Migration Best Practices

### 1. **Incremental Changes**
- Make one file split per commit/PR
- Run full test suite between changes
- Keep `zig build test` passing at all times

### 2. **Backward Compatibility**
- Keep old public APIs working during transition
- Use re-exports from new locations
- Deprecate old imports only after migration complete

### 3. **Documentation**
- Update architecture docs after each phase
- Document import changes in commit messages
- Update README with new structure

### 4. **Testing**
- Ensure test coverage doesn't drop
- Add tests for new module boundaries
- Test internal APIs of split modules directly

---

## Success Metrics

### Code Quality
- ✅ No file over 1,500 lines (except test suites)
- ✅ Each file has single, clear responsibility
- ✅ Easy to locate code by feature (intuitive navigation)

### Maintainability
- ✅ Changes to joins don't require recompiling aggregates
- ✅ New developers can find code without asking
- ✅ Test files clearly map to source files

### Performance
- ✅ Faster incremental compilation
- ✅ No performance regression in benchmarks
- ✅ Same or better query execution times

---

## Timeline Estimate

| Phase | Effort | Files Affected | Priority |
|-------|--------|----------------|----------|
| Phase 1: Split executor | 2-3 days | 1 → 7 files | HIGH |
| Phase 2: Split SQL parser | 1-2 days | 1 → 4 files | MEDIUM |
| Phase 3: Buffer management | 1 week | +2 new files | FUTURE |
| Phase 4: WAL splitting | 1 day | 1 → 3 files | LOW |

**Recommended:** Start with Phase 1 (executor splitting) as it provides the most value.

---

## Comparison to Generic Database Architecture

### Alignment with Standard Layers

| Standard Layer | zvdb Implementation | Match Quality |
|----------------|---------------------|---------------|
| **Storage Layer** | table.zig, btree.zig, index_manager.zig | ✅ Excellent |
| **Query Processing** | sql/ + executor/ (after refactor) | ✅ Good (will be excellent) |
| **Transaction Layer** | transaction.zig + MVCC in table.zig | ✅ Excellent |
| **Buffer/Memory** | *Implicit* | ⚠️ Could improve |
| **Recovery/Logging** | wal.zig, recovery.zig | ✅ Excellent |
| **Interface Layer** | zvdb.zig, database.zig | ✅ Excellent |

### Unique Features (Beyond Standard)
- **Vector Search Layer:** hnsw.zig (HNSW algorithm)
- **GraphRAG Support:** NodeMetadata, typed edges in hnsw.zig
- **Hybrid Queries:** SQL + vector search integration
- **Configurable Validation:** validator.zig with multiple modes

zvdb already aligns well with standard database architecture. The proposed refactoring will make it even more textbook!

---

## Questions for Discussion

1. **Priority agreement?** Should we start with executor.zig splitting?
2. **Naming preferences?** Any preference for executor subdirectory vs. executors plural?
3. **Buffer layer?** Worth implementing explicit buffer pool now, or defer to future?
4. **Testing approach?** Extract tests alongside code, or update test imports after?
5. **Migration pace?** All at once, or one module per week?

---

## Next Steps

1. **Review this plan** - Discuss and adjust priorities
2. **Create feature branch** - `refactor/split-executor` or similar
3. **Start Phase 1** - Begin with `where_evaluator.zig` extraction
4. **Iterate** - One file split at a time, keeping tests green
5. **Document** - Update architecture docs as we go

---

*This refactoring plan was generated through architectural analysis of zvdb's 25,000+ line codebase against industry-standard database architecture patterns.*
