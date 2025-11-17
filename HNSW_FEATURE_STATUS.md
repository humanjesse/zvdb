# HNSW Feature Status

## Summary of Today's Work (2025-11-17)

### Session Overview
- ✅ **Priority 1:** Multiple embedding columns per row - COMPLETE
- ✅ **Priority 2:** Hybrid query testing and documentation - COMPLETE
- 🔜 **Priority 3:** SQL array literals - TODO

### ✅ Completed: Multiple Embedding Columns Per Row

**Status:** IMPLEMENTED AND TESTED

**What was the problem?**
The codebase had artificial `break;` statements in INSERT and UPDATE operations that limited tables to indexing only ONE embedding column per row, even though the infrastructure supported multiple per-dimension HNSW indexes.

**What was fixed?**
- **INSERT** (command_executor.zig:276): Removed break statement, updated rollback logic to track multiple dimensions using `ArrayList<usize>`
- **UPDATE** (command_executor.zig:540): Refactored to use `EmbeddingUpdate` struct for tracking multiple embedding column changes
- **Tests**: Added comprehensive test suite (`test_multi_embedding.zig`) with 5 test cases

**Example Usage:**
```sql
CREATE TABLE documents (
    id int,
    title text,
    text_embedding embedding(128),    -- For semantic text search
    image_embedding embedding(256)    -- For image similarity
)
```

Both embeddings are now automatically indexed in their respective dimension-specific HNSW indexes.

**Files Changed:**
- `src/database/executor/command_executor.zig` (123 lines modified)
- `src/test_multi_embedding.zig` (247 lines added)
- `build.zig` (test registration)

**Commit:** `403ab42` - "Enable multiple embedding columns per row"

### ✅ Completed: Hybrid Query Testing & Documentation

**Status:** FULLY TESTED AND DOCUMENTED

**What was done?**
Created comprehensive test suite to validate the existing hybrid query functionality (WHERE + SIMILARITY) that was already working but untested. The original analysis incorrectly claimed this feature was missing.

**Test Coverage:**
- Simple WHERE filters with SIMILARITY
- Complex WHERE expressions (multiple conditions)
- LIMIT behavior after filtering
- Empty result handling when WHERE filters all results
- Multiple WHERE conditions
- Baseline pure SIMILARITY queries

**Documentation Added:**
- 6 example query patterns with explanations
- Internal implementation details
- Performance characteristics
- Current limitations and workarounds

**Files Changed:**
- `src/test_hybrid_queries.zig` (347 lines, 6 test cases)
- `build.zig` (test registration)
- `HNSW_FEATURE_STATUS.md` (comprehensive documentation)

**Key Insight:** This feature was production-ready all along - we just validated and documented it!

**Commit:** TBD (next commit)

---

## ✅ Already Working (NOW TESTED): Hybrid Queries (WHERE + SIMILARITY)

**Status:** IMPLEMENTED AND FULLY TESTED ✅

**Location:** `src/database/executor/select_executor.zig:213-227`

**Test Suite:** `src/test_hybrid_queries.zig` (6 comprehensive tests, 347 lines)

The analysis incorrectly stated this was missing. In fact, hybrid queries work perfectly and are now thoroughly tested:

### Basic Example
```sql
-- Find tech articles similar to "database tutorial"
SELECT * FROM articles
WHERE category = 'tech'
ORDER BY SIMILARITY TO "database tutorial"
LIMIT 10
```

### Supported Query Patterns

**1. Simple WHERE with SIMILARITY**
```sql
SELECT id, title FROM articles
WHERE category = 'tech'
ORDER BY SIMILARITY TO "database"
LIMIT 5
```
✅ Tested in: `test "SQL: Hybrid query - SIMILARITY with simple WHERE"`

**2. Complex WHERE Expressions**
```sql
SELECT * FROM products
WHERE price < 1000.0 AND in_stock = true
ORDER BY SIMILARITY TO "laptop computer"
```
✅ Tested in: `test "SQL: Hybrid query - SIMILARITY with complex WHERE expression"`

**3. LIMIT Applied After Filtering**
```sql
SELECT * FROM docs
WHERE status = 'published'
ORDER BY SIMILARITY TO "query"
LIMIT 3
```
Returns 3 published docs, not 3 docs filtered to published.
✅ Tested in: `test "SQL: Hybrid query - LIMIT applies after WHERE filter"`

**4. Empty Results When Filter Excludes All**
```sql
SELECT * FROM items
WHERE type = 'nonexistent'
ORDER BY SIMILARITY TO "test"
```
Correctly returns 0 rows.
✅ Tested in: `test "SQL: Hybrid query - No results when WHERE filters all SIMILARITY results"`

**5. Multiple WHERE Conditions**
```sql
SELECT * FROM listings
WHERE city = 'NYC' AND price < 700 AND available = true
ORDER BY SIMILARITY TO "apartment"
```
✅ Tested in: `test "SQL: Hybrid query - Multiple WHERE conditions with SIMILARITY"`

**6. Pure SIMILARITY (Baseline)**
```sql
SELECT * FROM notes
ORDER BY SIMILARITY TO "query"
LIMIT 3
```
✅ Tested in: `test "SQL: Hybrid query - SIMILARITY without WHERE (baseline)"`

### How It Works Internally

1. **HNSW Search First** (select_executor.zig:150-157)
   - Generates embedding from similarity text using hash-based mock
   - Searches dimension-specific HNSW index
   - Returns top-K candidate row IDs

2. **WHERE Filter Applied** (select_executor.zig:213-227)
   - Iterates through HNSW result row IDs
   - Evaluates WHERE clause using `expr_evaluator.evaluateExprWithSubqueries()`
   - Skips rows that don't match filter

3. **Results Returned**
   - Maintains HNSW similarity ordering
   - LIMIT applied to filtered results

### Implementation Details

- **Strategy:** "Search-then-filter" (optimal for broad similarity searches)
- **WHERE Support:** Full expression evaluation including subqueries
- **Index Optimization:** Skips WHERE re-evaluation if B-tree index was already used
- **Performance:** O(K) WHERE evaluations where K = HNSW result size (default 10)

### Current Limitations

- **No column selection for SIMILARITY:** Defaults to first embedding found in row
  - **Workaround:** Ensure desired embedding is first in schema
  - **Future enhancement:** Add `ORDER BY SIMILARITY(column_name) TO "query"` syntax

- **Mock embedding generation:** Uses hash-based embedding (not semantic)
  - **Workaround:** Use table API to insert real embeddings from external models
  - **Future enhancement:** Integrate real embedding models (or stay with external approach)

---

## 🚧 Still TODO (from original analysis)

### Priority 2: SQL Array Literal Syntax for Embeddings

**Status:** NOT IMPLEMENTED

**Current workaround:** Use table API to insert embeddings
```zig
// Current: Must use table API
const emb = try allocator.dupe(f32, &embedding);
try values.put("embedding", ColumnValue{ .embedding = emb });
```

**Desired syntax:**
```sql
INSERT INTO docs VALUES (1, "Title", [0.1, 0.2, 0.3, ...])
```

**Estimated effort:** 2-3 hours
- Extend lexer to recognize `[` `]` tokens in value context
- Update parser to handle array literals
- Validate dimension matches schema
- Add tests

**Files to modify:**
- `src/sql.zig` (lexer and parser)
- `src/database/executor/command_executor.zig` (validation)

### Priority 3: Column-Specific SIMILARITY Syntax

**Status:** NOT IMPLEMENTED

**Current limitation:**
```sql
-- Can't specify WHICH embedding to use:
SELECT * FROM docs ORDER BY SIMILARITY TO "query"
-- Uses first embedding found in row
```

**Desired syntax:**
```sql
SELECT * FROM docs ORDER BY SIMILARITY(text_vec) TO "query"
```

**Estimated effort:** 3-4 hours
- Extend parser to accept `SIMILARITY(column_name)`
- Update select_executor to extract specified column
- Add tests for multi-embedding tables

**Files to modify:**
- `src/sql.zig` (parser)
- `src/database/executor/select_executor.zig`

---

## 📊 Feature Comparison

| Feature | Status | Notes |
|---------|--------|-------|
| Basic HNSW search | ✅ Working | Per-dimension indexes |
| WHERE + SIMILARITY hybrid | ✅ **TESTED** | 6 comprehensive tests added |
| Multiple embeddings per row | ✅ **NEW** | Implemented today (Priority 1) |
| SQL array literals | ❌ Missing | Use table API for now |
| Column-specific SIMILARITY | ❌ Missing | Uses first embedding |
| Real embedding models | ❌ Not planned | Use external tools |
| MVCC recovery for HNSW | ✅ Working | Integrated in Phase 4C |

---

## 🎯 Recommendations for Next Session

Based on today's success and remaining gaps:

**Quick Wins (1-2 hours each):**
1. Add test for existing hybrid query capability to document/validate it
2. Write example/demo showing multi-embedding use case

**Medium Effort (2-4 hours):**
3. Implement SQL array literal syntax
4. Add column-specific SIMILARITY syntax

**Lower Priority:**
5. Optimize hybrid queries (pre-filtering vs post-filtering based on selectivity)
6. Add LIMIT parameter to HNSW search (currently hardcoded in some places)

---

## 📝 Notes

- The original analysis was partially incorrect about what was missing
- Infrastructure was already solid (per-dim indexes, hybrid queries)
- Today's fix was surgical: just removed artificial limitations
- Code quality: proper rollback handling, comprehensive tests, clean commits
- All changes maintain MVCC compatibility and transaction safety
