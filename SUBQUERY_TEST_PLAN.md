# Subquery Testing Plan

**Date:** 2025-11-15
**Goal:** Comprehensive test coverage for subquery implementation
**Estimated Effort:** 1-2 days

---

## Test Organization

We'll create a new test file: `src/test_subqueries.zig`

### Test Categories

1. **Parser Tests** - Verify SQL parsing
2. **IN/NOT IN Tests** - Basic and advanced IN operator tests
3. **EXISTS/NOT EXISTS Tests** - Existence checking
4. **Scalar Subquery Tests** - Single-value comparisons
5. **Edge Case Tests** - NULL handling, empty results, errors
6. **Integration Tests** - Subqueries with JOINs, aggregates, GROUP BY
7. **Nested Subquery Tests** - Subqueries within subqueries

---

## Test Suite Design

### Category 1: Parser Tests (Quick sanity checks)

```zig
test "parser: IN with subquery" {
    const query = "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)";
    // Verify parsing succeeds and creates correct AST
}

test "parser: NOT IN with subquery" {
    const query = "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)";
    // Verify parsing succeeds
}

test "parser: EXISTS with subquery" {
    const query = "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)";
    // Verify parsing succeeds
}

test "parser: scalar subquery" {
    const query = "SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)";
    // Verify parsing succeeds
}

test "parser: nested subqueries" {
    const query = "SELECT * FROM a WHERE id IN (SELECT b_id FROM b WHERE b_id IN (SELECT c_id FROM c))";
    // Verify parsing succeeds with correct nesting
}
```

### Category 2: IN Operator Tests

```zig
test "subquery: IN operator - basic" {
    // Setup: users table with ids 1,2,3
    //        orders table with user_ids 1,2
    // Query: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
    // Expected: Returns users 1 and 2
}

test "subquery: IN operator - no matches" {
    // Setup: users with ids 4,5,6
    //        orders with user_ids 1,2,3
    // Query: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
    // Expected: Returns 0 rows
}

test "subquery: IN operator - all match" {
    // Setup: users with ids 1,2
    //        orders with user_ids 1,2,3,4
    // Query: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
    // Expected: Returns all users
}

test "subquery: IN operator - with WHERE in subquery" {
    // Query: WHERE id IN (SELECT user_id FROM orders WHERE total > 100)
    // Expected: Returns only users with high-value orders
}

test "subquery: IN operator - empty subquery result" {
    // Setup: orders table is empty
    // Query: SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)
    // Expected: Returns 0 rows
}

test "subquery: IN operator - with text columns" {
    // Query: WHERE name IN (SELECT customer_name FROM vip_customers)
    // Expected: Works with text comparison
}
```

### Category 3: NOT IN Operator Tests

```zig
test "subquery: NOT IN operator - basic" {
    // Setup: users with ids 1,2,3
    //        orders with user_ids 1,2
    // Query: SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)
    // Expected: Returns user 3 only
}

test "subquery: NOT IN operator - all excluded" {
    // Setup: All users have orders
    // Query: SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)
    // Expected: Returns 0 rows
}

test "subquery: NOT IN operator - none excluded" {
    // Setup: No users have orders
    // Query: SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM orders)
    // Expected: Returns all users
}

test "subquery: NOT IN with NULL in subquery" {
    // This is a SQL edge case - NOT IN with NULL should return no rows
    // Query: WHERE id NOT IN (SELECT user_id FROM orders) where orders has NULL
    // Expected: SQL-compliant NULL handling
}
```

### Category 4: EXISTS Tests

```zig
test "subquery: EXISTS - basic" {
    // Setup: users 1,2,3 / orders for users 1,2
    // Query: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Expected: Returns users 1,2
}

test "subquery: EXISTS - none exist" {
    // Setup: orders table is empty
    // Query: SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Expected: Returns 0 rows
}

test "subquery: EXISTS - uncorrelated" {
    // Query: WHERE EXISTS (SELECT 1 FROM orders WHERE total > 1000)
    // Expected: If ANY order > 1000, return all users; else return none
}

test "subquery: EXISTS with complex condition" {
    // Query: WHERE EXISTS (SELECT 1 FROM orders WHERE user_id = users.id AND status = 'shipped')
    // Expected: Users with shipped orders
}
```

### Category 5: NOT EXISTS Tests

```zig
test "subquery: NOT EXISTS - basic" {
    // Setup: users 1,2,3 / orders for users 1,2
    // Query: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Expected: Returns user 3 only
}

test "subquery: NOT EXISTS - all exist" {
    // Setup: All users have orders
    // Query: SELECT * FROM users WHERE NOT EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
    // Expected: Returns 0 rows
}
```

### Category 6: Scalar Subquery Tests

```zig
test "subquery: scalar comparison - greater than AVG" {
    // Setup: products with prices 10, 20, 30
    // Query: SELECT * FROM products WHERE price > (SELECT AVG(price) FROM products)
    // Expected: Returns products with price > 20 (the 30 price product)
}

test "subquery: scalar comparison - equals MAX" {
    // Query: WHERE price = (SELECT MAX(price) FROM products)
    // Expected: Returns most expensive product(s)
}

test "subquery: scalar comparison - less than MIN" {
    // Query: WHERE price < (SELECT MIN(price) FROM products)
    // Expected: Returns 0 rows (nothing cheaper than minimum)
}

test "subquery: scalar subquery - empty result returns NULL" {
    // Setup: Empty products table
    // Query: SELECT * FROM items WHERE price > (SELECT AVG(price) FROM products)
    // Expected: Comparison with NULL returns no rows
}

test "subquery: scalar subquery - multiple rows error" {
    // Setup: Subquery that returns 2+ rows
    // Query: WHERE price > (SELECT price FROM products)
    // Expected: Error - SubqueryReturnedMultipleRows
}
```

### Category 7: Edge Cases

```zig
test "subquery: empty subquery result with IN" {
    // Setup: Empty orders table
    // Query: WHERE id IN (SELECT user_id FROM orders)
    // Expected: Returns 0 rows
}

test "subquery: NULL values in subquery result" {
    // Setup: orders with some NULL user_ids
    // Query: WHERE id IN (SELECT user_id FROM orders)
    // Expected: NULLs are ignored in IN comparison
}

test "subquery: NULL in outer column with IN" {
    // Setup: users with some NULL ids
    // Query: WHERE id IN (SELECT user_id FROM orders)
    // Expected: NULL never matches in IN
}

test "subquery: duplicate values in subquery result" {
    // Setup: orders with duplicate user_ids (1,1,2,2,3)
    // Query: WHERE id IN (SELECT user_id FROM orders)
    // Expected: Duplicates don't affect result (returns users 1,2,3)
}

test "subquery: subquery with LIMIT" {
    // Query: WHERE id IN (SELECT user_id FROM orders LIMIT 5)
    // Expected: Works correctly with limited subquery results
}

test "subquery: subquery with ORDER BY" {
    // Query: WHERE id IN (SELECT user_id FROM orders ORDER BY total DESC)
    // Expected: ORDER BY in subquery doesn't affect IN matching
}
```

### Category 8: Integration Tests

```zig
test "subquery: with aggregate functions" {
    // Query: SELECT dept, COUNT(*) FROM employees
    //        WHERE salary > (SELECT AVG(salary) FROM employees)
    //        GROUP BY dept
    // Expected: Aggregation works with subquery filter
}

test "subquery: with JOIN" {
    // Query: SELECT u.name FROM users u
    //        JOIN orders o ON u.id = o.user_id
    //        WHERE u.id IN (SELECT user_id FROM vip_customers)
    // Expected: Subquery works in JOIN query
}

test "subquery: with GROUP BY and HAVING (future)" {
    // Future test when HAVING is implemented
}

test "subquery: multiple subqueries in WHERE" {
    // Query: WHERE id IN (SELECT ...) AND age > (SELECT AVG(age) ...)
    // Expected: Multiple subqueries work together
}

test "subquery: subquery in UPDATE statement" {
    // Query: UPDATE users SET status = 'premium'
    //        WHERE id IN (SELECT user_id FROM high_value_orders)
    // Expected: UPDATE with subquery works
}
```

### Category 9: Nested Subqueries

```zig
test "subquery: nested IN subqueries - 2 levels" {
    // Query: WHERE a_id IN (SELECT b_id FROM b WHERE b_id IN (SELECT c_id FROM c))
    // Expected: Nested subqueries execute correctly
}

test "subquery: nested IN subqueries - 3 levels" {
    // Query: WHERE a IN (SELECT b FROM B WHERE b IN (SELECT c FROM C WHERE c IN (SELECT d FROM D)))
    // Expected: Deep nesting works
}

test "subquery: EXISTS with nested subquery" {
    // Query: WHERE EXISTS (SELECT 1 FROM b WHERE b_id IN (SELECT c_id FROM c))
    // Expected: EXISTS + IN combination works
}

test "subquery: scalar subquery containing subquery" {
    // Query: WHERE price > (SELECT AVG(price) FROM products WHERE category IN (SELECT id FROM premium_categories))
    // Expected: Scalar subquery with nested IN works
}
```

### Category 10: Error Handling

```zig
test "subquery: error - scalar subquery returns multiple rows" {
    // Query: WHERE price > (SELECT price FROM products)
    // Expected: Returns error.SubqueryReturnedMultipleRows
}

test "subquery: error - IN subquery returns multiple columns" {
    // Query: WHERE id IN (SELECT id, name FROM users)
    // Expected: Returns error.InvalidSubquery
}

test "subquery: error - table not found in subquery" {
    // Query: WHERE id IN (SELECT user_id FROM nonexistent_table)
    // Expected: Returns error.TableNotFound
}

test "subquery: error - column not found in subquery" {
    // Query: WHERE id IN (SELECT nonexistent_column FROM orders)
    // Expected: Returns error.ColumnNotFound
}
```

---

## Implementation Strategy

### Step 1: Test File Setup
1. Create `src/test_subqueries.zig`
2. Import necessary modules (Database, sql, std.testing)
3. Set up helper functions for test data

### Step 2: Basic Tests First
1. Implement parser tests (quick validation)
2. Implement IN operator tests (most common use case)
3. Implement NOT IN operator tests
4. Verify basic functionality works

### Step 3: Advanced Tests
1. EXISTS/NOT EXISTS tests
2. Scalar subquery tests
3. Edge case tests
4. Error handling tests

### Step 4: Integration Tests
1. Tests with JOINs
2. Tests with aggregates
3. Tests with GROUP BY
4. Nested subquery tests

### Step 5: Run and Fix
1. Run all tests: `zig build test`
2. Fix any failures
3. Add additional tests for uncovered cases

---

## Test Data Fixtures

We'll need consistent test data across tests:

```zig
fn setupBasicTables(db: *Database) !void {
    // Users table
    _ = try db.execute("CREATE TABLE users (id int, name text, age int)");
    _ = try db.execute("INSERT INTO users VALUES (1, 'Alice', 25)");
    _ = try db.execute("INSERT INTO users VALUES (2, 'Bob', 30)");
    _ = try db.execute("INSERT INTO users VALUES (3, 'Charlie', 35)");

    // Orders table
    _ = try db.execute("CREATE TABLE orders (id int, user_id int, total float)");
    _ = try db.execute("INSERT INTO orders VALUES (1, 1, 100.0)");
    _ = try db.execute("INSERT INTO orders VALUES (2, 1, 200.0)");
    _ = try db.execute("INSERT INTO orders VALUES (3, 2, 50.0)");
}

fn setupProductTables(db: *Database) !void {
    // Products table for scalar tests
    _ = try db.execute("CREATE TABLE products (id int, name text, price float, category text)");
    _ = try db.execute("INSERT INTO products VALUES (1, 'Widget', 10.0, 'tools')");
    _ = try db.execute("INSERT INTO products VALUES (2, 'Gadget', 20.0, 'electronics')");
    _ = try db.execute("INSERT INTO products VALUES (3, 'Doohickey', 30.0, 'tools')");
}
```

---

## Success Criteria

- [ ] All 40+ tests pass
- [ ] Parser correctly handles all subquery syntax
- [ ] IN/NOT IN work correctly with various data types
- [ ] EXISTS/NOT EXISTS work correctly
- [ ] Scalar subqueries work with all comparison operators
- [ ] Edge cases handled correctly (NULL, empty results, duplicates)
- [ ] Error cases return appropriate errors
- [ ] Integration with other SQL features works
- [ ] Nested subqueries work correctly
- [ ] No memory leaks (all results properly deinit'd)

---

## Timeline

**Day 1:**
- Morning: Create test file, setup fixtures
- Afternoon: Implement parser tests + IN/NOT IN tests (10-12 tests)

**Day 2:**
- Morning: EXISTS/NOT EXISTS + scalar subquery tests (10-12 tests)
- Afternoon: Edge cases + integration tests (10-15 tests)
- Evening: Nested subqueries + error handling (8-10 tests)

**Day 3 (if needed):**
- Fix any test failures
- Add additional edge case tests
- Performance testing (optional)

---

## Next Steps

Ready to start implementing! We'll:
1. Create `src/test_subqueries.zig`
2. Start with basic tests (IN operator)
3. Gradually add more complex tests
4. Fix any issues we discover

Let's begin! 🚀
