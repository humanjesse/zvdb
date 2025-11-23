# Contributing to ZVDB

Guide for contributing code, tests, and documentation.

## Development Setup

**Requirements:**
- Zig 0.15.2+
- Git

**Clone and build:**

```bash
git clone https://github.com/humanjesse/zvdb
cd zvdb
zig build
zig build test
```

## Code Style

### Zig Conventions

Follow standard Zig style:
- 4-space indentation
- `camelCase` for functions and variables
- `PascalCase` for types
- `snake_case` for file names
- `SCREAMING_SNAKE_CASE` for constants

Run formatter before committing:

```bash
zig fmt .
```

### Documentation

- Document all public functions with doc comments
- Explain complex algorithms inline
- Keep comments concise and accurate

```zig
/// Insert vector into HNSW index with optional custom ID.
/// Returns assigned external ID. Throws error.DuplicateExternalId if ID exists.
pub fn insert(self: *Self, point: []const T, external_id: ?u64) !u64 {
    // Implementation...
}
```

### Error Handling

- Use Zig error unions: `!ReturnType`
- Define specific error types
- Document possible errors in function comments
- Clean up resources with `defer`/`errdefer`

## Testing

### Writing Tests

Place tests in `src/test_*.zig` files.

**Test structure:**

```zig
const std = @import("std");
const testing = std.testing;
const zvdb = @import("zvdb.zig");

test "feature description" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Setup
    var db = zvdb.Database.init(allocator);
    defer db.deinit();

    // Exercise
    try db.execute("CREATE TABLE test (id int)");

    // Verify
    try testing.expect(db.tables.count() == 1);
}
```

**What to test:**
- Core functionality
- Edge cases (empty input, null values, max sizes)
- Error conditions
- MVCC scenarios
- Integration between modules

### Running Tests

```bash
# All tests
zig build test

# Specific test file
zig test src/test_hnsw.zig

# With coverage (future)
zig build test --summary all
```

### Benchmarks

Performance tests in `benchmarks/` directory.

Run:

```bash
zig build bench-single
zig build bench-multi
```

## Making Changes

### Workflow

1. **Create branch:**

```bash
git checkout -b feature/your-feature-name
```

2. **Make changes:**
   - Write code
   - Add tests
   - Update documentation

3. **Test thoroughly:**

```bash
zig build test
zig fmt .
```

4. **Commit:**

```bash
git add .
git commit -m "Add feature: brief description"
```

5. **Push and create PR:**

```bash
git push origin feature/your-feature-name
```

### Commit Messages

Format: `<type>: <description>`

Types:
- `feat:` New feature
- `fix:` Bug fix
- `docs:` Documentation only
- `test:` Add/update tests
- `refactor:` Code restructuring
- `perf:` Performance improvement

Examples:
- `feat: Add support for CROSS JOIN`
- `fix: Correct MVCC visibility check for concurrent updates`
- `docs: Update API reference for HNSW.search()`

## Areas Needing Help

### High Priority

1. **Query Optimizer**
   - Cost estimation improvements
   - Additional join strategies
   - Index selection heuristics

2. **Testing**
   - More MVCC concurrency tests
   - Stress tests for large datasets
   - Edge case coverage

3. **Performance**
   - HNSW parameter tuning
   - Memory allocator optimization
   - Vectorization opportunities

4. **Documentation**
   - More examples
   - Tutorial improvements
   - API clarifications

### Medium Priority

5. **Features**
   - Additional aggregate functions (STDDEV, MEDIAN)
   - Window functions
   - Common Table Expressions (CTEs)
   - UPSERT support

6. **Tooling**
   - CLI client
   - Benchmarking suite expansion
   - Migration tools

### Low Priority

7. **Nice to Have**
   - Compression
   - Replication
   - Query plan visualization

## Pull Request Guidelines

### Before Submitting

- [ ] Tests pass: `zig build test`
- [ ] Code formatted: `zig fmt .`
- [ ] Documentation updated
- [ ] New tests added for new features
- [ ] No compiler warnings

### PR Description

Include:
- **What:** Brief description of changes
- **Why:** Motivation and context
- **How:** Implementation approach
- **Testing:** How you tested the changes

Template:

```markdown
## Summary
Brief description of what this PR does.

## Motivation
Why is this change needed?

## Implementation
Key implementation details and design decisions.

## Testing
- [ ] Unit tests added
- [ ] Integration tests added
- [ ] Manual testing performed

## Checklist
- [ ] Tests pass
- [ ] Code formatted
- [ ] Documentation updated
```

### Review Process

1. Automated checks run (CI/CD)
2. Code review by maintainer
3. Address feedback
4. Approval and merge

## Code Review

### Reviewing Others' PRs

Focus on:
- Correctness
- Test coverage
- Code clarity
- Performance implications
- MVCC correctness (critical!)

Be constructive and specific.

### Receiving Feedback

- View feedback as improvement opportunity
- Ask questions if unclear
- Make requested changes promptly
- Update PR description if approach changes

## Debugging Tips

### Common Issues

**Memory leaks:**

```bash
zig build test --summary all
# Check for allocator leak reports
```

**MVCC bugs:**
- Print transaction IDs and xmin/xmax
- Check commit log state
- Verify snapshot creation

**Parser issues:**
- Add debug prints in tokenizer
- Check AST structure
- Test with minimal SQL

### Tools

- `std.debug.print()` for logging
- `@breakpoint()` for debugger integration
- Valgrind for memory analysis (Linux)

## Release Process

(Maintainers only)

1. Update version in build.zig.zon
2. Update CHANGELOG.md
3. Tag release: `git tag v0.x.0`
4. Push tag: `git push origin v0.x.0`
5. GitHub Actions builds and publishes

## Questions?

- Open GitHub issue for bugs
- Discussion forum for questions
- Email maintainers for security issues

## License

By contributing, you agree to license your contributions under the project's MIT license.
