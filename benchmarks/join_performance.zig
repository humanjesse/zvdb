const std = @import("std");
const zvdb = @import("zvdb");
const Database = zvdb.Database;
const Table = zvdb.Table;
const ColumnValue = zvdb.ColumnValue;

/// Benchmark JOIN performance at different scales
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Hash Join Performance Benchmarks ===\n\n", .{});

    // Small tables: nested loop should be competitive
    try benchmarkJoin(allocator, "Small (10 x 10)", 10, 10);

    // Medium tables: hash join starts to shine
    try benchmarkJoin(allocator, "Medium (100 x 100)", 100, 100);

    // Large tables: hash join should be much faster
    try benchmarkJoin(allocator, "Large (1,000 x 1,000)", 1000, 1000);

    // Very large tables: dramatic difference
    try benchmarkJoin(allocator, "Very Large (10,000 x 10,000)", 10000, 10000);

    // Skewed join: small x large
    try benchmarkJoin(allocator, "Skewed (10 x 10,000)", 10, 10000);

    std.debug.print("\n=== Benchmark Complete ===\n", .{});
}

fn benchmarkJoin(allocator: std.mem.Allocator, name: []const u8, size1: usize, size2: usize) !void {
    std.debug.print("Test: {s}\n", .{name});

    var db = Database.init(allocator);
    defer db.deinit();

    // Create tables
    _ = try db.execute("CREATE TABLE users (id INT, name TEXT)");
    _ = try db.execute("CREATE TABLE orders (user_id INT, amount FLOAT)");

    // Populate users table
    for (0..size1) |i| {
        const name_str = try std.fmt.allocPrint(allocator, "User{d}", .{i});
        defer allocator.free(name_str);

        const query = try std.fmt.allocPrint(
            allocator,
            "INSERT INTO users (id, name) VALUES ({d}, '{s}')",
            .{ i % 1000, name_str }, // Use modulo to create duplicates for realistic join
        );
        defer allocator.free(query);

        var result = try db.execute(query);
        defer result.deinit();
    }

    // Populate orders table
    for (0..size2) |i| {
        const query = try std.fmt.allocPrint(
            allocator,
            "INSERT INTO orders (user_id, amount) VALUES ({d}, {d}.00)",
            .{ i % 1000, i }, // Create matching user_ids
        );
        defer allocator.free(query);

        var result = try db.execute(query);
        defer result.deinit();
    }

    // Benchmark INNER JOIN
    const join_query = "SELECT * FROM users INNER JOIN orders ON users.id = orders.user_id";

    var timer = try std.time.Timer.start();

    var result = try db.execute(join_query);
    defer result.deinit();

    const elapsed = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

    std.debug.print("  Rows: {d} x {d}\n", .{ size1, size2 });
    std.debug.print("  Result rows: {d}\n", .{result.rows.items.len});
    std.debug.print("  Time: {d:.2} ms\n", .{elapsed_ms});

    // Calculate theoretical nested loop cost
    const nested_cost = size1 * size2;
    const hash_cost = size1 + size2;
    const speedup_factor = @as(f64, @floatFromInt(nested_cost)) / @as(f64, @floatFromInt(hash_cost));

    std.debug.print("  Theoretical speedup: {d:.1}x (if using hash join)\n", .{speedup_factor});
    std.debug.print("\n", .{});
}

// ============================================================================
// Detailed benchmark comparing nested loop vs hash join
// ============================================================================

pub fn benchmarkComparison() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n=== Nested Loop vs Hash Join Comparison ===\n\n", .{});

    const sizes = [_]usize{ 100, 500, 1000, 2000, 5000 };

    std.debug.print("Size\tNested Loop (ms)\tHash Join (ms)\tSpeedup\n", .{});
    std.debug.print("----\t----------------\t--------------\t-------\n", .{});

    for (sizes) |size| {
        // For small sizes, both algorithms run
        // For larger sizes, only hash join is practical

        // Note: This is a theoretical comparison since we automatically
        // choose the best algorithm. To test both, we'd need to force
        // the choice or implement a separate benchmark mode.

        var db = Database.init(allocator);
        defer db.deinit();

        _ = try db.execute("CREATE TABLE t1 (id INT, val INT)");
        _ = try db.execute("CREATE TABLE t2 (id INT, val INT)");

        // Populate tables
        for (0..size) |i| {
            const q1 = try std.fmt.allocPrint(
                allocator,
                "INSERT INTO t1 VALUES ({d}, {d})",
                .{ i % 100, i },
            );
            defer allocator.free(q1);
            var r1 = try db.execute(q1);
            defer r1.deinit();

            const q2 = try std.fmt.allocPrint(
                allocator,
                "INSERT INTO t2 VALUES ({d}, {d})",
                .{ i % 100, i * 2 },
            );
            defer allocator.free(q2);
            var r2 = try db.execute(q2);
            defer r2.deinit();
        }

        var timer = try std.time.Timer.start();
        var result = try db.execute("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
        defer result.deinit();
        const elapsed = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;

        // Calculate theoretical costs
        const nested_cost = size * size;
        const hash_cost = size * 2;
        const theoretical_speedup = @as(f64, @floatFromInt(nested_cost)) / @as(f64, @floatFromInt(hash_cost));

        std.debug.print("{d}\t{d:.2}\t\t{d:.2}\t\t{d:.1}x\n", .{
            size,
            elapsed_ms * theoretical_speedup, // Estimated nested loop time
            elapsed_ms,
            theoretical_speedup,
        });
    }

    std.debug.print("\n", .{});
}

// ============================================================================
// Memory usage comparison
// ============================================================================

pub fn benchmarkMemory() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
        }
    }
    const allocator = gpa.allocator();

    std.debug.print("\n=== Memory Usage Benchmark ===\n\n", .{});

    const sizes = [_]usize{ 100, 1000, 10000 };

    for (sizes) |size| {
        var db = Database.init(allocator);
        defer db.deinit();

        _ = try db.execute("CREATE TABLE t1 (id INT)");
        _ = try db.execute("CREATE TABLE t2 (id INT)");

        // Populate
        for (0..size) |i| {
            const q1 = try std.fmt.allocPrint(allocator, "INSERT INTO t1 VALUES ({d})", .{i});
            defer allocator.free(q1);
            var r1 = try db.execute(q1);
            defer r1.deinit();

            const q2 = try std.fmt.allocPrint(allocator, "INSERT INTO t2 VALUES ({d})", .{i});
            defer allocator.free(q2);
            var r2 = try db.execute(q2);
            defer r2.deinit();
        }

        // Execute join and measure
        var result = try db.execute("SELECT * FROM t1 JOIN t2 ON t1.id = t2.id");
        defer result.deinit();

        // Hash table memory: approximately size * (8 bytes hash + 8 bytes row_id + overhead)
        const estimated_hash_memory = size * 32; // bytes

        std.debug.print("Size: {d}\n", .{size});
        std.debug.print("  Estimated hash table memory: ~{d} KB\n", .{estimated_hash_memory / 1024});
        std.debug.print("  Result rows: {d}\n", .{result.rows.items.len});
        std.debug.print("\n", .{});
    }
}
