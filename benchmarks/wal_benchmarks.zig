const std = @import("std");
const zvdb = @import("zvdb");
const Database = zvdb.Database;
const WalWriter = @import("../src/wal.zig").WalWriter;
const WalReader = @import("../src/wal.zig").WalReader;
const WalRecord = @import("../src/wal.zig").WalRecord;
const WalRecordType = @import("../src/wal.zig").WalRecordType;
const Table = @import("../src/table.zig").Table;
const ColumnValue = @import("../src/table.zig").ColumnValue;

/// Benchmark configuration
const BenchmarkConfig = struct {
    num_operations: usize,
    table_name: []const u8,
    wal_dir: []const u8,
};

/// Benchmark results
const BenchmarkResult = struct {
    operation: []const u8,
    total_time_ns: u64,
    operations: usize,
    ops_per_second: f64,
    avg_latency_us: f64,

    pub fn print(self: BenchmarkResult) void {
        std.debug.print("\n{s} Benchmark Results:\n", .{self.operation});
        std.debug.print("  Operations: {d}\n", .{self.operations});
        std.debug.print("  Total time: {d:.2} seconds\n", .{@as(f64, @floatFromInt(self.total_time_ns)) / 1_000_000_000.0});
        std.debug.print("  Throughput: {d:.2} ops/sec\n", .{self.ops_per_second});
        std.debug.print("  Average latency: {d:.2} μs\n", .{self.avg_latency_us});
    }
};

/// Calculate benchmark result from timing data
fn calculateResult(operation: []const u8, total_time_ns: u64, operations: usize) BenchmarkResult {
    const ops_per_second = @as(f64, @floatFromInt(operations)) / (@as(f64, @floatFromInt(total_time_ns)) / 1_000_000_000.0);
    const avg_latency_us = @as(f64, @floatFromInt(total_time_ns)) / @as(f64, @floatFromInt(operations)) / 1_000.0;

    return BenchmarkResult{
        .operation = operation,
        .total_time_ns = total_time_ns,
        .operations = operations,
        .ops_per_second = ops_per_second,
        .avg_latency_us = avg_latency_us,
    };
}

/// Benchmark 1: Raw WAL write throughput
/// Measures how fast we can write records to WAL (no database operations)
pub fn benchmarkWalWriteThroughput(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // Clean up test directory
    std.fs.cwd().deleteTree(config.wal_dir) catch {};
    defer std.fs.cwd().deleteTree(config.wal_dir) catch {};

    var writer = try WalWriter.init(allocator, config.wal_dir);
    defer writer.deinit();

    // Prepare a sample record
    const sample_data = "sample_row_data_here";

    var timer = try std.time.Timer.start();

    // Write records as fast as possible
    var i: usize = 0;
    while (i < config.num_operations) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = config.table_name,
            .data = sample_data,
            .checksum = 0,
        });

        // Flush periodically (simulate commit)
        if (i % 100 == 0) {
            try writer.flush();
        }
    }

    // Final flush
    try writer.flush();

    const elapsed = timer.read();
    return calculateResult("WAL Write Throughput", elapsed, config.num_operations);
}

/// Benchmark 2: WAL read throughput
/// Measures how fast we can read records from WAL
pub fn benchmarkWalReadThroughput(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // First write some data
    {
        var writer = try WalWriter.init(allocator, config.wal_dir);
        defer writer.deinit();

        const sample_data = "sample_row_data_here";
        var i: usize = 0;
        while (i < config.num_operations) : (i += 1) {
            _ = try writer.writeRecord(.{
                .record_type = .insert_row,
                .tx_id = @intCast(i),
                .lsn = 0,
                .row_id = @intCast(i),
                .table_name = config.table_name,
                .data = sample_data,
                .checksum = 0,
            });
        }
        try writer.flush();
    }

    // Now benchmark reading
    const wal_file = try std.fmt.allocPrint(allocator, "{s}/wal.000000", .{config.wal_dir});
    defer allocator.free(wal_file);

    var timer = try std.time.Timer.start();

    var reader = try WalReader.init(allocator, wal_file);
    defer reader.deinit();

    var count: usize = 0;
    while (try reader.readRecord()) |record_opt| {
        var record = record_opt;
        defer record.deinit(allocator);
        count += 1;
    }

    const elapsed = timer.read();

    if (count != config.num_operations) {
        return error.RecordCountMismatch;
    }

    return calculateResult("WAL Read Throughput", elapsed, count);
}

/// Benchmark 3: Database INSERT with WAL overhead
/// Measures the overhead of WAL on INSERT operations
pub fn benchmarkInsertWithWal(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // Clean up
    std.fs.cwd().deleteTree(config.wal_dir) catch {};
    defer std.fs.cwd().deleteTree(config.wal_dir) catch {};

    var db = Database.init(allocator);
    defer db.deinit();

    try db.enableWal(config.wal_dir);

    // Create table
    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var timer = try std.time.Timer.start();

    // Insert records
    var i: usize = 0;
    while (i < config.num_operations) : (i += 1) {
        var buf: [100]u8 = undefined;
        const query = try std.fmt.bufPrint(&buf, "INSERT INTO users VALUES ({d}, \"user_{d}\", {d})", .{ i, i, 20 + (i % 50) });
        var result = try db.execute(query);
        defer result.deinit();
    }

    const elapsed = timer.read();
    return calculateResult("INSERT with WAL", elapsed, config.num_operations);
}

/// Benchmark 4: Database INSERT without WAL (baseline)
/// Measures baseline INSERT performance without WAL
pub fn benchmarkInsertWithoutWal(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    var db = Database.init(allocator);
    defer db.deinit();

    // Create table (no WAL)
    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var timer = try std.time.Timer.start();

    // Insert records
    var i: usize = 0;
    while (i < config.num_operations) : (i += 1) {
        var buf: [100]u8 = undefined;
        const query = try std.fmt.bufPrint(&buf, "INSERT INTO users VALUES ({d}, \"user_{d}\", {d})", .{ i, i, 20 + (i % 50) });
        var result = try db.execute(query);
        defer result.deinit();
    }

    const elapsed = timer.read();
    return calculateResult("INSERT without WAL (baseline)", elapsed, config.num_operations);
}

/// Benchmark 5: WAL Recovery time
/// Measures how long it takes to recover from WAL
pub fn benchmarkRecoveryTime(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // Clean up
    std.fs.cwd().deleteTree(config.wal_dir) catch {};
    defer std.fs.cwd().deleteTree(config.wal_dir) catch {};

    // Phase 1: Create WAL data
    {
        var db = Database.init(allocator);
        defer db.deinit();

        try db.enableWal(config.wal_dir);

        var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer create_result.deinit();

        var i: usize = 0;
        while (i < config.num_operations) : (i += 1) {
            var buf: [100]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO users VALUES ({d}, \"user_{d}\", {d})", .{ i, i, 20 + (i % 50) });
            var result = try db.execute(query);
            defer result.deinit();
        }
    }

    // Phase 2: Benchmark recovery
    var db = Database.init(allocator);
    defer db.deinit();

    try db.enableWal(config.wal_dir);

    var create_result = try db.execute("CREATE TABLE users (id int, name text, age int)");
    defer create_result.deinit();

    var timer = try std.time.Timer.start();

    const recovered = try db.recoverFromWal(config.wal_dir);

    const elapsed = timer.read();

    if (recovered != config.num_operations) {
        std.debug.print("Warning: Expected {d} recovered, got {d}\n", .{ config.num_operations, recovered });
    }

    return calculateResult("WAL Recovery", elapsed, config.num_operations);
}

/// Benchmark 6: WAL + HNSW Recovery time
/// Measures recovery time including HNSW index rebuild
pub fn benchmarkRecoveryWithHnsw(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // Clean up
    std.fs.cwd().deleteTree(config.wal_dir) catch {};
    defer std.fs.cwd().deleteTree(config.wal_dir) catch {};

    // Phase 1: Create WAL data with embeddings
    {
        var db = Database.init(allocator);
        defer db.deinit();

        try db.initVectorSearch(16, 200);
        try db.enableWal(config.wal_dir);

        var create_result = try db.execute("CREATE TABLE docs (id int, title text, vec embedding(768))");
        defer create_result.deinit();

        const table = db.tables.get("docs").?;

        var i: usize = 0;
        while (i < config.num_operations) : (i += 1) {
            var buf: [100]u8 = undefined;
            const query = try std.fmt.bufPrint(&buf, "INSERT INTO docs VALUES ({d}, \"doc_{d}\", NULL)", .{ i, i });
            var result = try db.execute(query);
            defer result.deinit();

            // Add embedding
            var embedding = [_]f32{@as(f32, @floatFromInt(i)) * 0.01} ** 128;
            const row = table.get(i).?;
            const emb = try allocator.dupe(f32, &embedding);
            defer allocator.free(emb);
            try row.set(allocator, "vec", ColumnValue{ .embedding = emb });
            _ = try db.hnsw.?.insert(&embedding, i);
        }
    }

    // Phase 2: Benchmark recovery + HNSW rebuild
    var db = Database.init(allocator);
    defer db.deinit();

    try db.initVectorSearch(16, 200);
    try db.enableWal(config.wal_dir);

    var create_result = try db.execute("CREATE TABLE docs (id int, title text, vec embedding(768))");
    defer create_result.deinit();

    var timer = try std.time.Timer.start();

    const recovered = try db.recoverFromWal(config.wal_dir);
    const vectors_indexed = try db.rebuildHnswFromTables();

    const elapsed = timer.read();

    if (recovered != config.num_operations) {
        std.debug.print("Warning: Expected {d} recovered, got {d}\n", .{ config.num_operations, recovered });
    }

    if (vectors_indexed != config.num_operations) {
        std.debug.print("Warning: Expected {d} vectors, got {d}\n", .{ config.num_operations, vectors_indexed });
    }

    return calculateResult("WAL Recovery + HNSW Rebuild", elapsed, config.num_operations);
}

/// Benchmark 7: WAL file rotation overhead
/// Measures the overhead of file rotation
pub fn benchmarkFileRotation(allocator: std.mem.Allocator, config: BenchmarkConfig) !BenchmarkResult {
    // Clean up
    std.fs.cwd().deleteTree(config.wal_dir) catch {};
    defer std.fs.cwd().deleteTree(config.wal_dir) catch {};

    // Use small file size to force frequent rotation
    var writer = try WalWriter.initWithOptions(allocator, config.wal_dir, .{
        .max_file_size = 4096, // 4KB - very small to force rotation
    });
    defer writer.deinit();

    const sample_data = "sample_row_data_here_with_some_extra_bytes_to_fill_space";

    var timer = try std.time.Timer.start();

    var i: usize = 0;
    while (i < config.num_operations) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = config.table_name,
            .data = sample_data,
            .checksum = 0,
        });
    }

    try writer.flush();

    const elapsed = timer.read();

    const final_sequence = writer.getCurrentSequence();
    std.debug.print("  File rotations: {d}\n", .{final_sequence});

    return calculateResult("WAL File Rotation", elapsed, config.num_operations);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("WAL Performance Benchmarks - Phase 2.5\n", .{});
    std.debug.print("=" ** 80 ++ "\n", .{});

    // Benchmark configurations for different scales
    const configs = [_]BenchmarkConfig{
        .{ .num_operations = 1000, .table_name = "users", .wal_dir = "benchmark_wal_1k" },
        .{ .num_operations = 10000, .table_name = "users", .wal_dir = "benchmark_wal_10k" },
        .{ .num_operations = 50000, .table_name = "users", .wal_dir = "benchmark_wal_50k" },
    };

    for (configs) |config| {
        std.debug.print("\n" ++ "-" ** 80 ++ "\n", .{});
        std.debug.print("Scale: {d} operations\n", .{config.num_operations});
        std.debug.print("-" ** 80 ++ "\n", .{});

        // Benchmark 1: Raw WAL write
        const wal_write = try benchmarkWalWriteThroughput(allocator, config);
        wal_write.print();

        // Benchmark 2: Raw WAL read
        const wal_read = try benchmarkWalReadThroughput(allocator, config);
        wal_read.print();

        // Benchmark 3 & 4: INSERT with and without WAL
        const insert_without = try benchmarkInsertWithoutWal(allocator, config);
        insert_without.print();

        const insert_with = try benchmarkInsertWithWal(allocator, config);
        insert_with.print();

        // Calculate overhead percentage
        const overhead_pct = ((insert_with.avg_latency_us - insert_without.avg_latency_us) / insert_without.avg_latency_us) * 100.0;
        std.debug.print("\n  WAL Overhead: {d:.2}%\n", .{overhead_pct});

        // Benchmark 5: Recovery time
        const recovery = try benchmarkRecoveryTime(allocator, config);
        recovery.print();

        // Benchmark 6: Recovery with HNSW (only for smaller datasets)
        if (config.num_operations <= 10000) {
            const recovery_hnsw = try benchmarkRecoveryWithHnsw(allocator, config);
            recovery_hnsw.print();
        }

        // Benchmark 7: File rotation (only for larger datasets)
        if (config.num_operations >= 10000) {
            const rotation = try benchmarkFileRotation(allocator, config);
            rotation.print();
        }
    }

    std.debug.print("\n" ++ "=" ** 80 ++ "\n", .{});
    std.debug.print("Benchmarks Complete!\n", .{});
    std.debug.print("=" ** 80 ++ "\n\n", .{});
}
