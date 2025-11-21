// ============================================================================
// Concurrent Delete Stress Test
// ============================================================================
//
// This test verifies the MVCC race condition fix in table.zig delete() function.
// It spawns 100 threads that all attempt to delete the same row simultaneously,
// ensuring that:
// 1. Only ONE thread succeeds (atomic CAS prevents race conditions)
// 2. All other threads receive SerializationFailure error
// 3. No data corruption occurs (no silent lost updates)
//
// This test specifically validates the fix using @cmpxchgWeak for atomic
// check-and-set operations on xmax field.
// ============================================================================

const std = @import("std");
const testing = std.testing;
const Database = @import("database/core.zig").Database;
const Table = @import("table.zig").Table;
const ColumnValue = @import("table.zig").ColumnValue;
const TransactionManager = @import("transaction.zig").TransactionManager;
const CommitLog = @import("transaction.zig").CommitLog;

// Test configuration
const NUM_THREADS = 100;
const NUM_ROWS = 10;

// Thread context for concurrent delete operations
const DeleteThreadContext = struct {
    table: *Table,
    row_id: u64,
    tx_id: u64,
    result: ?DeleteResult,
    clog: *CommitLog,
};

const DeleteResult = enum {
    success,
    serialization_failure,
    row_not_found,
    other_error,
};

// Thread function that attempts to delete a row
fn deleteThreadFn(context: *DeleteThreadContext) void {
    context.result = blk: {
        context.table.delete(context.row_id, context.tx_id, context.clog) catch |err| {
            break :blk switch (err) {
                error.SerializationFailure => DeleteResult.serialization_failure,
                error.RowNotFound => DeleteResult.row_not_found,
            };
        };
        break :blk DeleteResult.success;
    };
}

test "Concurrent Delete Stress Test: 100 threads deleting same row" {
    const allocator = testing.allocator;

    // Create table
    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    // Add columns
    try table.addColumn("id", .int);
    try table.addColumn("name", .text);

    // Create commit log for MVCC
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Insert a test row with transaction ID 1
    var values = std.StringHashMap(ColumnValue).init(allocator);
    defer {
        var it = values.iterator();
        while (it.next()) |entry| {
            var val = entry.value_ptr.*;
            val.deinit(allocator);
        }
        values.deinit();
    }

    try values.put("id", ColumnValue{ .int = 42 });
    try values.put("name", ColumnValue{ .text = try allocator.dupe(u8, "Test Row") });

    const tx_id: u64 = 1;
    try table.insertWithId(1, values, tx_id);

    // Mark transaction 1 as committed
    try clog.setStatus(tx_id, .committed);

    // Create contexts for NUM_THREADS concurrent delete attempts
    var contexts: [NUM_THREADS]DeleteThreadContext = undefined;
    var threads: [NUM_THREADS]std.Thread = undefined;

    // Initialize contexts - each thread gets a unique transaction ID
    for (&contexts, 0..) |*ctx, i| {
        ctx.* = DeleteThreadContext{
            .table = &table,
            .row_id = 1,
            .tx_id = @as(u64, i + 100), // TXIDs starting from 100
            .result = null,
            .clog = &clog,
        };
    }

    // Spawn all threads
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, deleteThreadFn, .{&contexts[i]});
    }

    // Wait for all threads to complete
    for (&threads) |*thread| {
        thread.join();
    }

    // Analyze results
    var success_count: usize = 0;
    var serialization_failure_count: usize = 0;
    var other_failures: usize = 0;

    for (contexts) |ctx| {
        const result = ctx.result orelse {
            std.debug.print("ERROR: Thread result is unexpectedly null\n", .{});
            return error.ThreadResultNull;
        };

        switch (result) {
            .success => success_count += 1,
            .serialization_failure => serialization_failure_count += 1,
            .row_not_found => other_failures += 1,
            .other_error => other_failures += 1,
        }
    }

    // Verification:
    // 1. Exactly ONE thread should succeed (the one that won the atomic CAS race)
    try testing.expectEqual(@as(usize, 1), success_count);

    // 2. All other threads should get SerializationFailure
    try testing.expectEqual(@as(usize, NUM_THREADS - 1), serialization_failure_count);

    // 3. No other errors should occur
    try testing.expectEqual(@as(usize, 0), other_failures);

    // 4. The row should be marked as deleted (xmax != 0)
    const chain_head = table.version_chains.get(1) orelse {
        std.debug.print("ERROR: Row 1 should still exist in version_chains after delete\n", .{});
        return error.RowNotFoundInVersionChains;
    };
    try testing.expect(chain_head.xmax != 0);
}

test "Concurrent Update Stress Test: 100 threads updating same row" {
    const allocator = testing.allocator;

    // Create table
    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    // Add columns
    try table.addColumn("id", .int);
    try table.addColumn("counter", .int);

    // Create commit log for MVCC
    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Insert a test row with transaction ID 1
    var values = std.StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();

    try values.put("id", ColumnValue{ .int = 1 });
    try values.put("counter", ColumnValue{ .int = 0 });

    const initial_tx_id: u64 = 1;
    try table.insertWithId(1, values, initial_tx_id);

    // Mark transaction 1 as committed
    try clog.setStatus(initial_tx_id, .committed);

    // Thread context for concurrent updates
    const UpdateThreadContext = struct {
        table: *Table,
        row_id: u64,
        tx_id: u64,
        result: ?DeleteResult, // Reuse enum
        clog: *CommitLog,
    };

    const updateThreadFn = struct {
        fn func(context: *UpdateThreadContext) void {
            context.result = blk: {
                context.table.update(
                    context.row_id,
                    "counter",
                    ColumnValue{ .int = @as(i64, @intCast(context.tx_id)) },
                    context.tx_id,
                    context.clog,
                ) catch |err| {
                    break :blk switch (err) {
                        error.SerializationFailure => DeleteResult.serialization_failure,
                        error.RowNotFound => DeleteResult.row_not_found,
                        else => DeleteResult.other_error,
                    };
                };
                break :blk DeleteResult.success;
            };
        }
    }.func;

    var update_contexts: [NUM_THREADS]UpdateThreadContext = undefined;
    var update_threads: [NUM_THREADS]std.Thread = undefined;

    // Initialize contexts
    for (&update_contexts, 0..) |*ctx, i| {
        ctx.* = UpdateThreadContext{
            .table = &table,
            .row_id = 1,
            .tx_id = @as(u64, i + 100),
            .result = null,
            .clog = &clog,
        };
    }

    // Spawn all threads
    for (&update_threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, updateThreadFn, .{&update_contexts[i]});
    }

    // Wait for all threads
    for (&update_threads) |*thread| {
        thread.join();
    }

    // Analyze results
    var success_count: usize = 0;
    var serialization_failure_count: usize = 0;

    for (update_contexts) |ctx| {
        const result = ctx.result orelse continue;
        switch (result) {
            .success => success_count += 1,
            .serialization_failure => serialization_failure_count += 1,
            else => {},
        }
    }

    // Exactly ONE thread should succeed in updating
    try testing.expectEqual(@as(usize, 1), success_count);

    // All others should get serialization failure
    try testing.expectEqual(@as(usize, NUM_THREADS - 1), serialization_failure_count);
}

test "Mixed Concurrent Operations: Deletes and Updates on same row" {
    const allocator = testing.allocator;

    var table = try Table.init(allocator, "test_table");
    defer table.deinit();

    try table.addColumn("id", .int);
    try table.addColumn("value", .int);

    var clog = CommitLog.init(allocator);
    defer clog.deinit();

    // Insert test row
    var values = std.StringHashMap(ColumnValue).init(allocator);
    defer values.deinit();
    try values.put("id", ColumnValue{ .int = 1 });
    try values.put("value", ColumnValue{ .int = 0 });

    try table.insertWithId(1, values, 1);
    try clog.setStatus(1, .committed);

    // Mixed operation context
    const MixedOpContext = struct {
        table: *Table,
        row_id: u64,
        tx_id: u64,
        is_delete: bool,
        result: ?DeleteResult,
        clog: *CommitLog,
    };

    const mixedOpThreadFn = struct {
        fn func(context: *MixedOpContext) void {
            context.result = blk: {
                if (context.is_delete) {
                    context.table.delete(context.row_id, context.tx_id, context.clog) catch |err| {
                        break :blk switch (err) {
                            error.SerializationFailure => DeleteResult.serialization_failure,
                            else => DeleteResult.other_error,
                        };
                    };
                } else {
                    context.table.update(
                        context.row_id,
                        "value",
                        ColumnValue{ .int = 999 },
                        context.tx_id,
                        context.clog,
                    ) catch |err| {
                        break :blk switch (err) {
                            error.SerializationFailure => DeleteResult.serialization_failure,
                            else => DeleteResult.other_error,
                        };
                    };
                }
                break :blk DeleteResult.success;
            };
        }
    }.func;

    const MIXED_THREADS = 50;
    var mixed_contexts: [MIXED_THREADS]MixedOpContext = undefined;
    var mixed_threads: [MIXED_THREADS]std.Thread = undefined;

    // Half threads delete, half update
    for (&mixed_contexts, 0..) |*ctx, i| {
        ctx.* = MixedOpContext{
            .table = &table,
            .row_id = 1,
            .tx_id = @as(u64, i + 200),
            .is_delete = (i % 2 == 0),
            .result = null,
            .clog = &clog,
        };
    }

    // Spawn threads
    for (&mixed_threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, mixedOpThreadFn, .{&mixed_contexts[i]});
    }

    // Wait
    for (&mixed_threads) |*thread| {
        thread.join();
    }

    // Count successes
    var total_success: usize = 0;
    for (mixed_contexts) |ctx| {
        if (ctx.result) |result| {
            if (result == .success) total_success += 1;
        }
    }

    // Only ONE operation (delete OR update) should succeed
    try testing.expectEqual(@as(usize, 1), total_success);
}
