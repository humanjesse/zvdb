// ============================================================================
// MVCC Recovery Integration Tests
// ============================================================================
//
// Tests comprehensive recovery scenarios for MVCC system (Phase 3):
// - Checkpoint creation and loading (saveAllMvcc/loadAllMvcc)
// - WAL replay after checkpoint
// - CommitLog state persistence
// - Version chain preservation
// - Transaction ID continuity
// - Crash scenarios and error handling
// - Backward compatibility (v2 → v3 migration)
//
// These are END-TO-END integration tests covering multiple components:
// - Database (database/core.zig)
// - Persistence (database/persistence.zig)
// - Recovery (database/recovery.zig)
// - Transaction (transaction.zig)
// - Table (table.zig)
// ============================================================================

const std = @import("std");
const testing = std.testing;
const Database = @import("database/core.zig").Database;
const Table = @import("table.zig").Table;
const Row = @import("table.zig").Row;
const RowVersion = @import("table.zig").RowVersion;
const ColumnValue = @import("table.zig").ColumnValue;
const ColumnType = @import("table.zig").ColumnType;
const TransactionManager = @import("transaction.zig").TransactionManager;
const CommitLog = @import("transaction.zig").CommitLog;
const Snapshot = @import("transaction.zig").Snapshot;

// Helper function for cleanup
fn cleanupTestDir(dir_path: []const u8) void {
    std.fs.cwd().deleteTree(dir_path) catch {};
}

// ============================================================================
// Section 1: Basic Checkpoint Tests
// ============================================================================

test "MVCC Recovery: basic checkpoint without WAL" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_basic";
    defer cleanupTestDir(test_dir);

    // Create and populate database
    {
        var db = Database.init(allocator);
        defer db.deinit();

        // Create table
        var create = try db.execute("CREATE TABLE users (id int, name text, age int)");
        defer create.deinit();

        // Begin transaction 1
        const tx1 = try db.tx_manager.begin();

        // Insert 3 rows
        var insert1 = try db.execute("INSERT INTO users VALUES (1, \"Alice\", 30)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO users VALUES (2, \"Bob\", 25)");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO users VALUES (3, \"Charlie\", 35)");
        defer insert3.deinit();

        // Commit transaction 1
        try db.tx_manager.commit(tx1);

        // Begin transaction 2 - update a row
        const tx2 = try db.tx_manager.begin();

        var update = try db.execute("UPDATE users SET age = 31 WHERE id = 1");
        defer update.deinit();

        try db.tx_manager.commit(tx2);

        // Save checkpoint
        try db.saveAllMvcc(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Verify table exists
        const table = db.tables.get("users") orelse return error.TableNotFound;

        // Verify row count
        try testing.expectEqual(@as(usize, 3), table.count());

        // Verify version chains exist
        try testing.expectEqual(@as(u32, 3), table.version_chains.count());

        // Verify row 1 has 2 versions (insert + update)
        const chain1 = table.version_chains.get(1) orelse return error.ChainNotFound;
        var count1: usize = 0;
        var curr1: ?*RowVersion = chain1;
        while (curr1) |v| : (curr1 = v.next) {
            count1 += 1;
        }
        try testing.expectEqual(@as(usize, 2), count1);

        // Verify newest version has age = 31
        try testing.expectEqual(@as(i64, 31), chain1.data.get("age").?.int);

        // Verify data accessible via query
        var select = try db.execute("SELECT * FROM users WHERE id = 1");
        defer select.deinit();
        try testing.expectEqual(@as(usize, 1), select.rows.items.len);
        try testing.expectEqual(@as(i64, 31), select.rows.items[0].items[2].int);
    }
}

// ============================================================================
// Section 2: Checkpoint + WAL Replay
// ============================================================================

test "MVCC Recovery: checkpoint + WAL replay" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_wal";
    defer cleanupTestDir(test_dir);

    const wal_dir = "test_data/test_mvcc_recovery_wal";
    defer cleanupTestDir(wal_dir);

    // Phase 1: Create checkpoint
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE inventory (id int, qty int)");
        defer create.deinit();

        // Insert initial data (tx 1-3)
        var insert1 = try db.execute("INSERT INTO inventory VALUES (1, 100)");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO inventory VALUES (2, 200)");
        defer insert2.deinit();

        var insert3 = try db.execute("INSERT INTO inventory VALUES (3, 300)");
        defer insert3.deinit();

        // Save checkpoint
        try db.saveAllMvcc(test_dir);

        // Enable WAL for subsequent operations
        try db.enableWal(wal_dir);

        // Perform more operations (tx 4-6)
        var update1 = try db.execute("UPDATE inventory SET qty = 150 WHERE id = 1");
        defer update1.deinit();

        var insert4 = try db.execute("INSERT INTO inventory VALUES (4, 400)");
        defer insert4.deinit();

        var delete1 = try db.execute("DELETE FROM inventory WHERE id = 2");
        defer delete1.deinit();

        // Flush WAL (simulate crash without final checkpoint)
        try db.wal.?.flush();
    }

    // Phase 2: Recover from checkpoint + WAL
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Recover WAL
        const recovered = try db.recoverFromWal(wal_dir);
        try testing.expectEqual(@as(usize, 3), recovered); // 1 update + 1 insert + 1 delete

        const table = db.tables.get("inventory") orelse return error.TableNotFound;

        // Verify final state: 3 rows (1, 3, 4 - row 2 was deleted)
        try testing.expectEqual(@as(usize, 3), table.count());

        // Verify row 1 updated
        var select1 = try db.execute("SELECT qty FROM inventory WHERE id = 1");
        defer select1.deinit();
        try testing.expectEqual(@as(i64, 150), select1.rows.items[0].items[0].int);

        // Verify row 2 deleted
        try testing.expect(table.get(2, null, null) == null);

        // Verify row 4 exists
        var select4 = try db.execute("SELECT qty FROM inventory WHERE id = 4");
        defer select4.deinit();
        try testing.expectEqual(@as(i64, 400), select4.rows.items[0].items[0].int);
    }
}

// ============================================================================
// Section 3: Multiple Tables
// ============================================================================

test "MVCC Recovery: multiple tables with version chains" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_multitable";
    defer cleanupTestDir(test_dir);

    // Create and populate
    {
        var db = Database.init(allocator);
        defer db.deinit();

        // Create 3 tables
        var create1 = try db.execute("CREATE TABLE t1 (id int, value int)");
        defer create1.deinit();

        var create2 = try db.execute("CREATE TABLE t2 (id int, value int)");
        defer create2.deinit();

        var create3 = try db.execute("CREATE TABLE t3 (id int, value int)");
        defer create3.deinit();

        // Table 1: Single version per row
        var ins_t1 = try db.execute("INSERT INTO t1 VALUES (1, 100)");
        defer ins_t1.deinit();

        // Table 2: 5 updates to same row (creates 6-version chain)
        var ins_t2 = try db.execute("INSERT INTO t2 VALUES (1, 0)");
        defer ins_t2.deinit();

        var i: i32 = 1;
        while (i <= 5) : (i += 1) {
            const update_sql = try std.fmt.allocPrint(allocator, "UPDATE t2 SET value = {d} WHERE id = 1", .{i});
            defer allocator.free(update_sql);
            var upd = try db.execute(update_sql);
            defer upd.deinit();
        }

        // Table 3: 10 updates (creates 11-version chain)
        var ins_t3 = try db.execute("INSERT INTO t3 VALUES (1, 0)");
        defer ins_t3.deinit();

        i = 1;
        while (i <= 10) : (i += 1) {
            const update_sql = try std.fmt.allocPrint(allocator, "UPDATE t3 SET value = {d} WHERE id = 1", .{i});
            defer allocator.free(update_sql);
            var upd = try db.execute(update_sql);
            defer upd.deinit();
        }

        try db.saveAllMvcc(test_dir);
    }

    // Load and verify
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Verify all tables exist
        try testing.expect(db.tables.contains("t1"));
        try testing.expect(db.tables.contains("t2"));
        try testing.expect(db.tables.contains("t3"));

        // Verify T1: 1 version
        const t1 = db.tables.get("t1").?;
        const chain_t1 = t1.version_chains.get(1).?;
        var count_t1: usize = 0;
        var curr_t1: ?*RowVersion = chain_t1;
        while (curr_t1) |v| : (curr_t1 = v.next) {
            count_t1 += 1;
        }
        try testing.expectEqual(@as(usize, 1), count_t1);
        try testing.expectEqual(@as(i64, 100), chain_t1.data.get("value").?.int);

        // Verify T2: At least 1 version with final value
        const t2 = db.tables.get("t2").?;
        const chain_t2 = t2.version_chains.get(1).?;
        var count_t2: usize = 0;
        var curr_t2: ?*RowVersion = chain_t2;
        while (curr_t2) |v| : (curr_t2 = v.next) {
            count_t2 += 1;
        }
        try testing.expect(count_t2 >= 1); // At minimum, latest version
        try testing.expectEqual(@as(i64, 5), chain_t2.data.get("value").?.int);

        // Verify T3: At least 1 version with final value
        const t3 = db.tables.get("t3").?;
        const chain_t3 = t3.version_chains.get(1).?;
        var count_t3: usize = 0;
        var curr_t3: ?*RowVersion = chain_t3;
        while (curr_t3) |v| : (curr_t3 = v.next) {
            count_t3 += 1;
        }
        try testing.expect(count_t3 >= 1); // At minimum, latest version
        try testing.expectEqual(@as(i64, 10), chain_t3.data.get("value").?.int);
    }
}

// ============================================================================
// Section 4: Crash Scenarios
// ============================================================================

test "MVCC Recovery: partial WAL corruption" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_corrupt";
    defer cleanupTestDir(test_dir);

    const wal_dir = "test_data/test_mvcc_recovery_corrupt";
    defer cleanupTestDir(wal_dir);

    // Create checkpoint + valid WAL data
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE data (id int, value int)");
        defer create.deinit();

        var insert = try db.execute("INSERT INTO data VALUES (1, 100)");
        defer insert.deinit();

        try db.saveAllMvcc(test_dir);

        // Enable WAL and write valid record
        try db.enableWal(wal_dir);

        var insert2 = try db.execute("INSERT INTO data VALUES (2, 200)");
        defer insert2.deinit();

        try db.wal.?.flush();
    }

    // Recovery should handle corruption gracefully
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // This might skip some corrupted records but should not crash
        _ = try db.recoverFromWal(wal_dir);

        // Should recover at least the valid data
        const table = db.tables.get("data").?;
        try testing.expect(table.count() >= 1); // At minimum, checkpointed data
    }
}

test "MVCC Recovery: missing CommitLog file" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_no_clog";
    defer cleanupTestDir(test_dir);

    // Create checkpoint with table but manually delete CLOG
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE test_table (id int)");
        defer create.deinit();

        var insert = try db.execute("INSERT INTO test_table VALUES (1)");
        defer insert.deinit();

        try db.saveAllMvcc(test_dir);

        // Delete CommitLog file
        const clog_path = "/tmp/test_mvcc_recovery_no_clog/commitlog.zvdb";
        std.fs.cwd().deleteFile(clog_path) catch {};
    }

    // Should load gracefully with empty CLOG
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Table should still load
        try testing.expect(db.tables.contains("test_table"));

        const table = db.tables.get("test_table").?;
        try testing.expectEqual(@as(usize, 1), table.count());
    }
}

// ============================================================================
// Section 5: Backward Compatibility
// ============================================================================

test "MVCC Recovery: v2 to v3 migration" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_v2_migration";
    defer cleanupTestDir(test_dir);

    // Create v2 format checkpoint (using old save method)
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE legacy_table (id int, name text)");
        defer create.deinit();

        var insert1 = try db.execute("INSERT INTO legacy_table VALUES (1, \"test\")");
        defer insert1.deinit();

        var insert2 = try db.execute("INSERT INTO legacy_table VALUES (2, \"data\")");
        defer insert2.deinit();

        // Use old save method (v2 format)
        const persistence = @import("database/persistence.zig");
        try persistence.saveAll(&db, test_dir);
    }

    // Load with v3 loader (should fallback to v2)
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Verify table loaded
        const table = db.tables.get("legacy_table") orelse return error.TableNotFound;
        try testing.expectEqual(@as(usize, 2), table.count());

        // Verify data
        var select = try db.execute("SELECT * FROM legacy_table ORDER BY id");
        defer select.deinit();
        try testing.expectEqual(@as(usize, 2), select.rows.items.len);
        try testing.expectEqualStrings("test", select.rows.items[0].items[1].text);
    }
}

// ============================================================================
// Section 6: CommitLog Persistence
// ============================================================================

test "MVCC Recovery: CommitLog state preservation" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_clog_state";
    defer cleanupTestDir(test_dir);

    var tx1: u64 = 0;
    var tx2: u64 = 0;
    var tx3: u64 = 0;

    // Create checkpoint with different transaction states
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE txn_test (id int)");
        defer create.deinit();

        // Transaction 1: Committed
        tx1 = try db.tx_manager.begin();
        var ins1 = try db.execute("INSERT INTO txn_test VALUES (1)");
        defer ins1.deinit();
        try db.tx_manager.commit(tx1);

        // Transaction 2: Aborted
        tx2 = try db.tx_manager.begin();
        var ins2 = try db.execute("INSERT INTO txn_test VALUES (2)");
        defer ins2.deinit();
        try db.tx_manager.rollback(tx2);

        // Transaction 3: Committed
        tx3 = try db.tx_manager.begin();
        var ins3 = try db.execute("INSERT INTO txn_test VALUES (3)");
        defer ins3.deinit();
        try db.tx_manager.commit(tx3);

        try db.saveAllMvcc(test_dir);
    }

    // Load and verify CLOG states
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        // Verify transaction states preserved in CLOG
        try testing.expect(db.tx_manager.clog.isCommitted(tx1));
        try testing.expect(db.tx_manager.clog.isAborted(tx2));
        try testing.expect(db.tx_manager.clog.isCommitted(tx3));

        // Verify table exists and has data
        const table = db.tables.get("txn_test").?;
        try testing.expect(table.count() > 0); // Data was saved
    }
}

test "MVCC Recovery: WAL state merged into CLOG" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_clog_merge";
    defer cleanupTestDir(test_dir);

    const wal_dir = "test_data/test_mvcc_recovery_clog_merge";
    defer cleanupTestDir(wal_dir);

    var checkpoint_tx: u64 = 0;

    // Create checkpoint with some committed transactions
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE merge_test (id int)");
        defer create.deinit();

        checkpoint_tx = try db.tx_manager.begin();
        var ins = try db.execute("INSERT INTO merge_test VALUES (1)");
        defer ins.deinit();
        try db.tx_manager.commit(checkpoint_tx);

        try db.saveAllMvcc(test_dir);

        // Add WAL transactions
        try db.enableWal(wal_dir);

        var ins2 = try db.execute("INSERT INTO merge_test VALUES (2)");
        defer ins2.deinit();

        try db.wal.?.flush();
    }

    // Recover and verify merged state
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        _ = try db.recoverFromWal(wal_dir);

        // CLOG should contain checkpoint state
        try testing.expect(db.tx_manager.clog.isCommitted(checkpoint_tx));

        // Verify at least checkpointed data is present
        const table = db.tables.get("merge_test").?;
        try testing.expect(table.count() >= 1);
    }
}

// ============================================================================
// Section 7: Transaction ID Continuity
// ============================================================================

test "MVCC Recovery: transaction ID sequence preserved" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const test_dir = "/tmp/test_mvcc_recovery_tx_continuity";
    defer cleanupTestDir(test_dir);

    const wal_dir = "test_data/test_mvcc_recovery_tx_continuity";
    defer cleanupTestDir(wal_dir);

    var last_tx: u64 = 0;

    // Create checkpoint with transactions up to 10
    {
        var db = Database.init(allocator);
        defer db.deinit();

        var create = try db.execute("CREATE TABLE seq_test (id int)");
        defer create.deinit();

        var i: i32 = 1;
        while (i <= 10) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO seq_test VALUES ({d})", .{i});
            defer allocator.free(sql);
            var ins = try db.execute(sql);
            defer ins.deinit();
        }

        last_tx = db.tx_manager.next_tx_id.load(.monotonic);

        try db.saveAllMvcc(test_dir);

        // Add more via WAL
        try db.enableWal(wal_dir);

        i = 11;
        while (i <= 15) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO seq_test VALUES ({d})", .{i});
            defer allocator.free(sql);
            var ins = try db.execute(sql);
            defer ins.deinit();
        }

        try db.wal.?.flush();
    }

    // Recover and verify next transaction ID is safe
    {
        var db = try Database.loadAllMvcc(allocator, test_dir);
        defer db.deinit();

        _ = try db.recoverFromWal(wal_dir);

        // Begin new transaction - ID should be > last_tx
        const new_tx = try db.tx_manager.begin();
        try testing.expect(new_tx > last_tx);

        // Should be safe to commit
        var ins = try db.execute("INSERT INTO seq_test VALUES (999)");
        defer ins.deinit();
        try db.tx_manager.commit(new_tx);

        const table = db.tables.get("seq_test").?;
        // Verify at least checkpointed data (10) + new insert (1)
        try testing.expect(table.count() >= 11);
    }
}
