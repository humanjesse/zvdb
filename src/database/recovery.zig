const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const wal = @import("../wal.zig");
const WalWriter = wal.WalWriter;
const WalReader = wal.WalReader;
const WalRecord = wal.WalRecord;
const WalRecordType = wal.WalRecordType;
const Row = @import("../table.zig").Row;
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;

// Transaction state for recovery
const TransactionState = enum {
    active, // Transaction started but not committed/aborted
    committed, // Transaction committed
    aborted, // Transaction rolled back
};

/// Enable Write-Ahead Logging for durability
/// Creates WAL directory and initializes WAL writer
/// If WAL files already exist, finds the next sequence number to use
pub fn enableWal(db: *Database, wal_dir: []const u8) !void {
    if (db.wal != null) {
        return error.WalAlreadyEnabled;
    }

    const wal_ptr = try db.allocator.create(WalWriter);
    errdefer db.allocator.destroy(wal_ptr);

    // Find the highest existing sequence number in the WAL directory
    const next_sequence = blk: {
        var dir = std.fs.cwd().openDir(wal_dir, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => {
                // Directory doesn't exist, start from sequence 0
                break :blk 0;
            },
            else => return err,
        };
        defer dir.close();

        var max_sequence: u64 = 0;
        var found_any = false;

        var dir_it = dir.iterate();
        while (try dir_it.next()) |entry| {
            if (entry.kind != .file) continue;

            // Check if filename matches "wal.NNNNNN" pattern
            if (std.mem.startsWith(u8, entry.name, "wal.")) {
                const seq_str = entry.name[4..]; // Skip "wal."
                if (std.fmt.parseInt(u64, seq_str, 10)) |seq| {
                    found_any = true;
                    if (seq > max_sequence) {
                        max_sequence = seq;
                    }
                } else |_| {
                    // Ignore files with invalid sequence numbers
                    continue;
                }
            }
        }

        // Start from next sequence after the highest found
        if (found_any) {
            break :blk max_sequence + 1;
        } else {
            break :blk 0;
        }
    };

    wal_ptr.* = try WalWriter.initWithOptions(db.allocator, wal_dir, .{
        .sequence = next_sequence,
    });
    db.wal = wal_ptr;
}

/// Helper function to write a WAL record and flush to disk
/// Centralizes transaction ID management and WAL writing logic
/// Returns the transaction ID used for this record
pub fn writeWalRecord(
    db: *Database,
    record_type: WalRecordType,
    table_name: []const u8,
    row_id: u64,
    data: []const u8,
) !u64 {
    const w = db.wal orelse return error.WalNotEnabled;

    // Get transaction ID and increment atomically from TransactionManager (single source of truth)
    const tx_id = db.tx_manager.next_tx_id.fetchAdd(1, .monotonic);

    // Create WAL record (writeRecord makes its own copy of table_name and data)
    const table_name_owned = try db.allocator.dupe(u8, table_name);
    defer db.allocator.free(table_name_owned);

    const record = WalRecord{
        .record_type = record_type,
        .tx_id = tx_id,
        .lsn = 0, // Will be assigned by WAL writer
        .row_id = row_id,
        .table_name = table_name_owned,
        .data = data,
        .checksum = 0, // Will be calculated during serialization
    };

    // Write WAL record and flush to disk (CRITICAL: must be durable before table mutation)
    _ = try w.writeRecord(record);

    // Write COMMIT record to mark this transaction as committed
    // (Each SQL statement is an auto-committed transaction)
    const commit_record = WalRecord{
        .record_type = .commit_tx,
        .tx_id = tx_id,
        .lsn = 0,
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    };
    _ = try w.writeRecord(commit_record);
    try w.flush();

    return tx_id;
}

/// Recover database from WAL files after a crash (Phase 2.4)
///
/// This function enables crash recovery.
/// Recovery process:
/// 1. Scan WAL directory for all wal.* files
/// 2. Read and validate WAL records using WalReader
/// 3. Replay committed transactions in order (by LSN)
/// 4. Apply INSERT/DELETE/UPDATE operations to tables
/// 5. Rebuild HNSW index from recovered table data (see rebuildHnswFromTables)
/// 6. Handle incomplete/corrupted records gracefully
/// 7. Optionally checkpoint and clean up old WAL files
///
/// Parameters:
///   - wal_dir: Directory containing WAL files (e.g., "data/wal")
///
/// Returns: Number of transactions recovered
///
/// Example usage:
/// ```
/// var db = Database.init(allocator);
/// defer db.deinit();
/// const recovered = try db.recoverFromWal("data/wal");
/// std.debug.print("Recovered {} transactions\n", .{recovered});
/// ```
pub fn recoverFromWal(db: *Database, wal_dir: []const u8) !usize {
    // Phase 2.4: WAL Recovery Implementation

    // Step 1: Find all WAL files in the directory
    var wal_files = ArrayList([]const u8).init(db.allocator);
    defer {
        for (wal_files.items) |file| {
            db.allocator.free(file);
        }
        wal_files.deinit();
    }

    // Open the WAL directory
    var dir = std.fs.cwd().openDir(wal_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            // No WAL directory = no recovery needed
            return 0;
        },
        else => return err,
    };
    defer dir.close();

    // Iterate through the directory to find all wal.* files
    var dir_it = dir.iterate();
    while (try dir_it.next()) |entry| {
        if (entry.kind != .file) continue;

        // Check if filename matches "wal.NNNNNN" pattern
        if (std.mem.startsWith(u8, entry.name, "wal.")) {
            const full_path = try std.fmt.allocPrint(
                db.allocator,
                "{s}/{s}",
                .{ wal_dir, entry.name },
            );
            try wal_files.append(full_path);
        }
    }

    if (wal_files.items.len == 0) {
        // No WAL files found
        return 0;
    }

    // Sort WAL files by sequence number (filename order)
    std.mem.sort([]const u8, wal_files.items, {}, struct {
        fn lessThan(_: void, a: []const u8, b: []const u8) bool {
            return std.mem.order(u8, a, b) == .lt;
        }
    }.lessThan);

    // Step 2: Track transactions to determine which are committed
    // Map: tx_id -> transaction state
    var transactions = std.AutoHashMap(u64, TransactionState).init(db.allocator);
    defer transactions.deinit();

    // Track corruption statistics
    var corrupted_records: usize = 0;

    // Step 3: First pass - scan all records to identify committed transactions
    for (wal_files.items) |wal_file| {
        var reader = WalReader.init(db.allocator, wal_file) catch |err| {
            std.debug.print("Warning: Failed to open WAL file {s}: {}\n", .{ wal_file, err });
            continue;
        };
        defer reader.deinit();

        // Read records with error handling - skip corrupted records
        while (true) {
            const record_result = reader.readRecord();
            const record_opt = record_result catch |err| switch (err) {
                // Recoverable errors - skip corrupted record and continue
                error.ChecksumMismatch,
                error.BufferTooSmall,
                error.InvalidRecordType,
                => {
                    corrupted_records += 1;
                    std.debug.print("Warning: Skipping corrupted record in {s}: {}\n", .{ wal_file, err });
                    break; // Skip rest of this file, move to next
                },
                // Fatal errors - propagate
                else => return err,
            };

            if (record_opt) |rec| {
                var record = rec;
                defer record.deinit(db.allocator);

                switch (record.record_type) {
                    .begin_tx => {
                        try transactions.put(record.tx_id, .active);
                    },
                    .commit_tx => {
                        try transactions.put(record.tx_id, .committed);
                    },
                    .rollback_tx => {
                        try transactions.put(record.tx_id, .aborted);
                    },
                    else => {}, // Data operations don't change transaction state
                }
            } else {
                // End of file
                break;
            }
        }
    }

    // Step 4: Second pass - replay committed transactions
    var recovered_count: usize = 0;
    var applied_operations: usize = 0;
    var max_tx_id: u64 = 0;

    for (wal_files.items) |wal_file| {
        var reader = WalReader.init(db.allocator, wal_file) catch |err| {
            std.debug.print("Warning: Failed to open WAL file {s}: {}\n", .{ wal_file, err });
            continue;
        };
        defer reader.deinit();

        // Read records with error handling - skip corrupted records
        while (true) {
            const record_result = reader.readRecord();
            const record_opt = record_result catch |err| switch (err) {
                // Recoverable errors - skip corrupted record and continue
                error.ChecksumMismatch,
                error.BufferTooSmall,
                error.InvalidRecordType,
                => {
                    // Already counted in first pass
                    std.debug.print("Warning: Skipping corrupted record in {s} (pass 2): {}\n", .{ wal_file, err });
                    break; // Skip rest of this file, move to next
                },
                // Fatal errors - propagate
                else => return err,
            };

            if (record_opt) |rec| {
                var record = rec;
                defer record.deinit(db.allocator);

                // Track maximum transaction ID
                if (record.tx_id > max_tx_id) {
                    max_tx_id = record.tx_id;
                }

                // Only process records from committed transactions
                const tx_state = transactions.get(record.tx_id) orelse .active;
                if (tx_state != .committed) {
                    continue; // Skip records from uncommitted/aborted transactions
                }

                switch (record.record_type) {
                    .insert_row => {
                        try replayInsert(db, &record);
                        applied_operations += 1;
                    },
                    .delete_row => {
                        try replayDelete(db, &record);
                        applied_operations += 1;
                    },
                    .update_row => {
                        try replayUpdate(db, &record);
                        applied_operations += 1;
                    },
                    .commit_tx => {
                        recovered_count += 1;
                    },
                    else => {}, // begin_tx, rollback_tx, checkpoint - no action needed
                }
            } else {
                // End of file
                break;
            }
        }
    }

    // Report recovery statistics
    if (corrupted_records > 0) {
        std.debug.print("WAL Recovery complete: {} transactions, {} operations applied ({} corrupted records skipped)\n", .{ recovered_count, applied_operations, corrupted_records });
    } else {
        std.debug.print("WAL Recovery complete: {} transactions, {} operations applied\n", .{ recovered_count, applied_operations });
    }

    // Update TransactionManager to prevent ID reuse in future transactions
    if (max_tx_id > 0) {
        db.tx_manager.next_tx_id.store(max_tx_id + 1, .monotonic);
    }

    return recovered_count;
}

/// Replay an INSERT operation from WAL
fn replayInsert(db: *Database, record: *const WalRecord) !void {
    // Get or create the table
    const table = db.tables.get(record.table_name) orelse {
        std.debug.print("Warning: Table '{s}' not found during recovery, skipping INSERT\n", .{record.table_name});
        return;
    };

    // Deserialize the row data
    var row = try Row.deserialize(record.data, db.allocator);
    errdefer row.deinit(db.allocator);

    // Check if row already exists (idempotency - may have been partially recovered)
    if (table.rows.contains(row.id)) {
        // Row already exists, skip (recovery is idempotent)
        row.deinit(db.allocator);
        return;
    }

    // Insert the row into the table
    try table.rows.put(row.id, row);

    // Update next_id to prevent ID conflicts with future inserts (atomic CAS loop)
    const desired_next = row.id + 1;
    while (true) {
        const current = table.next_id.load(.monotonic);
        if (current >= desired_next) break;
        _ = table.next_id.cmpxchgWeak(current, desired_next, .monotonic, .monotonic) orelse break;
    }
}

/// Replay a DELETE operation from WAL
fn replayDelete(db: *Database, record: *const WalRecord) !void {
    // Get the table
    const table = db.tables.get(record.table_name) orelse {
        std.debug.print("Warning: Table '{s}' not found during recovery, skipping DELETE\n", .{record.table_name});
        return;
    };

    // Remove the row if it exists (idempotent)
    if (table.rows.fetchRemove(record.row_id)) |entry| {
        var row = entry.value;
        row.deinit(db.allocator);
    }
}

/// Replay an UPDATE operation from WAL
fn replayUpdate(db: *Database, record: *const WalRecord) !void {
    // Get the table
    const table = db.tables.get(record.table_name) orelse {
        std.debug.print("Warning: Table '{s}' not found during recovery, skipping UPDATE\n", .{record.table_name});
        return;
    };

    // UPDATE record format: [old_size:u64][old_data][new_data]
    // Extract the new row data from the combined format
    if (record.data.len < 8) {
        std.debug.print("Warning: Invalid UPDATE record data (too short), skipping\n", .{});
        return;
    }

    const old_size = std.mem.readInt(u64, record.data[0..8], .little);
    const new_data_start = 8 + old_size;

    if (new_data_start > record.data.len) {
        std.debug.print("Warning: Invalid UPDATE record data (size mismatch), skipping\n", .{});
        return;
    }

    const new_data = record.data[new_data_start..];

    // Deserialize the new row state
    var updated_row = try Row.deserialize(new_data, db.allocator);
    errdefer updated_row.deinit(db.allocator);

    // Remove old row if it exists
    if (table.rows.fetchRemove(record.row_id)) |old_entry| {
        // Call deinit directly on the returned value to avoid extra copies
        // We can't use |*old_entry| because it's const
        var temp_row = old_entry.value;
        temp_row.deinit(db.allocator);
    }

    // Insert the updated row
    try table.rows.put(updated_row.id, updated_row);

    // Update next_id to prevent ID conflicts with future inserts (atomic CAS loop)
    const desired_next = updated_row.id + 1;
    while (true) {
        const current = table.next_id.load(.monotonic);
        if (current >= desired_next) break;
        _ = table.next_id.cmpxchgWeak(current, desired_next, .monotonic, .monotonic) orelse break;
    }
}
