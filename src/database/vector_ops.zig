const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const HNSW = @import("../hnsw.zig").HNSW;

/// Rebuild HNSW vector index from table data (Phase 2.4)
///
/// After WAL recovery, the HNSW index will be out of sync with table data
/// because HNSW operations are not logged in the WAL. This function scans
/// all tables for embedding columns and rebuilds the HNSW index.
///
/// Process:
/// 1. If HNSW is null, skip (no vector search enabled)
/// 2. Clear existing HNSW index
/// 3. Scan all tables in the database
/// 4. For each table, find embedding columns
/// 5. For each row, extract embedding and insert into HNSW with row_id
/// 6. Log progress for large datasets
///
/// This should be called after recoverFromWal() completes.
///
/// Returns: Number of vectors inserted into HNSW index
///
/// Example usage:
/// ```
/// const recovered_tx = try db.recoverFromWal("data/wal");
/// const vectors_indexed = try db.rebuildHnswFromTables();
/// std.debug.print("Recovered {} transactions, indexed {} vectors\n",
///                 .{recovered_tx, vectors_indexed});
/// ```
pub fn rebuildHnswFromTables(db: *Database) !usize {
    // Phase 2.4: HNSW Index Rebuild Implementation

    // Step 1: If HNSW is not enabled, skip
    if (db.hnsw == null) {
        std.debug.print("HNSW not enabled, skipping index rebuild\n", .{});
        return 0;
    }

    // Step 2: Clear and reinitialize HNSW to start fresh
    // We need to rebuild the entire index from scratch
    if (db.hnsw) |old_hnsw| {
        old_hnsw.deinit();
        db.allocator.destroy(old_hnsw);
    }

    // Reinitialize with the same parameters (use reasonable defaults if not set)
    const hnsw_ptr = try db.allocator.create(HNSW(f32));
    hnsw_ptr.* = HNSW(f32).init(db.allocator, 16, 200);
    db.hnsw = hnsw_ptr;

    const h = db.hnsw.?;

    var vectors_indexed: usize = 0;

    // Step 3: Scan all tables in the database
    var table_it = db.tables.iterator();
    while (table_it.next()) |table_entry| {
        const table_name = table_entry.key_ptr.*;
        const table = table_entry.value_ptr.*;

        // Step 4: Scan all rows in this table
        var row_it = table.rows.iterator();
        while (row_it.next()) |row_entry| {
            const row_id = row_entry.key_ptr.*;
            const row = row_entry.value_ptr.*;

            // Step 5: Find embedding columns in this row
            var value_it = row.values.iterator();
            while (value_it.next()) |value_entry| {
                const col_name = value_entry.key_ptr.*;
                const value = value_entry.value_ptr.*;

                // Step 6: If this is an embedding column, insert into HNSW
                if (value == .embedding) {
                    const embedding = value.embedding;

                    // Insert into HNSW with row_id as external_id
                    _ = try h.insert(embedding, row_id);
                    vectors_indexed += 1;

                    if (vectors_indexed % 1000 == 0) {
                        std.debug.print("HNSW rebuild progress: {} vectors indexed from table '{s}'\n", .{ vectors_indexed, table_name });
                    }

                    // Only process first embedding column per row
                    // (current limitation: one embedding per row)
                    break;
                }

                _ = col_name; // Unused but kept for clarity
            }
        }
    }

    std.debug.print("HNSW rebuild complete: {} vectors indexed\n", .{vectors_indexed});
    return vectors_indexed;
}
