const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const HNSW = @import("../hnsw.zig").HNSW;

/// Rebuild HNSW vector indexes from table data (Phase 2.4)
///
/// After WAL recovery, the HNSW indexes will be out of sync with table data
/// because HNSW operations are not logged in the WAL. This function scans
/// all tables for embedding columns and rebuilds the per-dimension HNSW indexes.
///
/// Process:
/// 1. Clear all existing HNSW indexes
/// 2. Scan all tables in the database
/// 3. For each table, find embedding columns
/// 4. For each row, extract embedding and determine its dimension
/// 5. Insert into the appropriate dimension-specific HNSW index
/// 6. Log progress for large datasets
///
/// This should be called after recoverFromWal() completes.
///
/// Returns: Number of vectors inserted into HNSW indexes
///
/// Example usage:
/// ```
/// const recovered_tx = try db.recoverFromWal("data/wal");
/// const vectors_indexed = try db.rebuildHnswFromTables();
/// std.debug.print("Recovered {} transactions, indexed {} vectors\n",
///                 .{recovered_tx, vectors_indexed});
/// ```
pub fn rebuildHnswFromTables(db: *Database) !usize {
    // Phase 2.4: Per-(Dimension,Column) HNSW Index Rebuild Implementation

    // Step 1: Clear all existing HNSW indexes
    var hnsw_it = db.hnsw_indexes.iterator();
    while (hnsw_it.next()) |entry| {
        var key = entry.key_ptr.*;
        key.deinit(db.allocator);  // Free column_name
        entry.value_ptr.*.deinit();
        db.allocator.destroy(entry.value_ptr.*);
    }
    db.hnsw_indexes.clearRetainingCapacity();

    var vectors_indexed: usize = 0;

    // Step 2: Scan all tables in the database
    var table_it = db.tables.iterator();
    while (table_it.next()) |table_entry| {
        const table_name = table_entry.key_ptr.*;
        const table = table_entry.value_ptr.*;

        // Step 3: Scan all rows in this table (using newest version)
        var row_it = table.version_chains.iterator();
        while (row_it.next()) |row_entry| {
            const row_id = row_entry.key_ptr.*;
            const version = row_entry.value_ptr.*;
            const row = &version.data;

            // Step 4: Find embedding columns in this row
            var value_it = row.values.iterator();
            while (value_it.next()) |value_entry| {
                const col_name = value_entry.key_ptr.*;
                const value = value_entry.value_ptr.*;

                // Step 5: If this is an embedding column, insert into appropriate HNSW
                if (value == .embedding) {
                    const embedding = value.embedding;
                    const dim = embedding.len;

                    // Get or create HNSW index for this (dimension, column) pair
                    const h = try db.getOrCreateHnswForColumn(dim, col_name);

                    // Insert into HNSW with row_id as external_id
                    _ = try h.insert(embedding, row_id);
                    vectors_indexed += 1;

                    if (vectors_indexed % 1000 == 0) {
                        std.debug.print("HNSW rebuild progress: {} vectors indexed from table '{s}'\n", .{ vectors_indexed, table_name });
                    }

                    // Continue processing all embedding columns (no break)
                }
            }
        }
    }

    std.debug.print("HNSW rebuild complete: {} vectors indexed\n", .{vectors_indexed});
    return vectors_indexed;
}
