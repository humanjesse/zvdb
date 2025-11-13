const std = @import("std");
const Allocator = std.mem.Allocator;

/// Write-Ahead Log (WAL) implementation for zvdb
///
/// The WAL ensures durability by logging all database mutations before
/// they are applied to the in-memory tables. This allows crash recovery
/// by replaying the log on startup.
///
/// WAL File Format:
/// ┌─────────────────────────────────────────────────────────────────┐
/// │ WAL Header (32 bytes)                                           │
/// ├─────────────────────────────────────────────────────────────────┤
/// │ Record 1 (variable length)                                      │
/// ├─────────────────────────────────────────────────────────────────┤
/// │ Record 2 (variable length)                                      │
/// ├─────────────────────────────────────────────────────────────────┤
/// │ ...                                                             │
/// └─────────────────────────────────────────────────────────────────┘

// ============================================================================
// WAL Record Types
// ============================================================================

/// Type of WAL record
pub const WalRecordType = enum(u8) {
    /// Transaction begin marker
    begin_tx = 0x01,

    /// Transaction commit marker - transaction is now durable
    commit_tx = 0x02,

    /// Transaction rollback marker - transaction was aborted
    rollback_tx = 0x03,

    /// INSERT operation - add new row to table
    insert_row = 0x10,

    /// DELETE operation - remove row from table
    delete_row = 0x11,

    /// UPDATE operation - modify existing row
    update_row = 0x12,

    /// Checkpoint marker - all data before this point is flushed to disk
    checkpoint = 0x20,

    pub fn fromU8(value: u8) !WalRecordType {
        return std.meta.intToEnum(WalRecordType, value) catch error.InvalidRecordType;
    }
};

// ============================================================================
// WAL File Header
// ============================================================================

/// WAL file header (36 bytes fixed size)
pub const WalHeader = struct {
    /// Magic number: 0x5741_4C00 ("WAL\0" in ASCII)
    magic: u32,

    /// Format version number (currently 1)
    version: u32,

    /// Page size for buffering (recommended: 4096 bytes)
    page_size: u32,

    /// Sequence number for this WAL file (used for rotation)
    sequence: u64,

    /// Timestamp when this WAL file was created
    created_at: i64,

    /// Reserved for future use
    reserved: [8]u8,

    pub const MAGIC: u32 = 0x5741_4C00; // "WAL\0"
    pub const VERSION: u32 = 1;
    pub const SIZE: usize = 36;

    pub fn init(sequence: u64, page_size: u32) WalHeader {
        return WalHeader{
            .magic = MAGIC,
            .version = VERSION,
            .page_size = page_size,
            .sequence = sequence,
            .created_at = std.time.timestamp(),
            .reserved = [_]u8{0} ** 8,
        };
    }

    /// Validate the header magic and version
    pub fn validate(self: *const WalHeader) !void {
        if (self.magic != MAGIC) {
            return error.InvalidWalMagic;
        }
        if (self.version != VERSION) {
            return error.UnsupportedWalVersion;
        }
    }

    /// Serialize header to bytes (little-endian)
    pub fn serialize(self: *const WalHeader, buffer: *[SIZE]u8) void {
        std.mem.writeInt(u32, buffer[0..4], self.magic, .little);
        std.mem.writeInt(u32, buffer[4..8], self.version, .little);
        std.mem.writeInt(u32, buffer[8..12], self.page_size, .little);
        std.mem.writeInt(u64, buffer[12..20], self.sequence, .little);
        std.mem.writeInt(i64, buffer[20..28], self.created_at, .little);
        @memcpy(buffer[28..36], &self.reserved);
    }

    /// Deserialize header from bytes (little-endian)
    pub fn deserialize(buffer: *const [SIZE]u8) WalHeader {
        return WalHeader{
            .magic = std.mem.readInt(u32, buffer[0..4], .little),
            .version = std.mem.readInt(u32, buffer[4..8], .little),
            .page_size = std.mem.readInt(u32, buffer[8..12], .little),
            .sequence = std.mem.readInt(u64, buffer[12..20], .little),
            .created_at = std.mem.readInt(i64, buffer[20..28], .little),
            .reserved = buffer[28..36].*,
        };
    }
};

// ============================================================================
// WAL Record
// ============================================================================

/// A single WAL record representing a database operation
///
/// Record Format (variable length):
/// ┌──────────────────────────────────────────────────────────────┐
/// │ Header (24 bytes fixed)                                      │
/// ├──────────────────────────────────────────────────────────────┤
/// │ table_name_len (u16)      | 2 bytes                          │
/// │ table_name (variable)     | table_name_len bytes             │
/// ├──────────────────────────────────────────────────────────────┤
/// │ data_len (u32)            | 4 bytes                          │
/// │ data (variable)           | data_len bytes                   │
/// ├──────────────────────────────────────────────────────────────┤
/// │ checksum (u32)            | 4 bytes (CRC32 of entire record) │
/// └──────────────────────────────────────────────────────────────┘
pub const WalRecord = struct {
    /// Type of this record
    record_type: WalRecordType,

    /// Transaction ID this record belongs to
    tx_id: u64,

    /// Logical Sequence Number (LSN) - monotonically increasing
    lsn: u64,

    /// Row ID being operated on (0 for tx control records)
    row_id: u64,

    /// Table name (owned by this record)
    table_name: []const u8,

    /// Serialized operation data (owned by this record)
    /// For INSERT: serialized Row
    /// For DELETE: empty (row_id is sufficient)
    /// For UPDATE: serialized old and new values
    data: []const u8,

    /// CRC32 checksum of the entire record (for corruption detection)
    checksum: u32,

    pub fn deinit(self: *WalRecord, allocator: Allocator) void {
        allocator.free(self.table_name);
        allocator.free(self.data);
    }

    /// Calculate the size this record will occupy when serialized
    pub fn serializedSize(self: *const WalRecord) usize {
        // Fixed header: type(1) + tx_id(8) + lsn(8) + row_id(8) = 25 bytes
        // Table name: len(2) + name(variable)
        // Data: len(4) + data(variable)
        // Checksum: 4 bytes
        return 25 + 2 + self.table_name.len + 4 + self.data.len + 4;
    }

    /// Serialize this record to a byte buffer
    ///
    /// The caller must ensure the buffer is large enough (use serializedSize())
    pub fn serialize(self: *const WalRecord, buffer: []u8, allocator: Allocator) !void {
        if (buffer.len < self.serializedSize()) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        // Write header (25 bytes)
        buffer[offset] = @intFromEnum(self.record_type);
        offset += 1;

        std.mem.writeInt(u64, buffer[offset..][0..8], self.tx_id, .little);
        offset += 8;

        std.mem.writeInt(u64, buffer[offset..][0..8], self.lsn, .little);
        offset += 8;

        std.mem.writeInt(u64, buffer[offset..][0..8], self.row_id, .little);
        offset += 8;

        // Write table name (2 + variable)
        const table_name_len: u16 = @intCast(self.table_name.len);
        std.mem.writeInt(u16, buffer[offset..][0..2], table_name_len, .little);
        offset += 2;

        @memcpy(buffer[offset..][0..self.table_name.len], self.table_name);
        offset += self.table_name.len;

        // Write data (4 + variable)
        const data_len: u32 = @intCast(self.data.len);
        std.mem.writeInt(u32, buffer[offset..][0..4], data_len, .little);
        offset += 4;

        @memcpy(buffer[offset..][0..self.data.len], self.data);
        offset += self.data.len;

        // Calculate and write checksum (CRC32 of everything before checksum)
        const checksum = std.hash.Crc32.hash(buffer[0..offset]);
        std.mem.writeInt(u32, buffer[offset..][0..4], checksum, .little);
        offset += 4;

        _ = allocator; // Unused for now, but may be needed for future extensions
        std.debug.assert(offset == self.serializedSize());
    }

    /// Deserialize a record from a byte buffer
    ///
    /// Returns the deserialized record and the number of bytes consumed.
    /// The record owns its table_name and data and must be deinit'd.
    pub fn deserialize(buffer: []const u8, allocator: Allocator) !struct { record: WalRecord, bytes_read: usize } {
        if (buffer.len < 25) {
            return error.BufferTooSmall;
        }

        var offset: usize = 0;

        // Read header (25 bytes)
        const record_type = try WalRecordType.fromU8(buffer[offset]);
        offset += 1;

        const tx_id = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        const lsn = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        const row_id = std.mem.readInt(u64, buffer[offset..][0..8], .little);
        offset += 8;

        // Read table name (2 + variable)
        if (buffer.len < offset + 2) return error.BufferTooSmall;
        const table_name_len = std.mem.readInt(u16, buffer[offset..][0..2], .little);
        offset += 2;

        if (buffer.len < offset + table_name_len) return error.BufferTooSmall;
        const table_name = try allocator.alloc(u8, table_name_len);
        errdefer allocator.free(table_name);
        @memcpy(table_name, buffer[offset..][0..table_name_len]);
        offset += table_name_len;

        // Read data (4 + variable)
        if (buffer.len < offset + 4) return error.BufferTooSmall;
        const data_len = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        if (buffer.len < offset + data_len) return error.BufferTooSmall;
        const data = try allocator.alloc(u8, data_len);
        errdefer allocator.free(data);
        @memcpy(data, buffer[offset..][0..data_len]);
        offset += data_len;

        // Read and verify checksum
        if (buffer.len < offset + 4) return error.BufferTooSmall;
        const stored_checksum = std.mem.readInt(u32, buffer[offset..][0..4], .little);
        offset += 4;

        // Verify checksum matches
        const calculated_checksum = std.hash.Crc32.hash(buffer[0 .. offset - 4]);
        if (stored_checksum != calculated_checksum) {
            return error.ChecksumMismatch;
        }

        return .{
            .record = WalRecord{
                .record_type = record_type,
                .tx_id = tx_id,
                .lsn = lsn,
                .row_id = row_id,
                .table_name = table_name,
                .data = data,
                .checksum = stored_checksum,
            },
            .bytes_read = offset,
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "WalHeader: init and validate" {
    const header = WalHeader.init(1, 4096);

    try testing.expectEqual(WalHeader.MAGIC, header.magic);
    try testing.expectEqual(WalHeader.VERSION, header.version);
    try testing.expectEqual(@as(u32, 4096), header.page_size);
    try testing.expectEqual(@as(u64, 1), header.sequence);

    try header.validate();
}

test "WalHeader: serialize and deserialize" {
    const header1 = WalHeader.init(42, 8192);

    var buffer: [WalHeader.SIZE]u8 = undefined;
    header1.serialize(&buffer);

    const header2 = WalHeader.deserialize(&buffer);

    try testing.expectEqual(header1.magic, header2.magic);
    try testing.expectEqual(header1.version, header2.version);
    try testing.expectEqual(header1.page_size, header2.page_size);
    try testing.expectEqual(header1.sequence, header2.sequence);
    try testing.expectEqual(header1.created_at, header2.created_at);
}

test "WalHeader: validate rejects bad magic" {
    var header = WalHeader.init(1, 4096);
    header.magic = 0xDEADBEEF;

    try testing.expectError(error.InvalidWalMagic, header.validate());
}

test "WalHeader: validate rejects bad version" {
    var header = WalHeader.init(1, 4096);
    header.version = 999;

    try testing.expectError(error.UnsupportedWalVersion, header.validate());
}

test "WalRecord: serialize and deserialize BEGIN" {
    const allocator = testing.allocator;

    const record1 = WalRecord{
        .record_type = .begin_tx,
        .tx_id = 123,
        .lsn = 1,
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    };

    // Serialize
    const size = record1.serializedSize();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    try record1.serialize(buffer, allocator);

    // Deserialize
    const result = try WalRecord.deserialize(buffer, allocator);
    var record2 = result.record;
    defer record2.deinit(allocator);

    try testing.expectEqual(record1.record_type, record2.record_type);
    try testing.expectEqual(record1.tx_id, record2.tx_id);
    try testing.expectEqual(record1.lsn, record2.lsn);
    try testing.expectEqual(record1.row_id, record2.row_id);
    try testing.expectEqual(size, result.bytes_read);
}

test "WalRecord: serialize and deserialize INSERT with data" {
    const allocator = testing.allocator;

    const table_name = try allocator.dupe(u8, "users");
    defer allocator.free(table_name);

    const data = try allocator.dupe(u8, "serialized_row_data_here");
    defer allocator.free(data);

    const record1 = WalRecord{
        .record_type = .insert_row,
        .tx_id = 456,
        .lsn = 2,
        .row_id = 789,
        .table_name = table_name,
        .data = data,
        .checksum = 0,
    };

    // Serialize
    const size = record1.serializedSize();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    try record1.serialize(buffer, allocator);

    // Deserialize
    const result = try WalRecord.deserialize(buffer, allocator);
    var record2 = result.record;
    defer record2.deinit(allocator);

    try testing.expectEqual(record1.record_type, record2.record_type);
    try testing.expectEqual(record1.tx_id, record2.tx_id);
    try testing.expectEqual(record1.lsn, record2.lsn);
    try testing.expectEqual(record1.row_id, record2.row_id);
    try testing.expectEqualStrings(record1.table_name, record2.table_name);
    try testing.expectEqualSlices(u8, record1.data, record2.data);
    try testing.expectEqual(size, result.bytes_read);
}

test "WalRecord: checksum detects corruption" {
    const allocator = testing.allocator;

    const table_name = try allocator.dupe(u8, "test");
    defer allocator.free(table_name);

    const data = try allocator.dupe(u8, "data");
    defer allocator.free(data);

    const record = WalRecord{
        .record_type = .insert_row,
        .tx_id = 1,
        .lsn = 1,
        .row_id = 1,
        .table_name = table_name,
        .data = data,
        .checksum = 0,
    };

    // Serialize
    const size = record.serializedSize();
    const buffer = try allocator.alloc(u8, size);
    defer allocator.free(buffer);

    try record.serialize(buffer, allocator);

    // Corrupt the data
    buffer[30] ^= 0xFF;

    // Deserialize should fail with checksum error
    try testing.expectError(error.ChecksumMismatch, WalRecord.deserialize(buffer, allocator));
}

test "WalRecord: all record types" {
    const allocator = testing.allocator;

    const record_types = [_]WalRecordType{
        .begin_tx,
        .commit_tx,
        .rollback_tx,
        .insert_row,
        .delete_row,
        .update_row,
        .checkpoint,
    };

    for (record_types) |rt| {
        const table_name = try allocator.dupe(u8, "table");
        defer allocator.free(table_name);

        const data = try allocator.dupe(u8, "test_data");
        defer allocator.free(data);

        const record1 = WalRecord{
            .record_type = rt,
            .tx_id = 100,
            .lsn = 50,
            .row_id = 200,
            .table_name = table_name,
            .data = data,
            .checksum = 0,
        };

        const size = record1.serializedSize();
        const buffer = try allocator.alloc(u8, size);
        defer allocator.free(buffer);

        try record1.serialize(buffer, allocator);

        const result = try WalRecord.deserialize(buffer, allocator);
        var record2 = result.record;
        defer record2.deinit(allocator);

        try testing.expectEqual(rt, record2.record_type);
    }
}
