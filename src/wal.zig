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
// Path Validation (Security)
// ============================================================================

/// Validate that a WAL directory path is safe to use.
///
/// Security checks:
/// - Rejects absolute paths (e.g., "/tmp", "C:\Windows")
/// - Rejects paths with ".." components (path traversal)
/// - Rejects paths with null bytes
/// - Enforces maximum path length
///
/// Allowed examples: "wal", "data/wal", "./wal"
/// Rejected examples: "/tmp/wal", "../wal", "wal/../etc"
pub fn validateWalPath(path: []const u8) !void {
    // Check for empty path
    if (path.len == 0) {
        return error.InvalidWalPath;
    }

    // Check for excessive length (common security limit)
    if (path.len > 255) {
        return error.WalPathTooLong;
    }

    // Check for null bytes (common security issue)
    if (std.mem.indexOfScalar(u8, path, 0) != null) {
        return error.InvalidWalPath;
    }

    // Check for absolute paths (Unix)
    if (path[0] == '/') {
        return error.AbsolutePathNotAllowed;
    }

    // Check for absolute paths (Windows drive letters: C:, D:, etc.)
    if (path.len >= 2 and path[1] == ':') {
        const first = path[0];
        if ((first >= 'A' and first <= 'Z') or (first >= 'a' and first <= 'z')) {
            return error.AbsolutePathNotAllowed;
        }
    }

    // Check for path traversal attempts
    // Split path by '/' and check each component
    var it = std.mem.splitScalar(u8, path, '/');
    while (it.next()) |component| {
        // Reject ".." components
        if (std.mem.eql(u8, component, "..")) {
            return error.PathTraversalNotAllowed;
        }

        // Also check Windows-style backslashes
        if (std.mem.indexOfScalar(u8, component, '\\') != null) {
            var it2 = std.mem.splitScalar(u8, component, '\\');
            while (it2.next()) |subcomp| {
                if (std.mem.eql(u8, subcomp, "..")) {
                    return error.PathTraversalNotAllowed;
                }
            }
        }
    }

    // Path is safe
}

/// Check if a path is a symlink (without following it)
///
/// This provides defense-in-depth against symlink attacks where an attacker
/// creates a symlink at the WAL location pointing to a sensitive file.
///
/// Note: This has a TOCTOU (Time-Of-Check-Time-Of-Use) race condition.
/// An attacker could create a symlink after this check but before file creation.
/// The mitigation is to combine this with:
/// - Using exclusive file creation flags
/// - Running the database with minimal filesystem permissions
/// - Using process isolation (containers, chroot)
///
/// Platform support:
/// - Unix/Linux/macOS: Full symlink checking via fstatat
/// - Windows: Not supported (returns false - Windows symlinks work differently)
pub fn isSymlink(dir_path: []const u8, file_path: []const u8) !bool {
    const builtin = @import("builtin");

    // Windows doesn't support fstatat/AT.SYMLINK_NOFOLLOW
    // Windows symlinks work differently (require admin privileges by default)
    // For Windows, we skip the symlink check
    if (builtin.os.tag == .windows) {
        return false;
    }

    // Unix/Linux/macOS implementation
    // Try to stat the file without following symlinks
    // We use fstatat with AT.SYMLINK_NOFOLLOW flag

    // First, open the directory
    var dir = std.fs.cwd().openDir(dir_path, .{}) catch |err| switch (err) {
        error.FileNotFound => return false, // Directory doesn't exist, so file can't be a symlink
        else => return err,
    };
    defer dir.close();

    // Get the file descriptor
    const dir_fd = dir.fd;

    // Prepare the filename as null-terminated
    var path_buf: [std.fs.max_path_bytes:0]u8 = undefined;
    if (file_path.len >= path_buf.len) {
        return error.NameTooLong;
    }
    @memcpy(path_buf[0..file_path.len], file_path);
    path_buf[file_path.len] = 0;

    // Use fstatat with SYMLINK_NOFOLLOW to check without following
    const stat_result = std.posix.fstatatZ(
        dir_fd,
        @ptrCast(&path_buf),
        std.posix.AT.SYMLINK_NOFOLLOW,
    ) catch |err| switch (err) {
        error.FileNotFound => return false, // File doesn't exist, not a symlink
        else => return err,
    };

    // Check if it's a symlink using mode field
    // S_IFLNK = 0xA000 on most Unix systems (symlink file type)
    // S_IFMT = 0xF000 (mask for file type bits)
    const S_IFMT: u32 = 0xF000;
    const S_IFLNK: u32 = 0xA000;
    const file_type = stat_result.mode & S_IFMT;
    return file_type == S_IFLNK;
}

// ============================================================================
// WAL Writer
// ============================================================================

/// WAL Writer - handles buffered writing of WAL records to disk
///
/// The writer buffers records in memory and flushes them to disk when:
/// 1. The buffer reaches the page size threshold
/// 2. flush() is called explicitly (e.g., on transaction commit)
/// 3. The file reaches the rotation size limit
///
/// Thread safety: NOT thread-safe. Caller must synchronize access.
pub const WalWriter = struct {
    /// File handle for the current WAL file
    file: std.fs.File,

    /// Directory where WAL files are stored
    wal_dir: []const u8,

    /// Write buffer for batching records
    buffer: std.array_list.Managed(u8),

    /// Current sequence number (for file rotation)
    sequence: u64,

    /// Next LSN (Logical Sequence Number) to assign
    next_lsn: u64,

    /// Page size for buffering (flush when buffer reaches this size)
    page_size: u32,

    /// Maximum file size before rotation (default: 16MB)
    max_file_size: u64,

    /// Current file size in bytes
    current_file_size: u64,

    /// Total size of all WAL files in bytes (for disk exhaustion protection)
    total_wal_size: u64,

    /// Maximum total WAL size before refusing new writes (default: 1GB)
    max_total_wal_size: u64,

    /// Allocator for buffer management
    allocator: Allocator,

    pub const DEFAULT_PAGE_SIZE: u32 = 4096; // 4KB
    pub const DEFAULT_MAX_FILE_SIZE: u64 = 16 * 1024 * 1024; // 16MB
    pub const DEFAULT_MAX_TOTAL_WAL_SIZE: u64 = 1 * 1024 * 1024 * 1024; // 1GB

    /// Initialize a new WAL writer
    ///
    /// Creates the WAL directory if it doesn't exist.
    /// Creates a new WAL file with sequence number 0.
    pub fn init(allocator: Allocator, wal_dir: []const u8) !WalWriter {
        return initWithOptions(allocator, wal_dir, .{});
    }

    pub const InitOptions = struct {
        page_size: u32 = DEFAULT_PAGE_SIZE,
        max_file_size: u64 = DEFAULT_MAX_FILE_SIZE,
        max_total_wal_size: u64 = DEFAULT_MAX_TOTAL_WAL_SIZE,
        sequence: u64 = 0,
    };

    pub fn initWithOptions(allocator: Allocator, wal_dir: []const u8, options: InitOptions) !WalWriter {
        // Validate path for security (prevent path traversal attacks)
        try validateWalPath(wal_dir);

        // Create WAL directory if it doesn't exist
        std.fs.cwd().makePath(wal_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };

        // Create first WAL file
        const file = try createWalFile(wal_dir, options.sequence, options.page_size);

        var buffer = std.array_list.Managed(u8).init(allocator);
        try buffer.ensureTotalCapacity(options.page_size);

        return WalWriter{
            .file = file,
            .wal_dir = wal_dir,
            .buffer = buffer,
            .sequence = options.sequence,
            .next_lsn = 1,
            .page_size = options.page_size,
            .max_file_size = options.max_file_size,
            .current_file_size = WalHeader.SIZE, // Header already written
            .total_wal_size = WalHeader.SIZE, // Start with just the header
            .max_total_wal_size = options.max_total_wal_size,
            .allocator = allocator,
        };
    }

    /// Create a new WAL file with the given sequence number
    fn createWalFile(wal_dir: []const u8, sequence: u64, page_size: u32) !std.fs.File {
        const allocator = std.heap.page_allocator;

        // Generate base filename (without directory)
        const base_filename = try std.fmt.allocPrint(allocator, "wal.{d:0>6}", .{sequence});
        defer allocator.free(base_filename);

        // Check if path is a symlink (defense-in-depth against symlink attacks)
        if (try isSymlink(wal_dir, base_filename)) {
            return error.SymlinkNotAllowed;
        }

        // Generate full path
        const filename = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, base_filename });
        defer allocator.free(filename);

        // Create the file with exclusive flag to prevent race conditions
        const file = try std.fs.cwd().createFile(filename, .{
            .read = true,
            .truncate = false,
            .exclusive = true, // Fail if file already exists (prevents TOCTOU)
        });

        // Write header
        const header = WalHeader.init(sequence, page_size);
        var header_buffer: [WalHeader.SIZE]u8 = undefined;
        header.serialize(&header_buffer);
        try file.writeAll(&header_buffer);

        return file;
    }

    pub fn deinit(self: *WalWriter) void {
        // Flush any remaining buffered data
        self.flush() catch |err| {
            std.debug.print("Warning: Failed to flush WAL on deinit: {}\n", .{err});
        };

        self.file.close();
        self.buffer.deinit();
    }

    /// Write a WAL record
    ///
    /// The record is buffered and will be flushed when:
    /// - Buffer reaches page_size
    /// - flush() is called explicitly
    /// - File rotation occurs
    pub fn writeRecord(self: *WalWriter, record: WalRecord) !u64 {
        // Assign LSN to this record
        const lsn = self.next_lsn;
        self.next_lsn += 1;

        // Create record with assigned LSN
        var record_with_lsn = record;
        record_with_lsn.lsn = lsn;

        // Calculate serialized size
        const size = record_with_lsn.serializedSize();

        // Check for disk exhaustion (prevent unlimited WAL growth)
        if (self.total_wal_size + size > self.max_total_wal_size) {
            return error.WalDiskQuotaExceeded;
        }

        // Check if we need to rotate the file
        if (self.current_file_size + size > self.max_file_size) {
            try self.rotate();
        }

        // Ensure buffer has space
        try self.buffer.ensureUnusedCapacity(size);

        // Serialize record into buffer
        const start = self.buffer.items.len;
        try self.buffer.resize(start + size);
        try record_with_lsn.serialize(self.buffer.items[start..], self.allocator);

        // Update file size estimates
        self.current_file_size += size;
        self.total_wal_size += size;

        // Flush if buffer is full
        if (self.buffer.items.len >= self.page_size) {
            try self.flush();
        }

        return lsn;
    }

    /// Flush buffered records to disk with fsync
    ///
    /// This ensures durability - data is guaranteed to be on disk after this returns.
    pub fn flush(self: *WalWriter) !void {
        if (self.buffer.items.len == 0) {
            return; // Nothing to flush
        }

        // Write buffer to file
        try self.file.writeAll(self.buffer.items);

        // CRITICAL: fsync to ensure data is physically on disk
        try self.file.sync();

        // Clear buffer
        self.buffer.clearRetainingCapacity();
    }

    /// Rotate to a new WAL file
    ///
    /// Closes the current file and creates a new one with incremented sequence number.
    fn rotate(self: *WalWriter) !void {
        // Flush current buffer
        try self.flush();

        // CRITICAL FIX: Create new file BEFORE closing the old one
        // This prevents leaving self.file in a broken state if createWalFile fails
        // (e.g., due to disk full, permissions, etc.)
        const new_sequence = self.sequence + 1;
        const new_file = try createWalFile(self.wal_dir, new_sequence, self.page_size);

        // Now it's safe to close the old file
        self.file.close();

        // Update to new file and sequence
        self.file = new_file;
        self.sequence = new_sequence;

        // Reset file size (just the header)
        self.current_file_size = WalHeader.SIZE;
    }

    /// Write a checkpoint marker
    ///
    /// After a checkpoint, all WAL files with sequence < checkpoint_sequence
    /// can be safely deleted.
    pub fn writeCheckpoint(self: *WalWriter) !u64 {
        const record = WalRecord{
            .record_type = .checkpoint,
            .tx_id = 0,
            .lsn = 0, // Will be assigned by writeRecord
            .row_id = 0,
            .table_name = "",
            .data = "",
            .checksum = 0,
        };

        const lsn = try self.writeRecord(record);
        try self.flush(); // Ensure checkpoint is durable

        return lsn;
    }

    /// Get the current sequence number
    pub fn getCurrentSequence(self: *const WalWriter) u64 {
        return self.sequence;
    }

    /// Get the next LSN that will be assigned
    pub fn getNextLsn(self: *const WalWriter) u64 {
        return self.next_lsn;
    }

    /// Get the current total WAL size across all files
    pub fn getTotalWalSize(self: *const WalWriter) u64 {
        return self.total_wal_size;
    }

    /// Get the maximum allowed total WAL size
    pub fn getMaxTotalWalSize(self: *const WalWriter) u64 {
        return self.max_total_wal_size;
    }

    /// Delete old WAL files before a given sequence number
    ///
    /// This should be called after a checkpoint to reclaim disk space.
    /// Reduces total_wal_size by the size of deleted files.
    ///
    /// WARNING: Only call this after ensuring the database state is
    /// persisted to disk (checkpoint complete).
    pub fn deleteOldWalFiles(self: *WalWriter, before_sequence: u64) !void {
        const allocator = std.heap.page_allocator;

        // Don't delete the current file
        if (before_sequence > self.sequence) {
            return error.CannotDeleteCurrentWalFile;
        }

        var seq: u64 = 0;
        while (seq < before_sequence) : (seq += 1) {
            // Generate filename
            const filename = try std.fmt.allocPrint(
                allocator,
                "{s}/wal.{d:0>6}",
                .{ self.wal_dir, seq },
            );
            defer allocator.free(filename);

            // Get file size before deleting (for accounting)
            const stat = std.fs.cwd().statFile(filename) catch |err| switch (err) {
                error.FileNotFound => continue, // Already deleted or never existed
                else => return err,
            };

            // Delete the file
            std.fs.cwd().deleteFile(filename) catch |err| switch (err) {
                error.FileNotFound => continue, // Race condition, already deleted
                else => return err,
            };

            // Update total WAL size
            if (self.total_wal_size >= stat.size) {
                self.total_wal_size -= stat.size;
            } else {
                // Shouldn't happen, but prevent underflow
                self.total_wal_size = 0;
            }
        }
    }
};

// ============================================================================
// WAL Reader
// ============================================================================

/// WAL Reader - reads WAL records from disk for recovery
pub const WalReader = struct {
    file: std.fs.File,
    allocator: Allocator,
    buffer: []u8,
    buffer_pos: usize,
    buffer_len: usize,
    eof: bool,

    pub const BUFFER_SIZE: usize = 65536; // 64KB read buffer

    /// Open a WAL file for reading
    pub fn init(allocator: Allocator, wal_path: []const u8) !WalReader {
        // Validate path for security (prevent path traversal attacks)
        try validateWalPath(wal_path);

        // Check if path is a symlink (additional security)
        if (std.fs.path.dirname(wal_path)) |dir_path| {
            const base = std.fs.path.basename(wal_path);
            if (try isSymlink(dir_path, base)) {
                return error.SymlinkNotAllowed;
            }
        } else {
            // No directory component, check in current directory
            if (try isSymlink(".", wal_path)) {
                return error.SymlinkNotAllowed;
            }
        }

        const file = try std.fs.cwd().openFile(wal_path, .{});
        errdefer file.close();

        // Read and validate header
        var header_buffer: [WalHeader.SIZE]u8 = undefined;
        const n = try file.readAll(&header_buffer);
        if (n != WalHeader.SIZE) {
            return error.InvalidWalFile;
        }

        const header = WalHeader.deserialize(&header_buffer);
        try header.validate();

        // Allocate read buffer
        const buffer = try allocator.alloc(u8, BUFFER_SIZE);

        return WalReader{
            .file = file,
            .allocator = allocator,
            .buffer = buffer,
            .buffer_pos = 0,
            .buffer_len = 0,
            .eof = false,
        };
    }

    pub fn deinit(self: *WalReader) void {
        self.file.close();
        self.allocator.free(self.buffer);
    }

    /// Read the next WAL record
    ///
    /// Returns null at end of file.
    /// Returned record must be deinit'd by the caller.
    pub fn readRecord(self: *WalReader) !?WalRecord {
        if (self.eof and self.buffer_len - self.buffer_pos == 0) {
            return null;
        }

        // Ensure we have enough data in buffer
        if (self.buffer_len - self.buffer_pos < 1024) { // Need at least minimal record size
            try self.fillBuffer();
            if (self.buffer_len == 0) {
                return null; // EOF
            }
        }

        // Try to deserialize a record
        const remaining = self.buffer[self.buffer_pos..self.buffer_len];
        const result = WalRecord.deserialize(remaining, self.allocator) catch |err| switch (err) {
            error.BufferTooSmall => {
                // Need more data
                try self.fillBuffer();
                if (self.buffer_len - self.buffer_pos == 0) {
                    return null; // EOF
                }
                // Try again with more data
                const remaining2 = self.buffer[self.buffer_pos..self.buffer_len];
                const result2 = try WalRecord.deserialize(remaining2, self.allocator);
                self.buffer_pos += result2.bytes_read;
                return result2.record;
            },
            else => return err,
        };

        self.buffer_pos += result.bytes_read;
        return result.record;
    }

    /// Fill the read buffer from the file
    fn fillBuffer(self: *WalReader) !void {
        // Move remaining data to start of buffer
        if (self.buffer_pos > 0 and self.buffer_pos < self.buffer_len) {
            const remaining = self.buffer_len - self.buffer_pos;
            std.mem.copyForwards(u8, self.buffer[0..remaining], self.buffer[self.buffer_pos..self.buffer_len]);
            self.buffer_len = remaining;
            self.buffer_pos = 0;
        } else if (self.buffer_pos >= self.buffer_len) {
            self.buffer_len = 0;
            self.buffer_pos = 0;
        }

        // Read more data from file
        const bytes_read = try self.file.read(self.buffer[self.buffer_len..]);
        if (bytes_read == 0) {
            self.eof = true;
        }
        self.buffer_len += bytes_read;
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

test "WalWriter: init and deinit" {
    const allocator = testing.allocator;

    // Create temporary directory for test
    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_init_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    try testing.expectEqual(@as(u64, 0), writer.sequence);
    try testing.expectEqual(@as(u64, 1), writer.next_lsn);
}

test "WalWriter: write and flush record" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_write_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    // Write a BEGIN record
    const record = WalRecord{
        .record_type = .begin_tx,
        .tx_id = 1,
        .lsn = 0, // Will be assigned
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    };

    const lsn = try writer.writeRecord(record);
    try testing.expectEqual(@as(u64, 1), lsn);
    try testing.expectEqual(@as(u64, 2), writer.next_lsn);

    // Flush to ensure data is on disk
    try writer.flush();
}

test "WalWriter: multiple records" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_multiple_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    // Write multiple records
    const lsn1 = try writer.writeRecord(.{
        .record_type = .begin_tx,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    });

    const lsn2 = try writer.writeRecord(.{
        .record_type = .insert_row,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 100,
        .table_name = "users",
        .data = "row_data",
        .checksum = 0,
    });

    const lsn3 = try writer.writeRecord(.{
        .record_type = .commit_tx,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    });

    try testing.expectEqual(@as(u64, 1), lsn1);
    try testing.expectEqual(@as(u64, 2), lsn2);
    try testing.expectEqual(@as(u64, 3), lsn3);

    try writer.flush();
}

test "WalWriter: checkpoint" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_checkpoint_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    // Write some records
    _ = try writer.writeRecord(.{
        .record_type = .begin_tx,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 0,
        .table_name = "",
        .data = "",
        .checksum = 0,
    });

    // Write checkpoint
    const checkpoint_lsn = try writer.writeCheckpoint();
    try testing.expect(checkpoint_lsn > 0);
}

test "WalReader: read records" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_read_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Write some records
    {
        var writer = try WalWriter.init(allocator, test_dir);
        defer writer.deinit();

        _ = try writer.writeRecord(.{
            .record_type = .begin_tx,
            .tx_id = 1,
            .lsn = 0,
            .row_id = 0,
            .table_name = "",
            .data = "",
            .checksum = 0,
        });

        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = 1,
            .lsn = 0,
            .row_id = 123,
            .table_name = "test_table",
            .data = "test_data",
            .checksum = 0,
        });

        _ = try writer.writeRecord(.{
            .record_type = .commit_tx,
            .tx_id = 1,
            .lsn = 0,
            .row_id = 0,
            .table_name = "",
            .data = "",
            .checksum = 0,
        });

        try writer.flush();
    }

    // Read the records back
    const wal_path = try std.fmt.allocPrint(allocator, "{s}/wal.{d:0>6}", .{ test_dir, 0 });
    defer allocator.free(wal_path);

    var reader = try WalReader.init(allocator, wal_path);
    defer reader.deinit();

    // Read first record (BEGIN)
    const record1 = try reader.readRecord();
    try testing.expect(record1 != null);
    var r1 = record1.?;
    defer r1.deinit(allocator);
    try testing.expectEqual(WalRecordType.begin_tx, r1.record_type);
    try testing.expectEqual(@as(u64, 1), r1.tx_id);
    try testing.expectEqual(@as(u64, 1), r1.lsn);

    // Read second record (INSERT)
    const record2 = try reader.readRecord();
    try testing.expect(record2 != null);
    var r2 = record2.?;
    defer r2.deinit(allocator);
    try testing.expectEqual(WalRecordType.insert_row, r2.record_type);
    try testing.expectEqual(@as(u64, 1), r2.tx_id);
    try testing.expectEqual(@as(u64, 2), r2.lsn);
    try testing.expectEqual(@as(u64, 123), r2.row_id);
    try testing.expectEqualStrings("test_table", r2.table_name);
    try testing.expectEqualStrings("test_data", r2.data);

    // Read third record (COMMIT)
    const record3 = try reader.readRecord();
    try testing.expect(record3 != null);
    var r3 = record3.?;
    defer r3.deinit(allocator);
    try testing.expectEqual(WalRecordType.commit_tx, r3.record_type);
    try testing.expectEqual(@as(u64, 1), r3.tx_id);
    try testing.expectEqual(@as(u64, 3), r3.lsn);

    // EOF
    const record4 = try reader.readRecord();
    try testing.expect(record4 == null);
}

test "WalWriter: file rotation" {
    const allocator = testing.allocator;

    // Generate unique directory name to avoid race conditions when tests run in parallel
    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_rotation_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create writer with small max file size to trigger rotation
    var writer = try WalWriter.initWithOptions(allocator, test_dir, .{
        .max_file_size = 1024, // 1KB - very small for testing
    });
    defer writer.deinit();

    const initial_sequence = writer.getCurrentSequence();

    // Write many records to trigger rotation
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test_table_with_long_name",
            .data = "some_data_here_to_make_record_larger",
            .checksum = 0,
        });
    }

    try writer.flush();

    // Should have rotated to a new file
    const final_sequence = writer.getCurrentSequence();
    try testing.expect(final_sequence > initial_sequence);
}

test "WalWriter and WalReader: round-trip with large data" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_roundtrip_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    const large_data = try allocator.alloc(u8, 10000);
    defer allocator.free(large_data);
    @memset(large_data, 'X');

    // Write a record with large data
    {
        var writer = try WalWriter.init(allocator, test_dir);
        defer writer.deinit();

        _ = try writer.writeRecord(.{
            .record_type = .update_row,
            .tx_id = 99,
            .lsn = 0,
            .row_id = 456,
            .table_name = "large_table",
            .data = large_data,
            .checksum = 0,
        });

        try writer.flush();
    }

    // Read it back
    const wal_path = try std.fmt.allocPrint(allocator, "{s}/wal.{d:0>6}", .{ test_dir, 0 });
    defer allocator.free(wal_path);

    var reader = try WalReader.init(allocator, wal_path);
    defer reader.deinit();

    const record = try reader.readRecord();
    try testing.expect(record != null);
    var r = record.?;
    defer r.deinit(allocator);

    try testing.expectEqual(WalRecordType.update_row, r.record_type);
    try testing.expectEqual(@as(u64, 99), r.tx_id);
    try testing.expectEqual(@as(u64, 456), r.row_id);
    try testing.expectEqualStrings("large_table", r.table_name);
    try testing.expectEqual(large_data.len, r.data.len);
}

// ============================================================================
// Security Tests
// ============================================================================

test "validateWalPath: accepts safe paths" {
    // These paths should be accepted
    const safe_paths = [_][]const u8{
        "wal",
        "data/wal",
        "./wal",
        "my_database/wal_files",
        "a/b/c/d/wal",
    };

    for (safe_paths) |path| {
        try validateWalPath(path);
    }
}

test "validateWalPath: rejects absolute Unix paths" {
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("/tmp/wal"));
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("/etc/shadow"));
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("/var/lib/db"));
}

test "validateWalPath: rejects absolute Windows paths" {
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("C:\\Windows\\System32"));
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("D:\\data"));
    try testing.expectError(error.AbsolutePathNotAllowed, validateWalPath("c:\\temp")); // lowercase too
}

test "validateWalPath: rejects path traversal attempts" {
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("../etc"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("../../tmp"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("../../../root"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("wal/../../../etc"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("good/../../bad"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("a/b/../../../c"));
}

test "validateWalPath: rejects Windows-style path traversal" {
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("wal\\..\\..\\etc"));
    try testing.expectError(error.PathTraversalNotAllowed, validateWalPath("..\\Windows"));
}

test "validateWalPath: rejects empty path" {
    try testing.expectError(error.InvalidWalPath, validateWalPath(""));
}

test "validateWalPath: rejects path with null byte" {
    const path_with_null = "wal\x00/bad";
    try testing.expectError(error.InvalidWalPath, validateWalPath(path_with_null));
}

test "validateWalPath: rejects excessively long path" {
    var long_path: [300]u8 = undefined;
    @memset(&long_path, 'a');
    try testing.expectError(error.WalPathTooLong, validateWalPath(&long_path));
}

test "WalWriter: rejects dangerous path on init" {
    const allocator = testing.allocator;

    // Should fail to initialize with dangerous path
    try testing.expectError(
        error.PathTraversalNotAllowed,
        WalWriter.init(allocator, "../../../tmp/evil"),
    );

    try testing.expectError(
        error.AbsolutePathNotAllowed,
        WalWriter.init(allocator, "/tmp/wal"),
    );
}

test "WalReader: rejects dangerous path on init" {
    const allocator = testing.allocator;

    // Should fail to open with dangerous path
    try testing.expectError(
        error.PathTraversalNotAllowed,
        WalReader.init(allocator, "../../../etc/shadow"),
    );

    try testing.expectError(
        error.AbsolutePathNotAllowed,
        WalReader.init(allocator, "/etc/passwd"),
    );
}

test "WalWriter: disk quota enforcement" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_quota_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create writer with very small quota (1KB)
    var writer = try WalWriter.initWithOptions(allocator, test_dir, .{
        .max_total_wal_size = 1024, // 1KB limit
    });
    defer writer.deinit();

    // Should be able to write some records
    var i: usize = 0;
    while (i < 10) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test",
            .data = "small_data",
            .checksum = 0,
        });
    }

    // Eventually should hit quota
    var hit_quota = false;
    i = 10;
    while (i < 1000) : (i += 1) {
        _ = writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test_table_with_longer_name",
            .data = "much_larger_data_to_fill_quota_faster",
            .checksum = 0,
        }) catch |err| {
            if (err == error.WalDiskQuotaExceeded) {
                hit_quota = true;
                break;
            }
            return err;
        };
    }

    try testing.expect(hit_quota);
    try testing.expect(writer.getTotalWalSize() <= writer.getMaxTotalWalSize());
}

test "WalWriter: cleanup old files reduces quota" {
    const allocator = testing.allocator;

    // Generate unique directory name to avoid race conditions when tests run in parallel
    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_cleanup_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.initWithOptions(allocator, test_dir, .{
        .max_file_size = 512, // Small files to trigger rotation
    });
    defer writer.deinit();

    // Write enough to create multiple files
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test_table_name",
            .data = "some_data_to_write_here",
            .checksum = 0,
        });
    }

    try writer.flush();

    // Should have rotated at least once
    const final_seq = writer.getCurrentSequence();
    try testing.expect(final_seq > 0);

    // Record size before cleanup
    const size_before = writer.getTotalWalSize();

    // Clean up old files
    try writer.deleteOldWalFiles(final_seq);

    // Size should be reduced (only current file remains)
    const size_after = writer.getTotalWalSize();
    try testing.expect(size_after < size_before);
}

test "WalWriter: rejects symlink" {
    const builtin = @import("builtin");

    // Skip on Windows: symlink creation requires special privileges
    // and the security concern is primarily Unix/Linux-specific
    if (builtin.os.tag == .windows) {
        return error.SkipZigTest;
    }

    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_symlink_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create directory
    try std.fs.cwd().makePath(test_dir);

    // Create a regular file
    {
        const target_file_path = try std.fmt.allocPrint(allocator, "{s}/target.txt", .{test_dir});
        defer allocator.free(target_file_path);
        const target_file = try std.fs.cwd().createFile(target_file_path, .{});
        defer target_file.close();
        try target_file.writeAll("target");
    }

    // Create symlink to the file (if supported by OS)
    const symlink_path = try std.fmt.allocPrint(allocator, "{s}/wal.000000", .{test_dir});
    defer allocator.free(symlink_path);
    std.posix.symlink("target.txt", symlink_path) catch |err| {
        // Skip test on systems without symlink support or if not permitted
        std.debug.print("Skipping symlink test: {}\n", .{err});
        return;
    };

    // WalWriter should detect and reject the symlink
    try testing.expectError(
        error.SymlinkNotAllowed,
        WalWriter.init(allocator, test_dir),
    );
}

test "WalWriter: total size tracking accurate" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_size_tracking_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    const initial_size = writer.getTotalWalSize();
    try testing.expectEqual(WalHeader.SIZE, initial_size);

    // Write a record and verify size increases
    const record = WalRecord{
        .record_type = .insert_row,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 100,
        .table_name = "users",
        .data = "test_data",
        .checksum = 0,
    };

    const expected_increase = record.serializedSize();
    _ = try writer.writeRecord(record);

    const size_after = writer.getTotalWalSize();
    try testing.expectEqual(initial_size + expected_increase, size_after);
}

test "WalReader: rejects symlink" {
    const builtin = @import("builtin");

    // Skip on Windows: symlink creation requires special privileges
    // and the security concern is primarily Unix/Linux-specific
    if (builtin.os.tag == .windows) {
        return error.SkipZigTest;
    }

    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_walreader_symlink_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create test directory
    try std.fs.cwd().makePath(test_dir);

    // Create a valid WAL file first
    var writer = try WalWriter.init(allocator, test_dir);
    _ = try writer.writeRecord(.{
        .record_type = .insert_row,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 1,
        .table_name = "test",
        .data = "data",
        .checksum = 0,
    });
    try writer.flush();
    writer.deinit();

    // Create symlink to the WAL file
    const symlink_path = try std.fmt.allocPrint(allocator, "{s}/wal_symlink.000000", .{test_dir});
    defer allocator.free(symlink_path);
    const target_path = "wal.000000";
    std.posix.symlink(target_path, symlink_path) catch |err| {
        std.debug.print("Skipping symlink test: {}\n", .{err});
        return;
    };

    // WalReader should detect and reject the symlink
    try testing.expectError(
        error.SymlinkNotAllowed,
        WalReader.init(allocator, symlink_path),
    );
}

test "isSymlink: returns false for regular file" {
    const test_dir = "test_isSymlink_regular";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create test directory
    try std.fs.cwd().makePath(test_dir);

    // Create a regular file
    {
        const file = try std.fs.cwd().createFile(test_dir ++ "/regular.txt", .{});
        defer file.close();
        try file.writeAll("regular file content");
    }

    // isSymlink should return false for regular file
    const result = try isSymlink(test_dir, "regular.txt");
    try testing.expectEqual(false, result);
}

test "isSymlink: returns false for directory" {
    const test_dir = "test_isSymlink_directory";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create test directory with a subdirectory
    try std.fs.cwd().makePath(test_dir ++ "/subdir");

    // isSymlink should return false for directory
    const result = try isSymlink(test_dir, "subdir");
    try testing.expectEqual(false, result);
}

test "isSymlink: returns false for non-existent file" {
    const test_dir = "test_isSymlink_nonexistent";
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create test directory only (no file)
    try std.fs.cwd().makePath(test_dir);

    // isSymlink should return false for non-existent file
    const result = try isSymlink(test_dir, "nonexistent.txt");
    try testing.expectEqual(false, result);
}

test "WalWriter: deleteOldWalFiles rejects deleting current file" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_delete_current_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    // Write some records
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test",
            .data = "data",
            .checksum = 0,
        });
    }

    const current_seq = writer.getCurrentSequence();

    // Trying to delete a sequence beyond current should error
    try testing.expectError(
        error.CannotDeleteCurrentWalFile,
        writer.deleteOldWalFiles(current_seq + 1),
    );
}

test "WalWriter: deleteOldWalFiles with sequence 0 is no-op" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_delete_zero_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    var writer = try WalWriter.init(allocator, test_dir);
    defer writer.deinit();

    // Write some records
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test",
            .data = "data",
            .checksum = 0,
        });
    }

    try writer.flush();

    const size_before = writer.getTotalWalSize();

    // Deleting with sequence 0 should be a no-op (no files before sequence 0)
    try writer.deleteOldWalFiles(0);

    const size_after = writer.getTotalWalSize();
    try testing.expectEqual(size_before, size_after);
}

test "WalWriter: quota enforcement with very small limit" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_small_quota_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create writer with extremely small quota (100 bytes)
    var writer = try WalWriter.initWithOptions(allocator, test_dir, .{
        .max_total_wal_size = 100,
    });
    defer writer.deinit();

    // First record should succeed (we start with just a header which is 36 bytes)
    _ = try writer.writeRecord(.{
        .record_type = .insert_row,
        .tx_id = 1,
        .lsn = 0,
        .row_id = 1,
        .table_name = "test",
        .data = "data",
        .checksum = 0,
    });

    // Second record should hit quota immediately
    try testing.expectError(
        error.WalDiskQuotaExceeded,
        writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = 2,
            .lsn = 0,
            .row_id = 2,
            .table_name = "test",
            .data = "data",
            .checksum = 0,
        }),
    );
}

test "WalWriter: size tracking across file rotation" {
    const allocator = testing.allocator;

    const random_id = std.crypto.random.int(u64);
    const test_dir = try std.fmt.allocPrint(allocator, "test_wal_rotation_size_{x}", .{random_id});
    defer allocator.free(test_dir);
    defer std.fs.cwd().deleteTree(test_dir) catch {};

    // Create writer with small max_file_size to force rotation
    var writer = try WalWriter.initWithOptions(allocator, test_dir, .{
        .max_file_size = 200, // Very small to force rotation quickly
    });
    defer writer.deinit();

    const initial_size = writer.getTotalWalSize();
    try testing.expectEqual(WalHeader.SIZE, initial_size);

    const initial_seq = writer.getCurrentSequence();

    // Write records to force rotation
    var i: usize = 0;
    while (i < 20) : (i += 1) {
        _ = try writer.writeRecord(.{
            .record_type = .insert_row,
            .tx_id = @intCast(i),
            .lsn = 0,
            .row_id = @intCast(i),
            .table_name = "test_table",
            .data = "data_here",
            .checksum = 0,
        });
    }

    try writer.flush();

    // Verify rotation occurred
    const final_seq = writer.getCurrentSequence();
    try testing.expect(final_seq > initial_seq);

    // Verify total_wal_size tracks all files (should be > 0 and account for multiple files)
    const total_size = writer.getTotalWalSize();
    try testing.expect(total_size > initial_size);

    // Total size should account for all rotated files
    // Each file has a header (36 bytes) plus data
    const expected_min_size = (final_seq - initial_seq + 1) * WalHeader.SIZE;
    try testing.expect(total_size >= expected_min_size);
}
