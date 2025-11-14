const std = @import("std");

// ============================================================================
// I/O Utilities
// ============================================================================

/// Helper to write integers in little-endian format to a file
/// This consolidates duplicate writeInt functions from table.zig and hnsw.zig
pub fn writeInt(file: std.fs.File, comptime T: type, value: T) !void {
    var bytes: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &bytes, value, .little);
    try file.writeAll(&bytes);
}

/// Helper to read integers in little-endian format from a file
/// This consolidates duplicate readInt functions from table.zig and hnsw.zig
pub fn readInt(file: std.fs.File, comptime T: type) !T {
    var bytes: [@sizeOf(T)]u8 = undefined;
    const n = try file.readAll(&bytes);
    if (n != @sizeOf(T)) return error.UnexpectedEOF;
    return std.mem.readInt(T, &bytes, .little);
}

// ============================================================================
// Future Utilities
// ============================================================================
// This module can be extended with:
// - Common HashMap patterns
// - Memory allocation helpers
// - String utilities
// - Error handling helpers
