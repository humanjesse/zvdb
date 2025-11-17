const std = @import("std");
const testing = std.testing;
const hnsw = @import("hnsw.zig");
const HNSW = hnsw.HNSW;
const NodeMetadata = hnsw.NodeMetadata;
const MetadataValue = hnsw.MetadataValue;

test "type index doesn't hold dangling pointers after metadata update" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create initial metadata with type "document"
    var meta1 = try NodeMetadata.init(allocator, "document", "file1.txt");
    const point1 = [_]f32{ 1.0, 2.0, 3.0 };

    const id1 = try index.insertWithMetadata(&point1, null, meta1);

    // Verify the node is in the type index
    const docs = try index.getNodesByType("document");
    defer allocator.free(docs);
    try testing.expectEqual(@as(usize, 1), docs.len);
    try testing.expectEqual(id1, docs[0]);

    // Update metadata to a different type
    var meta2 = try NodeMetadata.init(allocator, "image", "file2.jpg");
    try index.updateMetadata(id1, meta2);

    // The old type index entry should be cleaned up properly
    // No dangling pointer should exist
    const docs_after = try index.getNodesByType("document");
    defer allocator.free(docs_after);
    try testing.expectEqual(@as(usize, 0), docs_after.len);

    // New type should have the node
    const images = try index.getNodesByType("image");
    defer allocator.free(images);
    try testing.expectEqual(@as(usize, 1), images.len);
    try testing.expectEqual(id1, images[0]);
}

test "file path index doesn't hold dangling pointers after node removal" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create metadata with file path
    var meta = try NodeMetadata.init(allocator, "document", "test.txt");
    const point = [_]f32{ 1.0, 2.0, 3.0 };

    const id = try index.insertWithMetadata(&point, null, meta);

    // Verify the node is in the file path index
    const nodes = try index.getNodesByFilePath("test.txt");
    defer allocator.free(nodes);
    try testing.expectEqual(@as(usize, 1), nodes.len);
    try testing.expectEqual(id, nodes[0]);

    // Remove the node
    try index.removeNode(id);

    // The file path index entry should be cleaned up properly
    // No dangling pointer should exist
    const nodes_after = try index.getNodesByFilePath("test.txt");
    defer allocator.free(nodes_after);
    try testing.expectEqual(@as(usize, 0), nodes_after.len);
}

test "multiple nodes sharing same type - key cleanup only when last removed" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create multiple nodes with the same type
    var meta1 = try NodeMetadata.init(allocator, "document", "file1.txt");
    var meta2 = try NodeMetadata.init(allocator, "document", "file2.txt");
    var meta3 = try NodeMetadata.init(allocator, "document", "file3.txt");

    const point1 = [_]f32{ 1.0, 2.0, 3.0 };
    const point2 = [_]f32{ 4.0, 5.0, 6.0 };
    const point3 = [_]f32{ 7.0, 8.0, 9.0 };

    const id1 = try index.insertWithMetadata(&point1, null, meta1);
    const id2 = try index.insertWithMetadata(&point2, null, meta2);
    const id3 = try index.insertWithMetadata(&point3, null, meta3);

    // Verify all nodes are in the type index
    const docs = try index.getNodesByType("document");
    defer allocator.free(docs);
    try testing.expectEqual(@as(usize, 3), docs.len);

    // Remove first node
    try index.removeNode(id1);
    const docs_after_1 = try index.getNodesByType("document");
    defer allocator.free(docs_after_1);
    try testing.expectEqual(@as(usize, 2), docs_after_1.len);

    // Remove second node
    try index.removeNode(id2);
    const docs_after_2 = try index.getNodesByType("document");
    defer allocator.free(docs_after_2);
    try testing.expectEqual(@as(usize, 1), docs_after_2.len);

    // Remove third node - this should clean up the key
    try index.removeNode(id3);
    const docs_after_3 = try index.getNodesByType("document");
    defer allocator.free(docs_after_3);
    try testing.expectEqual(@as(usize, 0), docs_after_3.len);
}

test "multiple nodes sharing same file path - key cleanup only when last removed" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create multiple nodes with the same file path
    var meta1 = try NodeMetadata.init(allocator, "chunk", "document.txt");
    var meta2 = try NodeMetadata.init(allocator, "chunk", "document.txt");
    var meta3 = try NodeMetadata.init(allocator, "chunk", "document.txt");

    const point1 = [_]f32{ 1.0, 2.0, 3.0 };
    const point2 = [_]f32{ 4.0, 5.0, 6.0 };
    const point3 = [_]f32{ 7.0, 8.0, 9.0 };

    const id1 = try index.insertWithMetadata(&point1, null, meta1);
    const id2 = try index.insertWithMetadata(&point2, null, meta2);
    const id3 = try index.insertWithMetadata(&point3, null, meta3);

    // Verify all nodes are in the file path index
    const nodes = try index.getNodesByFilePath("document.txt");
    defer allocator.free(nodes);
    try testing.expectEqual(@as(usize, 3), nodes.len);

    // Remove first node
    try index.removeNode(id1);
    const nodes_after_1 = try index.getNodesByFilePath("document.txt");
    defer allocator.free(nodes_after_1);
    try testing.expectEqual(@as(usize, 2), nodes_after_1.len);

    // Remove second node
    try index.removeNode(id2);
    const nodes_after_2 = try index.getNodesByFilePath("document.txt");
    defer allocator.free(nodes_after_2);
    try testing.expectEqual(@as(usize, 1), nodes_after_2.len);

    // Remove third node - this should clean up the key
    try index.removeNode(id3);
    const nodes_after_3 = try index.getNodesByFilePath("document.txt");
    defer allocator.free(nodes_after_3);
    try testing.expectEqual(@as(usize, 0), nodes_after_3.len);
}

test "memory leak test with allocator tracking" {
    // Use a tracking allocator to detect leaks
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            std.debug.print("Memory leak detected!\n", .{});
            @panic("Memory leak in HNSW index memory management");
        }
    }
    const allocator = gpa.allocator();

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create and insert multiple nodes with metadata
    for (0..10) |i| {
        const type_name = try std.fmt.allocPrint(allocator, "type_{d}", .{i % 3});
        defer allocator.free(type_name);

        const file_name = try std.fmt.allocPrint(allocator, "file_{d}.txt", .{i % 5});
        defer allocator.free(file_name);

        var meta = try NodeMetadata.init(allocator, type_name, file_name);
        const point = [_]f32{ @floatFromInt(i), @floatFromInt(i + 1), @floatFromInt(i + 2) };
        _ = try index.insertWithMetadata(&point, null, meta);
    }

    // Query the indexes to make sure they work
    const type_0 = try index.getNodesByType("type_0");
    defer allocator.free(type_0);
    try testing.expect(type_0.len > 0);

    const file_0 = try index.getNodesByFilePath("file_0.txt");
    defer allocator.free(file_0);
    try testing.expect(file_0.len > 0);

    // Remove some nodes
    for (0..5) |i| {
        try index.removeNode(i);
    }

    // Update some metadata
    for (5..8) |i| {
        const new_type = try std.fmt.allocPrint(allocator, "updated_type_{d}", .{i});
        defer allocator.free(new_type);

        var new_meta = try NodeMetadata.init(allocator, new_type, null);
        try index.updateMetadata(i, new_meta);
    }

    // The deinit should clean up all remaining resources without leaks
}

test "stress test: rapid metadata updates" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create a node
    var meta = try NodeMetadata.init(allocator, "initial", "initial.txt");
    const point = [_]f32{ 1.0, 2.0, 3.0 };
    const id = try index.insertWithMetadata(&point, null, meta);

    // Rapidly update metadata with different types and file paths
    for (0..100) |i| {
        const type_name = try std.fmt.allocPrint(allocator, "type_{d}", .{i});
        defer allocator.free(type_name);

        const file_name = try std.fmt.allocPrint(allocator, "file_{d}.txt", .{i});
        defer allocator.free(file_name);

        var new_meta = try NodeMetadata.init(allocator, type_name, file_name);
        try index.updateMetadata(id, new_meta);
    }

    // Verify the final state
    const final_nodes = try index.getNodesByType("type_99");
    defer allocator.free(final_nodes);
    try testing.expectEqual(@as(usize, 1), final_nodes.len);

    const final_files = try index.getNodesByFilePath("file_99.txt");
    defer allocator.free(final_files);
    try testing.expectEqual(@as(usize, 1), final_files.len);
}

test "type index key ownership after updateMetadata changes type" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create initial metadata
    var meta1 = try NodeMetadata.init(allocator, "type_a", null);
    const point = [_]f32{ 1.0, 2.0, 3.0 };
    const id = try index.insertWithMetadata(&point, null, meta1);

    // The original string used for meta1 is now owned by the node
    // Update to a different type - the old key should be freed
    var meta2 = try NodeMetadata.init(allocator, "type_b", null);
    try index.updateMetadata(id, meta2);

    // Verify old type is gone
    const type_a_nodes = try index.getNodesByType("type_a");
    defer allocator.free(type_a_nodes);
    try testing.expectEqual(@as(usize, 0), type_a_nodes.len);

    // Verify new type has the node
    const type_b_nodes = try index.getNodesByType("type_b");
    defer allocator.free(type_b_nodes);
    try testing.expectEqual(@as(usize, 1), type_b_nodes.len);
}

test "file path index key ownership after updateMetadata changes path" {
    const allocator = testing.allocator;

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create initial metadata
    var meta1 = try NodeMetadata.init(allocator, "doc", "path_a.txt");
    const point = [_]f32{ 1.0, 2.0, 3.0 };
    const id = try index.insertWithMetadata(&point, null, meta1);

    // Update to a different path
    var meta2 = try NodeMetadata.init(allocator, "doc", "path_b.txt");
    try index.updateMetadata(id, meta2);

    // Verify old path is gone
    const path_a_nodes = try index.getNodesByFilePath("path_a.txt");
    defer allocator.free(path_a_nodes);
    try testing.expectEqual(@as(usize, 0), path_a_nodes.len);

    // Verify new path has the node
    const path_b_nodes = try index.getNodesByFilePath("path_b.txt");
    defer allocator.free(path_b_nodes);
    try testing.expectEqual(@as(usize, 1), path_b_nodes.len);
}

test "no double-free when removing last node of a type" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            @panic("Memory leak detected");
        }
    }
    const allocator = gpa.allocator();

    var index = HNSW(f32).init(allocator, 16, 200);
    defer index.deinit();

    // Create a single node
    var meta = try NodeMetadata.init(allocator, "unique_type", "unique.txt");
    const point = [_]f32{ 1.0, 2.0, 3.0 };
    const id = try index.insertWithMetadata(&point, null, meta);

    // Remove the node - should clean up type index key without double-free
    try index.removeNode(id);

    // Verify cleanup
    const nodes = try index.getNodesByType("unique_type");
    defer allocator.free(nodes);
    try testing.expectEqual(@as(usize, 0), nodes.len);
}

test "deinit properly cleans up all index keys" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const leaked = gpa.deinit();
        if (leaked == .leak) {
            @panic("Memory leak detected");
        }
    }
    const allocator = gpa.allocator();

    {
        var index = HNSW(f32).init(allocator, 16, 200);

        // Create multiple nodes with different types and file paths
        for (0..5) |i| {
            const type_name = try std.fmt.allocPrint(allocator, "type_{d}", .{i});
            defer allocator.free(type_name);

            const file_name = try std.fmt.allocPrint(allocator, "file_{d}.txt", .{i});
            defer allocator.free(file_name);

            var meta = try NodeMetadata.init(allocator, type_name, file_name);
            const point = [_]f32{ @floatFromInt(i), @floatFromInt(i + 1), @floatFromInt(i + 2) };
            _ = try index.insertWithMetadata(&point, null, meta);
        }

        // deinit should clean up all keys properly
        index.deinit();
    }

    // GPA will catch any leaks when it deinits
}
