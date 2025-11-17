// ============================================================================
// HNSW Self-Loop Edge Test
// ============================================================================
//
// This test verifies the deadlock fix in hnsw.zig connect() function.
// It tests creating self-loop edges (source == target) which previously
// caused a deadlock by attempting to lock the same mutex twice.
//
// The fix adds explicit handling for the self-loop case where only one
// lock is acquired instead of attempting to lock the same mutex twice.
//
// Tests cover:
// 1. Single self-loop edge creation
// 2. Multiple self-loop edges on different nodes
// 3. Concurrent self-loop creation (stress test)
// 4. Mixed self-loop and normal edge creation
// ============================================================================

const std = @import("std");
const testing = std.testing;
const HNSW = @import("hnsw.zig").HNSW;
const NodeMetadata = @import("hnsw.zig").NodeMetadata;
const MetadataValue = @import("hnsw.zig").MetadataValue;

test "HNSW: Create single self-loop edge" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Insert a single node
    const point = &[_]f32{ 1.0, 2.0, 3.0 };
    const node_id = try hnsw.insert(point, null);

    // Create self-loop edge (this would previously deadlock)
    const edge_type = "self_reference";
    try hnsw.addEdge(node_id, node_id, edge_type, 1.0);

    // Verify the edge exists
    const edges = try hnsw.getEdges(node_id, edge_type);
    defer allocator.free(edges);

    try testing.expectEqual(@as(usize, 1), edges.len);
    try testing.expectEqual(node_id, edges[0].src);
    try testing.expectEqual(node_id, edges[0].dst);
}

test "HNSW: Create multiple self-loops on different nodes" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    const num_nodes = 10;
    var node_ids: [num_nodes]u64 = undefined;

    // Insert nodes
    for (&node_ids, 0..) |*id, i| {
        const point = &[_]f32{ @as(f32, @floatFromInt(i)), 0.0, 0.0 };
        id.* = try hnsw.insert(point, null);
    }

    // Create self-loop on each node
    const edge_type = "recursive";
    for (node_ids) |id| {
        try hnsw.addEdge(id, id, edge_type, 0.8);
    }

    // Verify all self-loops exist
    for (node_ids) |id| {
        const edges = try hnsw.getEdges(id, edge_type);
        defer allocator.free(edges);

        try testing.expectEqual(@as(usize, 1), edges.len);
        try testing.expectEqual(id, edges[0].src);
        try testing.expectEqual(id, edges[0].dst);
    }
}

test "HNSW: Concurrent self-loop creation stress test" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    const num_nodes = 20;
    var node_ids: [num_nodes]u64 = undefined;

    // Insert nodes
    for (&node_ids, 0..) |*id, i| {
        const point = &[_]f32{
            @as(f32, @floatFromInt(i)),
            @as(f32, @floatFromInt(i * 2)),
            @as(f32, @floatFromInt(i * 3))
        };
        id.* = try hnsw.insert(point, null);
    }

    // Thread context for concurrent self-loop creation
    const SelfLoopContext = struct {
        hnsw_ptr: *HNSW(f32),
        node_id: u64,
        edge_type: []const u8,
        result: ?bool,
    };

    fn selfLoopThreadFn(context: *SelfLoopContext) void {
        context.hnsw_ptr.addEdge(
            context.node_id,
            context.node_id,
            context.edge_type,
            1.0
        ) catch {
            context.result = false;
            return;
        };
        context.result = true;
    }

    // Create threads that all try to add self-loops concurrently
    var contexts: [num_nodes]SelfLoopContext = undefined;
    var threads: [num_nodes]std.Thread = undefined;

    const edge_type = "concurrent_self_ref";

    for (&contexts, 0..) |*ctx, i| {
        ctx.* = SelfLoopContext{
            .hnsw_ptr = &hnsw,
            .node_id = node_ids[i],
            .edge_type = edge_type,
            .result = null,
        };
    }

    // Spawn all threads
    for (&threads, 0..) |*thread, i| {
        thread.* = try std.Thread.spawn(.{}, selfLoopThreadFn, .{&contexts[i]});
    }

    // Wait for completion
    for (&threads) |*thread| {
        thread.join();
    }

    // Verify all threads succeeded (no deadlocks)
    for (contexts) |ctx| {
        try testing.expect(ctx.result orelse false);
    }

    // Verify all self-loops were created
    for (node_ids) |id| {
        const edges = try hnsw.getEdges(id, edge_type);
        defer allocator.free(edges);
        try testing.expectEqual(@as(usize, 1), edges.len);
    }
}

test "HNSW: Mixed self-loop and normal edges" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Insert 5 nodes
    const node1 = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const node2 = try hnsw.insert(&[_]f32{ 0.0, 1.0, 0.0 }, null);
    const node3 = try hnsw.insert(&[_]f32{ 0.0, 0.0, 1.0 }, null);
    const node4 = try hnsw.insert(&[_]f32{ 1.0, 1.0, 0.0 }, null);
    const node5 = try hnsw.insert(&[_]f32{ 1.0, 1.0, 1.0 }, null);

    const edge_type = "mixed_edges";

    // Create normal edges
    try hnsw.addEdge(node1, node2, edge_type, 0.9);
    try hnsw.addEdge(node2, node3, edge_type, 0.8);
    try hnsw.addEdge(node3, node4, edge_type, 0.7);

    // Create self-loops
    try hnsw.addEdge(node1, node1, edge_type, 1.0); // Self-loop
    try hnsw.addEdge(node3, node3, edge_type, 1.0); // Self-loop
    try hnsw.addEdge(node5, node5, edge_type, 1.0); // Self-loop

    // More normal edges
    try hnsw.addEdge(node4, node5, edge_type, 0.6);

    // Verify self-loops
    const edges1 = try hnsw.getEdges(node1, edge_type);
    defer allocator.free(edges1);
    var has_self_loop1 = false;
    for (edges1) |edge| {
        if (edge.src == node1 and edge.dst == node1) {
            has_self_loop1 = true;
        }
    }
    try testing.expect(has_self_loop1);

    const edges3 = try hnsw.getEdges(node3, edge_type);
    defer allocator.free(edges3);
    var has_self_loop3 = false;
    for (edges3) |edge| {
        if (edge.src == node3 and edge.dst == node3) {
            has_self_loop3 = true;
        }
    }
    try testing.expect(has_self_loop3);

    const edges5 = try hnsw.getEdges(node5, edge_type);
    defer allocator.free(edges5);
    var has_self_loop5 = false;
    for (edges5) |edge| {
        if (edge.src == node5 and edge.dst == node5) {
            has_self_loop5 = true;
        }
    }
    try testing.expect(has_self_loop5);
}

test "HNSW: Self-loop with metadata" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create node with metadata
    var metadata = NodeMetadata.init(allocator);
    try metadata.setAttribute(allocator, "type", MetadataValue{ .string = "recursive_node" });
    try metadata.setAttribute(allocator, "depth", MetadataValue{ .int = 0 });

    const point = &[_]f32{ 1.0, 2.0, 3.0 };
    const node_id = try hnsw.insert(point, metadata);

    // Create self-loop edge
    try hnsw.addEdge(node_id, node_id, "self", 1.0);

    // Verify edge exists
    const edges = try hnsw.getEdges(node_id, "self");
    defer allocator.free(edges);

    try testing.expectEqual(@as(usize, 1), edges.len);
    try testing.expectEqual(node_id, edges[0].src);
    try testing.expectEqual(node_id, edges[0].dst);

    // Verify metadata is still intact
    const retrieved_metadata = hnsw.getNodeMetadata(node_id) orelse {
        try testing.expect(false);
        return;
    };

    const type_value = retrieved_metadata.getAttribute("type") orelse {
        try testing.expect(false);
        return;
    };

    switch (type_value) {
        .string => |s| try testing.expect(std.mem.eql(u8, "recursive_node", s)),
        else => try testing.expect(false),
    }
}

test "HNSW: Self-loop removal" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    const point = &[_]f32{ 1.0, 2.0, 3.0 };
    const node_id = try hnsw.insert(point, null);

    // Add self-loop
    try hnsw.addEdge(node_id, node_id, "temp_self", 1.0);

    // Verify it exists
    {
        const edges = try hnsw.getEdges(node_id, "temp_self");
        defer allocator.free(edges);
        try testing.expectEqual(@as(usize, 1), edges.len);
    }

    // Remove the self-loop
    try hnsw.removeEdge(node_id, node_id, "temp_self");

    // Verify it's gone
    {
        const edges = try hnsw.getEdges(node_id, "temp_self");
        defer allocator.free(edges);
        try testing.expectEqual(@as(usize, 0), edges.len);
    }
}

test "HNSW: Graph traversal with self-loops" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a chain with self-loops: 1 -> 2 -> 3
    // where 1 and 3 have self-loops
    const node1 = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const node2 = try hnsw.insert(&[_]f32{ 2.0, 0.0, 0.0 }, null);
    const node3 = try hnsw.insert(&[_]f32{ 3.0, 0.0, 0.0 }, null);

    const edge_type = "chain";

    // Create chain
    try hnsw.addEdge(node1, node2, edge_type, 1.0);
    try hnsw.addEdge(node2, node3, edge_type, 1.0);

    // Add self-loops
    try hnsw.addEdge(node1, node1, edge_type, 1.0);
    try hnsw.addEdge(node3, node3, edge_type, 1.0);

    // Traverse from node1 with depth 2
    const reachable = try hnsw.bfsTraverse(node1, edge_type, 2);
    defer allocator.free(reachable);

    // Should reach node1 (self), node2, and node3
    try testing.expect(reachable.len >= 2);

    // Verify we can reach node2 and node3
    var found_node2 = false;
    var found_node3 = false;
    for (reachable) |id| {
        if (id == node2) found_node2 = true;
        if (id == node3) found_node3 = true;
    }
    try testing.expect(found_node2);
    try testing.expect(found_node3);
}
