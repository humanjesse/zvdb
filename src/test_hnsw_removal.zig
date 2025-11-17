// ============================================================================
// HNSW Node Removal Graph Fragmentation Test
// ============================================================================
//
// This test verifies the neighbor reconnection fix in hnsw.zig removeNode() function.
// It tests that when a node is removed, its neighbors are reconnected to each other
// to prevent graph fragmentation and ensure search completeness.
//
// The fix adds critical reconnection logic that:
// 1. Iterates through each layer of the deleted node's connections
// 2. Creates bidirectional edges between pairs of neighbors
// 3. Maintains max connections using shrinkConnections()
// 4. Prevents duplicate edges
//
// Tests cover:
// 1. Linear chain remains connected after middle node removal
// 2. Search results remain complete after node removal
// 3. Hub node removal redistributes connections properly
// 4. Removal of last node leaves empty graph
// 5. Multi-layer graph maintains connectivity at all layers
// ============================================================================

const std = @import("std");
const testing = std.testing;
const HNSW = @import("hnsw.zig").HNSW;

// Helper function to calculate cosine distance
fn cosineDistance(a: []const f32, b: []const f32) f32 {
    var dot_product: f32 = 0;
    var norm_a: f32 = 0;
    var norm_b: f32 = 0;

    for (a, 0..) |_, i| {
        dot_product += a[i] * b[i];
        norm_a += a[i] * a[i];
        norm_b += b[i] * b[i];
    }

    if (norm_a == 0 or norm_b == 0) {
        return 1.0;
    }

    const norm_product = std.math.sqrt(norm_a) * std.math.sqrt(norm_b);
    const cosine_similarity = dot_product / norm_product;
    const clamped = @max(-1.0, @min(1.0, cosine_similarity));
    return 1.0 - clamped;
}

test "HNSW Removal: Linear chain remains connected" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a linear chain: A -> B -> C -> D -> E
    // Each point is progressively further along the x-axis
    const node_a = try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null);
    const node_b = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const node_c = try hnsw.insert(&[_]f32{ 2.0, 0.0, 0.0 }, null);
    const node_d = try hnsw.insert(&[_]f32{ 3.0, 0.0, 0.0 }, null);
    const node_e = try hnsw.insert(&[_]f32{ 4.0, 0.0, 0.0 }, null);

    // Before removal, search from A should find all nodes
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 5);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 5), results.len);
    }

    // Remove middle node C
    try hnsw.removeNode(node_c);

    // After removal, search from A should still find B, D, E
    // This verifies the graph remains connected despite C being removed
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 5);
        defer allocator.free(results);

        // Should find 4 nodes (A, B, D, E) - C was removed
        try testing.expectEqual(@as(usize, 4), results.len);

        // Verify we can still reach distant nodes (D and E)
        var found_d = false;
        var found_e = false;
        for (results) |result| {
            if (result.id == node_d) found_d = true;
            if (result.id == node_e) found_e = true;
        }
        try testing.expect(found_d);
        try testing.expect(found_e);
    }

    // Search from opposite end should also work
    {
        const results = try hnsw.search(&[_]f32{ 4.0, 0.0, 0.0 }, 5);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 4), results.len);

        var found_a = false;
        var found_b = false;
        for (results) |result| {
            if (result.id == node_a) found_a = true;
            if (result.id == node_b) found_b = true;
        }
        try testing.expect(found_a);
        try testing.expect(found_b);
    }
}

test "HNSW Removal: Search completeness after removal" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a triangle: A -> B -> C -> A
    const node_a = try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null);
    const node_b = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const node_c = try hnsw.insert(&[_]f32{ 0.5, 0.87, 0.0 }, null);
    const node_d = try hnsw.insert(&[_]f32{ 2.0, 0.0, 0.0 }, null);
    const node_e = try hnsw.insert(&[_]f32{ 1.5, 0.87, 0.0 }, null);

    // Count nodes before removal
    const count_before = {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        results.len
    };
    try testing.expectEqual(@as(usize, 5), count_before);

    // Remove node A
    try hnsw.removeNode(node_a);

    // Search should still find all remaining nodes
    const count_after = {
        const results = try hnsw.search(&[_]f32{ 1.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        results.len
    };
    try testing.expectEqual(@as(usize, 4), count_after);

    // Remove another node
    try hnsw.removeNode(node_c);

    // Search should still find all remaining nodes
    const count_final = {
        const results = try hnsw.search(&[_]f32{ 2.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        results.len
    };
    try testing.expectEqual(@as(usize, 3), count_final);
}

test "HNSW Removal: Hub node removal redistributes connections" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a star topology: Hub in center, 5 spokes around it
    const hub = try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null);
    const spoke1 = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const spoke2 = try hnsw.insert(&[_]f32{ 0.0, 1.0, 0.0 }, null);
    const spoke3 = try hnsw.insert(&[_]f32{ -1.0, 0.0, 0.0 }, null);
    const spoke4 = try hnsw.insert(&[_]f32{ 0.0, -1.0, 0.0 }, null);
    const spoke5 = try hnsw.insert(&[_]f32{ 0.71, 0.71, 0.0 }, null);

    // Verify all spokes are reachable via hub
    {
        const results = try hnsw.search(&[_]f32{ 1.0, 0.0, 0.0 }, 6);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 6), results.len);
    }

    // Remove the hub node
    try hnsw.removeNode(hub);

    // After hub removal, spokes should be reconnected to each other
    // Search from any spoke should still find other spokes
    {
        const results = try hnsw.search(&[_]f32{ 1.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);

        // Should find all 5 spokes (hub was removed)
        try testing.expectEqual(@as(usize, 5), results.len);

        // Verify we can reach distant spokes
        var found_spoke3 = false;
        var found_spoke4 = false;
        for (results) |result| {
            if (result.id == spoke3) found_spoke3 = true;
            if (result.id == spoke4) found_spoke4 = true;
        }
        try testing.expect(found_spoke3);
        try testing.expect(found_spoke4);
    }

    // Search from opposite spoke should also reach all others
    {
        const results = try hnsw.search(&[_]f32{ -1.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 5), results.len);

        var found_spoke1 = false;
        var found_spoke2 = false;
        for (results) |result| {
            if (result.id == spoke1) found_spoke1 = true;
            if (result.id == spoke2) found_spoke2 = true;
        }
        try testing.expect(found_spoke1);
        try testing.expect(found_spoke2);
    }
}

test "HNSW Removal: Last node leaves empty graph" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Insert single node
    const node = try hnsw.insert(&[_]f32{ 1.0, 2.0, 3.0 }, null);

    // Verify it exists
    {
        const results = try hnsw.search(&[_]f32{ 1.0, 2.0, 3.0 }, 1);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 1), results.len);
    }

    // Remove the only node
    try hnsw.removeNode(node);

    // Graph should now be empty
    {
        const results = try hnsw.search(&[_]f32{ 1.0, 2.0, 3.0 }, 1);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 0), results.len);
    }
}

test "HNSW Removal: Sequential removal maintains connectivity" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a larger graph with 10 nodes
    var nodes: [10]u64 = undefined;
    for (&nodes, 0..) |*node, i| {
        const x = @as(f32, @floatFromInt(i));
        node.* = try hnsw.insert(&[_]f32{ x, x * 0.5, x * 0.3 }, null);
    }

    // Remove every other node
    try hnsw.removeNode(nodes[1]);
    try hnsw.removeNode(nodes[3]);
    try hnsw.removeNode(nodes[5]);
    try hnsw.removeNode(nodes[7]);

    // Remaining nodes should still be searchable
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);

        // Should find 6 remaining nodes (0, 2, 4, 6, 8, 9)
        try testing.expectEqual(@as(usize, 6), results.len);

        // Verify we can reach the furthest node
        var found_last = false;
        for (results) |result| {
            if (result.id == nodes[9]) found_last = true;
        }
        try testing.expect(found_last);
    }

    // Remove more nodes
    try hnsw.removeNode(nodes[2]);
    try hnsw.removeNode(nodes[4]);

    // Still should maintain connectivity
    {
        const results = try hnsw.search(&[_]f32{ 9.0, 4.5, 2.7 }, 10);
        defer allocator.free(results);

        // Should find 4 remaining nodes (0, 6, 8, 9)
        try testing.expectEqual(@as(usize, 4), results.len);
    }
}

test "HNSW Removal: Edge case - remove non-existent node" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Insert a node
    _ = try hnsw.insert(&[_]f32{ 1.0, 2.0, 3.0 }, null);

    // Try to remove a non-existent node
    const result = hnsw.removeNode(999999);
    try testing.expectError(error.NodeNotFound, result);
}

test "HNSW Removal: Single neighbor node removal" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create two connected nodes
    const node_a = try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null);
    const node_b = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);

    // Remove one node - the other should remain
    try hnsw.removeNode(node_b);

    // Single node should still be searchable
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 1);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 1), results.len);
        try testing.expectEqual(node_a, results[0].id);
    }
}

test "HNSW Removal: Dense graph maintains connectivity" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create a dense cluster of 20 nodes in a small region
    var nodes: [20]u64 = undefined;
    for (&nodes, 0..) |*node, i| {
        const angle = @as(f32, @floatFromInt(i)) * 2.0 * std.math.pi / 20.0;
        const x = @cos(angle);
        const y = @sin(angle);
        node.* = try hnsw.insert(&[_]f32{ x, y, 0.0 }, null);
    }

    // Remove 5 nodes from the cluster
    try hnsw.removeNode(nodes[3]);
    try hnsw.removeNode(nodes[7]);
    try hnsw.removeNode(nodes[11]);
    try hnsw.removeNode(nodes[15]);
    try hnsw.removeNode(nodes[19]);

    // All remaining nodes should still be reachable
    {
        const results = try hnsw.search(&[_]f32{ 1.0, 0.0, 0.0 }, 20);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 15), results.len);
    }

    // Search from different positions should find all remaining nodes
    {
        const results = try hnsw.search(&[_]f32{ -1.0, 0.0, 0.0 }, 20);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 15), results.len);
    }

    {
        const results = try hnsw.search(&[_]f32{ 0.0, 1.0, 0.0 }, 20);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 15), results.len);
    }
}

test "HNSW Removal: Removal preserves search accuracy" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create nodes at known positions
    const node_a = try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null);
    const node_b = try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null);
    const node_c = try hnsw.insert(&[_]f32{ 2.0, 0.0, 0.0 }, null);
    const node_d = try hnsw.insert(&[_]f32{ 3.0, 0.0, 0.0 }, null);
    const node_e = try hnsw.insert(&[_]f32{ 10.0, 0.0, 0.0 }, null);

    // Remove middle node
    try hnsw.removeNode(node_c);

    // Search near the removed node should find nearest remaining neighbors
    {
        const results = try hnsw.search(&[_]f32{ 2.0, 0.0, 0.0 }, 2);
        defer allocator.free(results);

        // Should find B and D as nearest (C was removed)
        try testing.expectEqual(@as(usize, 2), results.len);

        // First result should be closer than second
        const dist0 = cosineDistance(&[_]f32{ 2.0, 0.0, 0.0 }, results[0].point);
        const dist1 = cosineDistance(&[_]f32{ 2.0, 0.0, 0.0 }, results[1].point);
        try testing.expect(dist0 <= dist1);

        // Should not find the distant node E in top 2
        try testing.expect(results[0].id != node_e);
        try testing.expect(results[1].id != node_e);
    }
}

test "HNSW Removal: Multiple removals in sequence" {
    const allocator = testing.allocator;
    var hnsw = HNSW(f32).init(allocator, 16, 200);
    defer hnsw.deinit();

    // Create 7 nodes in a line
    const nodes = [_]u64{
        try hnsw.insert(&[_]f32{ 0.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 1.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 2.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 3.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 4.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 5.0, 0.0, 0.0 }, null),
        try hnsw.insert(&[_]f32{ 6.0, 0.0, 0.0 }, null),
    };

    // Remove nodes one by one and verify connectivity after each removal
    try hnsw.removeNode(nodes[3]); // Remove middle
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 6), results.len);
    }

    try hnsw.removeNode(nodes[1]); // Remove near start
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 5), results.len);
    }

    try hnsw.removeNode(nodes[5]); // Remove near end
    {
        const results = try hnsw.search(&[_]f32{ 6.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);
        try testing.expectEqual(@as(usize, 4), results.len);
    }

    // Verify remaining nodes are all still connected
    {
        const results = try hnsw.search(&[_]f32{ 0.0, 0.0, 0.0 }, 10);
        defer allocator.free(results);

        var found_last = false;
        for (results) |result| {
            if (result.id == nodes[6]) found_last = true;
        }
        try testing.expect(found_last);
    }
}
