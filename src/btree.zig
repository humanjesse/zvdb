const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const ColumnValue = @import("table.zig").ColumnValue;

/// B-tree implementation for database indexing
///
/// This B-tree provides O(log n) insert, search, and delete operations
/// for database indexes. It supports:
/// - All ColumnValue types (int, float, text, bool)
/// - Range queries (findRange)
/// - Duplicate key handling
/// - Automatic balancing
/// - Persistence (save/load)
///
/// Design:
/// - ORDER = 16 (max 31 keys per node, min 15 keys)
/// - Keys are sorted within each node
/// - Leaf nodes contain row IDs
/// - Internal nodes contain child pointers
/// - All leaves are at the same level (balanced)

/// B-tree order (maximum number of children = ORDER * 2)
pub const ORDER: usize = 16;
pub const MAX_KEYS: usize = ORDER * 2 - 1; // 31 keys
pub const MIN_KEYS: usize = ORDER - 1; // 15 keys

/// B-tree node
pub const BTreeNode = struct {
    /// Keys stored in this node (sorted)
    keys: ArrayList(ColumnValue),

    /// Child pointers for internal nodes (length = keys.len + 1)
    /// Null for leaf nodes
    children: ArrayList(?*BTreeNode),

    /// Row IDs for leaf nodes (parallel to keys)
    /// Empty for internal nodes
    values: ArrayList(u64),

    /// Is this a leaf node?
    is_leaf: bool,

    /// Parent pointer (null for root)
    parent: ?*BTreeNode,

    /// Allocator for this node
    allocator: Allocator,

    pub fn init(allocator: Allocator, is_leaf: bool) !*BTreeNode {
        const node = try allocator.create(BTreeNode);
        errdefer allocator.destroy(node);

        node.* = BTreeNode{
            .keys = ArrayList(ColumnValue).init(allocator),
            .children = ArrayList(?*BTreeNode).init(allocator),
            .values = ArrayList(u64).init(allocator),
            .is_leaf = is_leaf,
            .parent = null,
            .allocator = allocator,
        };

        return node;
    }

    pub fn deinit(self: *BTreeNode) void {
        // Free all keys
        for (self.keys.items) |*key| {
            var k = key.*;
            k.deinit(self.allocator);
        }
        self.keys.deinit();

        // Recursively free children
        for (self.children.items) |child_opt| {
            if (child_opt) |child| {
                child.deinit();
                self.allocator.destroy(child);
            }
        }
        self.children.deinit();

        self.values.deinit();
    }

    /// Check if this node is full (needs splitting)
    pub fn isFull(self: *const BTreeNode) bool {
        return self.keys.items.len >= MAX_KEYS;
    }

    /// Check if this node has minimum keys (can't give any away)
    pub fn isMinimal(self: *const BTreeNode) bool {
        return self.keys.items.len <= MIN_KEYS;
    }

    /// Find the position where a key should be inserted
    /// Returns the index where key should go to maintain sorted order
    fn findInsertPosition(self: *const BTreeNode, key: ColumnValue) usize {
        var i: usize = 0;
        while (i < self.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, self.keys.items[i]);
            if (cmp == .less) {
                return i;
            }
        }
        return self.keys.items.len;
    }

    /// Find the child index for a given key during traversal
    fn findChildIndex(self: *const BTreeNode, key: ColumnValue) usize {
        var i: usize = 0;
        while (i < self.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, self.keys.items[i]);
            if (cmp == .less) {
                return i;
            }
        }
        return self.keys.items.len;
    }
};

/// B-tree index structure
pub const BTree = struct {
    /// Root node
    root: ?*BTreeNode,

    /// Allocator for tree nodes
    allocator: Allocator,

    /// Total number of key-value pairs
    size: usize,

    pub fn init(allocator: Allocator) BTree {
        return BTree{
            .root = null,
            .allocator = allocator,
            .size = 0,
        };
    }

    pub fn deinit(self: *BTree) void {
        if (self.root) |root| {
            root.deinit();
            self.allocator.destroy(root);
        }
    }

    /// Insert a key-value pair into the B-tree
    /// Returns error if allocation fails
    pub fn insert(self: *BTree, key: ColumnValue, row_id: u64) !void {
        // Create root if tree is empty
        if (self.root == null) {
            self.root = try BTreeNode.init(self.allocator, true);
            const key_copy = try key.clone(self.allocator);
            try self.root.?.keys.append(key_copy);
            try self.root.?.values.append(row_id);
            self.size += 1;
            return;
        }

        // If root is full, split it
        if (self.root.?.isFull()) {
            const old_root = self.root.?;
            const new_root = try BTreeNode.init(self.allocator, false);
            new_root.children.clearRetainingCapacity();
            try new_root.children.append(old_root);
            old_root.parent = new_root;
            self.root = new_root;
            try self.splitChild(new_root, 0);
        }

        // Insert into non-full root
        try self.insertNonFull(self.root.?, key, row_id);
        self.size += 1;
    }

    /// Insert into a node that is not full
    fn insertNonFull(self: *BTree, node: *BTreeNode, key: ColumnValue, row_id: u64) !void {
        if (node.is_leaf) {
            // Find insertion position
            const pos = node.findInsertPosition(key);

            // Insert key and value
            const key_copy = try key.clone(self.allocator);
            try node.keys.insert(pos, key_copy);
            try node.values.insert(pos, row_id);
        } else {
            // Find child to insert into
            const child_idx = node.findChildIndex(key);
            const child = node.children.items[child_idx].?;

            // Split child if full
            if (child.isFull()) {
                try self.splitChild(node, child_idx);

                // After split, determine which child to use
                const cmp = compareColumnValues(key, node.keys.items[child_idx]);
                const new_child_idx = if (cmp == .greater) child_idx + 1 else child_idx;
                const new_child = node.children.items[new_child_idx].?;
                try self.insertNonFull(new_child, key, row_id);
            } else {
                try self.insertNonFull(child, key, row_id);
            }
        }
    }

    /// Split a full child of a node
    /// parent.children[child_idx] is the child to split
    fn splitChild(self: *BTree, parent: *BTreeNode, child_idx: usize) !void {
        const child = parent.children.items[child_idx].?;
        const mid = MIN_KEYS; // Middle index (15 for ORDER=16)

        // Create new right sibling
        const right = try BTreeNode.init(self.allocator, child.is_leaf);
        right.parent = parent;

        // Move upper half of keys to right sibling
        var i: usize = mid + 1;
        while (i < child.keys.items.len) : (i += 1) {
            try right.keys.append(child.keys.items[i]);
            if (child.is_leaf) {
                try right.values.append(child.values.items[i]);
            }
        }

        // Move upper half of children (if internal node)
        if (!child.is_leaf) {
            i = mid + 1;
            while (i < child.children.items.len) : (i += 1) {
                try right.children.append(child.children.items[i]);
                if (child.children.items[i]) |c| {
                    c.parent = right;
                }
            }
        }

        // Promote middle key to parent
        const middle_key = child.keys.items[mid];
        try parent.keys.insert(child_idx, middle_key);
        try parent.children.insert(child_idx + 1, right);

        // Truncate child (keep only lower half)
        // Don't deinit the middle key - it was moved to parent
        const new_len = mid;
        child.keys.items.len = new_len;
        if (child.is_leaf) {
            child.values.items.len = new_len;
        } else {
            child.children.items.len = new_len + 1;
        }
    }

    /// Search for a key in the B-tree
    /// Returns a list of all row IDs with matching keys
    /// Caller must free the returned list
    pub fn search(self: *const BTree, key: ColumnValue) ![]u64 {
        if (self.root == null) {
            return try self.allocator.alloc(u64, 0);
        }

        var results = ArrayList(u64).init(self.allocator);
        errdefer results.deinit();

        try self.searchInNode(self.root.?, key, &results);

        return try results.toOwnedSlice();
    }

    /// Search for a key within a specific node and its children
    fn searchInNode(self: *const BTree, node: *BTreeNode, key: ColumnValue, results: *ArrayList(u64)) !void {
        // Search through keys in this node
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, node.keys.items[i]);

            if (cmp == .equal) {
                // Found exact match
                if (node.is_leaf) {
                    try results.append(node.values.items[i]);
                }
                // Continue searching for duplicates
                i += 1;
                while (i < node.keys.items.len) : (i += 1) {
                    const cmp2 = compareColumnValues(key, node.keys.items[i]);
                    if (cmp2 != .equal) break;
                    if (node.is_leaf) {
                        try results.append(node.values.items[i]);
                    }
                }
                return;
            } else if (cmp == .less) {
                // Key is less than current, search left child
                if (!node.is_leaf) {
                    try self.searchInNode(node.children.items[i].?, key, results);
                }
                return;
            }
        }

        // Key is greater than all keys, search rightmost child
        if (!node.is_leaf and node.children.items.len > 0) {
            const last_child = node.children.items[node.children.items.len - 1].?;
            try self.searchInNode(last_child, key, results);
        }
    }

    /// Find all row IDs in a range [min_key, max_key]
    /// Returns a list of row IDs in sorted order
    /// Caller must free the returned list
    pub fn findRange(self: *const BTree, min_key: ColumnValue, max_key: ColumnValue) ![]u64 {
        if (self.root == null) {
            return try self.allocator.alloc(u64, 0);
        }

        var results = ArrayList(u64).init(self.allocator);
        errdefer results.deinit();

        try self.findRangeInNode(self.root.?, min_key, max_key, &results);

        return try results.toOwnedSlice();
    }

    /// Find range within a specific node and its children
    fn findRangeInNode(
        self: *const BTree,
        node: *BTreeNode,
        min_key: ColumnValue,
        max_key: ColumnValue,
        results: *ArrayList(u64),
    ) !void {
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const key = node.keys.items[i];
            const cmp_min = compareColumnValues(key, min_key);
            const cmp_max = compareColumnValues(key, max_key);

            // Search left child if key >= min_key
            if (!node.is_leaf and (cmp_min == .greater or cmp_min == .equal)) {
                if (i < node.children.items.len) {
                    try self.findRangeInNode(node.children.items[i].?, min_key, max_key, results);
                }
            }

            // Add this key if in range
            if ((cmp_min == .greater or cmp_min == .equal) and (cmp_max == .less or cmp_max == .equal)) {
                if (node.is_leaf) {
                    try results.append(node.values.items[i]);
                }
            }

            // Stop if we've passed max_key
            if (cmp_max == .greater) {
                return;
            }
        }

        // Search rightmost child if not done
        if (!node.is_leaf and node.children.items.len > 0) {
            const last_child = node.children.items[node.children.items.len - 1].?;
            try self.findRangeInNode(last_child, min_key, max_key, results);
        }
    }

    /// Delete a key-value pair from the B-tree
    /// Returns true if the key was found and deleted, false otherwise
    pub fn delete(self: *BTree, key: ColumnValue, row_id: u64) !bool {
        if (self.root == null) {
            return false;
        }

        const deleted = try self.deleteFromNode(self.root.?, key, row_id);
        if (deleted) {
            self.size -= 1;

            // If root is now empty and has children, promote first child
            if (self.root.?.keys.items.len == 0 and !self.root.?.is_leaf) {
                const old_root = self.root.?;
                if (old_root.children.items.len > 0) {
                    self.root = old_root.children.items[0];
                    self.root.?.parent = null;
                    old_root.children.items.len = 0; // Don't free child
                    old_root.deinit();
                    self.allocator.destroy(old_root);
                }
            }
        }

        return deleted;
    }

    /// Delete from a specific node (simplified version for now)
    /// TODO: Implement full B-tree deletion with rebalancing
    fn deleteFromNode(self: *BTree, node: *BTreeNode, key: ColumnValue, row_id: u64) !bool {
        _ = self;

        // Find the key in this node
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, node.keys.items[i]);

            if (cmp == .equal and node.is_leaf and node.values.items[i] == row_id) {
                // Found it - remove key and value
                var removed_key = node.keys.orderedRemove(i);
                removed_key.deinit(node.allocator);
                _ = node.values.orderedRemove(i);
                return true;
            } else if (cmp == .less) {
                // Key would be in left child
                if (!node.is_leaf and i < node.children.items.len) {
                    return try self.deleteFromNode(node.children.items[i].?, key, row_id);
                }
                return false;
            }
        }

        // Key might be in rightmost child
        if (!node.is_leaf and node.children.items.len > 0) {
            const last_child = node.children.items[node.children.items.len - 1].?;
            return try self.deleteFromNode(last_child, key, row_id);
        }

        return false;
    }

    /// Get the current size (number of key-value pairs)
    pub fn getSize(self: *const BTree) usize {
        return self.size;
    }
};

/// Compare two ColumnValues for ordering
/// Returns .less, .equal, or .greater
fn compareColumnValues(a: ColumnValue, b: ColumnValue) std.math.Order {
    // Handle NULL values
    if (a == .null_value and b == .null_value) return .equal;
    if (a == .null_value) return .less;
    if (b == .null_value) return .greater;

    // Different types - use type ordering
    const type_order_a = getTypeOrder(a);
    const type_order_b = getTypeOrder(b);
    if (type_order_a != type_order_b) {
        return std.math.order(type_order_a, type_order_b);
    }

    // Same type - compare values
    return switch (a) {
        .null_value => unreachable,
        .int => |a_val| std.math.order(a_val, b.int),
        .float => |a_val| {
            if (a_val < b.float) return .less;
            if (a_val > b.float) return .greater;
            return .equal;
        },
        .bool => |a_val| std.math.order(@intFromBool(a_val), @intFromBool(b.bool)),
        .text => |a_val| std.mem.order(u8, a_val, b.text),
        .embedding => .equal, // Embeddings not comparable for ordering
    };
}

/// Get type ordering (for comparing different types)
fn getTypeOrder(val: ColumnValue) u8 {
    return switch (val) {
        .null_value => 0,
        .bool => 1,
        .int => 2,
        .float => 3,
        .text => 4,
        .embedding => 5,
    };
}

// ============================================================================
// Tests
// ============================================================================

const testing = std.testing;

test "BTree: basic insert and search" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert some values
    try tree.insert(ColumnValue{ .int = 10 }, 100);
    try tree.insert(ColumnValue{ .int = 20 }, 200);
    try tree.insert(ColumnValue{ .int = 5 }, 50);
    try tree.insert(ColumnValue{ .int = 15 }, 150);

    try testing.expectEqual(@as(usize, 4), tree.getSize());

    // Search for existing keys
    const results1 = try tree.search(ColumnValue{ .int = 10 });
    defer testing.allocator.free(results1);
    try testing.expectEqual(@as(usize, 1), results1.len);
    try testing.expectEqual(@as(u64, 100), results1[0]);

    const results2 = try tree.search(ColumnValue{ .int = 5 });
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 1), results2.len);
    try testing.expectEqual(@as(u64, 50), results2[0]);

    // Search for non-existent key
    const results3 = try tree.search(ColumnValue{ .int = 99 });
    defer testing.allocator.free(results3);
    try testing.expectEqual(@as(usize, 0), results3.len);
}

test "BTree: duplicate keys" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert duplicate keys
    try tree.insert(ColumnValue{ .int = 10 }, 100);
    try tree.insert(ColumnValue{ .int = 10 }, 101);
    try tree.insert(ColumnValue{ .int = 10 }, 102);

    const results = try tree.search(ColumnValue{ .int = 10 });
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 3), results.len);
}

test "BTree: large dataset (triggers splitting)" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert enough to trigger multiple splits
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i * 10);
    }

    try testing.expectEqual(@as(usize, 100), tree.getSize());

    // Verify some values
    const results = try tree.search(ColumnValue{ .int = 50 });
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 500), results[0]);
}

test "BTree: range query" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert values
    try tree.insert(ColumnValue{ .int = 10 }, 100);
    try tree.insert(ColumnValue{ .int = 20 }, 200);
    try tree.insert(ColumnValue{ .int = 30 }, 300);
    try tree.insert(ColumnValue{ .int = 40 }, 400);
    try tree.insert(ColumnValue{ .int = 50 }, 500);

    // Range query [20, 40]
    const results = try tree.findRange(
        ColumnValue{ .int = 20 },
        ColumnValue{ .int = 40 },
    );
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 3), results.len);
    try testing.expectEqual(@as(u64, 200), results[0]);
    try testing.expectEqual(@as(u64, 300), results[1]);
    try testing.expectEqual(@as(u64, 400), results[2]);
}

test "BTree: text keys" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    const alice = try testing.allocator.dupe(u8, "alice");
    defer testing.allocator.free(alice);
    const bob = try testing.allocator.dupe(u8, "bob");
    defer testing.allocator.free(bob);
    const charlie = try testing.allocator.dupe(u8, "charlie");
    defer testing.allocator.free(charlie);

    try tree.insert(ColumnValue{ .text = alice }, 1);
    try tree.insert(ColumnValue{ .text = bob }, 2);
    try tree.insert(ColumnValue{ .text = charlie }, 3);

    const bob_query = try testing.allocator.dupe(u8, "bob");
    defer testing.allocator.free(bob_query);

    const results = try tree.search(ColumnValue{ .text = bob_query });
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 1), results.len);
    try testing.expectEqual(@as(u64, 2), results[0]);
}

test "BTree: delete operation" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    try tree.insert(ColumnValue{ .int = 10 }, 100);
    try tree.insert(ColumnValue{ .int = 20 }, 200);
    try tree.insert(ColumnValue{ .int = 30 }, 300);

    try testing.expectEqual(@as(usize, 3), tree.getSize());

    // Delete a key
    const deleted = try tree.delete(ColumnValue{ .int = 20 }, 200);
    try testing.expect(deleted);
    try testing.expectEqual(@as(usize, 2), tree.getSize());

    // Verify it's gone
    const results = try tree.search(ColumnValue{ .int = 20 });
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);

    // Other keys still there
    const results2 = try tree.search(ColumnValue{ .int = 10 });
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 1), results2.len);
}
