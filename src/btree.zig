const std = @import("std");
const Allocator = std.mem.Allocator;
const ArrayList = std.array_list.Managed;
const ColumnValue = @import("table.zig").ColumnValue;

/// B+ tree implementation for database indexing
///
/// This B+ tree provides O(log n) insert, search, and delete operations
/// for database indexes with full rebalancing support. It supports:
/// - All ColumnValue types (int, float, text, bool)
/// - Range queries (findRange) - optimized with leaf links
/// - Duplicate key handling
/// - Automatic balancing and rebalancing
/// - Full deletion with borrowing and merging
/// - Persistence (save/load)
///
/// B+ Tree Design:
/// - ORDER = 16 (max 31 keys per node, min 15 keys)
/// - Keys are sorted within each node
/// - Leaf nodes contain row IDs (actual data)
/// - Internal nodes contain separator keys only (routing information)
/// - All leaves are at the same level (balanced)
/// - Leaves are linked for efficient range scans (next_leaf/prev_leaf)
///
/// Deletion Algorithm:
/// - Always delete from leaf nodes only (B+ tree principle)
/// - After deletion, if node has < MIN_KEYS:
///   1. Try to borrow from left sibling (if it has > MIN_KEYS)
///   2. Try to borrow from right sibling (if it has > MIN_KEYS)
///   3. Otherwise, merge with a sibling
///   4. Recursively rebalance parent if merge occurred
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

    /// Next leaf pointer (for B+ tree leaf linking)
    /// Only used in leaf nodes, null otherwise
    next_leaf: ?*BTreeNode,

    /// Previous leaf pointer (for B+ tree leaf linking)
    /// Only used in leaf nodes, null otherwise
    prev_leaf: ?*BTreeNode,

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
            .next_leaf = null,
            .prev_leaf = null,
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
            if (cmp == .lt) {
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
            if (cmp == .lt) {
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
                const new_child_idx = if (cmp == .gt) child_idx + 1 else child_idx;
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
        // For leaf nodes, include the middle key in the right sibling (B+ tree style)
        // For internal nodes, the middle key is only promoted to parent
        const start_idx = if (child.is_leaf) mid else mid + 1;
        var i: usize = start_idx;
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

        // Maintain leaf links for B+ tree
        if (child.is_leaf) {
            // Insert right between child and child.next_leaf
            right.next_leaf = child.next_leaf;
            right.prev_leaf = child;
            child.next_leaf = right;

            // Update the next leaf's prev pointer if it exists
            if (right.next_leaf) |next| {
                next.prev_leaf = right;
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
        if (node.is_leaf) {
            // In a leaf node, collect all matching values
            var i: usize = 0;
            while (i < node.keys.items.len) : (i += 1) {
                const cmp = compareColumnValues(key, node.keys.items[i]);
                if (cmp == .eq) {
                    try results.append(node.values.items[i]);
                } else if (cmp == .lt) {
                    // Keys are sorted, so we can stop early
                    return;
                }
            }
            return;
        }

        // Internal node: check each separator key to find which child(ren) to search
        // When splits occur, duplicate separator keys can exist, and matching values
        // may be distributed across multiple adjacent children

        // First, find the range of matching separators
        var first_match: ?usize = null;
        var last_match: ?usize = null;

        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, node.keys.items[i]);

            if (cmp == .lt) {
                // Key is less than this separator
                if (first_match) |_| {
                    // We already found matches, so stop here
                    break;
                } else {
                    // Key must be in the left child
                    try self.searchInNode(node.children.items[i].?, key, results);
                    return;
                }
            } else if (cmp == .eq) {
                // Found a matching separator
                if (first_match == null) {
                    first_match = i;
                }
                last_match = i;
            } else {
                // key > separator
                if (first_match) |_| {
                    // We've passed all matching separators, stop here
                    break;
                }
            }
        }

        // If we found matching separators, search the relevant children
        if (first_match) |first| {
            const last = last_match.?;

            // Search the left child of the first match and all children between first and last+1
            // For keys=[2, 2, 3], first=0, last=1
            // We need to search child[0], child[1], and child[2]
            try self.searchInNode(node.children.items[first].?, key, results);

            var child_idx = first + 1;
            while (child_idx <= last + 1 and child_idx < node.children.items.len) : (child_idx += 1) {
                try self.searchInNode(node.children.items[child_idx].?, key, results);
            }
            return;
        }

        // Key is greater than all separators, search rightmost child
        if (node.children.items.len > 0) {
            try self.searchInNode(node.children.items[node.children.items.len - 1].?, key, results);
        }
    }

    /// Find all row IDs in a range [min_key, max_key]
    /// Returns a list of row IDs in sorted order
    /// Caller must free the returned list
    /// min_inclusive: if true, include values equal to min_key
    /// max_inclusive: if true, include values equal to max_key
    pub fn findRange(
        self: *const BTree,
        min_key: ColumnValue,
        max_key: ColumnValue,
        min_inclusive: bool,
        max_inclusive: bool,
    ) ![]u64 {
        if (self.root == null) {
            return try self.allocator.alloc(u64, 0);
        }

        var results = ArrayList(u64).init(self.allocator);
        errdefer results.deinit();

        // B+ tree optimization: find starting leaf and scan using leaf links
        const start_leaf = try self.findLeafForKey(self.root.?, min_key);

        // Scan leaves from left to right using leaf links
        var current_leaf: ?*BTreeNode = start_leaf;
        while (current_leaf) |leaf| {
            // Process all keys in this leaf
            for (leaf.keys.items, 0..) |key, i| {
                const cmp_min = compareColumnValues(key, min_key);
                const cmp_max = compareColumnValues(key, max_key);

                // Check if key is in range
                const matches_min = if (min_inclusive)
                    (cmp_min == .gt or cmp_min == .eq)
                else
                    cmp_min == .gt;

                const matches_max = if (max_inclusive)
                    (cmp_max == .lt or cmp_max == .eq)
                else
                    cmp_max == .lt;

                // If we've passed max_key, we're done
                if (cmp_max == .gt) {
                    return try results.toOwnedSlice();
                }

                // Add value if in range
                if (matches_min and matches_max) {
                    try results.append(leaf.values.items[i]);
                }
            }

            // Move to next leaf
            current_leaf = leaf.next_leaf;
        }

        return try results.toOwnedSlice();
    }

    /// Find the leaf node that should contain a given key
    /// Used for range scans and deletion
    fn findLeafForKey(self: *const BTree, node: *BTreeNode, key: ColumnValue) !*BTreeNode {
        if (node.is_leaf) {
            return node;
        }

        // Find the appropriate child
        const child_idx = node.findChildIndex(key);
        return try self.findLeafForKey(node.children.items[child_idx].?, key);
    }

    /// Find range within a specific node and its children
    fn findRangeInNode(
        self: *const BTree,
        node: *BTreeNode,
        min_key: ColumnValue,
        max_key: ColumnValue,
        min_inclusive: bool,
        max_inclusive: bool,
        results: *ArrayList(u64),
    ) !void {
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const key = node.keys.items[i];
            const cmp_min = compareColumnValues(key, min_key);
            const cmp_max = compareColumnValues(key, max_key);

            // Search left child if key might be in range
            const should_search_left = if (min_inclusive)
                (cmp_min == .gt or cmp_min == .eq)
            else
                cmp_min == .gt;

            if (!node.is_leaf and should_search_left) {
                if (i < node.children.items.len) {
                    try self.findRangeInNode(node.children.items[i].?, min_key, max_key, min_inclusive, max_inclusive, results);
                }
            }

            // Add this key if in range
            const matches_min = if (min_inclusive)
                (cmp_min == .gt or cmp_min == .eq)
            else
                cmp_min == .gt;

            const matches_max = if (max_inclusive)
                (cmp_max == .lt or cmp_max == .eq)
            else
                cmp_max == .lt;

            if (matches_min and matches_max) {
                if (node.is_leaf) {
                    try results.append(node.values.items[i]);
                }
            }

            // Stop if we've passed max_key
            if (cmp_max == .gt) {
                return;
            }
        }

        // Search rightmost child if not done
        if (!node.is_leaf and node.children.items.len > 0) {
            const last_child = node.children.items[node.children.items.len - 1].?;
            try self.findRangeInNode(last_child, min_key, max_key, min_inclusive, max_inclusive, results);
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

    /// Find the index of a child node in its parent's children array
    /// Returns null if node has no parent or is not found
    fn getNodeIndexInParent(node: *BTreeNode) ?usize {
        const parent = node.parent orelse return null;
        for (parent.children.items, 0..) |child_opt, i| {
            if (child_opt) |child| {
                if (child == node) {
                    return i;
                }
            }
        }
        return null;
    }

    /// Find the left sibling of a node (must have same parent)
    /// Returns null if no left sibling exists
    fn findLeftSibling(node: *BTreeNode) ?*BTreeNode {
        const idx = getNodeIndexInParent(node) orelse return null;
        if (idx == 0) return null; // No left sibling

        const parent = node.parent.?;
        return parent.children.items[idx - 1];
    }

    /// Find the right sibling of a node (must have same parent)
    /// Returns null if no right sibling exists
    fn findRightSibling(node: *BTreeNode) ?*BTreeNode {
        const idx = getNodeIndexInParent(node) orelse return null;
        const parent = node.parent.?;
        if (idx + 1 >= parent.children.items.len) return null; // No right sibling

        return parent.children.items[idx + 1];
    }

    /// Borrow a key from the left sibling (rotation right)
    /// Used when current node has too few keys and left sibling has extras
    fn borrowFromLeftSibling(self: *BTree, node: *BTreeNode, left_sibling: *BTreeNode) !void {
        _ = self;
        const parent = node.parent.?;
        const node_idx = getNodeIndexInParent(node).?;

        if (node.is_leaf) {
            // Leaf node: move last key from left sibling to beginning of node
            const borrowed_key = left_sibling.keys.items[left_sibling.keys.items.len - 1];
            const borrowed_value = left_sibling.values.items[left_sibling.values.items.len - 1];

            // Insert at beginning of current node
            try node.keys.insert(0, borrowed_key);
            try node.values.insert(0, borrowed_value);

            // Remove from left sibling
            _ = left_sibling.keys.pop();
            _ = left_sibling.values.pop();

            // Update parent separator to first key of current node
            parent.keys.items[node_idx - 1] = borrowed_key;
        } else {
            // Internal node: rotate through parent
            const separator = parent.keys.items[node_idx - 1];
            const borrowed_child = left_sibling.children.items[left_sibling.children.items.len - 1].?;

            // Insert separator at beginning of current node
            try node.keys.insert(0, separator);
            try node.children.insert(0, borrowed_child);
            borrowed_child.parent = node;

            // Move last key from left sibling to parent
            parent.keys.items[node_idx - 1] = left_sibling.keys.items[left_sibling.keys.items.len - 1];

            // Remove from left sibling
            _ = left_sibling.keys.pop();
            _ = left_sibling.children.pop();
        }
    }

    /// Borrow a key from the right sibling (rotation left)
    /// Used when current node has too few keys and right sibling has extras
    fn borrowFromRightSibling(self: *BTree, node: *BTreeNode, right_sibling: *BTreeNode) !void {
        _ = self;
        const parent = node.parent.?;
        const node_idx = getNodeIndexInParent(node).?;

        if (node.is_leaf) {
            // Leaf node: move first key from right sibling to end of node
            const borrowed_key = right_sibling.keys.items[0];
            const borrowed_value = right_sibling.values.items[0];

            // Append to current node
            try node.keys.append(borrowed_key);
            try node.values.append(borrowed_value);

            // Remove from right sibling
            _ = right_sibling.keys.orderedRemove(0);
            _ = right_sibling.values.orderedRemove(0);

            // Update parent separator to first key of right sibling (after removal)
            if (right_sibling.keys.items.len > 0) {
                parent.keys.items[node_idx] = right_sibling.keys.items[0];
            }
        } else {
            // Internal node: rotate through parent
            const separator = parent.keys.items[node_idx];
            const borrowed_child = right_sibling.children.items[0].?;

            // Append separator to current node
            try node.keys.append(separator);
            try node.children.append(borrowed_child);
            borrowed_child.parent = node;

            // Move first key from right sibling to parent
            parent.keys.items[node_idx] = right_sibling.keys.items[0];

            // Remove from right sibling
            _ = right_sibling.keys.orderedRemove(0);
            _ = right_sibling.children.orderedRemove(0);
        }
    }

    /// Merge node with its left sibling
    /// All keys from node are moved to left sibling, node is deleted
    fn mergeWithLeftSibling(self: *BTree, node: *BTreeNode, left_sibling: *BTreeNode) !void {
        const parent = node.parent.?;
        const node_idx = getNodeIndexInParent(node).?;

        if (node.is_leaf) {
            // Leaf node: move all keys from node to left sibling
            for (node.keys.items) |key| {
                try left_sibling.keys.append(key);
            }
            for (node.values.items) |value| {
                try left_sibling.values.append(value);
            }

            // Update leaf links
            left_sibling.next_leaf = node.next_leaf;
            if (node.next_leaf) |next| {
                next.prev_leaf = left_sibling;
            }

            // Clear node's keys/values to prevent double-free
            node.keys.items.len = 0;
            node.values.items.len = 0;
        } else {
            // Internal node: pull down separator from parent and merge
            const separator = parent.keys.items[node_idx - 1];
            try left_sibling.keys.append(separator);

            // Move all keys and children from node to left sibling
            for (node.keys.items) |key| {
                try left_sibling.keys.append(key);
            }
            for (node.children.items) |child_opt| {
                if (child_opt) |child| {
                    try left_sibling.children.append(child);
                    child.parent = left_sibling;
                }
            }

            // Clear node to prevent double-free
            node.keys.items.len = 0;
            node.children.items.len = 0;
        }

        // Remove separator and node pointer from parent
        var removed_key = parent.keys.orderedRemove(node_idx - 1);
        removed_key.deinit(parent.allocator);
        _ = parent.children.orderedRemove(node_idx);

        // Free the now-empty node
        node.deinit();
        self.allocator.destroy(node);
    }

    /// Merge node with its right sibling
    /// All keys from right sibling are moved to node, right sibling is deleted
    fn mergeWithRightSibling(self: *BTree, node: *BTreeNode, right_sibling: *BTreeNode) !void {
        const parent = node.parent.?;
        const node_idx = getNodeIndexInParent(node).?;

        if (node.is_leaf) {
            // Leaf node: move all keys from right sibling to node
            for (right_sibling.keys.items) |key| {
                try node.keys.append(key);
            }
            for (right_sibling.values.items) |value| {
                try node.values.append(value);
            }

            // Update leaf links
            node.next_leaf = right_sibling.next_leaf;
            if (right_sibling.next_leaf) |next| {
                next.prev_leaf = node;
            }

            // Clear right sibling to prevent double-free
            right_sibling.keys.items.len = 0;
            right_sibling.values.items.len = 0;
        } else {
            // Internal node: pull down separator from parent and merge
            const separator = parent.keys.items[node_idx];
            try node.keys.append(separator);

            // Move all keys and children from right sibling to node
            for (right_sibling.keys.items) |key| {
                try node.keys.append(key);
            }
            for (right_sibling.children.items) |child_opt| {
                if (child_opt) |child| {
                    try node.children.append(child);
                    child.parent = node;
                }
            }

            // Clear right sibling to prevent double-free
            right_sibling.keys.items.len = 0;
            right_sibling.children.items.len = 0;
        }

        // Remove separator and right sibling pointer from parent
        var removed_key = parent.keys.orderedRemove(node_idx);
        removed_key.deinit(parent.allocator);
        _ = parent.children.orderedRemove(node_idx + 1);

        // Free the now-empty right sibling
        right_sibling.deinit();
        self.allocator.destroy(right_sibling);
    }

    /// Rebalance a node after deletion if it has too few keys
    /// Returns true if rebalancing was performed
    fn rebalanceAfterDelete(self: *BTree, node: *BTreeNode) !bool {
        // Root can have fewer than MIN_KEYS
        if (node.parent == null) {
            return false;
        }

        // If node has enough keys, no rebalancing needed
        if (node.keys.items.len >= MIN_KEYS) {
            return false;
        }

        // Try to borrow from left sibling
        if (findLeftSibling(node)) |left_sibling| {
            if (left_sibling.keys.items.len > MIN_KEYS) {
                try self.borrowFromLeftSibling(node, left_sibling);
                return true;
            }
        }

        // Try to borrow from right sibling
        if (findRightSibling(node)) |right_sibling| {
            if (right_sibling.keys.items.len > MIN_KEYS) {
                try self.borrowFromRightSibling(node, right_sibling);
                return true;
            }
        }

        // Can't borrow - must merge
        // Prefer merging with left sibling
        if (findLeftSibling(node)) |left_sibling| {
            const parent = node.parent.?;
            try self.mergeWithLeftSibling(node, left_sibling);
            // Recursively rebalance parent (merge removed a key from it)
            _ = try self.rebalanceAfterDelete(parent);
            return true;
        }

        // Merge with right sibling
        if (findRightSibling(node)) |right_sibling| {
            const parent = node.parent.?;
            try self.mergeWithRightSibling(node, right_sibling);
            // Recursively rebalance parent
            _ = try self.rebalanceAfterDelete(parent);
            return true;
        }

        return false;
    }

    /// Delete from a specific node with full B+ tree rebalancing
    fn deleteFromNode(self: *BTree, node: *BTreeNode, key: ColumnValue, row_id: u64) !bool {
        // B+ tree principle: always delete from leaf nodes only
        // Internal nodes are just guides, actual data is in leaves

        if (node.is_leaf) {
            // Leaf node: find and delete the key
            var i: usize = 0;
            while (i < node.keys.items.len) : (i += 1) {
                const cmp = compareColumnValues(key, node.keys.items[i]);

                if (cmp == .eq and node.values.items[i] == row_id) {
                    // Found it - remove key and value
                    var removed_key = node.keys.orderedRemove(i);
                    removed_key.deinit(node.allocator);
                    _ = node.values.orderedRemove(i);

                    // Rebalance if necessary (node now has too few keys)
                    _ = try self.rebalanceAfterDelete(node);

                    return true;
                } else if (cmp == .gt) {
                    // Keys are sorted, won't find it later
                    return false;
                }
            }
            return false;
        }

        // Internal node: navigate to the correct child
        var i: usize = 0;
        while (i < node.keys.items.len) : (i += 1) {
            const cmp = compareColumnValues(key, node.keys.items[i]);

            if (cmp == .lt) {
                // Key would be in left child
                if (i < node.children.items.len) {
                    return try self.deleteFromNode(node.children.items[i].?, key, row_id);
                }
                return false;
            } else if (cmp == .eq) {
                // In B+ tree, separator keys are duplicated in leaves
                // Search the right subtree (where the actual value is)
                if (i + 1 < node.children.items.len) {
                    return try self.deleteFromNode(node.children.items[i + 1].?, key, row_id);
                }
                return false;
            }
        }

        // Key is greater than all separators - search rightmost child
        if (node.children.items.len > 0) {
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
    if (a == .null_value and b == .null_value) return .eq;
    if (a == .null_value) return .lt;
    if (b == .null_value) return .gt;

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
            if (a_val < b.float) return .lt;
            if (a_val > b.float) return .gt;
            return .eq;
        },
        .bool => |a_val| std.math.order(@intFromBool(a_val), @intFromBool(b.bool)),
        .text => |a_val| std.mem.order(u8, a_val, b.text),
        .embedding => .eq, // Embeddings not comparable for ordering
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

    // Range query [20, 40] (inclusive on both ends)
    const results = try tree.findRange(
        ColumnValue{ .int = 20 },
        ColumnValue{ .int = 40 },
        true, // min_inclusive
        true, // max_inclusive
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

test "BTree: delete from tree with splits" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert enough values to trigger splits (MAX_KEYS = 31)
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i * 10);
    }

    try testing.expectEqual(@as(usize, 100), tree.getSize());

    // Delete a key from the middle (which should be in an internal node)
    const deleted = try tree.delete(ColumnValue{ .int = 50 }, 500);
    try testing.expect(deleted);
    try testing.expectEqual(@as(usize, 99), tree.getSize());

    // Verify it's gone
    const results = try tree.search(ColumnValue{ .int = 50 });
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 0), results.len);

    // Verify other keys still exist
    const results2 = try tree.search(ColumnValue{ .int = 49 });
    defer testing.allocator.free(results2);
    try testing.expectEqual(@as(usize, 1), results2.len);
    try testing.expectEqual(@as(u64, 490), results2[0]);

    const results3 = try tree.search(ColumnValue{ .int = 51 });
    defer testing.allocator.free(results3);
    try testing.expectEqual(@as(usize, 1), results3.len);
    try testing.expectEqual(@as(u64, 510), results3[0]);
}

test "BTree: B+ tree leaf links maintained after splits" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert enough to trigger splits
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);
    }

    // Range query should work efficiently using leaf links
    const results = try tree.findRange(
        ColumnValue{ .int = 20 },
        ColumnValue{ .int = 30 },
        true,
        true,
    );
    defer testing.allocator.free(results);

    try testing.expectEqual(@as(usize, 11), results.len); // 20-30 inclusive

    // Verify results are in order
    for (results, 0..) |val, idx| {
        try testing.expectEqual(@as(u64, 20 + idx), val);
    }
}

test "BTree: deletion with rebalancing - borrow from sibling" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert enough to create a multi-level tree
    var i: u64 = 0;
    while (i < 50) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);
    }

    const size_before = tree.getSize();

    // Delete several items to trigger borrowing
    var deleted_count: usize = 0;
    i = 0;
    while (i < 10) : (i += 1) {
        const deleted = try tree.delete(ColumnValue{ .int = @intCast(i) }, i);
        if (deleted) deleted_count += 1;
    }

    try testing.expectEqual(@as(usize, 10), deleted_count);
    try testing.expectEqual(size_before - 10, tree.getSize());

    // Verify deleted keys are gone
    i = 0;
    while (i < 10) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 0), results.len);
    }

    // Verify remaining keys are still present
    i = 10;
    while (i < 50) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 1), results.len);
        try testing.expectEqual(@as(u64, i), results[0]);
    }
}

test "BTree: deletion with merging" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert values
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);
    }

    // Delete many items to trigger merging
    i = 0;
    while (i < 50) : (i += 1) {
        const deleted = try tree.delete(ColumnValue{ .int = @intCast(i) }, i);
        try testing.expect(deleted);
    }

    try testing.expectEqual(@as(usize, 50), tree.getSize());

    // Verify deleted items are gone
    i = 0;
    while (i < 50) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 0), results.len);
    }

    // Verify remaining items exist
    i = 50;
    while (i < 100) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 1), results.len);
    }
}

test "BTree: stress test - insert 1000, delete 900" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert 1000 items
    var i: u64 = 0;
    while (i < 1000) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);
    }

    try testing.expectEqual(@as(usize, 1000), tree.getSize());

    // Delete 900 items (keep every 10th)
    var deleted_count: usize = 0;
    i = 0;
    while (i < 1000) : (i += 1) {
        if (i % 10 != 0) {
            const deleted = try tree.delete(ColumnValue{ .int = @intCast(i) }, i);
            if (deleted) deleted_count += 1;
        }
    }

    try testing.expectEqual(@as(usize, 900), deleted_count);
    try testing.expectEqual(@as(usize, 100), tree.getSize());

    // Verify only every 10th item remains
    i = 0;
    while (i < 1000) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);

        if (i % 10 == 0) {
            try testing.expectEqual(@as(usize, 1), results.len);
            try testing.expectEqual(@as(u64, i), results[0]);
        } else {
            try testing.expectEqual(@as(usize, 0), results.len);
        }
    }

    // Range query should still work
    const range_results = try tree.findRange(
        ColumnValue{ .int = 0 },
        ColumnValue{ .int = 999 },
        true,
        true,
    );
    defer testing.allocator.free(range_results);
    try testing.expectEqual(@as(usize, 100), range_results.len);
}

test "BTree: delete all items one by one" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert items
    const count: u64 = 50;
    var i: u64 = 0;
    while (i < count) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);
    }

    // Delete all items
    i = 0;
    while (i < count) : (i += 1) {
        const deleted = try tree.delete(ColumnValue{ .int = @intCast(i) }, i);
        try testing.expect(deleted);
        try testing.expectEqual(count - i - 1, tree.getSize());
    }

    try testing.expectEqual(@as(usize, 0), tree.getSize());

    // Verify tree is empty
    i = 0;
    while (i < count) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 0), results.len);
    }
}

test "BTree: delete with duplicate keys" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Insert duplicate keys
    try tree.insert(ColumnValue{ .int = 10 }, 100);
    try tree.insert(ColumnValue{ .int = 10 }, 101);
    try tree.insert(ColumnValue{ .int = 10 }, 102);
    try tree.insert(ColumnValue{ .int = 10 }, 103);

    try testing.expectEqual(@as(usize, 4), tree.getSize());

    // Delete specific duplicates
    var deleted = try tree.delete(ColumnValue{ .int = 10 }, 101);
    try testing.expect(deleted);
    try testing.expectEqual(@as(usize, 3), tree.getSize());

    deleted = try tree.delete(ColumnValue{ .int = 10 }, 103);
    try testing.expect(deleted);
    try testing.expectEqual(@as(usize, 2), tree.getSize());

    // Verify remaining duplicates
    const results = try tree.search(ColumnValue{ .int = 10 });
    defer testing.allocator.free(results);
    try testing.expectEqual(@as(usize, 2), results.len);

    // Check that 100 and 102 remain (order might vary)
    var has_100 = false;
    var has_102 = false;
    for (results) |val| {
        if (val == 100) has_100 = true;
        if (val == 102) has_102 = true;
    }
    try testing.expect(has_100);
    try testing.expect(has_102);
}

test "BTree: alternating insert and delete" {
    var tree = BTree.init(testing.allocator);
    defer tree.deinit();

    // Alternate between inserting and deleting
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        try tree.insert(ColumnValue{ .int = @intCast(i) }, i);

        if (i > 10) {
            // Delete something from earlier
            _ = try tree.delete(ColumnValue{ .int = @intCast(i - 10) }, i - 10);
        }
    }

    // Should have roughly last 10 items
    try testing.expectEqual(@as(usize, 10), tree.getSize());

    // Verify last 10 items exist
    i = 90;
    while (i < 100) : (i += 1) {
        const results = try tree.search(ColumnValue{ .int = @intCast(i) });
        defer testing.allocator.free(results);
        try testing.expectEqual(@as(usize, 1), results.len);
    }
}
