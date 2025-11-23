# GraphRAG API Reference

Graph relationships and metadata for Retrieval-Augmented Generation.

## Node Metadata

### `NodeMetadata`

```zig
struct {
    node_type: []const u8,           // "doc_chunk", "function", "entity", etc.
    content_ref: ?[]const u8,        // File path or URL
    start_offset: ?usize,            // Byte offset in content
    end_offset: ?usize,              // End byte offset
    attributes: StringHashMap(MetadataValue),  // Flexible key→value store
}
```

### `MetadataValue`

```zig
union(enum) {
    string: []const u8,
    int: i64,
    float: f64,
    bool: bool,
}
```

### Creating Metadata

```zig
var attrs = StringHashMap(MetadataValue).init(allocator);
try attrs.put("language", .{ .string = "zig" });
try attrs.put("line_count", .{ .int = 42 });

const metadata = NodeMetadata{
    .node_type = "function",
    .content_ref = "src/main.zig",
    .start_offset = 1024,
    .end_offset = 2048,
    .attributes = attrs,
};

const id = try hnsw.insertWithMetadata(embedding, null, metadata);
```

## Edges

### `Edge`

```zig
struct {
    src: u64,              // Source node external ID
    dst: u64,              // Destination node external ID
    edge_type: []const u8, // "calls", "references", "child_of", etc.
    weight: f32,           // Edge weight (default 1.0)
}
```

### `EdgeKey`

```zig
struct {
    src: u64,
    dst: u64,
    edge_type: []const u8,
}
```

Uniquely identifies an edge for lookups and removal.

## Edge Operations

### `addEdge(hnsw, src, dst, edge_type, weight) !void`

Create typed edge between nodes. Edges are directed.

```zig
try hnsw.addEdge(func_id, caller_id, "called_by", 1.0);
```

### `removeEdge(hnsw, src, dst, edge_type) !void`

Remove specific edge.

### `getEdges(hnsw, node_id, edge_type_filter) ![]const Edge`

Get all edges for node, optionally filtered by type.

```zig
const calls = try hnsw.getEdges(func_id, "calls");      // Outgoing calls
const all = try hnsw.getEdges(func_id, null);           // All edges
```

## Graph Traversal

### `traverse(hnsw, start_id, max_depth, edge_type_filter) ![]const u64`

Breadth-first traversal from start node.

- `start_id`: Starting node external ID
- `max_depth`: Maximum traversal depth (0 = immediate neighbors only)
- `edge_type_filter`: Only follow edges of this type (null = all types)
- Returns: Array of visited node IDs in BFS order

```zig
const reachable = try hnsw.traverse(root_id, 3, "child_of");
```

## Hybrid Search

### `searchThenTraverse(hnsw, query, k, max_depth, edge_type_filter) ![]const u64`

Two-phase retrieval:
1. Vector search to find k nearest nodes
2. Graph traversal from each result

Returns expanded set including both search results and graph neighbors.

### `searchByType(hnsw, query, k, node_type) ![]const SearchResult`

Vector search filtered to specific node type.

```zig
const funcs = try hnsw.searchByType(query, 10, "function");
```

## Type Indexing

### `getNodesByType(hnsw, node_type) ![]const u64`

Get all nodes of specified type without vector search.

### `getNodesByFilePath(hnsw, file_path) ![]const u64`

Get all nodes from specific file.

## Example: Code Knowledge Graph

```zig
// Add function node
var attrs = StringHashMap(MetadataValue).init(allocator);
try attrs.put("name", .{ .string = "parseSQL" });
try attrs.put("lines", .{ .int = 150 });

const meta = NodeMetadata{
    .node_type = "function",
    .content_ref = "src/sql.zig",
    .start_offset = 1000,
    .end_offset = 5000,
    .attributes = attrs,
};

const func_id = try hnsw.insertWithMetadata(func_embedding, null, meta);

// Add call relationship
try hnsw.addEdge(func_id, caller_id, "calls", 1.0);

// Find similar functions
const similar = try hnsw.searchByType(query_emb, 5, "function");

// Traverse call graph
const callees = try hnsw.traverse(func_id, 2, "calls");
```

## Memory Management

- Metadata strings are owned by nodes - don't free after insert
- Edge strings are duplicated internally
- Caller owns all returned arrays
- Updating metadata frees old metadata automatically
