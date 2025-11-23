# HNSW API Reference

Vector index using Hierarchical Navigable Small World algorithm for approximate nearest neighbor search.

## Type

```zig
pub fn HNSW(comptime T: type) type
```

Generic type parameterized by element type (`f32`, `f64`, etc.). Returns a struct with vector search capabilities.

## Initialization

### `init(allocator, m, ef_construction) Self`

Create new HNSW index.

- `m`: Maximum connections per node (typical: 16-32)
- `ef_construction`: Size of dynamic candidate list during construction (typical: 200-400)

Higher values improve recall at cost of memory and build time.

### `deinit(self) void`

Free all resources. Call before discarding index.

## Vector Operations

### `insert(self, point, external_id) !u64`

Insert vector into index.

- `point`: Vector slice of type `[]const T`
- `external_id`: Optional user ID (`?u64`). Auto-generated if null.
- Returns: External ID assigned to this vector

Throws `error.DuplicateExternalId` if ID already exists.

### `insertWithMetadata(self, point, external_id, metadata) !u64`

Insert vector with GraphRAG metadata (see graphrag.md).

### `search(self, query, k) ![]const SearchResult`

Find k nearest neighbors.

- `query`: Query vector `[]const T`
- `k`: Number of results
- Returns: Array of `SearchResult{external_id, point, distance}`

Uses cosine similarity. Caller owns result memory.

### `searchByType(self, query, k, node_type) ![]const SearchResult`

Search filtered by node type. Only returns nodes matching specified type.

### `removeNode(self, external_id) !void`

Remove node and reconnect its neighbors to prevent graph fragmentation.

## ID Management

### `getInternalId(self, external_id) ?usize`

Map external ID to internal ID. Returns null if not found.

### `getExternalId(self, internal_id) ?u64`

Map internal ID to external ID. Returns null if not found.

### `getByExternalId(self, external_id) ?Node`

Retrieve node by external ID.

## Type Indexing

### `getNodesByType(self, node_type) ![]const u64`

Get all external IDs with specified node type. Returns empty array if type not found.

### `getNodesByFilePath(self, file_path) ![]const u64`

Get all external IDs from specified file path.

## Metadata Operations

### `updateMetadata(self, external_id, new_metadata) !void`

Replace node metadata. Updates type and file path indexes automatically.

Throws `error.NodeNotFound` if ID doesn't exist.

## Persistence

### `save(self, path) !void`

Save index to binary file (v2 format with metadata and edges).

### `load(allocator, path) !Self`

Load index from file. Static function - call as `HNSW(f32).load(allocator, path)`.

## Types

### `SearchResult`

```zig
struct {
    external_id: u64,
    point: []const T,
    distance: T,
}
```

Result from search operations. Distance is cosine similarity (lower = more similar).

## Thread Safety

All public methods are thread-safe via internal mutexes. Safe for concurrent insertions and searches.

## Performance Notes

- Insert: O(log N) average, O(N) worst case
- Search: Sub-linear with proper parameters
- Memory: ~(M × 2 × dimension × sizeof(T)) per node
- Optimal M: 16 for most use cases, 32 for high recall
- Optimal ef_construction: 200-400 (higher = better quality, slower build)
