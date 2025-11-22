# Chat Context Store Design for CLI Chat App

## Overview

This document provides a production-ready schema design for storing chat conversations, tool calls, and results in ZVDB. The design supports:
- Multiple concurrent chat sessions
- Full conversation history with message ordering
- Tool calls and their results
- Semantic search over conversations
- Efficient retrieval of recent messages
- Filtering by message type, tool type, and session

## Database Suitability

✅ **ZVDB is well-suited for this use case:**
- Small data volume (thousands of messages) fits perfectly in-memory
- Text storage handles messages, tool calls, and JSON results
- B-tree indexes enable fast keyword/type filtering
- HNSW embeddings perfect for semantic search of conversation history
- WAL ensures durability (no data loss on crash)
- MVCC transactions support concurrent sessions
- ORDER BY with LIMIT perfect for "recent history" queries

## Schema Design

### Table 1: Sessions
Tracks individual chat sessions.

```sql
CREATE TABLE sessions (
    session_id int,
    created_at int,
    last_activity_at int,
    status text,
    metadata text
)
```

**Columns:**
- `session_id`: Unique session identifier (use timestamp or counter)
- `created_at`: Unix timestamp when session started
- `last_activity_at`: Unix timestamp of last message/activity
- `status`: Session status (active, completed, error)
- `metadata`: Optional JSON metadata (session config, user info, etc.)

**Indexes:**
```sql
CREATE INDEX idx_sessions_status ON sessions (status)
CREATE INDEX idx_sessions_last_activity ON sessions (last_activity_at)
```

### Table 2: Messages
Stores all messages in conversations.

```sql
CREATE TABLE messages (
    message_id int,
    session_id int,
    sequence_num int,
    role text,
    content text,
    content_embedding embedding(384),
    created_at int,
    message_type text,
    metadata text
)
```

**Columns:**
- `message_id`: Unique message identifier
- `session_id`: Foreign key to sessions table
- `sequence_num`: Message order within session (1, 2, 3...)
- `role`: Message role (user, assistant, system)
- `content`: The actual message text
- `content_embedding`: Vector embedding for semantic search (384-dim for MiniLM)
- `created_at`: Unix timestamp
- `message_type`: Type classification (text, tool_call, tool_result, error)
- `metadata`: Optional JSON metadata

**Indexes:**
```sql
CREATE INDEX idx_messages_session ON messages (session_id)
CREATE INDEX idx_messages_type ON messages (message_type)
CREATE INDEX idx_messages_role ON messages (role)
```

### Table 3: Tool Calls
Stores tool invocations and their metadata.

```sql
CREATE TABLE tool_calls (
    tool_call_id int,
    message_id int,
    session_id int,
    tool_name text,
    parameters text,
    status text,
    created_at int,
    completed_at int
)
```

**Columns:**
- `tool_call_id`: Unique tool call identifier
- `message_id`: Associated message (if tool call is embedded in a message)
- `session_id`: Session this tool call belongs to
- `tool_name`: Name of the tool invoked
- `parameters`: JSON string of tool parameters
- `status`: Execution status (pending, running, completed, failed)
- `created_at`: When tool was called
- `completed_at`: When tool finished (null if still running)

**Indexes:**
```sql
CREATE INDEX idx_tool_calls_session ON tool_calls (session_id)
CREATE INDEX idx_tool_calls_name ON tool_calls (tool_name)
CREATE INDEX idx_tool_calls_status ON tool_calls (status)
```

### Table 4: Tool Results
Stores results from tool executions.

```sql
CREATE TABLE tool_results (
    result_id int,
    tool_call_id int,
    session_id int,
    result_data text,
    result_summary text,
    result_embedding embedding(384),
    error_message text,
    created_at int
)
```

**Columns:**
- `result_id`: Unique result identifier
- `tool_call_id`: Foreign key to tool_calls
- `session_id`: Session for quick filtering
- `result_data`: Full result data (JSON or text)
- `result_summary`: Human-readable summary of result
- `result_embedding`: Vector for semantic search of results
- `error_message`: Error details if tool failed
- `created_at`: When result was stored

**Indexes:**
```sql
CREATE INDEX idx_tool_results_session ON tool_results (session_id)
CREATE INDEX idx_tool_results_tool_call ON tool_results (tool_call_id)
```

## Example Queries

### 1. Get Recent Messages from Current Session
```sql
SELECT content, role, created_at
FROM messages
WHERE session_id = 12345
ORDER BY sequence_num DESC
LIMIT 20
```

### 2. Search for Keywords in Conversation
```sql
SELECT m.content, m.role, m.created_at, s.status
FROM messages m
WHERE m.session_id = 12345
  AND m.content LIKE '%error%'
ORDER BY m.sequence_num DESC
```

**Note:** For full-text search, filter in application code after retrieval.

### 3. Semantic Search: Find Similar Conversations
```sql
SELECT content, role, created_at
FROM messages
WHERE session_id = 12345
ORDER BY SIMILARITY TO "database connection issues"
LIMIT 5
```

### 4. Get All Tool Calls of Specific Type
```sql
SELECT tc.tool_name, tc.parameters, tc.status, tc.created_at
FROM tool_calls tc
WHERE tc.session_id = 12345
  AND tc.tool_name = 'file_search'
ORDER BY tc.created_at DESC
```

### 5. Get Tool Call with Its Result
```sql
SELECT
    tc.tool_name,
    tc.parameters,
    tc.status,
    tr.result_summary,
    tr.error_message
FROM tool_calls tc
LEFT JOIN tool_results tr ON tc.tool_call_id = tr.tool_call_id
WHERE tc.session_id = 12345
  AND tc.tool_call_id = 789
```

**Note:** Since ZVDB doesn't have JOIN support with WHERE clauses on joined tables yet, you may need to:
1. Query `tool_calls` first
2. Then query `tool_results` with the `tool_call_id`

**Alternative approach:**
```sql
-- Step 1: Get tool call
SELECT tool_name, parameters, status FROM tool_calls WHERE tool_call_id = 789

-- Step 2: Get its result
SELECT result_summary, error_message FROM tool_results WHERE tool_call_id = 789
```

### 6. Get All Active Sessions
```sql
SELECT session_id, created_at, last_activity_at
FROM sessions
WHERE status = 'active'
ORDER BY last_activity_at DESC
```

### 7. Get Complete Session History
```sql
-- Get all messages for a session
SELECT message_id, sequence_num, role, content, message_type, created_at
FROM messages
WHERE session_id = 12345
ORDER BY sequence_num ASC
```

### 8. Find Sessions with Failed Tool Calls
```sql
SELECT DISTINCT session_id, COUNT(*) as failed_count
FROM tool_calls
WHERE status = 'failed'
GROUP BY session_id
ORDER BY failed_count DESC
```

### 9. Semantic Search Across All Sessions
```sql
SELECT m.session_id, m.content, m.role, m.created_at
FROM messages m
ORDER BY SIMILARITY TO "how to configure database"
LIMIT 10
```

## Best Practices

### 1. ID Generation
Use a simple counter or timestamp-based ID generation:
```zig
var next_message_id: u64 = 1;

fn generateMessageId() u64 {
    defer next_message_id += 1;
    return next_message_id;
}
```

### 2. Embedding Generation
Generate embeddings using an external model (e.g., sentence-transformers):
```zig
// Pseudo-code - you'll use actual embedding model
const embedding = await generateEmbedding(message.content);

// Store with embedding
try db.execute(
    "INSERT INTO messages VALUES (?, ?, ?, ?, ?, [?], ?, ?, ?)",
    .{
        message_id,
        session_id,
        sequence_num,
        role,
        content,
        embedding,  // Array of f32
        created_at,
        message_type,
        metadata
    }
);
```

### 3. Transaction Management for Concurrent Sessions
```zig
// Start transaction for each session operation
try db.execute("BEGIN");

// Insert message
try db.execute("INSERT INTO messages ...");

// Insert tool call if needed
if (has_tool_call) {
    try db.execute("INSERT INTO tool_calls ...");
}

// Commit transaction
try db.execute("COMMIT");
```

### 4. Efficient Recent Message Retrieval
For displaying recent messages, always use ORDER BY + LIMIT:
```sql
-- Get last 50 messages efficiently
SELECT content, role, created_at
FROM messages
WHERE session_id = ?
ORDER BY sequence_num DESC
LIMIT 50
```

### 5. Cleanup Old Sessions
Periodically vacuum old data:
```sql
-- Delete old completed sessions (pseudo-code)
DELETE FROM messages WHERE session_id IN
    (SELECT session_id FROM sessions
     WHERE status = 'completed' AND last_activity_at < ?)

DELETE FROM sessions
WHERE status = 'completed' AND last_activity_at < ?

-- Run VACUUM to reclaim memory
VACUUM
```

### 6. Error Handling
Always wrap database operations in error handling:
```zig
const result = db.execute(query) catch |err| {
    std.log.err("Database error: {}", .{err});
    // Handle error - maybe retry with exponential backoff
    return error.DatabaseError;
};
defer result.deinit();
```

### 7. Avoid Write Skew in Concurrent Sessions
Since ZVDB uses snapshot isolation (not serializable), be aware of potential write skew:
```zig
// Safe pattern: Each session has its own sequence
// Session 1 writes message_id=1, sequence=1
// Session 2 writes message_id=2, sequence=1  // OK - different sessions
// No conflict!

// Unsafe pattern: Shared counters without locking
// Both sessions read max(sequence_num) = 10
// Both try to insert sequence=11  // Potential conflict
```

**Solution:** Use per-session sequence numbers or include session_id in uniqueness checks.

## Performance Characteristics

### Expected Performance (based on ZVDB benchmarks)

**Writes:**
- Single message insert: < 1ms
- Batch insert (10 messages): < 5ms
- With WAL fsync: +5-10ms per commit

**Reads:**
- Recent messages (LIMIT 50): < 1ms (sequential scan)
- By session_id with B-tree index: < 0.1ms
- Semantic search (k=5): < 10ms (100k messages)
- JOIN emulation (2 queries): < 1ms total

**Concurrency:**
- Multiple read sessions: No contention
- Multiple write sessions: MVCC handles isolation
- Write-write conflicts: Rare (per-session operations)

## Example Application Code

Here's a complete example of using the schema:

```zig
const Database = @import("database.zig").Database;
const std = @import("std");

pub const ChatStore = struct {
    db: *Database,
    current_session_id: u64,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) !ChatStore {
        var db = try Database.init(allocator);

        // Create schema
        try db.execute("CREATE TABLE IF NOT EXISTS sessions (session_id int, created_at int, last_activity_at int, status text, metadata text)");
        try db.execute("CREATE TABLE IF NOT EXISTS messages (message_id int, session_id int, sequence_num int, role text, content text, content_embedding embedding(384), created_at int, message_type text, metadata text)");
        try db.execute("CREATE TABLE IF NOT EXISTS tool_calls (tool_call_id int, message_id int, session_id int, tool_name text, parameters text, status text, created_at int, completed_at int)");
        try db.execute("CREATE TABLE IF NOT EXISTS tool_results (result_id int, tool_call_id int, session_id int, result_data text, result_summary text, result_embedding embedding(384), error_message text, created_at int)");

        // Create indexes
        try db.execute("CREATE INDEX idx_messages_session ON messages (session_id)");
        try db.execute("CREATE INDEX idx_tool_calls_session ON tool_calls (session_id)");

        const session_id = @as(u64, @intCast(std.time.timestamp()));

        return ChatStore{
            .db = &db,
            .current_session_id = session_id,
            .allocator = allocator,
        };
    }

    pub fn addMessage(self: *ChatStore, role: []const u8, content: []const u8) !void {
        const timestamp = std.time.timestamp();

        // Get next sequence number
        const seq_query = try std.fmt.allocPrint(
            self.allocator,
            "SELECT MAX(sequence_num) FROM messages WHERE session_id = {d}",
            .{self.current_session_id}
        );
        defer self.allocator.free(seq_query);

        var result = try self.db.execute(seq_query);
        defer result.deinit();

        const next_seq: i64 = if (result.rows.items.len > 0)
            result.rows.items[0].items[0].int + 1
        else
            1;

        // Insert message (without embedding for now)
        const insert_query = try std.fmt.allocPrint(
            self.allocator,
            "INSERT INTO messages VALUES ({d}, {d}, {d}, '{s}', '{s}', NULL, {d}, 'text', NULL)",
            .{timestamp, self.current_session_id, next_seq, role, content, timestamp}
        );
        defer self.allocator.free(insert_query);

        var insert_result = try self.db.execute(insert_query);
        defer insert_result.deinit();
    }

    pub fn getRecentMessages(self: *ChatStore, limit: usize) !void {
        const query = try std.fmt.allocPrint(
            self.allocator,
            "SELECT role, content, created_at FROM messages WHERE session_id = {d} ORDER BY sequence_num DESC LIMIT {d}",
            .{self.current_session_id, limit}
        );
        defer self.allocator.free(query);

        var result = try self.db.execute(query);
        defer result.deinit();

        // Process results
        for (result.rows.items) |row| {
            const role = row.items[0].text;
            const content = row.items[1].text;
            const timestamp = row.items[2].int;

            std.debug.print("[{d}] {s}: {s}\n", .{timestamp, role, content});
        }
    }
};
```

## Summary

This schema design provides:
- ✅ Complete message history with ordering
- ✅ Tool call tracking with status
- ✅ Efficient recent message queries
- ✅ Semantic search capabilities
- ✅ Session isolation for concurrent users
- ✅ Flexible metadata storage as JSON
- ✅ Production-ready with indexes and best practices

**Next Steps:**
1. Implement ID generation strategy
2. Integrate embedding model (e.g., sentence-transformers via FFI)
3. Add application-level validation (uniqueness, foreign keys)
4. Implement cleanup/archiving for old sessions
5. Add monitoring and logging
