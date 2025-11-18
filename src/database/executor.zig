// ============================================================================
// SQL Query Executor - Main Coordinator
// ============================================================================
//
// This is the main entry point for SQL query execution. It delegates to
// specialized executor modules for different query types.
//
// Architecture:
//   - Modular design with single-responsibility sub-executors
//   - Clean separation of concerns for testability and maintainability
//   - Each sub-executor handles one aspect of query execution
//
// Sub-modules:
//   - select_executor: SELECT query routing and coordination
//   - command_executor: DDL (CREATE/DROP) and DML (INSERT/DELETE/UPDATE)
//   - transaction_executor: BEGIN, COMMIT, ROLLBACK
//   - join_executor: All JOIN strategies (nested loop, hash join, N-way)
//   - aggregate_executor: GROUP BY, HAVING, and aggregate functions
//   - sort_executor: ORDER BY implementation
//   - expr_evaluator: Expression and subquery evaluation
//
// Total code organization:
//   - executor.zig: ~80 lines (this file - main coordinator)
//   - Sub-modules: ~2900 lines (organized by responsibility)
//   - Previous: 3085 lines in one file
//
// ============================================================================

const std = @import("std");
const core = @import("core.zig");
const Database = core.Database;
const QueryResult = core.QueryResult;
const sql = @import("../sql.zig");
const ColumnValue = @import("../table.zig").ColumnValue;

// Import specialized executor modules
const select_executor = @import("executor/select_executor.zig");
const command_executor = @import("executor/command_executor.zig");
const transaction_executor = @import("executor/transaction_executor.zig");
const expr_evaluator = @import("executor/expr_evaluator.zig");

// ============================================================================
// Main Entry Point
// ============================================================================

/// Execute a SQL query
/// This is the main entry point for all SQL execution
pub fn execute(db: *Database, query: []const u8) !QueryResult {
    // Parse the SQL command
    var cmd = try sql.parse(db.allocator, query);
    defer cmd.deinit(db.allocator);

    // Check if we need auto-commit for DML operations
    const needs_auto_commit = switch (cmd) {
        .insert, .delete, .update => db.tx_manager.getCurrentTx() == null,
        else => false,
    };

    // Begin auto-commit transaction if needed
    var auto_commit_active = false;
    if (needs_auto_commit) {
        var begin_result = try transaction_executor.executeBegin(db);
        begin_result.deinit();
        auto_commit_active = true;
    }
    defer {
        // Clean up auto-commit transaction on error (if not committed)
        if (auto_commit_active) {
            // Transaction wasn't committed, so roll it back and clean up
            if (transaction_executor.executeRollback(db)) |rb| {
                var rb_mut = rb;
                rb_mut.deinit();
            } else |_| {
                // Rollback failed, but transaction manager will clean up
            }
        }
    }

    // Route to appropriate executor based on command type
    const result = switch (cmd) {
        // DDL Commands (Data Definition Language)
        .create_table => |create| try command_executor.executeCreateTable(db, create),
        .create_index => |create_idx| try command_executor.executeCreateIndex(db, create_idx),
        .drop_index => |drop_idx| try command_executor.executeDropIndex(db, drop_idx),
        .alter_table => |alter| try command_executor.executeAlterTable(db, alter),

        // DML Commands (Data Manipulation Language)
        .insert => |insert| try command_executor.executeInsert(db, insert),
        .delete => |delete| try command_executor.executeDelete(db, delete),
        .update => |update| try command_executor.executeUpdate(db, update),

        // DQL Commands (Data Query Language)
        .select => |select_cmd| try select_executor.executeSelect(db, select_cmd),

        // Transaction Control
        .begin => try transaction_executor.executeBegin(db),
        .commit => try transaction_executor.executeCommit(db),
        .rollback => try transaction_executor.executeRollback(db),

        // Maintenance Commands
        .vacuum => |vacuum| try command_executor.executeVacuum(db, vacuum),
    };

    // Commit auto-commit transaction if successful
    if (needs_auto_commit) {
        var commit_result = try transaction_executor.executeCommit(db);
        commit_result.deinit();
        auto_commit_active = false; // Mark as committed so defer doesn't rollback

        // Trigger auto-VACUUM after commit (so the transaction is no longer active)
        db.maybeAutoVacuum();
    }

    return result;
}

// ============================================================================
// Public API for Expression Evaluation (Backward Compatibility)
// ============================================================================

/// Enhanced expression evaluator that handles subqueries
///
/// This is a convenience wrapper around expr_evaluator.evaluateExprWithSubqueries
/// that automatically provides the executeSelect function pointer.
///
/// Used by:
/// - aggregate_executor (HAVING clause evaluation)
/// - command_executor (WHERE clause in UPDATE/DELETE)
/// - join_executor (WHERE clause in JOINs)
///
pub fn evaluateExprWithSubqueries(
    db: *Database,
    expr: sql.Expr,
    row_values: anytype,
) anyerror!bool {
    // Wrap expr_evaluator function and provide executeSelect as the callback
    return expr_evaluator.evaluateExprWithSubqueries(
        db,
        expr,
        row_values,
        select_executor.executeSelect, // Function pointer for subquery execution
    );
}
