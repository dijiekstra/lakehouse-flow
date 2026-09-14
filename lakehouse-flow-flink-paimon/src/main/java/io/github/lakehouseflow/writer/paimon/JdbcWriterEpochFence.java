package io.github.lakehouseflow.writer.paimon;

import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * PostgreSQL-compatible writer fence backed by Lakehouse Flow's writer binding row.
 *
 * <p>The row lock is held across the Paimon commit. Scheduler epoch allocation uses a conflicting
 * pessimistic lock on the same row, so either the old commit finishes before the new epoch exists,
 * or the old writer observes the new epoch and is rejected before publishing a snapshot.
 */
public final class JdbcWriterEpochFence implements WriterEpochFence {

    private static final String LOCK_WRITER_SQL = """
            SELECT table_asset_key, current_writer_epoch, active_processing_mode,
                   holder_intent_key, current_control_intent_key
            FROM writer_job_binding
            WHERE writer_job_key = ?
            FOR UPDATE
            """;

    private final String jdbcUrl;
    private final String username;
    private final String password;

    /** Create a JDBC fence using execution-plane credentials with binding-row read access. */
    public JdbcWriterEpochFence(String jdbcUrl, String username, String password) {
        this.jdbcUrl = requireText(jdbcUrl, "jdbcUrl");
        this.username = requireText(username, "username");
        this.password = password == null ? "" : password;
    }

    /** Acquire and retain the authoritative binding-row lock through the caller's commit. */
    @Override
    public CommitPermit acquireCommitPermit(WriterCommitContext context) throws Exception {
        if (context == null) {
            throw new IllegalArgumentException("writer commit context is required");
        }
        Connection connection = DriverManager.getConnection(jdbcUrl, username, password);
        boolean permitCreated = false;
        try {
            connection.setAutoCommit(false);
            WriterBindingState state = lockBinding(connection, context.writerJobKey());
            validateCurrentGeneration(context, state);
            permitCreated = true;
            return new JdbcCommitPermit(connection);
        } finally {
            if (!permitCreated) {
                rollbackAndClose(connection);
            }
        }
    }

    /** Read one binding while retaining its database row lock. */
    private WriterBindingState lockBinding(Connection connection, String writerJobKey) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(LOCK_WRITER_SQL)) {
            statement.setString(1, writerJobKey);
            try (ResultSet resultSet = statement.executeQuery()) {
                if (!resultSet.next()) {
                    throw new StaleWriterEpochException("Writer binding no longer exists: " + writerJobKey);
                }
                return new WriterBindingState(
                        resultSet.getString("table_asset_key"),
                        resultSet.getLong("current_writer_epoch"),
                        resultSet.getString("active_processing_mode"),
                        resultSet.getString("holder_intent_key"),
                        resultSet.getString("current_control_intent_key"));
            }
        }
    }

    /** Fail closed unless the database still authorizes every commit coordinate. */
    private void validateCurrentGeneration(WriterCommitContext context, WriterBindingState state) {
        if (!context.tableAssetKey().equals(state.tableAssetKey())) {
            throw stale(context, "physical table is now owned as " + state.tableAssetKey());
        }
        if (context.writerEpoch() != state.writerEpoch()) {
            throw stale(context, "current writer epoch is " + state.writerEpoch());
        }
        if (!context.processingMode().equals(state.processingMode())) {
            throw stale(context, "active processing mode is " + state.processingMode());
        }
        if (context.jobControlIntent()) {
            if (!context.intentKey().equals(state.currentControlIntentKey())
                    || !context.intentKey().equals(state.holderIntentKey())) {
                throw stale(context, "job-control intent no longer owns the writer");
            }
            return;
        }
        if (ScheduleNodeProcessingModes.BATCH.equals(context.processingMode())
                && !context.intentKey().equals(state.holderIntentKey())) {
            throw stale(context, "batch scheduling intent no longer owns the writer");
        }
        if (ScheduleNodeProcessingModes.STREAMING.equals(context.processingMode())
                && isBlank(state.currentControlIntentKey())) {
            throw stale(context, "streaming writer has no active platform control generation");
        }
    }

    /** Build one stable stale-writer failure message. */
    private StaleWriterEpochException stale(WriterCommitContext context, String detail) {
        return new StaleWriterEpochException("Writer " + context.writerJobKey()
                + " epoch " + context.writerEpoch() + " is fenced: " + detail);
    }

    /** Roll back the read transaction and close its connection without hiding the first failure. */
    private static void rollbackAndClose(Connection connection) throws SQLException {
        SQLException failure = null;
        try {
            connection.rollback();
        } catch (SQLException exception) {
            failure = exception;
        }
        try {
            connection.close();
        } catch (SQLException exception) {
            if (failure == null) {
                failure = exception;
            } else {
                failure.addSuppressed(exception);
            }
        }
        if (failure != null) {
            throw failure;
        }
    }

    /** Require one nonblank JDBC connection value. */
    private static String requireText(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    /** Check whether an optional binding coordinate is absent. */
    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }

    /** Snapshot of the locked writer binding used only while its JDBC transaction remains open. */
    private record WriterBindingState(
            String tableAssetKey,
            long writerEpoch,
            String processingMode,
            String holderIntentKey,
            String currentControlIntentKey) {
    }

    /** JDBC transaction retaining the binding-row lock until the Paimon commit returns. */
    private static final class JdbcCommitPermit implements CommitPermit {

        private final Connection connection;
        private boolean closed;

        /** Create a permit around one already locked connection. */
        private JdbcCommitPermit(Connection connection) {
            this.connection = connection;
        }

        /** Release the row lock exactly once by rolling back the read-only transaction. */
        @Override
        public void close() throws SQLException {
            if (!closed) {
                closed = true;
                rollbackAndClose(connection);
            }
        }
    }
}
