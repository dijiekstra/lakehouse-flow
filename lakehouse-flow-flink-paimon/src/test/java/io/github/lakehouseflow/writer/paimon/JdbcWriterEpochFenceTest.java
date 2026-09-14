package io.github.lakehouseflow.writer.paimon;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests JDBC row-lock fencing without requiring an external database process. */
class JdbcWriterEpochFenceTest {

    /** Verify the current control generation retains its JDBC transaction until permit close. */
    @Test
    void acquiresPermitForCurrentControlGeneration() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        stubBinding(resultSet, 2L, "STREAMING", "job-control:orders-writer:2");

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:test", "writer", "secret"))
                    .thenReturn(connection);
            JdbcWriterEpochFence fence = new JdbcWriterEpochFence("jdbc:test", "writer", "secret");

            try (WriterEpochFence.CommitPermit ignored = fence.acquireCommitPermit(controlContext(2L))) {
                verify(connection).setAutoCommit(false);
                verify(statement).setString(1, "orders-writer");
            }
        }

        verify(connection).rollback();
        verify(connection).close();
    }

    /** Verify a superseded writer generation is rejected and its row lock is released. */
    @Test
    void rejectsStaleWriterEpoch() throws Exception {
        Connection connection = mock(Connection.class);
        PreparedStatement statement = mock(PreparedStatement.class);
        ResultSet resultSet = mock(ResultSet.class);
        when(connection.prepareStatement(anyString())).thenReturn(statement);
        when(statement.executeQuery()).thenReturn(resultSet);
        when(resultSet.next()).thenReturn(true);
        stubBinding(resultSet, 2L, "STREAMING", "job-control:orders-writer:2");

        try (MockedStatic<DriverManager> driverManager = mockStatic(DriverManager.class)) {
            driverManager.when(() -> DriverManager.getConnection("jdbc:test", "writer", "secret"))
                    .thenReturn(connection);
            JdbcWriterEpochFence fence = new JdbcWriterEpochFence("jdbc:test", "writer", "secret");

            assertThrows(StaleWriterEpochException.class,
                    () -> fence.acquireCommitPermit(controlContext(1L)));
        }

        verify(connection).rollback();
        verify(connection).close();
    }

    /** Populate one locked writer binding result. */
    private void stubBinding(
            ResultSet resultSet,
            long epoch,
            String mode,
            String controlIntentKey) throws Exception {
        when(resultSet.getString("table_asset_key")).thenReturn("paimon.ods.orders");
        when(resultSet.getLong("current_writer_epoch")).thenReturn(epoch);
        when(resultSet.getString("active_processing_mode")).thenReturn(mode);
        when(resultSet.getString("holder_intent_key")).thenReturn(controlIntentKey);
        when(resultSet.getString("current_control_intent_key")).thenReturn(controlIntentKey);
    }

    /** Build one valid job-control commit context. */
    private WriterCommitContext controlContext(long epoch) {
        String intentKey = "job-control:orders-writer:" + epoch;
        return new WriterCommitContext(
                "paimon.ods.orders",
                "orders-writer",
                epoch,
                intentKey,
                WriterCommitContext.IntentKind.JOB_CONTROL,
                "STREAMING",
                Map.of(
                        SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                        SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY, intentKey,
                        SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, "orders-writer",
                        SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, Long.toString(epoch)));
    }
}
