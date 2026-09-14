package io.github.lakehouseflow.writer.paimon;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.manifest.ManifestCommittable;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.TableCommitImpl;
import org.apache.paimon.table.sink.TableWriteImpl;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Tests the reusable Paimon writer adapter independently from the E2E job. */
class FlinkPaimonWriterAdapterTest {

    /** Verify a commit retains its fence permit and writes every immutable snapshot property. */
    @Test
    void commitsAttributedSnapshotInsideFencePermit() throws Exception {
        Catalog catalog = mock(Catalog.class);
        TableWriteImpl<?> writer = mock(TableWriteImpl.class);
        TableCommitImpl committer = mock(TableCommitImpl.class);
        WriterEpochFence fence = mock(WriterEpochFence.class);
        WriterEpochFence.CommitPermit permit = mock(WriterEpochFence.CommitPermit.class);
        WriterCommitContext context = controlContext(2L);
        CommitMessage message = mock(CommitMessage.class);
        when(fence.acquireCommitPermit(context)).thenReturn(permit);
        FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                catalog, writer, committer, context, fence);

        adapter.commit(81L, List.of(message));

        ArgumentCaptor<ManifestCommittable> captor = ArgumentCaptor.forClass(ManifestCommittable.class);
        verify(committer).commit(captor.capture());
        assertEquals(81L, captor.getValue().identifier());
        assertEquals(context.snapshotProperties(), captor.getValue().properties());
        org.mockito.InOrder order = inOrder(fence, committer, permit);
        order.verify(fence).acquireCommitPermit(context);
        order.verify(committer).commit(captor.getValue());
        order.verify(permit).close();
    }

    /** Verify a stale epoch stops publication before Paimon receives a committable. */
    @Test
    void rejectsCommitWhenFenceRejectsWriter() throws Exception {
        Catalog catalog = mock(Catalog.class);
        TableWriteImpl<?> writer = mock(TableWriteImpl.class);
        TableCommitImpl committer = mock(TableCommitImpl.class);
        WriterEpochFence fence = mock(WriterEpochFence.class);
        WriterCommitContext context = controlContext(1L);
        when(fence.acquireCommitPermit(context)).thenThrow(
                new StaleWriterEpochException("epoch 1 is fenced"));
        FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                catalog, writer, committer, context, fence);

        assertThrows(StaleWriterEpochException.class,
                () -> adapter.commit(82L, List.of(mock(CommitMessage.class))));

        verify(committer, never()).commit(org.mockito.ArgumentMatchers.any(ManifestCommittable.class));
    }

    /** Verify row writing and checkpoint preparation remain separate from snapshot publication. */
    @Test
    void preparesCheckpointWithoutCommitting() throws Exception {
        Catalog catalog = mock(Catalog.class);
        TableWriteImpl<?> writer = mock(TableWriteImpl.class);
        TableCommitImpl committer = mock(TableCommitImpl.class);
        InternalRow row = mock(InternalRow.class);
        CommitMessage message = mock(CommitMessage.class);
        when(writer.prepareCommit(false, 12L)).thenReturn(List.of(message));
        FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                catalog, writer, committer, controlContext(2L), ignored -> () -> { });

        adapter.write(row);
        List<CommitMessage> messages = adapter.prepareCheckpoint(12L);

        assertEquals(List.of(message), messages);
        verify(writer).write(row);
        verify(committer, never()).commit(org.mockito.ArgumentMatchers.any(ManifestCommittable.class));
    }

    /** Verify bounded preparation and checkpoint abort delegate to the matching Paimon APIs. */
    @Test
    void preparesBatchAndAbortsPreparedMessages() throws Exception {
        Catalog catalog = mock(Catalog.class);
        TableWriteImpl<?> writer = mock(TableWriteImpl.class);
        TableCommitImpl committer = mock(TableCommitImpl.class);
        CommitMessage message = mock(CommitMessage.class);
        when(writer.prepareCommit()).thenReturn(List.of(message));
        FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                catalog, writer, committer, controlContext(2L), ignored -> () -> { });

        List<CommitMessage> messages = adapter.prepareBatch();
        adapter.abort(messages);

        assertEquals(List.of(message), messages);
        verify(committer).abort(messages);
        verify(committer, never()).commit(org.mockito.ArgumentMatchers.any(ManifestCommittable.class));
    }

    /** Verify all owned Paimon resources close in writer-to-catalog order. */
    @Test
    void closesOwnedResources() throws Exception {
        Catalog catalog = mock(Catalog.class);
        TableWriteImpl<?> writer = mock(TableWriteImpl.class);
        TableCommitImpl committer = mock(TableCommitImpl.class);
        FlinkPaimonWriterAdapter adapter = new FlinkPaimonWriterAdapter(
                catalog, writer, committer, controlContext(2L), ignored -> () -> { });

        adapter.close();

        org.mockito.InOrder order = inOrder(writer, committer, catalog);
        order.verify(writer).close();
        order.verify(committer).close();
        order.verify(catalog).close();
    }

    /** Verify bounded intent identifiers are stable and nonnegative. */
    @Test
    void derivesStableCommitIdentifier() {
        long first = FlinkPaimonWriterAdapter.stableCommitIdentifier("task-instance:42");
        long second = FlinkPaimonWriterAdapter.stableCommitIdentifier("task-instance:42");

        assertEquals(first, second);
        assertEquals(true, first >= 0);
    }

    /** Build valid control attribution for one writer epoch. */
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
