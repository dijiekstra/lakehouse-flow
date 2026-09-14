package io.github.lakehouseflow.writer.paimon;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests fail-closed validation of writer and snapshot attribution coordinates. */
class WriterCommitContextTest {

    /** Verify a job-control commit accepts only its own writer generation marker. */
    @Test
    void acceptsValidJobControlProperties() {
        WriterCommitContext context = new WriterCommitContext(
                "paimon.ods.orders",
                "orders-writer",
                2L,
                "job-control:orders-writer:2",
                WriterCommitContext.IntentKind.JOB_CONTROL,
                "STREAMING",
                controlProperties(2L));

        assertTrue(context.jobControlIntent());
        assertEquals("paimon.ods.orders", context.tableAssetKey());
    }

    /** Verify a data-processing commit accepts a partition target owned by its table writer. */
    @Test
    void acceptsValidDataProcessingProperties() {
        Map<String, String> properties = new LinkedHashMap<>(commonProperties(4L));
        properties.put(SnapshotEvidenceContract.INTENT_KEY_PROPERTY, "task-instance:44");
        properties.put(SnapshotEvidenceContract.TARGET_ASSET_PROPERTY,
                "paimon.dwd.order_detail.dt=2026-09-14");
        properties.put(SnapshotEvidenceContract.BIZ_DATE_PROPERTY, "2026-09-14");
        properties.put(SnapshotEvidenceContract.FINAL_PROPERTY, SnapshotEvidenceContract.FINAL_VALUE);

        WriterCommitContext context = new WriterCommitContext(
                "paimon.dwd.order_detail",
                "orders-writer",
                4L,
                "task-instance:44",
                WriterCommitContext.IntentKind.DATA_PROCESSING,
                "BATCH",
                properties);

        assertEquals("BATCH", context.processingMode());
    }

    /** Verify a stale property epoch is rejected before any Paimon resource is opened. */
    @Test
    void rejectsPropertyEpochMismatch() {
        assertThrows(IllegalArgumentException.class, () -> new WriterCommitContext(
                "paimon.ods.orders",
                "orders-writer",
                3L,
                "job-control:orders-writer:3",
                WriterCommitContext.IntentKind.JOB_CONTROL,
                "STREAMING",
                controlProperties(2L)));
    }

    /** Verify data-processing properties cannot target a table owned by another writer. */
    @Test
    void rejectsForeignTargetTable() {
        Map<String, String> properties = new LinkedHashMap<>(commonProperties(4L));
        properties.put(SnapshotEvidenceContract.INTENT_KEY_PROPERTY, "task-instance:44");
        properties.put(SnapshotEvidenceContract.TARGET_ASSET_PROPERTY, "paimon.ads.gmv.dt=2026-09-14");
        properties.put(SnapshotEvidenceContract.FINAL_PROPERTY, SnapshotEvidenceContract.FINAL_VALUE);

        assertThrows(IllegalArgumentException.class, () -> new WriterCommitContext(
                "paimon.dwd.order_detail",
                "orders-writer",
                4L,
                "task-instance:44",
                WriterCommitContext.IntentKind.DATA_PROCESSING,
                "BATCH",
                properties));
    }

    /** Build valid control snapshot properties for one epoch. */
    private Map<String, String> controlProperties(long epoch) {
        Map<String, String> properties = new LinkedHashMap<>(commonProperties(epoch));
        properties.put(
                SnapshotEvidenceContract.JOB_CONTROL_INTENT_KEY_PROPERTY,
                "job-control:orders-writer:" + epoch);
        return properties;
    }

    /** Build snapshot properties shared by both immutable intent kinds. */
    private Map<String, String> commonProperties(long epoch) {
        return Map.of(
                SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE,
                SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, "orders-writer",
                SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, Long.toString(epoch));
    }
}
