package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.model.LakehouseEvent;

import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Resolves the format-neutral business-data classification of durable snapshot events.
 *
 * <p>New events use the typed {@code dataChange} column. Payload and Paimon commit-kind fallbacks
 * remain only for events persisted before the typed contract was introduced.
 */
final class SnapshotEventChangeClassifier {

    private static final Set<String> LEGACY_DATA_COMMIT_KINDS = Set.of("APPEND", "OVERWRITE");

    /** Prevent construction of this stateless classifier. */
    private SnapshotEventChangeClassifier() {
    }

    /**
     * Determine whether one snapshot represents a business-data change.
     *
     * @param event durable snapshot event
     * @return true only for adapter-classified data snapshots or compatible legacy evidence
     */
    static boolean isDataChange(LakehouseEvent event) {
        if (event == null) {
            return false;
        }
        if (event.getDataChange() != null) {
            return event.getDataChange();
        }
        Map<String, Object> payload = event.getPayloadJson();
        if (payload != null && payload.containsKey(SnapshotEvidenceContract.DATA_CHANGE_FIELD)) {
            return Boolean.TRUE.equals(payload.get(SnapshotEvidenceContract.DATA_CHANGE_FIELD));
        }
        return event.getCommitKind() != null
                && LEGACY_DATA_COMMIT_KINDS.contains(event.getCommitKind().trim().toUpperCase(Locale.ROOT));
    }
}
