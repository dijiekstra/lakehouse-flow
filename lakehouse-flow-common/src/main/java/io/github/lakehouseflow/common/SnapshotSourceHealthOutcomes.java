package io.github.lakehouseflow.common;

import java.util.Set;

/**
 * Persisted source-reconciliation outcomes used by snapshot confirmation.
 */
public final class SnapshotSourceHealthOutcomes {

    /** Source range, durable offset, event ledger, and projection are consistent. */
    public static final String HEALTHY = "HEALTHY";

    /** A bounded scan or projection replay may restore complete evidence. */
    public static final String REPAIRABLE = "REPAIRABLE";

    /** Evidence is incomplete and cannot be repaired without risking a gap. */
    public static final String BLOCKED = "BLOCKED";

    /** External source result used when timeout confirmation is not trustworthy. */
    public static final String SOURCE_BLOCKED = "SOURCE_BLOCKED";

    private static final Set<String> PERSISTED = Set.of(HEALTHY, REPAIRABLE, BLOCKED);

    /** Prevent construction of this constants-only class. */
    private SnapshotSourceHealthOutcomes() {
    }

    /**
     * Validate one persisted source-reconciliation outcome.
     *
     * @param outcome candidate outcome
     * @return validated outcome
     */
    public static String requirePersisted(String outcome) {
        if (outcome == null || !PERSISTED.contains(outcome)) {
            throw new IllegalArgumentException("Unsupported snapshot source health outcome: " + outcome);
        }
        return outcome;
    }
}
