package io.github.lakehouseflow.common;

import java.util.Locale;
import java.util.Set;

/**
 * Stable categories for scheduler, transport, and snapshot-evidence blockers.
 *
 * <p>These values classify why Lakehouse Flow cannot currently advance its own decision or
 * evidence. They are not downstream execution states.
 */
public final class OperationalBlockerTypes {

    public static final String INPUT_SNAPSHOT = "INPUT_SNAPSHOT";
    public static final String DAG_DEPENDENCY = "DAG_DEPENDENCY";
    public static final String DATE_CONCURRENCY = "DATE_CONCURRENCY";
    public static final String INTENT_PUBLICATION = "INTENT_PUBLICATION";
    public static final String TARGET_SNAPSHOT = "TARGET_SNAPSHOT";
    public static final String SOURCE_BLOCKED = "SOURCE_BLOCKED";
    public static final String DELIVERY_EXHAUSTED = "DELIVERY_EXHAUSTED";

    private static final Set<String> SUPPORTED = Set.of(
            INPUT_SNAPSHOT,
            DAG_DEPENDENCY,
            DATE_CONCURRENCY,
            INTENT_PUBLICATION,
            TARGET_SNAPSHOT,
            SOURCE_BLOCKED,
            DELIVERY_EXHAUSTED);

    /** Prevent construction of this constants-only class. */
    private OperationalBlockerTypes() {
    }

    /**
     * Normalize an optional operations filter and reject unknown categories.
     *
     * @param blockerType optional blocker category
     * @return normalized category, or null when no filter was supplied
     */
    public static String normalizeOptional(String blockerType) {
        if (blockerType == null || blockerType.isBlank()) {
            return null;
        }
        String normalized = blockerType.trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported operational blocker type: " + blockerType);
        }
        return normalized;
    }
}
