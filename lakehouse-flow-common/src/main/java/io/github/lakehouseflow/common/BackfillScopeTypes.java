package io.github.lakehouseflow.common;

import java.util.Locale;

/**
 * Supported immutable node scopes for scheduler-side backfill batches.
 */
public final class BackfillScopeTypes {

    public static final String FULL_FLOW = "FULL_FLOW";
    public static final String NODE_SUBGRAPH = "NODE_SUBGRAPH";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillScopeTypes() {
    }

    /**
     * Normalize a persisted or requested backfill scope.
     *
     * A missing scope is treated as NODE_SUBGRAPH for compatibility with
     * batches created before scope unification.
     *
     * @param scopeType requested or persisted scope type
     * @return normalized supported scope type
     */
    public static String normalize(String scopeType) {
        String normalized = scopeType == null || scopeType.isBlank()
                ? NODE_SUBGRAPH
                : scopeType.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case FULL_FLOW, NODE_SUBGRAPH -> normalized;
            default -> throw new IllegalArgumentException("Unsupported backfill scopeType: " + normalized);
        };
    }
}
