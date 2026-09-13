package io.github.lakehouseflow.common;

import java.util.Locale;

/**
 * Supported graph scopes for node-oriented backfill operations.
 */
public final class BackfillCascadePolicies {

    public static final String NO_CASCADE = "NO_CASCADE";
    public static final String DIRECT_DOWNSTREAM = "DIRECT_DOWNSTREAM";
    public static final String TRANSITIVE_DOWNSTREAM = "TRANSITIVE_DOWNSTREAM";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillCascadePolicies() {
    }

    /**
     * Normalize a caller-supplied policy and apply the safe no-cascade default.
     *
     * @param policy caller-supplied policy
     * @return normalized supported policy
     */
    public static String normalize(String policy) {
        if (policy == null || policy.isBlank()) {
            return NO_CASCADE;
        }
        String normalized = policy.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case NO_CASCADE, DIRECT_DOWNSTREAM, TRANSITIVE_DOWNSTREAM -> normalized;
            default -> throw new IllegalArgumentException("Unsupported cascadePolicy: " + policy);
        };
    }
}
