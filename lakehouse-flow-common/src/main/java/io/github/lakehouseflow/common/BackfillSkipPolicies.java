package io.github.lakehouseflow.common;

import java.util.Locale;

/**
 * Policies for safely omitting business dates from a backfill expansion.
 *
 * A skip policy never treats the mere existence of an AssetState as successful
 * historical processing. Only durable task snapshot-confirmation evidence may
 * justify skipping, and the scheduler skips a complete selected date subgraph
 * rather than reusing old parent evidence inside a new workflow instance.
 */
public final class BackfillSkipPolicies {

    public static final String NONE = "NONE";
    public static final String SKIP_FULLY_CONFIRMED_DATES = "SKIP_FULLY_CONFIRMED_DATES";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillSkipPolicies() {
    }

    /**
     * Normalize an optional skip policy while preserving force-backfill behavior by default.
     *
     * @param policy requested policy
     * @return supported normalized policy
     */
    public static String normalize(String policy) {
        String normalized = policy == null || policy.isBlank()
                ? NONE
                : policy.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case NONE, SKIP_FULLY_CONFIRMED_DATES -> normalized;
            default -> throw new IllegalArgumentException("Unsupported backfill skipPolicy: " + normalized);
        };
    }
}
