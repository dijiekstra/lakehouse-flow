package io.github.lakehouseflow.common;

import java.util.Locale;

/**
 * Supported strategies for replacing a snapshot-failed backfill batch.
 */
public final class BackfillRecoveryStrategies {

    public static final String FULL_SCOPE = "FULL_SCOPE";
    public static final String FAILED_NODE_CASCADE = "FAILED_NODE_CASCADE";

    /**
     * Prevent utility class instantiation.
     */
    private BackfillRecoveryStrategies() {
    }

    /**
     * Normalize a caller-supplied recovery strategy.
     *
     * A missing strategy preserves the historical full-scope recovery behavior.
     *
     * @param strategy caller-supplied recovery strategy
     * @return normalized supported strategy
     */
    public static String normalize(String strategy) {
        if (strategy == null || strategy.isBlank()) {
            return FULL_SCOPE;
        }
        String normalized = strategy.trim().toUpperCase(Locale.ROOT);
        return switch (normalized) {
            case FULL_SCOPE, FAILED_NODE_CASCADE -> normalized;
            default -> throw new IllegalArgumentException("Unsupported backfill recoveryStrategy: " + strategy);
        };
    }
}
