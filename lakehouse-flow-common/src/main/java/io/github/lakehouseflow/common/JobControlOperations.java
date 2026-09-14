package io.github.lakehouseflow.common;

import java.util.Locale;
import java.util.Set;

/**
 * Platform operations supported by the writer job control contract.
 */
public final class JobControlOperations {

    /** Start a writer that has never received a writer epoch. */
    public static final String START_JOB = "START_JOB";

    /** Fence the previous writer generation and activate a new one. */
    public static final String RESTART_JOB = "RESTART_JOB";

    private static final Set<String> SUPPORTED = Set.of(START_JOB, RESTART_JOB);

    /** Prevent construction of this constants-only class. */
    private JobControlOperations() {
    }

    /**
     * Normalize and validate a platform writer operation.
     *
     * @param operationType requested operation
     * @return normalized supported operation
     */
    public static String normalize(String operationType) {
        if (operationType == null || operationType.isBlank()) {
            throw new IllegalArgumentException("job control operation type is required");
        }
        String normalized = operationType.trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported job control operation: " + operationType);
        }
        return normalized;
    }
}
