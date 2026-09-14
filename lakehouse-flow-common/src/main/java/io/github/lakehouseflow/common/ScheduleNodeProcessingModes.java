package io.github.lakehouseflow.common;

import java.util.Locale;
import java.util.Set;

/**
 * Engine-neutral processing modes understood by the scheduling model.
 */
public final class ScheduleNodeProcessingModes {

    /** A continuously running writer consumes upstream changes by itself. */
    public static final String STREAMING = "STREAMING";

    /** One bounded scheduling intent authorizes one finite data-processing range. */
    public static final String BATCH = "BATCH";

    private static final Set<String> SUPPORTED = Set.of(STREAMING, BATCH);

    /** Prevent construction of this constants-only class. */
    private ScheduleNodeProcessingModes() {
    }

    /**
     * Normalize a processing mode while keeping legacy definitions batch-oriented.
     *
     * @param value configured processing mode, or null for the compatibility default
     * @return normalized supported processing mode
     */
    public static String normalize(String value) {
        String normalized = value == null || value.isBlank()
                ? BATCH
                : value.trim().toUpperCase(Locale.ROOT);
        if (!SUPPORTED.contains(normalized)) {
            throw new IllegalArgumentException("Unsupported processing mode: " + value);
        }
        return normalized;
    }

    /**
     * Check whether a node is continuously running.
     *
     * @param value configured processing mode
     * @return true when the normalized mode is streaming
     */
    public static boolean isStreaming(String value) {
        return STREAMING.equals(normalize(value));
    }
}
