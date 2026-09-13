package io.github.lakehouseflow.common;

import java.math.BigInteger;

/**
 * Utilities for comparing adapter-normalized lakehouse snapshot coordinates.
 *
 * <p>Adapters must expose a monotonic decimal or lexicographically sortable coordinate through the
 * common event model. A format-native identifier which does not provide that ordering belongs in
 * the raw payload. This comparator uses numeric ordering when both sides are decimal and stable
 * lexicographic ordering otherwise.
 */
public final class SnapshotIds {

    /**
     * Prevent utility class instantiation.
     */
    private SnapshotIds() {
    }

    /**
     * Compare two snapshot identifiers with numeric ordering when possible.
     *
     * @param left left snapshot identifier
     * @param right right snapshot identifier
     * @return negative, zero, or positive according to snapshot ordering
     */
    public static int compare(String left, String right) {
        if (left == null && right == null) {
            return 0;
        }
        if (left == null) {
            return -1;
        }
        if (right == null) {
            return 1;
        }

        String normalizedLeft = left.trim();
        String normalizedRight = right.trim();

        BigInteger leftNumber = parseNonNegativeInteger(normalizedLeft);
        BigInteger rightNumber = parseNonNegativeInteger(normalizedRight);
        if (leftNumber != null && rightNumber != null) {
            return leftNumber.compareTo(rightNumber);
        }

        return normalizedLeft.compareTo(normalizedRight);
    }

    /**
     * Check whether a candidate snapshot is strictly after the current snapshot.
     *
     * @param candidate candidate snapshot identifier
     * @param current current baseline snapshot identifier
     * @return true when candidate represents forward progress
     */
    public static boolean isAfter(String candidate, String current) {
        return compare(candidate, current) > 0;
    }

    /**
     * Check whether a candidate snapshot reaches a required minimum snapshot.
     *
     * @param candidate candidate snapshot identifier
     * @param required required minimum snapshot identifier
     * @return true when candidate is greater than or equal to required
     */
    public static boolean isGreaterThanOrEqual(String candidate, String required) {
        return compare(candidate, required) >= 0;
    }

    /**
     * Parse a non-negative integer snapshot id for numeric comparison.
     *
     * @param value normalized snapshot identifier
     * @return parsed integer or null when value is not purely numeric
     */
    private static BigInteger parseNonNegativeInteger(String value) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        for (int i = 0; i < value.length(); i++) {
            if (!Character.isDigit(value.charAt(i))) {
                return null;
            }
        }
        return new BigInteger(value);
    }
}
