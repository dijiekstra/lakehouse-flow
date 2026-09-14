package io.github.lakehouseflow.common;

/**
 * Stable version and discriminator constants for data-processing scheduling intents.
 *
 * <p>This contract is independent from lake-format snapshot evidence. Compatible 1.x
 * revisions may add fields, while a different major version requires explicit support.
 */
public final class SchedulingIntentContract {

    /** LF-1.0 data-processing payload version. */
    public static final String CONTRACT_VERSION = "1.3";

    /** Stable discriminator for a data-processing scheduling instruction. */
    public static final String INTENT_KIND = "DATA_PROCESSING";

    /** Prevent construction of this constants-only class. */
    private SchedulingIntentContract() {
    }
}
