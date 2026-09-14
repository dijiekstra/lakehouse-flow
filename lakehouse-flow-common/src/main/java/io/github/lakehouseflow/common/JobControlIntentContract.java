package io.github.lakehouseflow.common;

/**
 * Version and result constants for independent writer lifecycle intents.
 */
public final class JobControlIntentContract {

    /** First stable job-control payload version. */
    public static final String CONTRACT_VERSION = "1.0";

    /** Stable discriminator for an independent writer lifecycle instruction. */
    public static final String INTENT_KIND = "JOB_CONTROL";

    /** A control intent is still waiting for attributable target data. */
    public static final String WAITING = "WAITING";

    /** The selected writer epoch produced an attributable business snapshot. */
    public static final String SNAPSHOT_CONFIRMED = "SNAPSHOT_CONFIRMED";

    /** A healthy source observed no attributable data within the confirmation window. */
    public static final String SNAPSHOT_NOT_ADVANCED = "SNAPSHOT_NOT_ADVANCED";

    /** Prevent construction of this constants-only class. */
    private JobControlIntentContract() {
    }
}
