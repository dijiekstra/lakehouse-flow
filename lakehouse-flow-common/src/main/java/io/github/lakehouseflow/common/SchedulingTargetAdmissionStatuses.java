package io.github.lakehouseflow.common;

/**
 * Lifecycle states of one scheduler-owned target and business-date admission slot.
 *
 * The state controls scheduling-intent publication only. It never describes
 * downstream execution or resource occupancy.
 */
public final class SchedulingTargetAdmissionStatuses {

    /** The slot has no current scheduling-intent holder. */
    public static final String AVAILABLE = "AVAILABLE";

    /** One scheduling intent currently owns the publication lease. */
    public static final String ACTIVE = "ACTIVE";

    /** Prevent utility-class construction. */
    private SchedulingTargetAdmissionStatuses() {
    }
}
