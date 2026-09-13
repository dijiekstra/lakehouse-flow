package io.github.lakehouseflow.integration.source;

/**
 * Signals that a lakehouse metadata source cannot provide a reliable snapshot sequence.
 */
public class SnapshotSourceException extends RuntimeException {

    /**
     * Create a source failure with its owning cause.
     *
     * @param message diagnostic message
     * @param cause source adapter failure
     */
    public SnapshotSourceException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Create a source failure without a nested cause.
     *
     * @param message diagnostic message
     */
    public SnapshotSourceException(String message) {
        super(message);
    }
}
