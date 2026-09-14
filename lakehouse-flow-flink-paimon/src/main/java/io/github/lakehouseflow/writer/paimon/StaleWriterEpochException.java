package io.github.lakehouseflow.writer.paimon;

/**
 * Signals that a writer or intent no longer owns the physical table generation it tried to commit.
 */
public class StaleWriterEpochException extends IllegalStateException {

    /** Create a fencing rejection with an operator-readable reason. */
    public StaleWriterEpochException(String message) {
        super(message);
    }
}
