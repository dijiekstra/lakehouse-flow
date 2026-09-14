package io.github.lakehouseflow.writer.paimon;

/**
 * Execution-plane SPI that guards a physical-table commit with the current writer epoch.
 *
 * <p>The permit must remain held until the Paimon commit is fully published. This closes the
 * race in which a restart allocates a new epoch between a stale writer's check and commit.
 */
@FunctionalInterface
public interface WriterEpochFence {

    /**
     * Acquire permission to commit for the supplied immutable writer context.
     *
     * @param context writer identity and intent correlation
     * @return permit held across the physical Paimon commit
     * @throws Exception when the fence store is unavailable or the writer is stale
     */
    CommitPermit acquireCommitPermit(WriterCommitContext context) throws Exception;

    /** Permit retaining the authoritative writer-generation lock. */
    @FunctionalInterface
    interface CommitPermit extends AutoCloseable {

        /** Release the writer-generation lock after commit success or failure. */
        @Override
        void close() throws Exception;
    }
}
