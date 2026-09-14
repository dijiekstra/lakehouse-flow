package io.github.lakehouseflow.model;

import java.time.LocalDateTime;

/**
 * Shared mutable transport fields used by both outbound intent delivery tables.
 *
 * This interface contains infrastructure evidence only and deliberately exposes
 * no workflow, task, engine, or snapshot-result state.
 */
public interface ReliableIntentDelivery {

    /** Return the transport-only delivery state. */
    String getStatus();

    /** Change the transport-only delivery state. */
    void setStatus(String status);

    /** Return the number of publication attempts. */
    Integer getAttemptCount();

    /** Change the number of publication attempts. */
    void setAttemptCount(Integer attemptCount);

    /** Return the latest transport error. */
    String getLastError();

    /** Change the latest transport error. */
    void setLastError(String lastError);

    /** Change the latest publication attempt timestamp. */
    void setLastAttemptAt(LocalDateTime lastAttemptAt);

    /** Change the earliest retry timestamp. */
    void setNextAttemptAt(LocalDateTime nextAttemptAt);

    /** Change the current claim owner. */
    void setClaimOwner(String claimOwner);

    /** Return the current fencing token. */
    String getClaimToken();

    /** Change the current fencing token. */
    void setClaimToken(String claimToken);

    /** Change the claim lease deadline. */
    void setClaimExpiresAt(LocalDateTime claimExpiresAt);

    /** Return the external publication admission deadline. */
    LocalDateTime getDeliverBefore();

    /** Change the successful transport acknowledgement timestamp. */
    void setPublishedAt(LocalDateTime publishedAt);

    /** Change the terminal transport dead-letter timestamp. */
    void setDeadLetteredAt(LocalDateTime deadLetteredAt);
}
