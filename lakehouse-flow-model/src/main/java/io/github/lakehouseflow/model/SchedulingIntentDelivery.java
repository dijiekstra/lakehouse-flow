package io.github.lakehouseflow.model;

import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Index;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import jakarta.persistence.Version;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

/**
 * Infrastructure evidence for publishing one scheduling intent through a channel.
 *
 * Delivery status proves only that an instruction was made available through a
 * database table, HTTP endpoint, or message broker. It is never interpreted as
 * downstream execution success or failure.
 */
@Entity
@Table(
    name = "scheduling_intent_delivery",
    uniqueConstraints = {
        @UniqueConstraint(
                name = "uk_scheduling_intent_delivery_intent",
                columnNames = "scheduling_intent_id")
    },
    indexes = {
        @Index(
                name = "idx_intent_delivery_claimable",
                columnList = "status,next_attempt_at ASC,claim_expires_at ASC,created_at ASC"),
        @Index(name = "idx_intent_delivery_intent", columnList = "scheduling_intent_id")
    }
)
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SchedulingIntentDelivery implements ReliableIntentDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Optimistic concurrency version for internal transport updates. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Immutable scheduling intent being transported through its selected route. */
    @Column(name = "scheduling_intent_id", nullable = false, unique = true, updatable = false)
    private Long schedulingIntentId;

    /** Transport channel such as DATABASE_TABLE, HTTP, or MQ. */
    @Column(name = "channel", nullable = false, updatable = false, length = 64)
    private String channel;

    /** Channel-specific table, endpoint, or topic identifier. */
    @Column(name = "destination", nullable = false, updatable = false, length = 512)
    private String destination;

    /** Transport-only state that never represents downstream execution. */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /** Number of infrastructure publication attempts. */
    @Column(name = "attempt_count", nullable = false)
    private Integer attemptCount;

    /** Last transport error, without any downstream task result. */
    @Column(name = "last_error", columnDefinition = "TEXT")
    private String lastError;

    /** Last time an internal publisher attempted this route. */
    @Column(name = "last_attempt_at")
    private LocalDateTime lastAttemptAt;

    /** Earliest time at which an infrastructure retry may be claimed. */
    @Column(name = "next_attempt_at")
    private LocalDateTime nextAttemptAt;

    /** Scheduler process that currently owns the publication attempt. */
    @Column(name = "claim_owner", length = 255)
    private String claimOwner;

    /** Fencing token that prevents a stale publisher from completing a newer claim. */
    @Column(name = "claim_token", length = 64)
    private String claimToken;

    /** Time at which another scheduler process may reclaim an abandoned attempt. */
    @Column(name = "claim_expires_at")
    private LocalDateTime claimExpiresAt;

    /**
     * Latest time at which the selected transport may publish this intent.
     *
     * This mirrors the target admission deadline sent to downstream systems so
     * an infrastructure retry cannot start stale work after the slot expires.
     */
    @Column(name = "deliver_before")
    private LocalDateTime deliverBefore;

    /** Time at which the transport made the instruction available. */
    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    /** Time at which this transport entered its terminal dead-letter state. */
    @Column(name = "dead_lettered_at")
    private LocalDateTime deadLetteredAt;

    /** Record creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Record update timestamp. */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /** Initialize delivery defaults and audit timestamps before insert. */
    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        if (status == null) {
            status = SchedulingIntentDeliveryStatuses.PENDING;
        }
        if (attemptCount == null) {
            attemptCount = 0;
        }
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    /** Refresh the update timestamp before changing transport evidence. */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
