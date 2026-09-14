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
 * Transport-only evidence for an independent job-control intent.
 *
 * A published row proves receipt by the configured channel only. It never
 * represents whether an engine process started, restarted, or remained healthy.
 */
@Entity
@Table(
        name = "job_control_intent_delivery",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_job_control_delivery_intent", columnNames = "job_control_intent_id")
        },
        indexes = {
            @Index(
                    name = "idx_job_control_delivery_claim",
                    columnList = "status,next_attempt_at ASC,claim_expires_at ASC,created_at ASC")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class JobControlIntentDelivery implements ReliableIntentDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Optimistic concurrency version for transport updates. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Immutable job-control intent transported by this record. */
    @Column(name = "job_control_intent_id", nullable = false, unique = true, updatable = false)
    private Long jobControlIntentId;

    /** DATABASE_TABLE, HTTP, or optional MQ channel. */
    @Column(name = "channel", nullable = false, updatable = false, length = 64)
    private String channel;

    /** Channel-specific table, endpoint, or topic. */
    @Column(name = "destination", nullable = false, updatable = false, length = 512)
    private String destination;

    /** Transport state independent from writer snapshot results. */
    @Column(name = "status", nullable = false, length = 32)
    private String status;

    /** Number of infrastructure publication attempts. */
    @Column(name = "attempt_count", nullable = false)
    private Integer attemptCount;

    /** Latest transport error, if any. */
    @Column(name = "last_error", columnDefinition = "text")
    private String lastError;

    /** Last publisher attempt timestamp. */
    @Column(name = "last_attempt_at")
    private LocalDateTime lastAttemptAt;

    /** Earliest infrastructure retry time. */
    @Column(name = "next_attempt_at")
    private LocalDateTime nextAttemptAt;

    /** Scheduler process owning the current publication attempt. */
    @Column(name = "claim_owner", length = 255)
    private String claimOwner;

    /** Fencing token for the current claim. */
    @Column(name = "claim_token", length = 64)
    private String claimToken;

    /** Time at which another scheduler may reclaim this publication. */
    @Column(name = "claim_expires_at")
    private LocalDateTime claimExpiresAt;

    /** Latest time the platform may act on this control instruction. */
    @Column(name = "deliver_before", nullable = false, updatable = false)
    private LocalDateTime deliverBefore;

    /** Time the configured transport acknowledged receipt. */
    @Column(name = "published_at")
    private LocalDateTime publishedAt;

    /** Time the route exhausted its transport attempts or admission window. */
    @Column(name = "dead_lettered_at")
    private LocalDateTime deadLetteredAt;

    /** Record creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Record update timestamp. */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /** Initialize transport defaults and audit timestamps before insert. */
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

    /** Refresh the update timestamp before a transport evidence change. */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
