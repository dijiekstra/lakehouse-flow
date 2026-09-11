package io.github.lakehouseflow.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.time.LocalDateTime;

/**
 * Event Consumer Offset - Tracks the last processed offset for each event source.
 *
 * Used by event ingestion loops to resume from where they left off.
 * For example:
 * - source_type: "PAIMON", source_name: "paimon_catalog.ods.orders"
 * - offset_value: "1001" (last snapshot ID processed)
 * - updated_at: when this offset was last updated
 *
 * Design:
 * - One row per (source_type, source_name) pair
 * - offset_value is flexible (can be snapshot ID, timestamp, message offset, etc.)
 * - Updated atomically within same transaction as event ingestion
 */
@Entity
@Table(name = "event_consumer_offset")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class EventConsumerOffset {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /**
     * Type of event source: PAIMON, ICEBERG, HUDI, KAFKA, etc.
     */
    @Column(name = "source_type", nullable = false, length = 50)
    private String sourceType;

    /**
     * Name of the event source instance.
     * For Paimon: "catalog_name.database_name.table_name"
     * For Kafka: "topic_name"
     */
    @Column(name = "source_name", nullable = false, length = 255)
    private String sourceName;

    /**
     * Current offset value.
     * For Paimon: snapshot ID (e.g., "1001")
     * For Kafka: topic/partition/offset (e.g., "0/0/12345")
     * Semantics: events with offset <= this value have been processed
     */
    @Column(name = "offset_value", nullable = false, length = 255)
    private String offsetValue;

    /**
     * Timestamp when this offset was last updated.
     */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /**
     * Timestamp when this offset was created.
     */
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    /**
     * JPA lifecycle callback - set timestamps on persist and update.
     */
    @PrePersist
    public void prePersist() {
        LocalDateTime now = LocalDateTime.now();
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    @PreUpdate
    public void preUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
