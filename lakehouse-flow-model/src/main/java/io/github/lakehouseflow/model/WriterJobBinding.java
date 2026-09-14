package io.github.lakehouseflow.model;

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
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Global ownership record for the only writer job allowed to mutate one physical table.
 *
 * The record stores scheduler fencing coordinates, not engine runtime status. Actual
 * process lifecycle and old-writer termination remain responsibilities of the platform
 * execution plane.
 */
@Entity
@Table(
        name = "writer_job_binding",
        uniqueConstraints = {
            @UniqueConstraint(name = "uk_writer_job_binding_key", columnNames = "writer_job_key"),
            @UniqueConstraint(name = "uk_writer_job_binding_table", columnNames = "table_asset_key")
        },
        indexes = {
            @Index(name = "idx_writer_binding_table", columnList = "table_asset_key")
        })
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class WriterJobBinding {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** Optimistic version complementing pessimistic epoch allocation locks. */
    @Version
    @Column(name = "version", nullable = false)
    private Long version;

    /** Stable platform routing key for the sole writer of the table. */
    @Column(name = "writer_job_key", nullable = false, unique = true, updatable = false, length = 255)
    private String writerJobKey;

    /** Normalized catalog.database.table key owned by this writer. */
    @Column(name = "table_asset_key", nullable = false, unique = true, updatable = false, length = 255)
    private String tableAssetKey;

    /** Engine-neutral processing modes this writer definition accepts. */
    @Column(name = "allowed_processing_modes_json", nullable = false, columnDefinition = "jsonb")
    @JdbcTypeCode(SqlTypes.JSON)
    private List<String> allowedProcessingModes;

    /** Latest monotonically allocated fencing generation. */
    @Builder.Default
    @Column(name = "current_writer_epoch", nullable = false)
    private Long currentWriterEpoch = 0L;

    /** Mode authorized for the current generation, without implying process state. */
    @Column(name = "active_processing_mode", length = 32)
    private String activeProcessingMode;

    /** Intent currently owning bounded writer admission or the active stream generation. */
    @Column(name = "holder_intent_key", length = 255)
    private String holderIntentKey;

    /** Expiry of a bounded data-intent holder; null for a current stream generation. */
    @Column(name = "holder_expires_at")
    private LocalDateTime holderExpiresAt;

    /** Current control intent that allocated the active streaming generation. */
    @Column(name = "current_control_intent_key", length = 255)
    private String currentControlIntentKey;

    /** Record creation timestamp. */
    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    /** Record update timestamp. */
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    /** Initialize immutable defaults and audit timestamps before insert. */
    @PrePersist
    protected void onCreate() {
        LocalDateTime now = LocalDateTime.now();
        if (allowedProcessingModes == null) {
            allowedProcessingModes = List.of();
        }
        if (currentWriterEpoch == null) {
            currentWriterEpoch = 0L;
        }
        if (createdAt == null) {
            createdAt = now;
        }
        if (updatedAt == null) {
            updatedAt = now;
        }
    }

    /** Refresh the update timestamp after a writer generation change. */
    @PreUpdate
    protected void onUpdate() {
        updatedAt = LocalDateTime.now();
    }
}
