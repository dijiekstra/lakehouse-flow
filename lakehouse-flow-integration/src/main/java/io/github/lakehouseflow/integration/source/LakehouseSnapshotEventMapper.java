package io.github.lakehouseflow.integration.source;

import io.github.lakehouseflow.model.LakehouseEvent;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Maps format-neutral snapshot observations into durable scheduler events.
 */
@Component
public class LakehouseSnapshotEventMapper {

    /**
     * Map one source snapshot without interpreting task or workflow execution status.
     *
     * @param identity source table identity
     * @param snapshot immutable snapshot observation
     * @return event ready for transactional projection
     */
    public LakehouseEvent map(LakehouseSourceIdentity identity, LakehouseSnapshot snapshot) {
        String eventId = "%s:%s:%s:%s:%s".formatted(
                identity.sourceType(),
                identity.catalogName(),
                identity.databaseName(),
                identity.tableName(),
                snapshot.snapshotId());
        Map<String, Object> payload = new LinkedHashMap<>(snapshot.payload());
        payload.put("dataChange", snapshot.dataChange());
        payload.put("snapshotProperties", snapshot.snapshotProperties());
        payload.put("changedPartitions", snapshot.changedPartitions());
        return LakehouseEvent.builder()
                .eventId(eventId)
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType(identity.sourceType())
                .catalogName(identity.catalogName())
                .databaseName(identity.databaseName())
                .tableName(identity.tableName())
                .snapshotId(snapshot.snapshotId())
                .schemaId(snapshot.schemaId())
                .watermark(snapshot.watermark())
                .commitKind(snapshot.commitKind())
                .dataChange(snapshot.dataChange())
                .commitTime(snapshot.commitTime())
                .payloadJson(payload)
                .build();
    }
}
