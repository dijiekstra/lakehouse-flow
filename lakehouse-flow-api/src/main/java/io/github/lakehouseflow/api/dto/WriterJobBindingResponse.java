package io.github.lakehouseflow.api.dto;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Read-only physical-table writer ownership response.
 *
 * @param id binding id
 * @param writerJobKey stable platform writer key
 * @param tableAssetKey normalized physical table
 * @param allowedProcessingModes supported engine-neutral modes
 * @param currentWriterEpoch latest allocated fencing generation
 * @param activeProcessingMode mode authorized for the current generation
 * @param holderIntentKey intent owning current writer admission
 * @param holderExpiresAt bounded holder expiry, or null for a stream
 * @param currentControlIntentKey current platform control intent, if any
 * @param createdAt binding creation timestamp
 * @param updatedAt binding update timestamp
 */
public record WriterJobBindingResponse(
        Long id,
        String writerJobKey,
        String tableAssetKey,
        List<String> allowedProcessingModes,
        Long currentWriterEpoch,
        String activeProcessingMode,
        String holderIntentKey,
        LocalDateTime holderExpiresAt,
        String currentControlIntentKey,
        LocalDateTime createdAt,
        LocalDateTime updatedAt) {
}
