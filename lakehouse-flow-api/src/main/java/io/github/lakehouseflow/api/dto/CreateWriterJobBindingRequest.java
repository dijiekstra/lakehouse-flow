package io.github.lakehouseflow.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;

/**
 * Request for registering the sole writer of one physical table.
 *
 * @param writerJobKey stable platform writer routing key
 * @param tableAssetKey catalog.database.table asset key
 * @param allowedProcessingModes supported STREAMING and/or BATCH modes
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CreateWriterJobBindingRequest(
        @NotBlank String writerJobKey,
        @NotBlank String tableAssetKey,
        @NotEmpty List<String> allowedProcessingModes) {
}
