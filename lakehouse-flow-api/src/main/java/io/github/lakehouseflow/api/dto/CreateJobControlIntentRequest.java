package io.github.lakehouseflow.api.dto;

import jakarta.validation.constraints.NotBlank;

/**
 * Idempotent platform request to start or restart one writer.
 *
 * @param requestKey unique caller operation key
 * @param requestedBy operator or system audit identity
 * @param reason optional operation reason
 */
public record CreateJobControlIntentRequest(
        @NotBlank String requestKey,
        @NotBlank String requestedBy,
        String reason) {
}
