package io.github.lakehouseflow.api.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

import java.time.LocalDate;

/**
 * Request DTO group for scheduler-side action APIs.
 */
public final class ActionRequests {

    /**
     * Prevent utility DTO holder instantiation.
     */
    private ActionRequests() {
    }

    /**
     * Request for rerunning a workflow scheduling instance.
     *
     * @param workflowInstanceId workflow instance id to rerun
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RerunWorkflowRequest(
            @NotNull Long workflowInstanceId,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for rerunning a node from a published FlowPlan version.
     *
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param nodeCode node code inside the published version
     * @param bizDate business date for the rerun intent
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RerunNodeRequest(
            @NotNull Long flowPlanVersionId,
            @NotBlank String nodeCode,
            @NotNull LocalDate bizDate,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for creating backfill workflow scheduling decisions.
     *
     * @param workflowCode workflow definition code
     * @param workflowVersion workflow definition version
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param progressionMode PARALLEL, SERIAL, or PARALLEL_WITH_LIMIT
     * @param maxActiveDates positive date limit required by PARALLEL_WITH_LIMIT
     * @param skipPolicy NONE or SKIP_FULLY_CONFIRMED_DATES
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BackfillWorkflowRequest(
            @NotBlank String workflowCode,
            Integer workflowVersion,
            @NotNull LocalDate startBizDate,
            @NotNull LocalDate endBizDate,
            String progressionMode,
            Integer maxActiveDates,
            String skipPolicy,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for creating node-scoped backfill task intents from a FlowPlan version.
     *
     * @param flowPlanVersionId published FlowPlanVersion id
     * @param startNodeCode node where the backfill starts
     * @param startBizDate inclusive start business date
     * @param endBizDate inclusive end business date
     * @param cascadePolicy NO_CASCADE, DIRECT_DOWNSTREAM, or TRANSITIVE_DOWNSTREAM
     * @param progressionMode PARALLEL, SERIAL, or PARALLEL_WITH_LIMIT
     * @param maxActiveDates positive date limit required by PARALLEL_WITH_LIMIT
     * @param skipPolicy NONE or SKIP_FULLY_CONFIRMED_DATES
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BackfillNodeRequest(
            @NotNull Long flowPlanVersionId,
            @NotBlank String startNodeCode,
            @NotNull LocalDate startBizDate,
            @NotNull LocalDate endBizDate,
            String cascadePolicy,
            String progressionMode,
            Integer maxActiveDates,
            String skipPolicy,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for replacing a snapshot-failed backfill batch.
     *
     * @param backfillBatchId failed source batch id
     * @param recoveryStrategy full-scope or failed-node-cascade strategy
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record RecoverBackfillRequest(
            @NotNull Long backfillBatchId,
            String recoveryStrategy,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for controlling delivery of a backfill batch.
     *
     * @param backfillBatchId target backfill batch id
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record BackfillBatchActionRequest(
            @NotNull Long backfillBatchId,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for a workflow-targeted action.
     *
     * @param workflowInstanceId workflow instance id
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record WorkflowInstanceActionRequest(
            @NotNull Long workflowInstanceId,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }

    /**
     * Request for a task-targeted action.
     *
     * @param taskInstanceId task instance id
     * @param actionKey caller-provided idempotency key
     * @param requestedBy user or system identity requesting the action
     * @param reason human-readable reason
     */
    @JsonIgnoreProperties(ignoreUnknown = true)
    public record TaskInstanceActionRequest(
            @NotNull Long taskInstanceId,
            @NotBlank String actionKey,
            String requestedBy,
            String reason) {
    }
}
