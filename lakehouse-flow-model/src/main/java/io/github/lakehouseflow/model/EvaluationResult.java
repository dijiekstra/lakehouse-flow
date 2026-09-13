package io.github.lakehouseflow.model;

import lombok.*;

/**
 * Evaluation Result - Output of dependency condition evaluation.
 *
 * Returned by ConditionEvaluator after evaluating a single condition
 * or a set of AND/OR composed conditions.
 */
@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class EvaluationResult {

    /**
     * Whether the condition(s) are satisfied.
     */
    private Boolean satisfied;

    /**
     * If not satisfied, reason why the workflow/task is waiting.
     * E.g., "Waiting for snapshot >= 1000 (current: 950)"
     * E.g., "Waiting for watermark >= 2026-09-11T23:59 (current: 2026-09-11T20:00)"
     */
    private String waitingReason;

    /**
     * Human-readable description of the evaluation.
     * E.g., "Snapshot 1000 found, watermark 2026-09-12T00:30 >= required 2026-09-11T23:59: PASS"
     * Used for auditing and logging.
     */
    private String description;

    /**
     * Asset key being evaluated.
     */
    private String assetKey;

    /**
     * Current snapshot ID (if applicable).
     */
    private String snapshotId;

    /**
     * Current watermark (if applicable).
     */
    private String watermark;

    /**
     * Event ID that triggered this evaluation (if applicable).
     */
    private String eventId;

    /**
     * Evaluation timestamp.
     */
    private Long evaluatedAt;

    /**
     * Creates a satisfied result.
     */
    public static EvaluationResult satisfied() {
        return EvaluationResult.builder()
                .satisfied(true)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Creates a satisfied result with description.
     */
    public static EvaluationResult satisfied(String description) {
        return EvaluationResult.builder()
                .satisfied(true)
                .description(description)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Creates an unsatisfied result with waiting reason.
     */
    public static EvaluationResult unsatisfied(String waitingReason) {
        return EvaluationResult.builder()
                .satisfied(false)
                .waitingReason(waitingReason)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Creates an unsatisfied result with both waiting reason and description.
     */
    public static EvaluationResult unsatisfied(String waitingReason, String description) {
        return EvaluationResult.builder()
                .satisfied(false)
                .waitingReason(waitingReason)
                .description(description)
                .evaluatedAt(System.currentTimeMillis())
                .build();
    }

    /**
     * Render a compact condition-evaluation summary for logs and debugging.
     */
    @Override
    public String toString() {
        return "EvaluationResult{" +
                "satisfied=" + satisfied +
                ", assetKey='" + assetKey + '\'' +
                ", snapshotId='" + snapshotId + '\'' +
                ", waitingReason='" + waitingReason + '\'' +
                ", description='" + description + '\'' +
                '}';
    }
}
