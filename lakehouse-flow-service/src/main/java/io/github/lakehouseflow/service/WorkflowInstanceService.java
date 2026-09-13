package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.WorkflowInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing workflow scheduling instances.
 *
 * State transitions:
 * CREATED → WAITING_SNAPSHOT → READY_TO_SCHEDULE → SCHEDULED → SNAPSHOT_CONFIRMED
 *                                                    ↘ CANCELLED ↘ SNAPSHOT_NOT_ADVANCED
 *
 * This service records scheduling progress only. Workflow success or failure is
 * inferred later from target snapshot progress, not executor callbacks.
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class WorkflowInstanceService {

    private final WorkflowInstanceRepository workflowInstanceRepository;

    /**
     * Create a new workflow scheduling instance.
     *
     * Idempotency: instance_key ensures duplicate creation is prevented.
     */
    public WorkflowInstance createInstance(
            String workflowCode,
            Integer workflowVersion,
            LocalDateTime bizDate,
            String triggerType,
            String triggerEventId,
            String triggerReason) {

        return createInstance(
                workflowCode,
                workflowVersion,
                bizDate,
                triggerType,
                triggerEventId,
                triggerReason,
                null);
    }

    /**
     * Create a workflow scheduling instance linked to a FlowPlanVersion.
     *
     * The link is an audit and query anchor only; Lakehouse Flow still emits a
     * scheduling decision rather than executing or monitoring the workflow.
     *
     * @param workflowCode workflow definition code
     * @param workflowVersion workflow definition version
     * @param bizDate business date associated with the scheduling decision
     * @param triggerType trigger category such as SNAPSHOT_DRIVEN or RERUN_TASK
     * @param triggerEventId trigger identity used to deduplicate this decision
     * @param triggerReason human-readable scheduling reason
     * @param flowPlanVersionId optional FlowPlanVersion id that produced this instance
     * @return newly created or existing workflow scheduling instance
     */
    public WorkflowInstance createInstance(
            String workflowCode,
            Integer workflowVersion,
            LocalDateTime bizDate,
            String triggerType,
            String triggerEventId,
            String triggerReason,
            Long flowPlanVersionId) {

        String instanceKey = buildInstanceKey(workflowCode, workflowVersion, bizDate, triggerType, triggerEventId);

        // Check if already exists
        Optional<WorkflowInstance> existing = workflowInstanceRepository.findByInstanceKey(instanceKey);
        if (existing.isPresent()) {
            log.debug("Workflow instance {} already exists, skipping creation", instanceKey);
            return existing.get();
        }

        log.info("Creating workflow instance {} for {} v{} on {}", 
                 instanceKey, workflowCode, workflowVersion, bizDate);

        WorkflowInstance instance = WorkflowInstance.builder()
                .instanceKey(instanceKey)
                .workflowCode(workflowCode)
                .workflowVersion(workflowVersion)
                .flowPlanVersionId(flowPlanVersionId)
                .bizDate(bizDate)
                .triggerType(triggerType)
                .triggerEventId(triggerEventId)
                .triggerReason(triggerReason)
                .state(SchedulingStates.CREATED)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        return workflowInstanceRepository.save(instance);
    }

    /**
     * Transition scheduling state with validation.
     */
    public void transitionState(Long instanceId, String newState) {
        Optional<WorkflowInstance> instance = workflowInstanceRepository.findById(instanceId);
        if (instance.isEmpty()) {
            throw new RuntimeException("Workflow instance not found: " + instanceId);
        }

        WorkflowInstance w = instance.get();
        String oldState = w.getState();

        // Validate state transition
        if (!isValidTransition(oldState, newState)) {
            log.warn("Invalid state transition from {} to {}", oldState, newState);
            throw new RuntimeException("Invalid state transition: " + oldState + " -> " + newState);
        }

        w.setState(newState);
        
        LocalDateTime now = LocalDateTime.now();
        if (SchedulingStates.SCHEDULED.equals(newState)) {
            w.setScheduledAt(now);
        } else if (SchedulingStates.isSnapshotOutcome(newState)) {
            w.setLastSnapshotCheckAt(now);
        }

        workflowInstanceRepository.save(w);
        log.info("Workflow instance {} transitioned from {} to {}", w.getInstanceKey(), oldState, newState);
    }

    /**
     * Validate state transition
     */
    boolean isValidTransition(String fromState, String toState) {
        return switch (fromState) {
            case SchedulingStates.CREATED -> toState.equals(SchedulingStates.WAITING_SNAPSHOT)
                    || toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED);
            case SchedulingStates.WAITING_SNAPSHOT -> toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED);
            case SchedulingStates.READY_TO_SCHEDULE -> toState.equals(SchedulingStates.SCHEDULED)
                    || toState.equals(SchedulingStates.CANCELLED);
            case SchedulingStates.SCHEDULED -> toState.equals(SchedulingStates.SNAPSHOT_CONFIRMED)
                    || toState.equals(SchedulingStates.SNAPSHOT_NOT_ADVANCED)
                    || toState.equals(SchedulingStates.CANCELLED);
            case SchedulingStates.SNAPSHOT_NOT_ADVANCED -> toState.equals(SchedulingStates.WAITING_SNAPSHOT)
                    || toState.equals(SchedulingStates.READY_TO_SCHEDULE)
                    || toState.equals(SchedulingStates.CANCELLED);
            default -> false;
        };
    }

    /**
     * Mark workflow as waiting for snapshot evidence.
     */
    public void markWaitingForSnapshot(Long instanceId) {
        transitionState(instanceId, SchedulingStates.WAITING_SNAPSHOT);
    }

    /**
     * Mark workflow as schedulable.
     */
    public void markSchedulable(Long instanceId) {
        transitionState(instanceId, SchedulingStates.READY_TO_SCHEDULE);
    }

    /**
     * Mark that Lakehouse Flow emitted the scheduling decision.
     */
    public void markScheduled(Long instanceId) {
        transitionState(instanceId, SchedulingStates.SCHEDULED);
    }

    /**
     * Cancel a workflow scheduling instance.
     *
     * @param instanceId workflow instance id
     */
    public void cancel(Long instanceId) {
        transitionState(instanceId, SchedulingStates.CANCELLED);
    }

    /**
     * Mark that the target snapshot evidence confirms the workflow result.
     */
    public void confirmSnapshotProgress(Long instanceId) {
        transitionState(instanceId, SchedulingStates.SNAPSHOT_CONFIRMED);
    }

    /**
     * Mark that the target snapshot evidence has not advanced beyond the baseline.
     */
    public void markSnapshotNotAdvanced(Long instanceId) {
        transitionState(instanceId, SchedulingStates.SNAPSHOT_NOT_ADVANCED);
    }

    /**
     * Get workflow instance
     */
    @Transactional(readOnly = true)
    public Optional<WorkflowInstance> getInstance(Long instanceId) {
        return workflowInstanceRepository.findById(instanceId);
    }

    /**
     * Get instance by key
     */
    @Transactional(readOnly = true)
    public Optional<WorkflowInstance> getInstanceByKey(String instanceKey) {
        return workflowInstanceRepository.findByInstanceKey(instanceKey);
    }

    /**
     * Build instance key for idempotency
     *
     * @param workflowCode workflow definition code
     * @param workflowVersion workflow definition version
     * @param bizDate business date associated with the scheduling decision
     * @param triggerType trigger category such as SNAPSHOT_DRIVEN
     * @param triggerId trigger identity used to deduplicate this decision
     * @return stable workflow instance key
     */
    private String buildInstanceKey(String workflowCode, Integer workflowVersion, 
                                   LocalDateTime bizDate, String triggerType, String triggerId) {
        return String.format("%s:%d:%s:%s:%s",
                workflowCode,
                workflowVersion,
                bizDate,
                triggerType,
                triggerId != null ? triggerId : "manual");
    }
}
