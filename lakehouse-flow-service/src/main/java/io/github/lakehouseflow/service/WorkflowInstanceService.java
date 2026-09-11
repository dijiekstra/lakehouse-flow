package io.github.lakehouseflow.service;

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
 * Service for managing workflow instances.
 *
 * State transitions:
 * CREATED → WAITING → RUNNING → SUCCESS
 *       ↘ FAILED
 *       ↘ TIMEOUT
 *       ↘ CANCELLED
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class WorkflowInstanceService {

    private final WorkflowInstanceRepository workflowInstanceRepository;

    /**
     * Create a new workflow instance.
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
                .bizDate(bizDate)
                .triggerType(triggerType)
                .triggerEventId(triggerEventId)
                .triggerReason(triggerReason)
                .state("CREATED")
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        return workflowInstanceRepository.save(instance);
    }

    /**
     * Transition instance state with validation
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
        
        // Set timestamps
        if ("RUNNING".equals(newState)) {
            w.setStartTime(LocalDateTime.now());
        } else if ("SUCCESS".equals(newState) || "FAILED".equals(newState) || "TIMEOUT".equals(newState)) {
            w.setEndTime(LocalDateTime.now());
        }

        workflowInstanceRepository.save(w);
        log.info("Workflow instance {} transitioned from {} to {}", w.getInstanceKey(), oldState, newState);
    }

    /**
     * Validate state transition
     */
    private boolean isValidTransition(String fromState, String toState) {
        return switch (fromState) {
            case "CREATED" -> toState.equals("WAITING") || toState.equals("RUNNING") || toState.equals("CANCELLED");
            case "WAITING" -> toState.equals("RUNNING") || toState.equals("CANCELLED");
            case "RUNNING" -> toState.equals("SUCCESS") || toState.equals("FAILED") || toState.equals("TIMEOUT");
            case "FAILED" -> toState.equals("RUNNING"); // Manual rerun
            case "TIMEOUT" -> toState.equals("RUNNING"); // Manual rerun
            default -> false;
        };
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
