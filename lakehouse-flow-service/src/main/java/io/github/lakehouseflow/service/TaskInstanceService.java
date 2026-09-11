package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.model.TaskInstance;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;

/**
 * Service for managing task instances.
 *
 * State transitions:
 * CREATED → WAITING_DEPENDENCY → READY → DISPATCHING → RUNNING → SUCCESS
 *                                                            ↘ FAILED
 *                                                            ↘ TIMEOUT
 * FAILED → RETRY_WAITING → READY (if retries remain)
 */
@Service
@Slf4j
@RequiredArgsConstructor
@Transactional
public class TaskInstanceService {

    private final TaskInstanceRepository taskInstanceRepository;

    /**
     * Create a new task instance.
     *
     * Idempotency: instance_key ensures duplicate creation is prevented.
     */
    public TaskInstance createInstance(
            Long workflowInstanceId,
            String taskCode,
            Integer taskVersion,
            LocalDateTime bizDate) {

        String instanceKey = buildInstanceKey(workflowInstanceId, taskCode, 1);

        // Check if already exists
        Optional<TaskInstance> existing = taskInstanceRepository.findByInstanceKey(instanceKey);
        if (existing.isPresent()) {
            log.debug("Task instance {} already exists, skipping creation", instanceKey);
            return existing.get();
        }

        log.info("Creating task instance {} for workflow {}", instanceKey, workflowInstanceId);

        TaskInstance instance = TaskInstance.builder()
                .instanceKey(instanceKey)
                .workflowInstanceId(workflowInstanceId)
                .taskCode(taskCode)
                .taskVersion(taskVersion)
                .bizDate(bizDate)
                .state("CREATED")
                .tryNumber(1)
                .maxRetries(3)
                .createdAt(LocalDateTime.now())
                .updatedAt(LocalDateTime.now())
                .build();

        return taskInstanceRepository.save(instance);
    }

    /**
     * Transition task state with validation
     */
    public void transitionState(Long taskId, String newState, String waitingReason) {
        Optional<TaskInstance> task = taskInstanceRepository.findById(taskId);
        if (task.isEmpty()) {
            throw new RuntimeException("Task instance not found: " + taskId);
        }

        TaskInstance t = task.get();
        String oldState = t.getState();

        // Validate state transition
        if (!isValidTransition(oldState, newState)) {
            log.warn("Invalid state transition from {} to {}", oldState, newState);
            throw new RuntimeException("Invalid state transition: " + oldState + " -> " + newState);
        }

        t.setState(newState);
        t.setWaitingReason(waitingReason);

        // Set timestamps
        if ("DISPATCHING".equals(newState)) {
            t.setSubmitTime(LocalDateTime.now());
        } else if ("RUNNING".equals(newState)) {
            t.setStartTime(LocalDateTime.now());
        } else if ("SUCCESS".equals(newState) || "FAILED".equals(newState) || "TIMEOUT".equals(newState)) {
            t.setEndTime(LocalDateTime.now());
        }

        taskInstanceRepository.save(t);
        log.info("Task instance {} transitioned from {} to {}", t.getInstanceKey(), oldState, newState);
    }

    /**
     * Validate state transition
     */
    private boolean isValidTransition(String fromState, String toState) {
        return switch (fromState) {
            case "CREATED" -> toState.equals("WAITING_DEPENDENCY") || toState.equals("READY") || toState.equals("SKIPPED");
            case "WAITING_DEPENDENCY" -> toState.equals("READY") || toState.equals("CANCELLED");
            case "READY" -> toState.equals("DISPATCHING") || toState.equals("SKIPPED");
            case "DISPATCHING" -> toState.equals("RUNNING");
            case "RUNNING" -> toState.equals("SUCCESS") || toState.equals("FAILED") || toState.equals("TIMEOUT");
            case "FAILED" -> toState.equals("RETRY_WAITING");
            case "RETRY_WAITING" -> toState.equals("READY");
            default -> false;
        };
    }

    /**
     * Mark task as waiting for dependencies
     */
    public void markWaiting(Long taskId, String reason) {
        transitionState(taskId, "WAITING_DEPENDENCY", reason);
    }

    /**
     * Mark task as ready to dispatch
     */
    public void markReady(Long taskId) {
        transitionState(taskId, "READY", null);
    }

    /**
     * Record external job ID when task is dispatched
     */
    public void recordExternalJob(Long taskId, String executorType, String externalJobId) {
        Optional<TaskInstance> task = taskInstanceRepository.findById(taskId);
        if (task.isPresent()) {
            TaskInstance t = task.get();
            t.setExecutorType(executorType);
            t.setExternalJobId(externalJobId);
            taskInstanceRepository.save(t);
            log.info("Recorded external job {} for task {}", externalJobId, taskId);
        }
    }

    /**
     * Handle task retry
     */
    public void handleRetry(Long taskId) {
        Optional<TaskInstance> task = taskInstanceRepository.findById(taskId);
        if (task.isPresent()) {
            TaskInstance t = task.get();
            if (t.getTryNumber() < t.getMaxRetries()) {
                log.info("Task {} will retry (attempt {} of {})", taskId, t.getTryNumber() + 1, t.getMaxRetries());
                transitionState(taskId, "RETRY_WAITING", null);
            } else {
                log.warn("Task {} exceeded max retries ({})", taskId, t.getMaxRetries());
                throw new RuntimeException("Max retries exceeded");
            }
        }
    }

    /**
     * Get task instance
     */
    @Transactional(readOnly = true)
    public Optional<TaskInstance> getInstance(Long taskId) {
        return taskInstanceRepository.findById(taskId);
    }

    /**
     * Find tasks waiting for dependencies
     */
    @Transactional(readOnly = true)
    public List<TaskInstance> findWaitingForDependencies() {
        return taskInstanceRepository.findWaitingForDependencies();
    }

    /**
     * Find tasks ready to dispatch
     */
    @Transactional(readOnly = true)
    public List<TaskInstance> findReadyTasks() {
        return taskInstanceRepository.findByStateOrderByCreatedAtAsc("READY");
    }

    /**
     * Build instance key for idempotency
     */
    private String buildInstanceKey(Long workflowInstanceId, String taskCode, Integer tryNumber) {
        return String.format("%d:%s:%d", workflowInstanceId, taskCode, tryNumber);
    }
}
