package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resolves immutable FlowPlan version policies for scheduler runtime decisions.
 *
 * Policy values affect only intent admission and snapshot confirmation. They do
 * not describe downstream execution concurrency, attempts, or runtime status.
 */
@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class FlowPlanPolicyService {

    private static final String MODE_PARALLEL = "PARALLEL";
    private static final String MODE_SERIAL_WAIT = "SERIAL_WAIT";
    private static final int UNBOUNDED_ACTIVE_INSTANCES = Integer.MAX_VALUE;

    private final FlowPlanVersionRepository flowPlanVersionRepository;
    private final ScheduleNodeRepository scheduleNodeRepository;

    /**
     * Effective scheduler controls for one immutable task decision.
     *
     * @param confirmationTimeout maximum wait for attributable target snapshot progress
     * @param targetAdmissionLease target and business-date publication lease
     * @param concurrencyMode version-level scheduling admission mode
     * @param maxActiveInstances maximum active workflow scheduling instances for the version
     */
    public record EffectivePolicy(
            Duration confirmationTimeout,
            Duration targetAdmissionLease,
            String concurrencyMode,
            int maxActiveInstances) {
    }

    /**
     * Resolve version defaults and node confirmation overrides for one task.
     *
     * Legacy tasks without a FlowPlanVersion keep the supplied global defaults.
     * A node can override only confirmation fields; concurrency remains frozen
     * at the version level.
     *
     * @param task task scheduling decision to resolve
     * @param defaultConfirmationTimeout global confirmation fallback
     * @param defaultTargetAdmissionLease global target-admission fallback
     * @return validated effective runtime policy
     */
    public EffectivePolicy resolve(
            TaskInstance task,
            Duration defaultConfirmationTimeout,
            Duration defaultTargetAdmissionLease) {
        if (task == null) {
            throw new IllegalArgumentException("task must not be null");
        }
        Duration fallbackTimeout = requirePositiveDuration(
                defaultConfirmationTimeout,
                "defaultConfirmationTimeout");
        Duration fallbackLease = requirePositiveDuration(
                defaultTargetAdmissionLease,
                "defaultTargetAdmissionLease");
        if (task.getFlowPlanVersionId() == null) {
            return new EffectivePolicy(
                    fallbackTimeout,
                    fallbackLease,
                    MODE_PARALLEL,
                    UNBOUNDED_ACTIVE_INSTANCES);
        }

        FlowPlanVersion version = flowPlanVersionRepository.findById(task.getFlowPlanVersionId())
                .orElseThrow(() -> new IllegalStateException(
                        "FlowPlanVersion not found for task policy: " + task.getFlowPlanVersionId()));
        ScheduleNode node = resolveNode(task, version);
        return resolve(version, node, fallbackTimeout, fallbackLease);
    }

    /**
     * Validate every effective node policy before a FlowPlanVersion is published.
     *
     * @param version version whose immutable policies will enter runtime decisions
     * @param nodes complete node list belonging to the version
     */
    public void validateVersionPolicies(FlowPlanVersion version, List<ScheduleNode> nodes) {
        if (version == null) {
            throw new IllegalArgumentException("version must not be null");
        }
        List<ScheduleNode> effectiveNodes = nodes == null ? List.of() : nodes;
        if (effectiveNodes.isEmpty()) {
            resolve(version, null, Duration.ofHours(1), Duration.ofHours(1));
            return;
        }
        for (ScheduleNode node : effectiveNodes) {
            if (!version.getId().equals(node.getFlowPlanVersionId())) {
                throw new IllegalArgumentException(
                        "ScheduleNode does not belong to FlowPlanVersion " + version.getId()
                                + ": " + node.getId());
            }
            resolve(version, node, Duration.ofHours(1), Duration.ofHours(1));
        }
    }

    /** Resolve and verify the optional task node within its frozen version. */
    private ScheduleNode resolveNode(TaskInstance task, FlowPlanVersion version) {
        if (task.getScheduleNodeId() == null) {
            return null;
        }
        ScheduleNode node = scheduleNodeRepository.findById(task.getScheduleNodeId())
                .orElseThrow(() -> new IllegalStateException(
                        "ScheduleNode not found for task policy: " + task.getScheduleNodeId()));
        if (!version.getId().equals(node.getFlowPlanVersionId())) {
            throw new IllegalStateException(
                    "Task policy node does not belong to FlowPlanVersion " + version.getId()
                            + ": " + node.getId());
        }
        return node;
    }

    /** Merge version policy and node confirmation overrides into runtime values. */
    private EffectivePolicy resolve(
            FlowPlanVersion version,
            ScheduleNode node,
            Duration defaultConfirmationTimeout,
            Duration defaultTargetAdmissionLease) {
        Map<String, Object> versionConfirmation = safeMap(version.getConfirmationPolicyJson());
        Map<String, Object> nodeConfirmation = node == null
                ? Map.of()
                : safeMap(node.getConfirmationPolicyJson());
        Map<String, Object> concurrency = safeMap(version.getConcurrencyPolicyJson());

        validateUnsupportedConcurrencyFields(concurrency);
        Duration confirmationTimeout = readDuration(
                nodeConfirmation,
                versionConfirmation,
                "timeout",
                "confirmationTimeout",
                defaultConfirmationTimeout);
        String concurrencyMode = readConcurrencyMode(concurrency);
        int maxActiveInstances = readMaxActiveInstances(concurrency, concurrencyMode);
        Duration targetAdmissionLease = readDuration(
                concurrency,
                Map.of(),
                "targetAdmissionLease",
                null,
                concurrency.containsKey("targetAdmissionLease")
                        ? defaultTargetAdmissionLease
                        : confirmationTimeout);
        if (targetAdmissionLease.compareTo(confirmationTimeout) < 0) {
            throw new IllegalArgumentException(
                    "targetAdmissionLease must be greater than or equal to confirmation timeout");
        }
        validateStaleAction(nodeConfirmation, versionConfirmation);
        return new EffectivePolicy(
                confirmationTimeout,
                targetAdmissionLease,
                concurrencyMode,
                maxActiveInstances);
    }

    /** Reject declared concurrency controls whose scheduling semantics are not implemented. */
    private void validateUnsupportedConcurrencyFields(Map<String, Object> concurrencyPolicy) {
        for (String field : List.of("priority", "dedupeWindow")) {
            if (concurrencyPolicy.containsKey(field)) {
                throw new IllegalArgumentException(
                        "Unsupported concurrency policy field: " + field
                                + "; remove it until its scheduling and audit semantics are implemented");
            }
        }
    }

    /** Read the supported concurrency mode and reject unimplemented queue semantics. */
    private String readConcurrencyMode(Map<String, Object> concurrencyPolicy) {
        Object rawMode = concurrencyPolicy.get("mode");
        String mode = rawMode == null
                ? MODE_PARALLEL
                : rawMode.toString().trim().toUpperCase(Locale.ROOT);
        return switch (mode) {
            case MODE_PARALLEL, MODE_SERIAL_WAIT -> mode;
            default -> throw new IllegalArgumentException(
                    "Unsupported concurrency policy mode: " + mode
                            + "; supported modes are PARALLEL and SERIAL_WAIT");
        };
    }

    /** Resolve a positive version-level active workflow limit. */
    private int readMaxActiveInstances(Map<String, Object> concurrencyPolicy, String mode) {
        Object rawLimit = concurrencyPolicy.get("maxActiveInstances");
        if (rawLimit == null) {
            return MODE_SERIAL_WAIT.equals(mode) ? 1 : UNBOUNDED_ACTIVE_INSTANCES;
        }
        int limit;
        try {
            limit = Integer.parseInt(rawLimit.toString());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("maxActiveInstances must be a positive integer", e);
        }
        if (limit <= 0) {
            throw new IllegalArgumentException("maxActiveInstances must be a positive integer");
        }
        return limit;
    }

    /** Resolve an ISO-8601 duration from primary and inherited policy maps. */
    private Duration readDuration(
            Map<String, Object> primary,
            Map<String, Object> inherited,
            String key,
            String alias,
            Duration fallback) {
        Object raw = readPolicyValue(primary, key, alias);
        if (raw == null) {
            raw = readPolicyValue(inherited, key, alias);
        }
        if (raw == null) {
            return requirePositiveDuration(fallback, key + " fallback");
        }
        try {
            return requirePositiveDuration(Duration.parse(raw.toString().trim()), key);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException(key + " must be an ISO-8601 duration", e);
        }
    }

    /** Read one canonical policy key while accepting its earlier documented alias. */
    private Object readPolicyValue(Map<String, Object> policy, String key, String alias) {
        if (policy.containsKey(key)) {
            return policy.get(key);
        }
        return alias == null ? null : policy.get(alias);
    }

    /** Restrict timeout behavior to the scheduler state transition currently implemented. */
    private void validateStaleAction(
            Map<String, Object> nodeConfirmation,
            Map<String, Object> versionConfirmation) {
        Object raw = nodeConfirmation.containsKey("staleAction")
                ? nodeConfirmation.get("staleAction")
                : versionConfirmation.get("staleAction");
        if (raw == null) {
            return;
        }
        String action = raw.toString().trim().toUpperCase(Locale.ROOT);
        if (!"MARK_NOT_ADVANCED".equals(action)) {
            throw new IllegalArgumentException(
                    "Unsupported confirmation staleAction: " + action
                            + "; supported value is MARK_NOT_ADVANCED");
        }
    }

    /** Require a positive duration used by scheduler expiration decisions. */
    private Duration requirePositiveDuration(Duration duration, String field) {
        if (duration == null || duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException(field + " must be a positive duration");
        }
        return duration;
    }

    /** Normalize nullable JSON policy columns to an immutable empty map. */
    private Map<String, Object> safeMap(Map<String, Object> policy) {
        return policy == null ? Map.of() : policy;
    }
}
