package io.github.lakehouseflow.service;

import io.github.lakehouseflow.dao.FlowPlanVersionRepository;
import io.github.lakehouseflow.dao.ScheduleNodeRepository;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.TaskInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.when;

/**
 * Tests frozen FlowPlan policy validation and runtime resolution.
 */
@ExtendWith(MockitoExtension.class)
class FlowPlanPolicyServiceTest {

    @Mock
    private FlowPlanVersionRepository flowPlanVersionRepository;

    @Mock
    private ScheduleNodeRepository scheduleNodeRepository;

    @InjectMocks
    private FlowPlanPolicyService flowPlanPolicyService;

    /** Verify legacy tasks retain the scanner and publication global defaults. */
    @Test
    void resolveUsesGlobalDefaultsForLegacyTask() {
        TaskInstance task = TaskInstance.builder().id(1L).build();

        FlowPlanPolicyService.EffectivePolicy policy = flowPlanPolicyService.resolve(
                task,
                Duration.ofHours(2),
                Duration.ofHours(3));

        assertEquals(Duration.ofHours(2), policy.confirmationTimeout());
        assertEquals(Duration.ofHours(3), policy.targetAdmissionLease());
        assertEquals("PARALLEL", policy.concurrencyMode());
        assertEquals(Integer.MAX_VALUE, policy.maxActiveInstances());
    }

    /** Verify node timeout overrides its version while concurrency remains version-scoped. */
    @Test
    void resolveMergesNodeConfirmationAndVersionConcurrency() {
        FlowPlanVersion version = version(
                Map.of("timeout", "PT2H"),
                Map.of(
                        "mode", "SERIAL_WAIT",
                        "targetAdmissionLease", "PT3H"));
        ScheduleNode node = node(Map.of("timeout", "PT30M"));
        TaskInstance task = TaskInstance.builder()
                .id(1L)
                .flowPlanVersionId(10L)
                .scheduleNodeId(20L)
                .build();
        when(flowPlanVersionRepository.findById(10L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findById(20L)).thenReturn(Optional.of(node));

        FlowPlanPolicyService.EffectivePolicy policy = flowPlanPolicyService.resolve(
                task,
                Duration.ofHours(1),
                Duration.ofHours(1));

        assertEquals(Duration.ofMinutes(30), policy.confirmationTimeout());
        assertEquals(Duration.ofHours(3), policy.targetAdmissionLease());
        assertEquals("SERIAL_WAIT", policy.concurrencyMode());
        assertEquals(1, policy.maxActiveInstances());
    }

    /** Verify explicit parallel limits remain scheduler-side workflow admission controls. */
    @Test
    void resolveSupportsBoundedParallelPolicy() {
        FlowPlanVersion version = version(
                Map.of("confirmationTimeout", "PT45M"),
                Map.of("mode", "PARALLEL", "maxActiveInstances", 4));
        TaskInstance task = TaskInstance.builder().id(1L).flowPlanVersionId(10L).build();
        when(flowPlanVersionRepository.findById(10L)).thenReturn(Optional.of(version));

        FlowPlanPolicyService.EffectivePolicy policy = flowPlanPolicyService.resolve(
                task,
                Duration.ofHours(1),
                Duration.ofHours(1));

        assertEquals(Duration.ofMinutes(45), policy.confirmationTimeout());
        assertEquals(Duration.ofMinutes(45), policy.targetAdmissionLease());
        assertEquals(4, policy.maxActiveInstances());
    }

    /** Verify publication rejects a target lease that can expire before confirmation. */
    @Test
    void validateVersionPoliciesRejectsLeaseShorterThanNodeTimeout() {
        FlowPlanVersion version = version(
                Map.of("timeout", "PT1H"),
                Map.of("targetAdmissionLease", "PT30M"));

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanPolicyService.validateVersionPolicies(version, List.of(node(Map.of()))));
    }

    /** Verify publication rejects concurrency modes whose queue semantics are not implemented. */
    @Test
    void validateVersionPoliciesRejectsUnsupportedConcurrencyMode() {
        FlowPlanVersion version = version(
                Map.of(),
                Map.of("mode", "SERIAL_DISCARD"));

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanPolicyService.validateVersionPolicies(version, List.of(node(Map.of()))));
    }

    /** Verify malformed duration text fails before a version becomes immutable. */
    @Test
    void validateVersionPoliciesRejectsMalformedTimeout() {
        FlowPlanVersion version = version(
                Map.of("timeout", "one hour"),
                Map.of());

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanPolicyService.validateVersionPolicies(version, List.of(node(Map.of()))));
    }

    /** Verify a node from another version cannot provide a policy override. */
    @Test
    void resolveRejectsNodeOutsideTaskVersion() {
        FlowPlanVersion version = version(Map.of(), Map.of());
        ScheduleNode foreignNode = node(Map.of());
        foreignNode.setFlowPlanVersionId(11L);
        TaskInstance task = TaskInstance.builder()
                .id(1L)
                .flowPlanVersionId(10L)
                .scheduleNodeId(20L)
                .build();
        when(flowPlanVersionRepository.findById(10L)).thenReturn(Optional.of(version));
        when(scheduleNodeRepository.findById(20L)).thenReturn(Optional.of(foreignNode));

        assertThrows(
                IllegalStateException.class,
                () -> flowPlanPolicyService.resolve(
                        task,
                        Duration.ofHours(1),
                        Duration.ofHours(1)));
    }

    /** Verify zero or negative version concurrency limits fail closed. */
    @Test
    void validateVersionPoliciesRejectsNonPositiveActiveLimit() {
        FlowPlanVersion version = version(
                Map.of(),
                Map.of("maxActiveInstances", 0));

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanPolicyService.validateVersionPolicies(version, List.of(node(Map.of()))));
    }

    /** Verify timeout actions without scheduler state semantics cannot be published. */
    @Test
    void validateVersionPoliciesRejectsUnsupportedStaleAction() {
        FlowPlanVersion version = version(
                Map.of("staleAction", "ESCALATE"),
                Map.of());

        assertThrows(
                IllegalArgumentException.class,
                () -> flowPlanPolicyService.validateVersionPolicies(version, List.of(node(Map.of()))));
    }

    /** Verify a version-only policy remains valid before nodes are attached to runtime tasks. */
    @Test
    void validateVersionPoliciesAcceptsEmptyNodeList() {
        FlowPlanVersion version = version(
                Map.of("timeout", "PT20M"),
                Map.of("targetAdmissionLease", "PT30M"));

        flowPlanPolicyService.validateVersionPolicies(version, List.of());
    }

    /** Build one version policy fixture. */
    private FlowPlanVersion version(
            Map<String, Object> confirmationPolicy,
            Map<String, Object> concurrencyPolicy) {
        return FlowPlanVersion.builder()
                .id(10L)
                .confirmationPolicyJson(confirmationPolicy)
                .concurrencyPolicyJson(concurrencyPolicy)
                .build();
    }

    /** Build one node confirmation override fixture. */
    private ScheduleNode node(Map<String, Object> confirmationPolicy) {
        return ScheduleNode.builder()
                .id(20L)
                .flowPlanVersionId(10L)
                .confirmationPolicyJson(confirmationPolicy)
                .build();
    }
}
