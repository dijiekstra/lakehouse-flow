package io.github.lakehouseflow.service;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.OperationalBlockerTypes;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillBatchRepository;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.model.BackfillBatch;
import io.github.lakehouseflow.model.BackfillItem;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.PageRequest;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Tests the unified scheduler-owned blocker aggregation.
 */
@ExtendWith(MockitoExtension.class)
class SchedulingBlockerQueryServiceTest {

    private static final LocalDateTime NOW = LocalDateTime.of(2026, 9, 14, 10, 0);

    @Mock
    private TaskInstanceRepository taskInstanceRepository;
    @Mock
    private WorkflowInstanceRepository workflowInstanceRepository;
    @Mock
    private BackfillItemRepository backfillItemRepository;
    @Mock
    private BackfillBatchRepository backfillBatchRepository;
    @Mock
    private JobControlIntentRepository jobControlIntentRepository;
    @Mock
    private SchedulingIntentDeliveryQueryService schedulingDeliveryQueryService;
    @Mock
    private JobControlIntentDeliveryQueryService jobControlDeliveryQueryService;

    @InjectMocks
    private SchedulingBlockerQueryService queryService;

    /** Verify all scheduler-owned blocker families are merged and ordered by evidence time. */
    @Test
    void findBlockersAggregatesSchedulerSnapshotAndDeliveryEvidence() {
        TaskInstance task = TaskInstance.builder()
                .id(11L)
                .instanceKey("1:node.dwd")
                .workflowInstanceId(1L)
                .taskCode("node.dwd")
                .targetAssetKey("lake.dwd.orders")
                .bizDate(NOW.toLocalDate().atStartOfDay())
                .state(SchedulingStates.SCHEDULED)
                .sourceHealth("SOURCE_BLOCKED")
                .waitingReason("source gap")
                .updatedAt(NOW.minusMinutes(4))
                .build();
        WorkflowInstance workflow = WorkflowInstance.builder()
                .id(1L)
                .workflowCode("flow.orders")
                .build();
        BackfillItem item = BackfillItem.builder()
                .id(21L)
                .backfillBatchId(2L)
                .nodeCode("node.ads")
                .targetAssetKey("lake.ads.gmv")
                .bizDate(LocalDate.of(2026, 9, 13))
                .status(BackfillItemStatuses.WAITING_DEPENDENCY)
                .updatedAt(NOW.minusMinutes(3))
                .build();
        BackfillBatch batch = BackfillBatch.builder()
                .id(2L)
                .batchKey("backfill-orders")
                .workflowCode("flow.orders")
                .build();
        JobControlIntent control = JobControlIntent.builder()
                .id(31L)
                .intentKey("job-control:writer.orders:2")
                .writerJobKey("writer.orders")
                .tableAssetKey("lake.ods.orders")
                .snapshotResult("WAITING")
                .createdAt(NOW.minusMinutes(2))
                .build();
        when(taskInstanceRepository.findOperationalBlockers(null, null, null, PageRequest.of(0, 100)))
                .thenReturn(List.of(task));
        when(workflowInstanceRepository.findAllById(List.of(1L))).thenReturn(List.of(workflow));
        when(backfillItemRepository.findOperationalBlockers(null, null, null, PageRequest.of(0, 100)))
                .thenReturn(List.of(item));
        when(backfillBatchRepository.findAllById(List.of(2L))).thenReturn(List.of(batch));
        when(jobControlIntentRepository.findOperationalBlockers(null, null, null, PageRequest.of(0, 100)))
                .thenReturn(List.of(control));
        when(schedulingDeliveryQueryService.findDeadLetters(null, null, null, 100)).thenReturn(List.of(
                new SchedulingIntentDeliveryQueryService.DeadLetterDelivery(
                        41L, 401L, "task-instance:12", 12L, "flow.orders", "node.dws",
                        "lake.dws.gmv", NOW.toLocalDate().atStartOfDay(), "HTTP", "endpoint", 8,
                        "timeout", NOW.minusMinutes(2), NOW.minusMinutes(1), NOW)));
        when(jobControlDeliveryQueryService.findDeadLetters(null, null, null, 100)).thenReturn(List.of());

        List<SchedulingBlockerQueryService.OperationalBlocker> result =
                queryService.findBlockers(null, null, null, null);

        assertEquals(4, result.size());
        assertEquals(OperationalBlockerTypes.DELIVERY_EXHAUSTED, result.get(0).blockerType());
        assertEquals(Set.of(
                        OperationalBlockerTypes.SOURCE_BLOCKED,
                        OperationalBlockerTypes.DAG_DEPENDENCY,
                        OperationalBlockerTypes.TARGET_SNAPSHOT,
                        OperationalBlockerTypes.DELIVERY_EXHAUSTED),
                result.stream().map(SchedulingBlockerQueryService.OperationalBlocker::blockerType)
                        .collect(java.util.stream.Collectors.toSet()));
    }

    /** Verify a category filter only queries the record families that can produce it. */
    @Test
    void findBlockersQueriesOnlyBackfillForDagDependency() {
        when(backfillItemRepository.findOperationalBlockers(
                OperationalBlockerTypes.DAG_DEPENDENCY,
                "flow.orders",
                "lake.ads.gmv",
                PageRequest.of(0, 25)))
                .thenReturn(List.of());
        when(backfillBatchRepository.findAllById(List.of())).thenReturn(List.of());

        assertEquals(0, queryService.findBlockers(
                " dag_dependency ", " flow.orders ", " lake.ads.gmv ", 25).size());

        verify(backfillItemRepository).findOperationalBlockers(
                OperationalBlockerTypes.DAG_DEPENDENCY,
                "flow.orders",
                "lake.ads.gmv",
                PageRequest.of(0, 25));
        verifyNoInteractions(
                taskInstanceRepository,
                workflowInstanceRepository,
                jobControlIntentRepository,
                schedulingDeliveryQueryService,
                jobControlDeliveryQueryService);
    }

    /** Verify invalid categories, assets, and unbounded requests fail early. */
    @Test
    void findBlockersRejectsInvalidFilters() {
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findBlockers("EXECUTOR_FAILED", null, null, 10));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findBlockers(null, null, null, 0));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findBlockers(null, null, null, 501));
        assertThrows(IllegalArgumentException.class,
                () -> queryService.findBlockers(
                        OperationalBlockerTypes.TARGET_SNAPSHOT, null, "invalid", 10));
    }
}
