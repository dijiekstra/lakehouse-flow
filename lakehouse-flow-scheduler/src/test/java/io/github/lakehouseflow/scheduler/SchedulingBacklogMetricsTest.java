package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.BackfillItemStatusCount;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.TaskInstanceStateCount;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests bounded task and backfill scheduling-backlog gauges.
 */
class SchedulingBacklogMetricsTest {

    /** Verify grouped repository counts refresh gauges and stale values return to zero. */
    @Test
    void refreshBacklogPublishesCurrentSchedulerCounts() {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        TaskInstanceRepository taskRepository = mock(TaskInstanceRepository.class);
        BackfillItemRepository backfillRepository = mock(BackfillItemRepository.class);
        SchedulingBacklogMetrics metrics =
                new SchedulingBacklogMetrics(registry, taskRepository, backfillRepository);
        TaskInstanceStateCount taskCount = mock(TaskInstanceStateCount.class);
        BackfillItemStatusCount backfillCount = mock(BackfillItemStatusCount.class);
        when(taskCount.getState()).thenReturn(SchedulingStates.READY_TO_SCHEDULE);
        when(taskCount.getInstanceCount()).thenReturn(4L);
        when(backfillCount.getStatus()).thenReturn(BackfillItemStatuses.WAITING_DEPENDENCY);
        when(backfillCount.getItemCount()).thenReturn(2L);
        when(taskRepository.countNonTerminalByState())
                .thenReturn(List.of(taskCount))
                .thenReturn(List.of());
        when(backfillRepository.countBlockedByStatus())
                .thenReturn(List.of(backfillCount))
                .thenReturn(List.of());

        metrics.refreshBacklog();
        assertEquals(4.0, taskGauge(registry));
        assertEquals(2.0, backfillGauge(registry));

        metrics.refreshBacklog();
        assertEquals(0.0, taskGauge(registry));
        assertEquals(0.0, backfillGauge(registry));
    }

    /** Read the ready-to-publish task backlog gauge. */
    private double taskGauge(SimpleMeterRegistry registry) {
        return registry.get("lakehouse.flow.scheduling.backlog.task.instances")
                .tags("state", SchedulingStates.READY_TO_SCHEDULE, "wait_phase", "INTENT_PUBLICATION")
                .gauge()
                .value();
    }

    /** Read the DAG-blocked backfill item gauge. */
    private double backfillGauge(SimpleMeterRegistry registry) {
        return registry.get("lakehouse.flow.scheduling.backlog.backfill.blocked.items")
                .tags("status", BackfillItemStatuses.WAITING_DEPENDENCY, "category", "DAG_DEPENDENCY")
                .gauge()
                .value();
    }
}
