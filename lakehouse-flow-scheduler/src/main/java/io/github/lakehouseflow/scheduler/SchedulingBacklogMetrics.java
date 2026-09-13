package io.github.lakehouseflow.scheduler;

import io.github.lakehouseflow.common.BackfillItemStatuses;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.BackfillItemRepository;
import io.github.lakehouseflow.dao.BackfillItemStatusCount;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.TaskInstanceStateCount;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded backlog gauges for scheduler-owned task and backfill waiting states.
 *
 * <p>The gauges describe pending scheduling decisions and snapshot evidence. They do not
 * represent downstream executor state, and no free-form waiting reason is used as a tag.
 */
@Component
public class SchedulingBacklogMetrics {

    private static final String METRIC_PREFIX = "lakehouse.flow.scheduling.backlog";
    private static final Map<String, String> TASK_WAIT_PHASES = Map.of(
            SchedulingStates.CREATED, "INITIALIZATION",
            SchedulingStates.WAITING_SNAPSHOT, "DEPENDENCY_SNAPSHOT",
            SchedulingStates.READY_TO_SCHEDULE, "INTENT_PUBLICATION",
            SchedulingStates.SCHEDULED, "TARGET_SNAPSHOT");
    private static final Map<String, String> BACKFILL_BLOCK_CATEGORIES = Map.of(
            BackfillItemStatuses.WAITING_CONCURRENCY, "DATE_CONCURRENCY",
            BackfillItemStatuses.WAITING_DEPENDENCY, "DAG_DEPENDENCY");

    private final TaskInstanceRepository taskInstanceRepository;
    private final BackfillItemRepository backfillItemRepository;
    private final Map<String, AtomicLong> taskBacklog = new LinkedHashMap<>();
    private final Map<String, AtomicLong> backfillBlocked = new LinkedHashMap<>();

    /**
     * Create and register every bounded task-state and backfill-block gauge.
     *
     * @param meterRegistry application meter registry
     * @param taskInstanceRepository task repository used for grouped state counts
     * @param backfillItemRepository backfill repository used for grouped block counts
     */
    public SchedulingBacklogMetrics(
            MeterRegistry meterRegistry,
            TaskInstanceRepository taskInstanceRepository,
            BackfillItemRepository backfillItemRepository) {
        this.taskInstanceRepository = taskInstanceRepository;
        this.backfillItemRepository = backfillItemRepository;
        TASK_WAIT_PHASES.forEach((state, waitPhase) ->
                taskBacklog.put(state, registerTaskGauge(meterRegistry, state, waitPhase)));
        BACKFILL_BLOCK_CATEGORIES.forEach((status, category) ->
                backfillBlocked.put(status, registerBackfillGauge(meterRegistry, status, category)));
    }

    /**
     * Refresh task and backfill waiting gauges from two grouped database queries.
     */
    @Scheduled(fixedDelayString = "${lakehouse-flow.scheduling-backlog.metrics.fixed-delay-ms:10000}")
    public void refreshBacklog() {
        taskBacklog.values().forEach(value -> value.set(0L));
        for (TaskInstanceStateCount count : taskInstanceRepository.countNonTerminalByState()) {
            AtomicLong value = taskBacklog.get(count.getState());
            if (value != null) {
                value.set(count.getInstanceCount());
            }
        }

        backfillBlocked.values().forEach(value -> value.set(0L));
        for (BackfillItemStatusCount count : backfillItemRepository.countBlockedByStatus()) {
            AtomicLong value = backfillBlocked.get(count.getStatus());
            if (value != null) {
                value.set(count.getItemCount());
            }
        }
    }

    /** Register one task-state gauge with its fixed scheduler wait phase. */
    private AtomicLong registerTaskGauge(
            MeterRegistry meterRegistry,
            String state,
            String waitPhase) {
        AtomicLong value = new AtomicLong();
        Gauge.builder(METRIC_PREFIX + ".task.instances", value, AtomicLong::get)
                .tags("state", state, "wait_phase", waitPhase)
                .register(meterRegistry);
        return value;
    }

    /** Register one backfill block gauge with bounded status and category tags. */
    private AtomicLong registerBackfillGauge(
            MeterRegistry meterRegistry,
            String status,
            String category) {
        AtomicLong value = new AtomicLong();
        Gauge.builder(METRIC_PREFIX + ".backfill.blocked.items", value, AtomicLong::get)
                .tags("status", status, "category", category)
                .register(meterRegistry);
        return value;
    }
}
