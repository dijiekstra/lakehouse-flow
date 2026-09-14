package io.github.lakehouseflow.e2e.job;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.github.lakehouseflow.common.ScheduleNodeProcessingModes;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryChannels;
import io.github.lakehouseflow.common.SchedulingIntentDeliveryStatuses;
import io.github.lakehouseflow.common.SchedulingIntentContract;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.common.SnapshotEvidenceContract;
import io.github.lakehouseflow.common.SnapshotSourceHealthOutcomes;
import io.github.lakehouseflow.dao.AssetStateRepository;
import io.github.lakehouseflow.dao.EventConsumerOffsetRepository;
import io.github.lakehouseflow.dao.JobControlIntentDeliveryRepository;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.dao.SchedulingIntentDeliveryRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.SchedulingTargetAdmissionRepository;
import io.github.lakehouseflow.dao.SnapshotSourceHealthRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.integration.event.SnapshotIngestionTransactionService;
import io.github.lakehouseflow.model.FlowPlan;
import io.github.lakehouseflow.model.FlowPlanVersion;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.JobControlIntentDelivery;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.ScheduleNode;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.SchedulingIntentDelivery;
import io.github.lakehouseflow.model.SnapshotConfirmationResult;
import io.github.lakehouseflow.model.SnapshotSourceHealth;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import io.github.lakehouseflow.scheduler.JobControlIntentDeliveryScanner;
import io.github.lakehouseflow.scheduler.SchedulingIntentDeliveryScanner;
import io.github.lakehouseflow.scheduler.delivery.HttpSchedulingIntentPublisher;
import io.github.lakehouseflow.service.FlowPlanService;
import io.github.lakehouseflow.service.JobControlIntentDeliveryQueryService;
import io.github.lakehouseflow.service.JobControlIntentService;
import io.github.lakehouseflow.service.SchedulingIntentDeliveryService;
import io.github.lakehouseflow.service.SchedulingIntentDeliveryQueryService;
import io.github.lakehouseflow.service.SchedulingIntentService;
import io.github.lakehouseflow.service.SchedulingTargetAdmissionService;
import io.github.lakehouseflow.service.SnapshotConfirmationService;
import io.github.lakehouseflow.service.TaskInstanceService;
import io.github.lakehouseflow.service.WorkflowInstanceService;
import io.github.lakehouseflow.service.WriterJobBindingService;
import io.github.lakehouseflow.service.delivery.SchedulingIntentPublication;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.MigrationVersion;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;

/**
 * Whole-system PostgreSQL recovery proof for scheduler coordination boundaries.
 *
 * <p>Each peer is a separate Spring application context connected to the same Testcontainers
 * PostgreSQL database. The tests use production services, repositories, transactions, row locks,
 * and HTTP publisher code while keeping every execution-result decision snapshot-only.
 */
@Testcontainers
@ActiveProfiles("e2e")
@SpringBootTest(
        classes = OrderToGmvE2EIT.E2EApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {
            "lakehouse-flow.scheduling-intent-outbox.enabled=false",
            "lakehouse-flow.scheduling-intent-delivery.channel=HTTP",
            "lakehouse-flow.scheduling-intent-delivery.destination=http://127.0.0.1/disabled",
            "lakehouse-flow.scheduling-intent-delivery.publisher.enabled=false",
            "lakehouse-flow.job-control-intent-delivery.publisher.enabled=false",
            "lakehouse-flow.snapshot-confirmation.enabled=false",
            "lakehouse-flow.job-control-snapshot-confirmation.enabled=false",
            "lakehouse-flow.snapshot-sources.scanner.enabled=false",
            "lakehouse-flow.snapshot-sources.reconciliation.enabled=false",
            "lakehouse-flow.scheduling-backlog.metrics.fixed-delay-ms=3600000",
            "lakehouse-flow.scheduling-intent-delivery.metrics.fixed-delay-ms=3600000"
        })
class SchedulerRecoveryE2EIT {

    private static final Comparator<String> NUMERIC_OFFSET_COMPARATOR =
            Comparator.comparingLong(Long::parseLong);

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("lakehouse_flow_recovery")
            .withUsername("lakehouse_flow")
            .withPassword("lakehouse_flow");

    @Autowired
    private SchedulingIntentDeliveryService deliveryService;

    @Autowired
    private HttpSchedulingIntentPublisher httpPublisher;

    @Autowired
    private SchedulingTargetAdmissionService targetAdmissionService;

    @Autowired
    private SchedulingIntentService schedulingIntentService;

    @Autowired
    private SnapshotConfirmationService snapshotConfirmationService;

    @Autowired
    private SnapshotIngestionTransactionService snapshotIngestionService;

    @Autowired
    private FlowPlanService flowPlanService;

    @Autowired
    private WriterJobBindingService writerJobBindingService;

    @Autowired
    private WorkflowInstanceService workflowInstanceService;

    @Autowired
    private TaskInstanceService taskInstanceService;

    @Autowired
    private SchedulingIntentRepository schedulingIntentRepository;

    @Autowired
    private SchedulingIntentDeliveryRepository deliveryRepository;

    @Autowired
    private JobControlIntentRepository jobControlIntentRepository;

    @Autowired
    private JobControlIntentDeliveryRepository jobControlDeliveryRepository;

    @Autowired
    private SchedulingTargetAdmissionRepository targetAdmissionRepository;

    @Autowired
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private LakehouseEventRepository lakehouseEventRepository;

    @Autowired
    private EventConsumerOffsetRepository eventConsumerOffsetRepository;

    @Autowired
    private AssetStateRepository assetStateRepository;

    @Autowired
    private SnapshotSourceHealthRepository snapshotSourceHealthRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager transactionManager;

    /** Bind the primary scheduler context to the real PostgreSQL container. */
    @DynamicPropertySource
    static void configureDatabase(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
    }

    /** Remove domain rows between scenarios while preserving the migrated schema. */
    @BeforeEach
    void resetDomainState() {
        jdbcTemplate.execute("""
                TRUNCATE TABLE
                    flow_plan,
                    workflow_instance,
                    lakehouse_event,
                    asset_state,
                    snapshot_source_health,
                    event_consumer_offset,
                    scheduling_target_admission,
                    writer_job_binding
                RESTART IDENTITY CASCADE
                """);
    }

    /**
     * Verify two scheduler processes coordinate claims, reclaim crashes, and fence stale ACKs.
     *
     * @throws Exception when the peer context, HTTP endpoint, or race cannot be completed
     */
    @Test
    void reclaimsInterruptedHttpDeliveriesAcrossSchedulerProcesses() throws Exception {
        try (IdempotentHttpEndpoint endpoint = IdempotentHttpEndpoint.start();
                ConfigurableApplicationContext peer = openPeerContext()) {
            SchedulingIntentDeliveryService peerDeliveryService =
                    peer.getBean(SchedulingIntentDeliveryService.class);

            SchedulingIntentDelivery beforeHttp = createPendingHttpDelivery(
                    "before-http",
                    endpoint.url());
            List<RaceResult<List<SchedulingIntentPublication>>> competingClaims = race(
                    () -> deliveryService.claimDueDeliveries(
                            "scheduler-a", 1, Duration.ofSeconds(30)),
                    () -> peerDeliveryService.claimDueDeliveries(
                            "scheduler-b", 1, Duration.ofSeconds(30)));
            assertThat(competingClaims).allMatch(RaceResult::succeeded);
            List<SchedulingIntentPublication> schedulerAClaims = competingClaims.get(0).value();
            List<SchedulingIntentPublication> schedulerBClaims = competingClaims.get(1).value();
            assertThat(schedulerAClaims.size() + schedulerBClaims.size()).isEqualTo(1);

            boolean schedulerAWon = !schedulerAClaims.isEmpty();
            SchedulingIntentPublication abandoned = schedulerAWon
                    ? schedulerAClaims.get(0)
                    : schedulerBClaims.get(0);
            SchedulingIntentDeliveryService staleService = schedulerAWon
                    ? deliveryService
                    : peerDeliveryService;
            SchedulingIntentDeliveryService recoveryService = schedulerAWon
                    ? peerDeliveryService
                    : deliveryService;
            String recoveryOwner = schedulerAWon ? "scheduler-b" : "scheduler-a";

            expireClaim(beforeHttp.getId());
            SchedulingIntentPublication recovered = recoveryService.claimDueDeliveries(
                    recoveryOwner,
                    1,
                    Duration.ofSeconds(30)).get(0);
            assertThat(recovered.claimToken()).isNotEqualTo(abandoned.claimToken());
            assertThat(staleService.recordPublished(beforeHttp.getId(), abandoned.claimToken())).isFalse();
            httpPublisher.publish(recovered);
            assertThat(recoveryService.recordPublished(beforeHttp.getId(), recovered.claimToken())).isTrue();

            SchedulingIntentDelivery afterHttp = createPendingHttpDelivery(
                    "after-http",
                    endpoint.url());
            SchedulingIntentPublication deliveredBeforeCrash = deliveryService.claimDueDeliveries(
                    "scheduler-a",
                    1,
                    Duration.ofSeconds(30)).get(0);
            httpPublisher.publish(deliveredBeforeCrash);
            expireClaim(afterHttp.getId());
            SchedulingIntentPublication redelivered = peerDeliveryService.claimDueDeliveries(
                    "scheduler-b",
                    1,
                    Duration.ofSeconds(30)).get(0);
            assertThat(deliveryService.recordPublished(
                    afterHttp.getId(),
                    deliveredBeforeCrash.claimToken())).isFalse();
            httpPublisher.publish(redelivered);
            assertThat(peerDeliveryService.recordPublished(afterHttp.getId(), redelivered.claimToken())).isTrue();

            assertThat(endpoint.requestCount()).isEqualTo(3);
            assertThat(endpoint.uniqueIntentCount()).isEqualTo(2);
            assertThat(deliveryRepository.findById(beforeHttp.getId()).orElseThrow().getStatus())
                    .isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            SchedulingIntentDelivery recoveredAfterHttp = deliveryRepository.findById(afterHttp.getId())
                    .orElseThrow();
            assertThat(recoveredAfterHttp.getStatus())
                    .isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            assertThat(recoveredAfterHttp.getAttemptCount()).isEqualTo(2);
        }
    }

    /**
     * Verify PostgreSQL serializes target-date and Flow-version admission across schedulers.
     *
     * @throws Exception when the peer context or concurrent decisions cannot be completed
     */
    @Test
    void serializesTargetAndFlowAdmissionAcrossSchedulerProcesses() throws Exception {
        try (ConfigurableApplicationContext peer = openPeerContext()) {
            SchedulingTargetAdmissionService peerTargetAdmission =
                    peer.getBean(SchedulingTargetAdmissionService.class);
            SchedulingIntentService peerSchedulingIntent = peer.getBean(SchedulingIntentService.class);

            LocalDate targetDate = LocalDate.of(2026, 9, 14);
            List<RaceResult<SchedulingTargetAdmissionService.AdmissionDecision>> targetDecisions = race(
                    () -> targetAdmissionService.acquire(
                            "ha.ods.orders",
                            targetDate,
                            101L,
                            "task-instance:101",
                            "SCHEDULE",
                            Duration.ofMinutes(5)),
                    () -> peerTargetAdmission.acquire(
                            "ha.ods.orders",
                            targetDate,
                            102L,
                            "task-instance:102",
                            "BACKFILL",
                            Duration.ofMinutes(5)));
            assertThat(targetDecisions).allMatch(RaceResult::succeeded);
            assertThat(targetDecisions.stream().filter(result -> result.value().admitted())).hasSize(1);
            SchedulingTargetAdmissionService.AdmissionDecision winner = targetDecisions.stream()
                    .map(RaceResult::value)
                    .filter(SchedulingTargetAdmissionService.AdmissionDecision::admitted)
                    .findFirst()
                    .orElseThrow();
            assertThat(targetAdmissionRepository.findAll()).hasSize(1);

            expireTargetAdmission("ha.ods.orders", targetDate);
            SchedulingTargetAdmissionService.AdmissionDecision replacement = peerTargetAdmission.acquire(
                    "ha.ods.orders",
                    targetDate,
                    103L,
                    "task-instance:103",
                    "RECOVERY",
                    Duration.ofMinutes(5));
            assertThat(replacement.admitted()).isTrue();
            assertThat(replacement.reclaimedExpiredAdmission()).isTrue();
            assertThat(targetAdmissionService.release(
                    "ha.ods.orders",
                    targetDate,
                    winner.holderTaskInstanceId(),
                    "STALE_RELEASE")).isFalse();

            FlowFixture flow = createPublishedBatchFlow(
                    "ha-flow-concurrency",
                    "ha-output-writer",
                    "ha.output.orders",
                    1);
            ReadyTask first = createReadyTask(flow, LocalDate.of(2026, 9, 15), "first");
            ReadyTask second = createReadyTask(flow, LocalDate.of(2026, 9, 16), "second");
            List<RaceResult<SchedulingIntentService.TaskSchedulingIntent>> publications = race(
                    () -> schedulingIntentService.publishTaskIntent(first.task().getId()),
                    () -> peerSchedulingIntent.publishTaskIntent(second.task().getId()));

            assertThat(publications.stream().filter(RaceResult::succeeded)).hasSize(1);
            assertThat(publications.stream().filter(result -> !result.succeeded())).hasSize(1);
            assertThat(publications.stream()
                    .filter(result -> !result.succeeded())
                    .map(result -> result.failure().getMessage()))
                    .allMatch(message -> message.contains("active instance limit reached"));
            assertThat(schedulingIntentRepository.findAll()).hasSize(1);
            assertThat(taskInstanceRepository.findAll())
                    .filteredOn(task -> SchedulingStates.SCHEDULED.equals(task.getState()))
                    .hasSize(1);
            assertThat(taskInstanceRepository.findAll())
                    .filteredOn(task -> SchedulingStates.READY_TO_SCHEDULE.equals(task.getState()))
                    .hasSize(1);
        }
    }

    /**
     * Verify a rolled-back source projection is replayed and concurrent offsets never regress.
     *
     * @throws Exception when the peer context or concurrent ingestion cannot be completed
     */
    @Test
    void rollsBackAndRecoversSourceProjectionAcrossSchedulerProcesses() throws Exception {
        TransactionTemplate transactionTemplate = new TransactionTemplate(transactionManager);
        assertThatThrownBy(() -> transactionTemplate.executeWithoutResult(ignored -> {
            snapshotIngestionService.processSnapshot(
                    "ha.ods.source_orders",
                    "1",
                    NUMERIC_OFFSET_COMPARATOR,
                    sourceEvent("rollback", "1"));
            throw new SimulatedProcessInterruption("interrupt before source transaction commit");
        })).isInstanceOf(SimulatedProcessInterruption.class);

        assertThat(lakehouseEventRepository.findByEventId("ha-source-rollback")).isEmpty();
        assertThat(assetStateRepository.findByAssetKey("ha.ods.source_orders")).isEmpty();
        assertThat(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "PAIMON", "ha.ods.source_orders")).isEmpty();

        try (ConfigurableApplicationContext peer = openPeerContext()) {
            SnapshotIngestionTransactionService peerIngestion =
                    peer.getBean(SnapshotIngestionTransactionService.class);
            List<RaceResult<SnapshotIngestionTransactionService.SnapshotIngestionResult>> ingestions = race(
                    () -> snapshotIngestionService.processSnapshot(
                            "ha.ods.source_orders",
                            "10",
                            NUMERIC_OFFSET_COMPARATOR,
                            sourceEvent("snapshot-10", "10")),
                    () -> peerIngestion.processSnapshot(
                            "ha.ods.source_orders",
                            "11",
                            NUMERIC_OFFSET_COMPARATOR,
                            sourceEvent("snapshot-11", "11")));
            assertThat(ingestions).allMatch(RaceResult::succeeded);
        }

        assertThat(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "PAIMON", "ha.ods.source_orders").orElseThrow().getOffsetValue()).isEqualTo("11");
        assertThat(assetStateRepository.findByAssetKey("ha.ods.source_orders")
                .orElseThrow().getLatestSnapshotId()).isEqualTo("11");
        assertThat(lakehouseEventRepository.findAll()).hasSize(2);

        SnapshotIngestionTransactionService.SnapshotIngestionResult duplicate =
                snapshotIngestionService.processSnapshot(
                        "ha.ods.source_orders",
                        "11",
                        NUMERIC_OFFSET_COMPARATOR,
                        sourceEvent("snapshot-11", "11"));
        assertThat(duplicate.inserted()).isFalse();
        assertThat(lakehouseEventRepository.findAll()).hasSize(2);
        assertThat(eventConsumerOffsetRepository.findBySourceTypeAndSourceName(
                "PAIMON", "ha.ods.source_orders").orElseThrow().getOffsetValue()).isEqualTo("11");
    }

    /**
     * Verify a peer resumes confirmation after the event commit without duplicating DAG effects.
     *
     * @throws Exception when the peer context or concurrent confirmation cannot be completed
     */
    @Test
    void resumesSnapshotConfirmationIdempotentlyAcrossSchedulerProcesses() throws Exception {
        FlowFixture flow = createPublishedBatchFlow(
                "ha-confirmation-flow",
                "ha-confirmation-writer",
                "ha.confirm.orders",
                2);
        ReadyTask readyTask = createReadyTask(flow, LocalDate.of(2026, 9, 17), "confirmation");
        SchedulingIntentService.TaskSchedulingIntent publication =
                schedulingIntentService.publishTaskIntent(readyTask.task().getId());
        SchedulingIntent intent = schedulingIntentRepository.findById(publication.intentId()).orElseThrow();
        lakehouseEventRepository.save(attributedEvent(intent, "1"));

        try (ConfigurableApplicationContext peer = openPeerContext()) {
            SnapshotConfirmationService peerConfirmation = peer.getBean(SnapshotConfirmationService.class);
            List<RaceResult<SnapshotConfirmationResult>> confirmations = race(
                    () -> snapshotConfirmationService.checkTaskSnapshotProgress(
                            readyTask.task().getId(), Duration.ofMinutes(5)),
                    () -> peerConfirmation.checkTaskSnapshotProgress(
                            readyTask.task().getId(), Duration.ofMinutes(5)));
            assertThat(confirmations).allMatch(RaceResult::succeeded);
            assertThat(confirmations).allMatch(result -> result.value().snapshotAdvanced());
        }

        assertThat(taskInstanceRepository.findById(readyTask.task().getId()).orElseThrow().getState())
                .isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
        assertThat(workflowInstanceRepository.findById(readyTask.workflow().getId()).orElseThrow().getState())
                .isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
        assertThat(schedulingIntentRepository.findAll()).hasSize(1);
        assertThat(lakehouseEventRepository.findAll()).hasSize(1);
        assertThat(deliveryRepository.findBySchedulingIntentId(intent.getId()).orElseThrow().getStatus())
                .isEqualTo(SchedulingIntentDeliveryStatuses.PENDING);
        assertThat(targetAdmissionRepository.findAll()).allSatisfy(admission ->
                assertThat(admission.getStatus()).isEqualTo("AVAILABLE"));
    }

    /** Verify database-table delivery makes both immutable intent kinds atomically visible. */
    @Test
    void publishesDataAndControlIntentsThroughDatabaseTables() {
        try (ConfigurableApplicationContext databaseScheduler = openPeerContext(Map.of(
                "lakehouse-flow.scheduling-intent-delivery.channel", "DATABASE_TABLE",
                "lakehouse-flow.scheduling-intent-delivery.destination", "scheduling_intent",
                "lakehouse-flow.job-control-intent-delivery.channel", "DATABASE_TABLE",
                "lakehouse-flow.job-control-intent-delivery.destination", "job_control_intent"))) {
            SchedulingIntentService databaseIntentService =
                    databaseScheduler.getBean(SchedulingIntentService.class);
            JobControlIntentService databaseControlService =
                    databaseScheduler.getBean(JobControlIntentService.class);

            FlowFixture flow = createPublishedBatchFlow(
                    "database-delivery-flow",
                    "database-delivery-writer",
                    "ha.database.orders",
                    2);
            ReadyTask readyTask = createReadyTask(flow, LocalDate.of(2026, 9, 18), "database-delivery");
            SchedulingIntentService.TaskSchedulingIntent dataIntent =
                    databaseIntentService.publishTaskIntent(readyTask.task().getId());
            SchedulingIntentService.TaskSchedulingIntent duplicateDataIntent =
                    databaseIntentService.publishTaskIntent(readyTask.task().getId());

            writerJobBindingService.createBinding(new WriterJobBindingService.CreateWriterJobBindingCommand(
                    "database-control-writer",
                    "ha.database.control_orders",
                    List.of(ScheduleNodeProcessingModes.STREAMING)));
            JobControlIntentService.CreateJobControlIntentCommand command =
                    new JobControlIntentService.CreateJobControlIntentCommand(
                            "database-control-start",
                            "e2e",
                            "verify database-table control publication");
            JobControlIntentService.JobControlIntentView controlIntent =
                    databaseControlService.startJob("database-control-writer", command);
            JobControlIntentService.JobControlIntentView duplicateControlIntent =
                    databaseControlService.startJob("database-control-writer", command);

            assertThat(dataIntent.intentId()).isEqualTo(duplicateDataIntent.intentId());
            assertThat(dataIntent.deliveryChannel()).isEqualTo(SchedulingIntentDeliveryChannels.DATABASE_TABLE);
            assertThat(dataIntent.deliveryStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            assertThat(dataIntent.deliveryAttemptCount()).isEqualTo(1);
            assertThat(dataIntent.publishedAt()).isNotNull();
            assertThat(controlIntent.intentId()).isEqualTo(duplicateControlIntent.intentId());
            assertThat(controlIntent.deliveryChannel()).isEqualTo(SchedulingIntentDeliveryChannels.DATABASE_TABLE);
            assertThat(controlIntent.deliveryStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            assertThat(controlIntent.deliveryAttemptCount()).isEqualTo(1);
            assertThat(controlIntent.snapshotResult()).isEqualTo("WAITING");
            assertThat(schedulingIntentRepository.findAll()).hasSize(1);
            assertThat(deliveryRepository.findAll()).singleElement().satisfies(delivery -> {
                assertThat(delivery.getDestination()).isEqualTo("scheduling_intent");
                assertThat(delivery.getStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            });
            assertThat(jobControlIntentRepository.findAll()).hasSize(1);
            assertThat(jobControlDeliveryRepository.findAll()).singleElement().satisfies(delivery -> {
                assertThat(delivery.getDestination()).isEqualTo("job_control_intent");
                assertThat(delivery.getStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.PUBLISHED);
            });
            assertThat(workflowInstanceRepository.findAll()).hasSize(1);
            assertThat(taskInstanceRepository.findAll()).hasSize(1);
        }
    }

    /** Verify HTTP exhaustion remains independent from source and snapshot conclusions. */
    @Test
    void keepsDeliveryExhaustionSourceBlockingAndSnapshotTimeoutOrthogonal() {
        try (TimeoutHttpEndpoint endpoint = TimeoutHttpEndpoint.start();
                ConfigurableApplicationContext httpScheduler = openPeerContext(Map.ofEntries(
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.channel", "HTTP"),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.destination", endpoint.url()),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.http.request-timeout", "PT0.1S"),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.publisher.enabled", true),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.publisher.fixed-delay-ms", 3_600_000),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.publisher.max-attempts", 2),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.publisher.initial-backoff", "PT0.01S"),
                        Map.entry("lakehouse-flow.scheduling-intent-delivery.publisher.maximum-backoff", "PT0.01S"),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.channel", "HTTP"),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.destination", endpoint.url()),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.publisher.enabled", true),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.publisher.fixed-delay-ms", 3_600_000),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.publisher.max-attempts", 2),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.publisher.initial-backoff", "PT0.01S"),
                        Map.entry("lakehouse-flow.job-control-intent-delivery.publisher.maximum-backoff", "PT0.01S")))) {
            SchedulingIntentService httpIntentService = httpScheduler.getBean(SchedulingIntentService.class);
            JobControlIntentService httpControlService = httpScheduler.getBean(JobControlIntentService.class);
            SchedulingIntentDeliveryScanner dataScanner =
                    httpScheduler.getBean(SchedulingIntentDeliveryScanner.class);
            JobControlIntentDeliveryScanner controlScanner =
                    httpScheduler.getBean(JobControlIntentDeliveryScanner.class);

            FlowFixture blockedFlow = createPublishedBatchFlow(
                    "http-exhausted-flow",
                    "http-exhausted-writer",
                    "ha.http.exhausted_orders",
                    2);
            ReadyTask blockedTask = createReadyTask(
                    blockedFlow,
                    LocalDate.of(2026, 9, 19),
                    "http-exhausted");
            SchedulingIntentService.TaskSchedulingIntent dataIntent =
                    httpIntentService.publishTaskIntent(blockedTask.task().getId());

            writerJobBindingService.createBinding(new WriterJobBindingService.CreateWriterJobBindingCommand(
                    "http-control-writer",
                    "ha.http.control_orders",
                    List.of(ScheduleNodeProcessingModes.STREAMING)));
            JobControlIntentService.JobControlIntentView controlIntent = httpControlService.startJob(
                    "http-control-writer",
                    new JobControlIntentService.CreateJobControlIntentCommand(
                            "http-control-start",
                            "e2e",
                            "verify HTTP control exhaustion"));

            dataScanner.publishDueDeliveries();
            controlScanner.publishDueDeliveries();
            awaitRetryWindow(dataIntent.intentId(), controlIntent.intentId());
            dataScanner.publishDueDeliveries();
            controlScanner.publishDueDeliveries();

            assertThat(deliveryRepository.findBySchedulingIntentId(dataIntent.intentId()).orElseThrow())
                    .satisfies(delivery -> {
                        assertThat(delivery.getStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.EXHAUSTED);
                        assertThat(delivery.getAttemptCount()).isEqualTo(2);
                        assertThat(delivery.getDeadLetteredAt()).isNotNull();
                    });
            assertThat(jobControlDeliveryRepository.findByJobControlIntentId(controlIntent.intentId()).orElseThrow())
                    .satisfies(delivery -> {
                        assertThat(delivery.getStatus()).isEqualTo(SchedulingIntentDeliveryStatuses.EXHAUSTED);
                        assertThat(delivery.getAttemptCount()).isEqualTo(2);
                        assertThat(delivery.getDeadLetteredAt()).isNotNull();
                    });
            assertThat(endpoint.requestCount()).isEqualTo(4);
            assertThat(endpoint.uniqueIntentCount()).isEqualTo(2);

            SchedulingIntentDeliveryQueryService dataQuery =
                    httpScheduler.getBean(SchedulingIntentDeliveryQueryService.class);
            JobControlIntentDeliveryQueryService controlQuery =
                    httpScheduler.getBean(JobControlIntentDeliveryQueryService.class);
            assertThat(dataQuery.findDeadLetters("HTTP", 10)).singleElement().satisfies(deadLetter ->
                    assertThat(deadLetter.intentKey()).isEqualTo(dataIntent.intentKey()));
            assertThat(controlQuery.findDeadLetters("HTTP", 10)).singleElement().satisfies(deadLetter ->
                    assertThat(deadLetter.intentKey()).isEqualTo(controlIntent.intentKey()));

            moveTaskPastConfirmationDeadline(blockedTask.task().getId());
            SnapshotConfirmationResult sourceBlocked = snapshotConfirmationService.checkTaskSnapshotProgress(
                    blockedTask.task().getId(), Duration.ofMinutes(5));
            assertThat(sourceBlocked.resultingState()).isEqualTo(SchedulingStates.SCHEDULED);
            assertThat(sourceBlocked.sourceHealth()).isEqualTo(SnapshotSourceHealthOutcomes.SOURCE_BLOCKED);
            assertThat(deliveryRepository.findBySchedulingIntentId(dataIntent.intentId()).orElseThrow().getStatus())
                    .isEqualTo(SchedulingIntentDeliveryStatuses.EXHAUSTED);

            FlowFixture timeoutFlow = createPublishedBatchFlow(
                    "snapshot-timeout-flow",
                    "snapshot-timeout-writer",
                    "ha.timeout.orders",
                    2);
            ReadyTask timeoutTask = createReadyTask(
                    timeoutFlow,
                    LocalDate.of(2026, 9, 20),
                    "snapshot-timeout");
            SchedulingIntentService.TaskSchedulingIntent timeoutIntent =
                    httpIntentService.publishTaskIntent(timeoutTask.task().getId());
            persistHealthySource(timeoutFlow.tableAssetKey());
            moveTaskPastConfirmationDeadline(timeoutTask.task().getId());
            SnapshotConfirmationResult notAdvanced = snapshotConfirmationService.checkTaskSnapshotProgress(
                    timeoutTask.task().getId(), Duration.ofMinutes(5));
            assertThat(notAdvanced.resultingState()).isEqualTo(SchedulingStates.SNAPSHOT_NOT_ADVANCED);
            assertThat(notAdvanced.sourceHealth()).isEqualTo(SnapshotSourceHealthOutcomes.HEALTHY);
            assertThat(deliveryRepository.findBySchedulingIntentId(timeoutIntent.intentId()).orElseThrow().getStatus())
                    .isEqualTo(SchedulingIntentDeliveryStatuses.PENDING);
            assertThat(jobControlIntentRepository.findById(controlIntent.intentId()).orElseThrow().getSnapshotResult())
                    .isEqualTo("WAITING");
        }
    }

    /** Verify an existing V22 scheduling row survives the real V23 forward migration. */
    @Test
    void upgradesExistingTaskEvidenceFromV22ToV23() {
        String schema = "lf_v23_upgrade";
        jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        try {
            Flyway v22 = migrationFlyway(schema, MigrationVersion.fromVersion("22.0"));
            v22.migrate();
            Long workflowId = jdbcTemplate.queryForObject("""
                    INSERT INTO lf_v23_upgrade.workflow_instance(
                        instance_key, workflow_code, workflow_version, biz_date, trigger_type, state)
                    VALUES ('upgrade-workflow', 'upgrade-flow', 1, CURRENT_TIMESTAMP, 'E2E', 'SCHEDULED')
                    RETURNING id
                    """, Long.class);
            Long taskId = jdbcTemplate.queryForObject("""
                    INSERT INTO lf_v23_upgrade.task_instance(
                        instance_key, workflow_instance_id, task_code, task_version, biz_date, state, target_asset_key)
                    VALUES ('upgrade-task', ?, 'upgrade-node', 1, CURRENT_TIMESTAMP, 'SCHEDULED', 'ha.upgrade.orders')
                    RETURNING id
                    """, Long.class, workflowId);

            Flyway current = migrationFlyway(schema, null);
            current.migrate();
            current.validate();

            Integer sourceColumns = jdbcTemplate.queryForObject("""
                    SELECT COUNT(*)
                    FROM information_schema.columns
                    WHERE table_schema = ?
                      AND table_name = 'task_instance'
                      AND column_name IN ('source_health', 'source_health_detail', 'source_evidence_checked_at')
                    """, Integer.class, schema);
            assertThat(sourceColumns).isEqualTo(3);
            Map<String, Object> upgraded = jdbcTemplate.queryForMap(
                    "SELECT id, state, source_health FROM lf_v23_upgrade.task_instance WHERE id = ?",
                    taskId);
            assertThat(((Number) upgraded.get("id")).longValue()).isEqualTo(taskId);
            assertThat(upgraded.get("state")).isEqualTo(SchedulingStates.SCHEDULED);
            assertThat(upgraded.get("source_health")).isNull();
        } finally {
            jdbcTemplate.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
        }
    }

    /** Wait until both transport retries are due after their first failed HTTP attempts. */
    private void awaitRetryWindow(Long schedulingIntentId, Long jobControlIntentId) {
        await().atMost(Duration.ofSeconds(5)).until(() -> {
            SchedulingIntentDelivery dataDelivery = deliveryRepository
                    .findBySchedulingIntentId(schedulingIntentId)
                    .orElseThrow();
            JobControlIntentDelivery controlDelivery = jobControlDeliveryRepository
                    .findByJobControlIntentId(jobControlIntentId)
                    .orElseThrow();
            LocalDateTime now = LocalDateTime.now();
            return SchedulingIntentDeliveryStatuses.RETRY_WAIT.equals(dataDelivery.getStatus())
                    && SchedulingIntentDeliveryStatuses.RETRY_WAIT.equals(controlDelivery.getStatus())
                    && dataDelivery.getNextAttemptAt() != null
                    && controlDelivery.getNextAttemptAt() != null
                    && !dataDelivery.getNextAttemptAt().isAfter(now)
                    && !controlDelivery.getNextAttemptAt().isAfter(now);
        });
    }

    /** Move one scheduled task behind its immutable five-minute confirmation deadline. */
    private void moveTaskPastConfirmationDeadline(Long taskInstanceId) {
        TaskInstance task = taskInstanceRepository.findById(taskInstanceId).orElseThrow();
        task.setScheduledAt(LocalDateTime.now().minusMinutes(6));
        taskInstanceRepository.saveAndFlush(task);
    }

    /** Persist a current source proof that permits a trustworthy not-advanced conclusion. */
    private void persistHealthySource(String tableAssetKey) {
        snapshotSourceHealthRepository.saveAndFlush(SnapshotSourceHealth.builder()
                .sourceType("PAIMON")
                .sourceName("e2e:" + tableAssetKey)
                .tableAssetKey(tableAssetKey)
                .outcome(SnapshotSourceHealthOutcomes.HEALTHY)
                .offsetStatus("CAUGHT_UP")
                .projectionStatus("CONSISTENT")
                .durableOffset("0")
                .latestSourceOffset("0")
                .evidenceCheckedAt(LocalDateTime.now())
                .detail("E2E source range and projection are complete")
                .build());
    }

    /** Configure Flyway against one isolated schema and an optional target version. */
    private Flyway migrationFlyway(String schema, MigrationVersion target) {
        org.flywaydb.core.api.configuration.FluentConfiguration configuration = Flyway.configure()
                .dataSource(POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword())
                .defaultSchema(schema)
                .schemas(schema)
                .locations("classpath:db/migration");
        if (target != null) {
            configuration.target(target);
        }
        return configuration.load();
    }

    /** Create a pending HTTP delivery backed by real workflow, task, and intent rows. */
    private SchedulingIntentDelivery createPendingHttpDelivery(String suffix, String destination) {
        LocalDateTime now = LocalDateTime.now();
        WorkflowInstance workflow = workflowInstanceService.createInstance(
                "delivery-recovery",
                1,
                now,
                "E2E",
                "delivery-" + suffix,
                "delivery recovery fixture");
        TaskInstance task = taskInstanceService.createInstance(
                workflow.getId(),
                "delivery-" + suffix,
                1,
                now,
                "ha.delivery." + suffix.replace('-', '_'));
        String intentKey = "ha-delivery:" + suffix;
        SchedulingIntent intent = schedulingIntentRepository.save(SchedulingIntent.builder()
                .contractVersion(SchedulingIntentContract.CONTRACT_VERSION)
                .intentKey(intentKey)
                .taskInstanceId(task.getId())
                .workflowInstanceId(workflow.getId())
                .triggerType("E2E")
                .taskCode(task.getTaskCode())
                .taskVersion(1)
                .bizDate(now)
                .targetAssetKey(task.getTargetAssetKey())
                .writerJobKey("writer-" + suffix)
                .writerEpoch(1L)
                .processingMode(ScheduleNodeProcessingModes.BATCH)
                .inputSnapshotVectorJson(List.of())
                .instructionPayloadJson(Map.of(
                        "intentKey", intentKey,
                        "contractVersion", SchedulingIntentContract.CONTRACT_VERSION))
                .createdAt(now)
                .build());
        return deliveryRepository.save(SchedulingIntentDelivery.builder()
                .schedulingIntentId(intent.getId())
                .channel(SchedulingIntentDeliveryChannels.HTTP)
                .destination(destination)
                .status(SchedulingIntentDeliveryStatuses.PENDING)
                .attemptCount(0)
                .deliverBefore(now.plusMinutes(5))
                .createdAt(now)
                .updatedAt(now)
                .build());
    }

    /** Force one claimed delivery lease into the reclaimable past. */
    private void expireClaim(Long deliveryId) {
        int updated = jdbcTemplate.update(
                "UPDATE scheduling_intent_delivery "
                        + "SET claim_expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' WHERE id = ?",
                deliveryId);
        assertThat(updated).isEqualTo(1);
    }

    /** Force one target-date admission lease into the reclaimable past. */
    private void expireTargetAdmission(String targetAssetKey, LocalDate bizDate) {
        int updated = jdbcTemplate.update(
                "UPDATE scheduling_target_admission "
                        + "SET expires_at = CURRENT_TIMESTAMP - INTERVAL '1 second' "
                        + "WHERE target_asset_key = ? AND biz_date = ?",
                targetAssetKey,
                bizDate);
        assertThat(updated).isEqualTo(1);
    }

    /** Register and publish one single-node batch Flow with a bounded active-instance limit. */
    private FlowFixture createPublishedBatchFlow(
            String flowCode,
            String writerJobKey,
            String tableAssetKey,
            int maxActiveInstances) {
        writerJobBindingService.createBinding(new WriterJobBindingService.CreateWriterJobBindingCommand(
                writerJobKey,
                tableAssetKey,
                List.of(ScheduleNodeProcessingModes.BATCH)));
        FlowPlan plan = flowPlanService.createDraftPlan(new FlowPlanService.CreateFlowPlanCommand(
                flowCode,
                flowCode,
                "ha-e2e",
                "e2e",
                "scheduler recovery fixture"));
        FlowPlanVersion version = flowPlanService.createDraftVersion(
                new FlowPlanService.CreateFlowPlanVersionCommand(
                        plan.getId(),
                        1,
                        Map.of(),
                        Map.of(),
                        Map.of("type", "SNAPSHOT_DRIVEN"),
                        Map.of("timeout", "PT5M"),
                        Map.of("mode", "SERIAL_WAIT", "maxActiveInstances", maxActiveInstances)));
        ScheduleNode node = flowPlanService.addNode(new FlowPlanService.CreateScheduleNodeCommand(
                version.getId(),
                flowCode + "-node",
                flowCode + " node",
                "ASSET_OUTPUT",
                ScheduleNodeProcessingModes.BATCH,
                List.of(),
                Map.of(),
                tableAssetKey + ".dt=${bizDate}",
                writerJobKey,
                Map.of("timeout", "PT5M"),
                10));
        FlowPlanVersion published = flowPlanService.publishVersion(version.getId(), "e2e");
        return new FlowFixture(published, node, tableAssetKey);
    }

    /** Create one READY_TO_SCHEDULE workflow and task linked to a published Flow node. */
    private ReadyTask createReadyTask(FlowFixture flow, LocalDate bizDate, String suffix) {
        LocalDateTime bizTime = bizDate.atStartOfDay();
        WorkflowInstance workflow = workflowInstanceService.createInstance(
                flow.version().getFlowCode(),
                flow.version().getVersion(),
                bizTime,
                "E2E",
                "trigger-" + suffix,
                "scheduler recovery fixture",
                flow.version().getId());
        TaskInstance task = taskInstanceService.createInstance(
                workflow.getId(),
                flow.node().getNodeCode(),
                1,
                bizTime,
                flow.tableAssetKey() + ".dt=" + bizDate,
                flow.version().getId(),
                flow.node().getId());
        workflowInstanceService.markSchedulable(workflow.getId());
        taskInstanceService.markSchedulable(task.getId());
        return new ReadyTask(workflow, task);
    }

    /** Build one source event for transaction, offset, and monotonic projection checks. */
    private LakehouseEvent sourceEvent(String eventSuffix, String snapshotId) {
        LocalDateTime eventTime = LocalDateTime.of(2026, 9, 14, 0, 0)
                .plusSeconds(Long.parseLong(snapshotId));
        return LakehouseEvent.builder()
                .eventId("ha-source-" + eventSuffix)
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName("ha")
                .databaseName("ods")
                .tableName("source_orders")
                .snapshotId(snapshotId)
                .watermark(eventTime)
                .commitKind("APPEND")
                .dataChange(true)
                .commitTime(eventTime)
                .payloadJson(Map.of())
                .observedAt(eventTime)
                .build();
    }

    /** Build one final data snapshot carrying every property frozen in an intent. */
    private LakehouseEvent attributedEvent(SchedulingIntent intent, String snapshotId) {
        String[] target = intent.getTargetAssetKey().split("\\.", 4);
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put(SnapshotEvidenceContract.SOURCE_PROPERTY, SnapshotEvidenceContract.INTENT_SOURCE);
        properties.put(SnapshotEvidenceContract.INTENT_KEY_PROPERTY, intent.getIntentKey());
        properties.put(SnapshotEvidenceContract.WRITER_JOB_KEY_PROPERTY, intent.getWriterJobKey());
        properties.put(SnapshotEvidenceContract.WRITER_EPOCH_PROPERTY, intent.getWriterEpoch().toString());
        properties.put(SnapshotEvidenceContract.TARGET_ASSET_PROPERTY, intent.getTargetAssetKey());
        properties.put(SnapshotEvidenceContract.BIZ_DATE_PROPERTY, intent.getBizDate().toLocalDate().toString());
        properties.put(SnapshotEvidenceContract.FINAL_PROPERTY, SnapshotEvidenceContract.FINAL_VALUE);
        return LakehouseEvent.builder()
                .eventId("ha-confirmation-" + intent.getIntentKey())
                .eventType("SNAPSHOT_COMMITTED")
                .sourceType("PAIMON")
                .catalogName(target[0])
                .databaseName(target[1])
                .tableName(target[2])
                .partitionName(target[3])
                .snapshotId(snapshotId)
                .watermark(intent.getBizDate())
                .commitKind("APPEND")
                .dataChange(true)
                .commitTime(LocalDateTime.now())
                .payloadJson(Map.of(
                        "snapshotProperties", Map.copyOf(properties),
                        "changedPartitions", List.of(target[3])))
                .observedAt(intent.getCreatedAt().plusNanos(1))
                .build();
    }

    /** Start one independent scheduler application context against the shared database. */
    private ConfigurableApplicationContext openPeerContext() {
        return openPeerContext(Map.of());
    }

    /** Start one peer scheduler with scenario-specific transport properties. */
    private ConfigurableApplicationContext openPeerContext(Map<String, Object> overrides) {
        Map<String, Object> properties = peerProperties();
        properties.putAll(overrides);
        return new SpringApplicationBuilder(OrderToGmvE2EIT.E2EApplication.class)
                .web(WebApplicationType.NONE)
                .properties(properties)
                .run();
    }

    /** Build properties that disable background scans while preserving production services. */
    private Map<String, Object> peerProperties() {
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("spring.datasource.url", POSTGRES.getJdbcUrl());
        properties.put("spring.datasource.username", POSTGRES.getUsername());
        properties.put("spring.datasource.password", POSTGRES.getPassword());
        properties.put("spring.datasource.driver-class-name", "org.postgresql.Driver");
        properties.put("spring.flyway.enabled", false);
        properties.put("spring.jpa.hibernate.ddl-auto", "validate");
        properties.put("lakehouse-flow.scheduling-intent-outbox.enabled", false);
        properties.put("lakehouse-flow.scheduling-intent-delivery.channel", "HTTP");
        properties.put(
                "lakehouse-flow.scheduling-intent-delivery.destination",
                "http://127.0.0.1/disabled");
        properties.put("lakehouse-flow.scheduling-intent-delivery.publisher.enabled", false);
        properties.put("lakehouse-flow.job-control-intent-delivery.publisher.enabled", false);
        properties.put("lakehouse-flow.snapshot-confirmation.enabled", false);
        properties.put("lakehouse-flow.job-control-snapshot-confirmation.enabled", false);
        properties.put("lakehouse-flow.snapshot-sources.scanner.enabled", false);
        properties.put("lakehouse-flow.snapshot-sources.reconciliation.enabled", false);
        properties.put("lakehouse-flow.scheduling-backlog.metrics.fixed-delay-ms", 3_600_000);
        properties.put("lakehouse-flow.scheduling-intent-delivery.metrics.fixed-delay-ms", 3_600_000);
        return properties;
    }

    /** Run two operations at the same instant and capture values or failures deterministically. */
    private <T> List<RaceResult<T>> race(Callable<T> first, Callable<T> second) throws Exception {
        CountDownLatch start = new CountDownLatch(1);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            List<Future<RaceResult<T>>> futures = new ArrayList<>();
            futures.add(executor.submit(() -> invokeAfter(start, first)));
            futures.add(executor.submit(() -> invokeAfter(start, second)));
            start.countDown();
            List<RaceResult<T>> results = new ArrayList<>();
            for (Future<RaceResult<T>> future : futures) {
                results.add(future.get());
            }
            return List.copyOf(results);
        } finally {
            executor.shutdownNow();
        }
    }

    /** Wait for the race signal and preserve one operation failure as test evidence. */
    private <T> RaceResult<T> invokeAfter(CountDownLatch start, Callable<T> operation) {
        try {
            start.await();
            return RaceResult.success(operation.call());
        } catch (Throwable failure) {
            return RaceResult.failure(failure);
        }
    }

    /** Published Flow definition used by one recovery scenario. */
    private record FlowFixture(
            FlowPlanVersion version,
            ScheduleNode node,
            String tableAssetKey) {
    }

    /** Ready scheduler-side workflow and task pair. */
    private record ReadyTask(
            WorkflowInstance workflow,
            TaskInstance task) {
    }

    /** Captured outcome of one concurrent scheduler operation. */
    private record RaceResult<T>(T value, Throwable failure) {

        /** Build one successful race result. */
        private static <T> RaceResult<T> success(T value) {
            return new RaceResult<>(value, null);
        }

        /** Build one failed race result. */
        private static <T> RaceResult<T> failure(Throwable failure) {
            return new RaceResult<>(null, failure);
        }

        /** Return whether the operation completed without throwing. */
        private boolean succeeded() {
            return failure == null;
        }
    }

    /** Explicit test interruption used to force the outer ingestion transaction to roll back. */
    private static final class SimulatedProcessInterruption extends RuntimeException {

        /** Create one deterministic interruption at a transaction boundary. */
        private SimulatedProcessInterruption(String message) {
            super(message);
        }
    }

    /** HTTP endpoint that exceeds every request timeout while recording idempotency keys. */
    private static final class TimeoutHttpEndpoint implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final AtomicInteger requestCount = new AtomicInteger();
        private final Set<String> uniqueIntentKeys = ConcurrentHashMap.newKeySet();

        /** Create and start one deterministic slow endpoint. */
        private TimeoutHttpEndpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newCachedThreadPool();
            server.setExecutor(executor);
            server.createContext("/intents", this::receive);
            server.start();
        }

        /** Start one endpoint and convert checked setup failures into test failures. */
        private static TimeoutHttpEndpoint start() {
            try {
                return new TimeoutHttpEndpoint();
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to start timeout HTTP endpoint", exception);
            }
        }

        /** Record the physical attempt and delay acknowledgement beyond the client deadline. */
        private void receive(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            requestCount.incrementAndGet();
            uniqueIntentKeys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            try {
                Thread.sleep(500);
                exchange.sendResponseHeaders(202, -1);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            } finally {
                exchange.close();
            }
        }

        /** Return the deliberately slow publication destination. */
        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/intents";
        }

        /** Return all timed-out physical HTTP attempts. */
        private int requestCount() {
            return requestCount.get();
        }

        /** Return unique downstream idempotency keys observed across retries. */
        private int uniqueIntentCount() {
            return uniqueIntentKeys.size();
        }

        /** Stop the endpoint and its request executor. */
        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }

    /** Downstream HTTP consumer that applies the required intent-key idempotency contract. */
    private static final class IdempotentHttpEndpoint implements AutoCloseable {

        private final HttpServer server;
        private final ExecutorService executor;
        private final AtomicInteger requestCount = new AtomicInteger();
        private final Set<String> uniqueIntentKeys = ConcurrentHashMap.newKeySet();

        /** Create and start one local endpoint. */
        private IdempotentHttpEndpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newSingleThreadExecutor();
            server.setExecutor(executor);
            server.createContext("/intents", this::receive);
            server.start();
        }

        /** Start one endpoint and convert checked setup failures into test failures. */
        private static IdempotentHttpEndpoint start() {
            try {
                return new IdempotentHttpEndpoint();
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to start recovery HTTP endpoint", exception);
            }
        }

        /** Record every delivery while admitting each idempotency key only once. */
        private void receive(HttpExchange exchange) throws IOException {
            exchange.getRequestBody().readAllBytes();
            requestCount.incrementAndGet();
            uniqueIntentKeys.add(exchange.getRequestHeaders().getFirst("Idempotency-Key"));
            exchange.sendResponseHeaders(202, -1);
            exchange.close();
        }

        /** Return the local publication destination. */
        private String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/intents";
        }

        /** Return all physical HTTP attempts. */
        private int requestCount() {
            return requestCount.get();
        }

        /** Return downstream work admitted after intent-key deduplication. */
        private int uniqueIntentCount() {
            return uniqueIntentKeys.size();
        }

        /** Stop the endpoint and its request executor. */
        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }
    }
}
