package io.github.lakehouseflow.e2e.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.github.lakehouseflow.common.SchedulingStates;
import io.github.lakehouseflow.dao.JobControlIntentRepository;
import io.github.lakehouseflow.dao.LakehouseEventRepository;
import io.github.lakehouseflow.dao.SchedulingIntentRepository;
import io.github.lakehouseflow.dao.TaskInstanceRepository;
import io.github.lakehouseflow.dao.WorkflowInstanceRepository;
import io.github.lakehouseflow.integration.event.SnapshotSourceReconciliationService;
import io.github.lakehouseflow.model.JobControlIntent;
import io.github.lakehouseflow.model.LakehouseEvent;
import io.github.lakehouseflow.model.SchedulingIntent;
import io.github.lakehouseflow.model.TaskInstance;
import io.github.lakehouseflow.model.WorkflowInstance;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Bean;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.ScheduledAnnotationBeanPostProcessor;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * System-level Testcontainers proof for the streaming ODS to batch ADS GMV chain.
 *
 * <p>Lakehouse Flow is configured only through its public REST API. Its HTTP delivery is
 * consumed by a test-only execution endpoint that immediately submits real jobs to Flink.
 * Assertions use Lakehouse Flow snapshot evidence, never Flink runtime state, as the result.
 */
@Testcontainers
@ActiveProfiles("e2e")
@SpringBootTest(
        classes = OrderToGmvE2EIT.E2EApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class OrderToGmvE2EIT {

    private static final String FLINK_IMAGE = "flink:1.20.3-scala_2.12-java17";
    private static final String FLINK_JOB_MANAGER_ALIAS = "flink-jobmanager";
    private static final String MYSQL_ALIAS = "orders-mysql";
    private static final String POSTGRES_ALIAS = "lakehouse-flow-postgres";
    private static final String JOB_JAR_IN_CONTAINER = "/opt/lakehouse-flow/e2e-jobs.jar";
    private static final String CATALOG = "e2e";
    private static final String BIZ_DATE_TEMPLATE = "dt=${bizDate}";
    private static final Duration E2E_TIMEOUT = Duration.ofMinutes(5);
    private static final Duration STREAM_START_TIMEOUT = Duration.ofSeconds(30);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();
    private static final Network NETWORK = Network.newNetwork();
    private static final Path WAREHOUSE = createWarehouse();
    private static final IntentExecutionEndpoint EXECUTION_ENDPOINT = IntentExecutionEndpoint.start();
    private static ScheduledAnnotationBeanPostProcessor scheduledTaskProcessor;

    @Container
    private static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>("postgres:16-alpine")
            .withDatabaseName("lakehouse_flow")
            .withUsername("lakehouse_flow")
            .withPassword("lakehouse_flow")
            .withNetwork(NETWORK)
            .withNetworkAliases(POSTGRES_ALIAS);

    @Container
    private static final MySQLContainer<?> MYSQL = new MySQLContainer<>("mysql:8.0.36")
            .withDatabaseName("commerce")
            .withUsername("orders")
            .withPassword("orders")
            .withCopyFileToContainer(
                    MountableFile.forClasspathResource("mysql/init.sql"),
                    "/docker-entrypoint-initdb.d/10-lakehouse-flow.sql")
            .withCommand(
                    "--server-id=1",
                    "--log-bin=mysql-bin",
                    "--binlog-format=ROW",
                    "--binlog-row-image=FULL")
            .withNetwork(NETWORK)
            .withNetworkAliases(MYSQL_ALIAS);

    @Container
    private static final GenericContainer<?> FLINK_JOB_MANAGER = new GenericContainer<>(
            DockerImageName.parse(FLINK_IMAGE))
            .withNetwork(NETWORK)
            .withNetworkAliases(FLINK_JOB_MANAGER_ALIAS)
            .withFileSystemBind(WAREHOUSE.toString(), WAREHOUSE.toString(), BindMode.READ_WRITE)
            .withEnv("FLINK_PROPERTIES", flinkProperties())
            .withExposedPorts(8081)
            .withCommand("jobmanager")
            .waitingFor(Wait.forHttp("/overview")
                    .forPort(8081)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @Container
    private static final GenericContainer<?> FLINK_TASK_MANAGER = new GenericContainer<>(
            DockerImageName.parse(FLINK_IMAGE))
            .withNetwork(NETWORK)
            .withFileSystemBind(WAREHOUSE.toString(), WAREHOUSE.toString(), BindMode.READ_WRITE)
            .withEnv("FLINK_PROPERTIES", flinkProperties())
            .withCommand("taskmanager")
            .dependsOn(FLINK_JOB_MANAGER)
            .waitingFor(Wait.forLogMessage(".*Successful registration at resource manager.*\\n", 1)
                    .withStartupTimeout(Duration.ofMinutes(2)));

    @LocalServerPort
    private int serverPort;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private WorkflowInstanceRepository workflowInstanceRepository;

    @Autowired
    private TaskInstanceRepository taskInstanceRepository;

    @Autowired
    private SchedulingIntentRepository schedulingIntentRepository;

    @Autowired
    private JobControlIntentRepository jobControlIntentRepository;

    @Autowired
    private LakehouseEventRepository lakehouseEventRepository;

    @Autowired
    private SnapshotSourceReconciliationService snapshotSourceReconciliationService;

    /** Keep local random-port API calls outside host-level HTTP proxy configuration. */
    @BeforeEach
    void bypassHostProxyForLocalApi() {
        SimpleClientHttpRequestFactory requestFactory = new SimpleClientHttpRequestFactory();
        requestFactory.setProxy(Proxy.NO_PROXY);
        restTemplate.getRestTemplate().setRequestFactory(requestFactory);
    }

    /** Test-only application shell that assembles every production scheduler layer. */
    @SpringBootConfiguration
    @EnableAutoConfiguration
    @EnableScheduling
    @ComponentScan(basePackages = "io.github.lakehouseflow")
    @EntityScan(basePackages = "io.github.lakehouseflow.model")
    @EnableJpaRepositories(basePackages = "io.github.lakehouseflow.dao")
    static class E2EApplication {

        /** Provide the metrics registry normally supplied by the production Boot module. */
        @Bean
        MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    /** Supply real container endpoints and the shared Paimon warehouse to Spring Boot. */
    @DynamicPropertySource
    static void configureApplication(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", POSTGRES::getJdbcUrl);
        registry.add("spring.datasource.username", POSTGRES::getUsername);
        registry.add("spring.datasource.password", POSTGRES::getPassword);
        registry.add("spring.datasource.driver-class-name", () -> "org.postgresql.Driver");
        registry.add("lakehouse-flow.scheduling-intent-delivery.channel", () -> "HTTP");
        registry.add("lakehouse-flow.scheduling-intent-delivery.destination", EXECUTION_ENDPOINT::url);
        registry.add("lakehouse-flow.scheduling-intent-delivery.publisher.enabled", () -> true);
        registry.add("lakehouse-flow.scheduling-intent-delivery.publisher.fixed-delay-ms", () -> 100);
        registry.add("lakehouse-flow.job-control-intent-delivery.channel", () -> "HTTP");
        registry.add("lakehouse-flow.job-control-intent-delivery.destination", EXECUTION_ENDPOINT::url);
        registry.add("lakehouse-flow.job-control-intent-delivery.publisher.enabled", () -> true);
        registry.add("lakehouse-flow.job-control-intent-delivery.publisher.fixed-delay-ms", () -> 100);
        registry.add("lakehouse-flow.scheduling-intent-outbox.enabled", () -> true);
        registry.add("lakehouse-flow.scheduling-intent-outbox.fixed-delay-ms", () -> 100);
        registry.add("lakehouse-flow.snapshot-confirmation.enabled", () -> true);
        registry.add("lakehouse-flow.snapshot-confirmation.fixed-delay-ms", () -> 100);
        registry.add("lakehouse-flow.snapshot-confirmation.timeout", () -> "PT2M");
        registry.add("lakehouse-flow.job-control-snapshot-confirmation.enabled", () -> true);
        registry.add("lakehouse-flow.job-control-snapshot-confirmation.fixed-delay-ms", () -> 100);
        registry.add("lakehouse-flow.job-control-intent-delivery.confirmation-timeout", () -> "PT2M");
        registry.add("lakehouse-flow.snapshot-sources.scanner.enabled", () -> true);
        registry.add("lakehouse-flow.snapshot-sources.scanner.fixed-delay-ms", () -> 250);
        registry.add("lakehouse-flow.snapshot-sources.scanner.initial-delay-ms", () -> 10_000);
        registry.add("lakehouse-flow.snapshot-sources.paimon.enabled", () -> true);
        registry.add("lakehouse-flow.snapshot-sources.paimon.zone-id", () -> "Asia/Shanghai");
        registry.add("lakehouse-flow.snapshot-sources.paimon.catalogs[0].name", () -> CATALOG);
        registry.add(
                "lakehouse-flow.snapshot-sources.paimon.catalogs[0].options[warehouse]",
                () -> WAREHOUSE.toUri().toString());
        registry.add(
                "lakehouse-flow.snapshot-sources.paimon.catalogs[0].options[metastore]",
                () -> "filesystem");
        registerPaimonTable(registry, 0, "ods", "orders");
        registerPaimonTable(registry, 1, "dwd", "order_detail");
        registerPaimonTable(registry, 2, "dws", "daily_gmv");
        registerPaimonTable(registry, 3, "ads", "gmv");
    }

    /** Copy the job artifact, initialize Paimon tables, and activate the passive HTTP executor. */
    @BeforeAll
    static void prepareExecutionEndpoint(ConfigurableApplicationContext context) throws Exception {
        scheduledTaskProcessor = context.getBean(ScheduledAnnotationBeanPostProcessor.class);
        Path jobJar = Path.of(System.getProperty("lakehouse.flow.e2e.job-jar")).toAbsolutePath();
        assertThat(jobJar).exists();
        FLINK_JOB_MANAGER.copyFileToContainer(
                MountableFile.forHostPath(jobJar),
                JOB_JAR_IN_CONTAINER);
        EXECUTION_ENDPOINT.configure(new ExecutionEnvironment(
                FLINK_JOB_MANAGER,
                URI.create("http://" + FLINK_JOB_MANAGER.getHost() + ":"
                        + FLINK_JOB_MANAGER.getMappedPort(8081)),
                URI.create("http://127.0.0.1:"
                        + context.getEnvironment().getRequiredProperty("local.server.port") + "/"),
                WAREHOUSE.toUri().toString(),
                MYSQL_ALIAS,
                MYSQL.getDatabaseName(),
                MYSQL.getUsername(),
                MYSQL.getPassword(),
                "jdbc:postgresql://" + POSTGRES_ALIAS + ":5432/" + POSTGRES.getDatabaseName(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()));
        EXECUTION_ENDPOINT.initializeTables();
    }

    /** Stop scheduler polling and the local endpoint before Testcontainers removes databases. */
    @AfterAll
    static void stopExecutionEndpoint() {
        scheduledTaskProcessor.destroy();
        EXECUTION_ENDPOINT.close();
    }

    /**
     * Verify ordinary scheduling, lifecycle control, every release-critical action, and mixed DAG recovery.
     *
     * @throws Exception when test data insertion or final Paimon inspection fails
     */
    @Test
    void streamsOrdersAndBuildsAttributedGmvSnapshots() throws Exception {
        long flowPlanVersionId = configureOrderToGmvFlow();

        Map<String, Object> startControl = post(
                "/api/v1/writer-jobs/orders-ods-stream/start",
                Map.of(
                        "requestKey", "e2e-start-orders-ods",
                        "requestedBy", "e2e",
                        "reason", "start the CDC writer"));
        String startIntentKey = stringValue(startControl, "intentKey");
        EXECUTION_ENDPOINT.awaitSuccess(startIntentKey, E2E_TIMEOUT);

        LocalDate businessDate = LocalDate.now(ZoneId.of("Asia/Shanghai"));
        insertOrdersAfterCdcStarted(businessDate);

        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertSchedulerSnapshotOutcome(businessDate, 1));

        EXECUTION_ENDPOINT.verifyGmv(businessDate, 12_500L);

        Map<String, Object> restartControl = post(
                "/api/v1/writer-jobs/orders-ods-stream/restart",
                Map.of(
                        "requestKey", "e2e-restart-orders-ods",
                        "requestedBy", "e2e",
                        "reason", "verify writer epoch fencing"));
        assertThat(longValue(restartControl, "previousWriterEpoch")).isEqualTo(1L);
        assertThat(longValue(restartControl, "writerEpoch")).isEqualTo(2L);
        String restartIntentKey = stringValue(restartControl, "intentKey");
        EXECUTION_ENDPOINT.awaitSuccess(restartIntentKey, E2E_TIMEOUT);
        EXECUTION_ENDPOINT.verifyStaleEpochRejected(startIntentKey);

        insertOrderAfterRestart(businessDate);
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertSchedulerSnapshotOutcome(businessDate, 2));

        EXECUTION_ENDPOINT.verifyGmv(businessDate, 15_500L);
        verifyNodeBackfill(flowPlanVersionId, businessDate);
        verifyWorkflowRerun(businessDate);
        verifyTaskRerun(businessDate);
        verifyNodeRerun(flowPlanVersionId, businessDate);
        verifyFullFlowBackfill(businessDate);
        verifyBackfillRecovery(flowPlanVersionId, businessDate);
        verifyMixedInputEvidence();
        EXECUTION_ENDPOINT.verifyGmv(businessDate, 15_500L);
        assertThat(EXECUTION_ENDPOINT.successfulJobTypes())
                .contains("ODS", "ODS_REPLAY", "DWD", "DWS", "ADS");
        assertThat(EXECUTION_ENDPOINT.failures()).isEmpty();
    }

    /** Register four sole writers and publish the mixed STREAMING/BATCH FlowPlan through REST. */
    private long configureOrderToGmvFlow() {
        createWriter("orders-ods-stream", "e2e.ods.orders", List.of("STREAMING"));
        createWriter("order-detail-batch", "e2e.dwd.order_detail", List.of("BATCH"));
        createWriter("daily-gmv-batch", "e2e.dws.daily_gmv", List.of("BATCH"));
        createWriter("gmv-ads-batch", "e2e.ads.gmv", List.of("BATCH"));

        Map<String, Object> plan = post("/api/v1/flow-plans", Map.of(
                "flowCode", "order-to-gmv",
                "flowName", "Order to GMV",
                "flowSpaceCode", "e2e",
                "owner", "e2e",
                "description", "Flink CDC ODS followed by Flink batch DWD, DWS and ADS"));
        long planId = longValue(plan, "id");
        Map<String, Object> rootDependency = Map.of(
                "operator", "AND",
                "conditions", List.of(Map.of(
                        "type", "SNAPSHOT_ADVANCED",
                        "assetKey", "e2e.ods.orders." + BIZ_DATE_TEMPLATE)));
        Map<String, Object> version = post(
                "/api/v1/flow-plans/" + planId + "/versions",
                Map.of(
                        "version", 1,
                        "graphJson", Map.of(),
                        "dependencySpecJson", Map.of(),
                        "triggerPolicyJson", Map.of("type", "SNAPSHOT_DRIVEN"),
                        "confirmationPolicyJson", Map.of("timeout", "PT2M"),
                        "concurrencyPolicyJson", Map.of("maxActiveInstances", 1)));
        long versionId = longValue(version, "id");

        createNode(versionId, "ods-orders", "STREAMING", List.of(), rootDependency,
                "e2e.ods.orders." + BIZ_DATE_TEMPLATE, "orders-ods-stream", 10);
        createNode(versionId, "dwd-order-detail", "BATCH", List.of("ods-orders"), Map.of(),
                "e2e.dwd.order_detail." + BIZ_DATE_TEMPLATE, "order-detail-batch", 20);
        createNode(versionId, "dws-daily-gmv", "BATCH", List.of("dwd-order-detail"), Map.of(),
                "e2e.dws.daily_gmv." + BIZ_DATE_TEMPLATE, "daily-gmv-batch", 30);
        createNode(versionId, "ads-gmv", "BATCH", List.of("dws-daily-gmv"), Map.of(),
                "e2e.ads.gmv." + BIZ_DATE_TEMPLATE, "gmv-ads-batch", 40);
        post("/api/v1/flow-plan-versions/" + versionId + "/publish", Map.of("publishedBy", "e2e"));

        ResponseEntity<List<Map<String, Object>>> nodes = restTemplate.exchange(
                baseUrl() + "/api/v1/flow-plan-versions/" + versionId + "/nodes",
                HttpMethod.GET,
                HttpEntity.EMPTY,
                new ParameterizedTypeReference<>() {
                });
        assertThat(nodes.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(nodes.getBody()).hasSize(4);
        return versionId;
    }

    /**
     * Run a real DWD-to-ADS node-subgraph backfill and verify snapshot-driven progression.
     *
     * @param flowPlanVersionId published order-to-GMV definition
     * @param businessDate date replayed through the selected subgraph
     */
    private void verifyNodeBackfill(long flowPlanVersionId, LocalDate businessDate) {
        String actionKey = "e2e-backfill-order-to-gmv";
        Map<String, Object> action = post(
                "/api/v1/scheduling-actions/backfill-node",
                Map.of(
                        "flowPlanVersionId", flowPlanVersionId,
                        "startNodeCode", "dwd-order-detail",
                        "startBizDate", businessDate.toString(),
                        "endBizDate", businessDate.toString(),
                        "cascadePolicy", "TRANSITIVE_DOWNSTREAM",
                        "progressionMode", "SERIAL",
                        "skipPolicy", "NONE",
                        "actionKey", actionKey,
                        "requestedBy", "e2e",
                        "reason", "verify real Paimon node backfill"));
        assertThat(stringValue(action, "status")).isEqualTo("APPLIED");

        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    Map<String, Object> batch = get("/api/v1/backfills/by-action/" + actionKey);
                    assertThat(stringValue(batch, "status")).isEqualTo("COMPLETED");
                    List<Map<String, Object>> items = getList(
                            "/api/v1/backfills/" + longValue(batch, "id") + "/items");
                    assertThat(items).hasSize(3);
                    assertThat(items).allSatisfy(item ->
                            assertThat(stringValue(item, "status")).isEqualTo("SNAPSHOT_CONFIRMED"));
                });

        Map<String, Object> batch = get("/api/v1/backfills/by-action/" + actionKey);
        List<Map<String, Object>> items = getList(
                "/api/v1/backfills/" + longValue(batch, "id") + "/items");
        assertThat(items).extracting(item -> stringValue(item, "nodeCode"))
                .containsExactly("dwd-order-detail", "dws-daily-gmv", "ads-gmv");
        assertThat(workflowInstanceRepository.findAll()).hasSize(3);
        assertThat(taskInstanceRepository.findAll()).hasSize(9);
        assertThat(schedulingIntentRepository.findAll()).hasSize(9);

        List<SchedulingIntent> backfillIntents = schedulingIntentRepository.findAll().stream()
                .filter(intent -> intent.getBackfillBatchId() != null)
                .sorted(java.util.Comparator.comparing(SchedulingIntent::getCreatedAt))
                .toList();
        assertThat(backfillIntents).hasSize(3);
        assertThat(backfillIntents).extracting(SchedulingIntent::getTaskCode)
                .containsExactly("dwd-order-detail", "dws-daily-gmv", "ads-gmv");
        assertThat(backfillIntents.get(0).getInputSnapshotVectorJson()).isEmpty();
        assertThat(backfillIntents.get(1).getInputSnapshotVectorJson().get(0)
                .get("upstreamTaskInstanceId")).isNotNull();
        assertThat(backfillIntents.get(2).getInputSnapshotVectorJson().get(0)
                .get("upstreamTaskInstanceId")).isNotNull();
    }

    /** Rerun one complete published workflow and require all four target snapshots. */
    private void verifyWorkflowRerun(LocalDate businessDate) {
        WorkflowInstance source = workflowInstanceRepository.findAll().stream()
                .filter(workflow -> "SNAPSHOT_DRIVEN".equals(workflow.getTriggerType()))
                .filter(workflow -> SchedulingStates.SNAPSHOT_CONFIRMED.equals(workflow.getState()))
                .min(java.util.Comparator.comparing(WorkflowInstance::getId))
                .orElseThrow();
        Map<String, Object> action = post(
                "/api/v1/scheduling-actions/rerun-workflow",
                Map.of(
                        "workflowInstanceId", source.getId(),
                        "actionKey", "e2e-rerun-order-workflow",
                        "requestedBy", "e2e",
                        "reason", "verify complete workflow rerun"));
        assertThat(stringValue(action, "status")).isEqualTo("APPLIED");
        long workflowId = longList(action, "workflowInstanceIds").get(0);
        awaitWorkflowSnapshots(workflowId, 4);
        assertThat(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(workflowId))
                .extracting(TaskInstance::getTaskCode)
                .containsExactly("ods-orders", "dwd-order-detail", "dws-daily-gmv", "ads-gmv");
        EXECUTION_ENDPOINT.awaitStreamingWriter("orders-ods-stream", E2E_TIMEOUT);
        assertThat(workflowInstanceRepository.findById(workflowId).orElseThrow().getBizDate().toLocalDate())
                .isEqualTo(businessDate);
    }

    /** Rerun one existing task as an explicit action entry and confirm only its target snapshot. */
    private void verifyTaskRerun(LocalDate businessDate) {
        TaskInstance source = taskInstanceRepository.findAll().stream()
                .filter(task -> "dwd-order-detail".equals(task.getTaskCode()))
                .filter(task -> SchedulingStates.SNAPSHOT_CONFIRMED.equals(task.getState()))
                .min(java.util.Comparator.comparing(TaskInstance::getId))
                .orElseThrow();
        Map<String, Object> action = post(
                "/api/v1/scheduling-actions/rerun-task",
                Map.of(
                        "taskInstanceId", source.getId(),
                        "actionKey", "e2e-rerun-order-task",
                        "requestedBy", "e2e",
                        "reason", "verify task rerun"));
        assertThat(stringValue(action, "status")).isEqualTo("APPLIED");
        long taskId = longValue(action, "taskInstanceId");
        awaitTaskSnapshot(taskId);
        TaskInstance rerun = taskInstanceRepository.findById(taskId).orElseThrow();
        assertThat(rerun.getBizDate().toLocalDate()).isEqualTo(businessDate);
        assertThat(rerun.isParentDependencyBypassed()).isTrue();
    }

    /** Rerun one published node without borrowing a parent confirmation from another instance. */
    private void verifyNodeRerun(long flowPlanVersionId, LocalDate businessDate) {
        Map<String, Object> action = post(
                "/api/v1/scheduling-actions/rerun-node",
                Map.of(
                        "flowPlanVersionId", flowPlanVersionId,
                        "nodeCode", "ads-gmv",
                        "bizDate", businessDate.toString(),
                        "actionKey", "e2e-rerun-ads-node",
                        "requestedBy", "e2e",
                        "reason", "verify published node rerun"));
        assertThat(stringValue(action, "status")).isEqualTo("APPLIED");
        long taskId = longValue(action, "taskInstanceId");
        awaitTaskSnapshot(taskId);
        SchedulingIntent intent = schedulingIntentRepository.findByTaskInstanceId(taskId).orElseThrow();
        assertThat(intent.getInputSnapshotVectorJson()).isEmpty();
        assertThat(intent.getTriggerType()).isEqualTo("RERUN_TASK");
    }

    /** Backfill the complete Flow, including a controlled streaming-writer replay and restart. */
    private void verifyFullFlowBackfill(LocalDate businessDate) {
        String actionKey = "e2e-full-flow-backfill";
        Map<String, Object> action = post(
                "/api/v1/scheduling-actions/backfill-workflow",
                Map.of(
                        "workflowCode", "order-to-gmv",
                        "workflowVersion", 1,
                        "startBizDate", businessDate.toString(),
                        "endBizDate", businessDate.toString(),
                        "progressionMode", "SERIAL",
                        "skipPolicy", "NONE",
                        "actionKey", actionKey,
                        "requestedBy", "e2e",
                        "reason", "verify complete Flow backfill"));
        assertThat(stringValue(action, "status")).isEqualTo("APPLIED");
        Map<String, Object> batch = awaitBackfillStatus(actionKey, "COMPLETED");
        List<Map<String, Object>> items = getList(
                "/api/v1/backfills/" + longValue(batch, "id") + "/items");
        assertThat(items).hasSize(4);
        assertThat(items).extracting(item -> stringValue(item, "nodeCode"))
                .containsExactly("ods-orders", "dwd-order-detail", "dws-daily-gmv", "ads-gmv");
        assertThat(items).allSatisfy(item ->
                assertThat(stringValue(item, "status")).isEqualTo("SNAPSHOT_CONFIRMED"));
        EXECUTION_ENDPOINT.awaitStreamingWriter("orders-ods-stream", E2E_TIMEOUT);
    }

    /** Fail one acknowledged backfill snapshot and recover it through a replacement batch. */
    private void verifyBackfillRecovery(long flowPlanVersionId, LocalDate businessDate) {
        String failedActionKey = "e2e-failed-node-backfill";
        EXECUTION_ENDPOINT.suppressSnapshot(
                failedActionKey + ":" + businessDate,
                "dwd-order-detail");
        Map<String, Object> failedAction = post(
                "/api/v1/scheduling-actions/backfill-node",
                Map.of(
                        "flowPlanVersionId", flowPlanVersionId,
                        "startNodeCode", "dwd-order-detail",
                        "startBizDate", businessDate.toString(),
                        "endBizDate", businessDate.toString(),
                        "cascadePolicy", "TRANSITIVE_DOWNSTREAM",
                        "progressionMode", "SERIAL",
                        "skipPolicy", "NONE",
                        "actionKey", failedActionKey,
                        "requestedBy", "e2e",
                        "reason", "inject missing target snapshot"));
        assertThat(stringValue(failedAction, "status")).isEqualTo("APPLIED");

        Map<String, Object> failedBatch = get("/api/v1/backfills/by-action/" + failedActionKey);
        long failedBatchId = longValue(failedBatch, "id");
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(250))
                .untilAsserted(() -> {
                    Map<String, Object> firstItem = getList(
                            "/api/v1/backfills/" + failedBatchId + "/items").get(0);
                    TaskInstance task = taskInstanceRepository.findById(
                            longValue(firstItem, "taskInstanceId")).orElseThrow();
                    assertThat(task.getState()).isEqualTo(SchedulingStates.SCHEDULED);
                });
        Map<String, Object> failedItem = getList(
                "/api/v1/backfills/" + failedBatchId + "/items").get(0);
        TaskInstance failedTask = taskInstanceRepository.findById(
                longValue(failedItem, "taskInstanceId")).orElseThrow();
        failedTask.setScheduledAt(LocalDateTime.now().minusMinutes(3));
        taskInstanceRepository.saveAndFlush(failedTask);
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(snapshotSourceReconciliationService.reconcileAllSources())
                        .anySatisfy(result -> {
                            assertThat(result.identity().assetKey()).isEqualTo("e2e.dwd.order_detail");
                            assertThat(result.outcome().name()).isEqualTo("HEALTHY");
                        }));

        awaitBackfillStatus(failedActionKey, "FAILED");
        TaskInstance timedOut = taskInstanceRepository.findById(failedTask.getId()).orElseThrow();
        assertThat(timedOut.getState()).isEqualTo(SchedulingStates.SNAPSHOT_NOT_ADVANCED);
        assertThat(timedOut.getSourceHealth()).isEqualTo("HEALTHY");

        String recoveryActionKey = "e2e-recover-node-backfill";
        Map<String, Object> recoveryAction = post(
                "/api/v1/scheduling-actions/recover-backfill",
                Map.of(
                        "backfillBatchId", failedBatchId,
                        "recoveryStrategy", "FULL_SCOPE",
                        "actionKey", recoveryActionKey,
                        "requestedBy", "e2e",
                        "reason", "verify replacement batch recovery"));
        assertThat(stringValue(recoveryAction, "status")).isEqualTo("APPLIED");
        Map<String, Object> recoveredBatch = awaitBackfillStatus(recoveryActionKey, "COMPLETED");
        assertThat(longValue(recoveredBatch, "sourceBackfillBatchId")).isEqualTo(failedBatchId);
        assertThat(getList("/api/v1/backfills/" + longValue(recoveredBatch, "id") + "/items"))
                .hasSize(3)
                .allSatisfy(item -> assertThat(stringValue(item, "status"))
                        .isEqualTo("SNAPSHOT_CONFIRMED"));
    }

    /** Verify streaming and same-instance batch parent evidence stay distinct in frozen vectors. */
    private void verifyMixedInputEvidence() {
        List<SchedulingIntent> intents = schedulingIntentRepository.findAll();
        SchedulingIntent ordinaryDwd = intents.stream()
                .filter(intent -> "dwd-order-detail".equals(intent.getTaskCode()))
                .filter(intent -> "SNAPSHOT_DRIVEN".equals(intent.getTriggerType()))
                .findFirst()
                .orElseThrow();
        assertThat(ordinaryDwd.getInputSnapshotVectorJson()).singleElement().satisfies(input -> {
            assertThat(input.get("assetKey").toString()).contains("e2e.ods.orders");
            assertThat(input.get("upstreamTaskInstanceId")).isNull();
        });

        SchedulingIntent ordinaryDws = intents.stream()
                .filter(intent -> "dws-daily-gmv".equals(intent.getTaskCode()))
                .filter(intent -> "SNAPSHOT_DRIVEN".equals(intent.getTriggerType()))
                .findFirst()
                .orElseThrow();
        assertThat(ordinaryDws.getInputSnapshotVectorJson()).singleElement().satisfies(input ->
                assertThat(input.get("upstreamTaskInstanceId")).isNotNull());

        SchedulingIntent actionDwd = intents.stream()
                .filter(intent -> "dwd-order-detail".equals(intent.getTaskCode()))
                .filter(intent -> "RERUN".equals(intent.getTriggerType()))
                .findFirst()
                .orElseThrow();
        assertThat(actionDwd.getInputSnapshotVectorJson()).singleElement().satisfies(input ->
                assertThat(input.get("upstreamTaskInstanceId")).isNotNull());
        assertThat(intents).allSatisfy(intent -> {
            String payload = intent.getInstructionPayloadJson().toString().toLowerCase(java.util.Locale.ROOT);
            assertThat(payload).doesNotContain("flink", "spark");
        });
    }

    /** Wait for every task in one action-owned workflow to reach snapshot confirmation. */
    private void awaitWorkflowSnapshots(long workflowInstanceId, int expectedTasks) {
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> {
                    WorkflowInstance workflow = workflowInstanceRepository.findById(workflowInstanceId).orElseThrow();
                    assertThat(workflow.getState()).isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
                    assertThat(taskInstanceRepository.findByWorkflowInstanceIdOrderByCreatedAtAsc(workflowInstanceId))
                            .hasSize(expectedTasks)
                            .allSatisfy(task -> assertThat(task.getState())
                                    .isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED));
                });
    }

    /** Wait for one action-owned task to be confirmed by its target snapshot. */
    private void awaitTaskSnapshot(long taskInstanceId) {
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(taskInstanceRepository.findById(taskInstanceId)
                        .orElseThrow().getState()).isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED));
    }

    /** Wait for one backfill batch to reach an expected scheduler-side terminal state. */
    private Map<String, Object> awaitBackfillStatus(String actionKey, String expectedStatus) {
        await().atMost(E2E_TIMEOUT)
                .pollInterval(Duration.ofMillis(500))
                .untilAsserted(() -> assertThat(stringValue(
                        get("/api/v1/backfills/by-action/" + actionKey), "status"))
                        .isEqualTo(expectedStatus));
        return get("/api/v1/backfills/by-action/" + actionKey);
    }

    /** Create one globally unique physical-table writer through the platform API. */
    private void createWriter(String writerJobKey, String tableAssetKey, List<String> modes) {
        post("/api/v1/writer-jobs", Map.of(
                "writerJobKey", writerJobKey,
                "tableAssetKey", tableAssetKey,
                "allowedProcessingModes", modes));
    }

    /** Add one engine-neutral stream or batch node to the draft FlowPlan version. */
    private void createNode(
            long versionId,
            String code,
            String mode,
            List<String> dependencies,
            Map<String, Object> inputSpec,
            String outputAsset,
            String writerJobKey,
            int sortOrder) {
        post("/api/v1/flow-plan-versions/" + versionId + "/nodes", Map.of(
                "nodeCode", code,
                "nodeName", code,
                "nodeType", "ASSET_OUTPUT",
                "processingMode", mode,
                "dependsOnNodes", dependencies,
                "inputDependencySpecJson", inputSpec,
                "outputAssetKey", outputAsset,
                "writerJobKey", writerJobKey,
                "confirmationPolicyJson", Map.of("timeout", "PT2M"),
                "sortOrder", sortOrder));
    }

    /** Insert binlog changes only after the unbounded Flink CDC job is running. */
    private void insertOrdersAfterCdcStarted(LocalDate businessDate) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO orders(order_id, user_id, amount_cents, order_date, status) "
                                + "VALUES (?, ?, ?, ?, ?)")) {
            addOrder(statement, 1L, 101L, 10_000L, businessDate, "PAID");
            addOrder(statement, 2L, 102L, 2_500L, businessDate, "PAID");
            addOrder(statement, 3L, 103L, 9_900L, businessDate, "CANCELLED");
            statement.executeBatch();
        }
    }

    /** Insert one additional paid order after the replacement CDC generation is active. */
    private void insertOrderAfterRestart(LocalDate businessDate) throws Exception {
        try (Connection connection = DriverManager.getConnection(
                MYSQL.getJdbcUrl(), MYSQL.getUsername(), MYSQL.getPassword());
                PreparedStatement statement = connection.prepareStatement(
                        "INSERT INTO orders(order_id, user_id, amount_cents, order_date, status) "
                                + "VALUES (?, ?, ?, ?, ?)")) {
            addOrder(statement, 4L, 104L, 3_000L, businessDate, "PAID");
            statement.executeBatch();
        }
    }

    /** Add one deterministic commerce order to a JDBC batch. */
    private void addOrder(
            PreparedStatement statement,
            long orderId,
            long userId,
            long amountCents,
            LocalDate orderDate,
            String status) throws Exception {
        statement.setLong(1, orderId);
        statement.setLong(2, userId);
        statement.setLong(3, amountCents);
        statement.setString(4, orderDate.toString());
        statement.setString(5, status);
        statement.addBatch();
    }

    /** Assert only snapshot evidence closes the job-control and three batch decisions. */
    private void assertSchedulerSnapshotOutcome(LocalDate businessDate, int expectedRuns) throws Exception {
        assertThat(EXECUTION_ENDPOINT.failures()).isEmpty();
        assertThat(EXECUTION_ENDPOINT.activeWriterFailure())
                .as("Flink diagnostics; Lakehouse Flow result assertions remain snapshot-only")
                .isNull();

        List<JobControlIntent> controls = jobControlIntentRepository.findAll();
        assertThat(controls).hasSize(expectedRuns);
        assertThat(controls).allSatisfy(control -> {
            assertThat(control.getSnapshotResult()).isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
            assertThat(control.getObservedSnapshotId()).isNotBlank();
        });

        List<WorkflowInstance> workflows = workflowInstanceRepository.findAll();
        assertThat(workflows).hasSize(expectedRuns);
        assertThat(workflows).allSatisfy(workflow -> {
            assertThat(workflow.getBizDate().toLocalDate()).isEqualTo(businessDate);
            assertThat(workflow.getState()).isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
        });

        List<TaskInstance> tasks = taskInstanceRepository.findAll();
        assertThat(tasks).hasSize(expectedRuns * 3);
        assertThat(tasks).allSatisfy(task -> {
            assertThat(task.getState()).isEqualTo(SchedulingStates.SNAPSHOT_CONFIRMED);
            assertThat(task.getObservedSnapshotId()).isNotBlank();
        });
        assertThat(tasks).filteredOn(task -> "dwd-order-detail".equals(task.getTaskCode()))
                .hasSize(expectedRuns);
        assertThat(tasks).filteredOn(task -> "dws-daily-gmv".equals(task.getTaskCode()))
                .hasSize(expectedRuns);
        assertThat(tasks).filteredOn(task -> "ads-gmv".equals(task.getTaskCode()))
                .hasSize(expectedRuns);

        List<SchedulingIntent> intents = schedulingIntentRepository.findAll();
        assertThat(intents).hasSize(expectedRuns * 3);
        assertThat(intents).allSatisfy(intent -> {
            assertThat(intent.getWriterJobKey()).isNotBlank();
            assertThat(intent.getWriterEpoch()).isPositive();
            assertThat(intent.getInstructionPayloadJson()).containsKeys("processing", "snapshotEvidence");
        });

        List<LakehouseEvent> events = lakehouseEventRepository.findAll();
        assertThat(events).filteredOn(event -> Boolean.TRUE.equals(event.getDataChange()))
                .extracting(LakehouseEvent::getTableName)
                .contains("orders", "order_detail", "daily_gmv", "gmv");
    }

    /** POST one JSON-compatible request and require a successful object response. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> post(String path, Map<String, Object> request) {
        ResponseEntity<Map> response = restTemplate.postForEntity(baseUrl() + path, request, Map.class);
        assertThat(response.getStatusCode().is2xxSuccessful())
                .as("POST %s returned %s", path, response.getStatusCode())
                .isTrue();
        assertThat(response.getBody()).isNotNull();
        return (Map<String, Object>) response.getBody();
    }

    /** GET one JSON object from the public API and require a successful response. */
    @SuppressWarnings("unchecked")
    private Map<String, Object> get(String path) {
        ResponseEntity<Map> response = restTemplate.getForEntity(baseUrl() + path, Map.class);
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotNull();
        return (Map<String, Object>) response.getBody();
    }

    /** GET one JSON object list from the public API and require a successful response. */
    private List<Map<String, Object>> getList(String path) {
        ResponseEntity<List<Map<String, Object>>> response = restTemplate.exchange(
                baseUrl() + path,
                HttpMethod.GET,
                HttpEntity.EMPTY,
                new ParameterizedTypeReference<>() {
                });
        assertThat(response.getStatusCode().is2xxSuccessful()).isTrue();
        assertThat(response.getBody()).isNotNull();
        return response.getBody();
    }

    /** Build the local random-port application URL. */
    private String baseUrl() {
        return "http://127.0.0.1:" + serverPort;
    }

    /** Read one required string from an API response map. */
    private String stringValue(Map<String, Object> value, String key) {
        Object result = value.get(key);
        assertThat(result).as(key).isInstanceOf(String.class);
        return result.toString();
    }

    /** Read one required numeric identifier from an API response map. */
    private long longValue(Map<String, Object> value, String key) {
        Object result = value.get(key);
        assertThat(result).as(key).isInstanceOf(Number.class);
        return ((Number) result).longValue();
    }

    /** Read one required numeric identifier list from an API response map. */
    private List<Long> longList(Map<String, Object> value, String key) {
        Object result = value.get(key);
        assertThat(result).as(key).isInstanceOf(List.class);
        return ((List<?>) result).stream()
                .map(item -> {
                    assertThat(item).isInstanceOf(Number.class);
                    return ((Number) item).longValue();
                })
                .toList();
    }

    /** Register one Paimon source-table binding in Spring's dynamic test properties. */
    private static void registerPaimonTable(
            DynamicPropertyRegistry registry,
            int index,
            String database,
            String table) {
        String prefix = "lakehouse-flow.snapshot-sources.paimon.catalogs[0].tables[" + index + "]";
        registry.add(prefix + ".database", () -> database);
        registry.add(prefix + ".table", () -> table);
        registry.add(prefix + ".source-name", () -> CATALOG + "." + database + "." + table);
        registry.add(prefix + ".batch-size", () -> 100);
        registry.add(prefix + ".startup-mode", () -> "LATEST");
    }

    /** Create a bind-mounted host path writable by the non-root Flink container user on Linux. */
    private static Path createWarehouse() {
        try {
            Path target = Path.of("target").toAbsolutePath();
            Files.createDirectories(target);
            Path warehouse = Files.createTempDirectory(target, "paimon-e2e-");
            Files.setPosixFilePermissions(warehouse, PosixFilePermissions.fromString("rwxrwxrwx"));
            return warehouse;
        } catch (IOException exception) {
            throw new IllegalStateException("Unable to create the E2E Paimon warehouse", exception);
        }
    }

    /** Build the shared Flink Session Cluster configuration. */
    private static String flinkProperties() {
        return "jobmanager.rpc.address: " + FLINK_JOB_MANAGER_ALIAS + "\n"
                + "rest.address: " + FLINK_JOB_MANAGER_ALIAS + "\n"
                + "rest.bind-address: 0.0.0.0\n"
                + "taskmanager.numberOfTaskSlots: 4\n"
                + "parallelism.default: 1\n";
    }

    /** Container and source settings required by the test-only HTTP execution endpoint. */
    private record ExecutionEnvironment(
            GenericContainer<?> jobManager,
            URI jobManagerRestUri,
            URI lakehouseFlowRestUri,
            String warehouse,
            String mysqlHost,
            String mysqlDatabase,
            String mysqlUsername,
            String mysqlPassword,
            String fenceJdbcUrl,
            String fenceJdbcUsername,
            String fenceJdbcPassword) {
    }

    /**
     * Passive HTTP consumer that immediately submits each delivered intent to Flink.
     *
     * <p>The endpoint acknowledges transport before execution finishes. It never sends job status
     * back to Lakehouse Flow; the scheduler can observe only snapshots written by those jobs.
     */
    private static final class IntentExecutionEndpoint implements AutoCloseable {

        private static final Pattern FLINK_JOB_ID = Pattern.compile("JobID ([0-9a-fA-F]+)");

        private final HttpServer server;
        private final ExecutorService executor;
        private final Map<String, CompletableFuture<Void>> submissions = new ConcurrentHashMap<>();
        private final Map<String, Throwable> failures = new ConcurrentHashMap<>();
        private final Map<String, JsonNode> deliveredPayloads = new ConcurrentHashMap<>();
        private final Map<String, String> activeWriterJobs = new ConcurrentHashMap<>();
        private final Map<String, String> suspendedWriterCheckpoints = new ConcurrentHashMap<>();
        private final Set<SuppressedInstruction> suppressedInstructions = ConcurrentHashMap.newKeySet();
        private final Set<String> successfulJobTypes = ConcurrentHashMap.newKeySet();
        private volatile ExecutionEnvironment environment;

        /** Start an ephemeral local HTTP endpoint before the Spring context resolves destinations. */
        private IntentExecutionEndpoint() throws IOException {
            server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            executor = Executors.newSingleThreadExecutor(runnable -> {
                Thread thread = new Thread(runnable, "e2e-flink-intent-executor");
                thread.setDaemon(true);
                return thread;
            });
            server.createContext("/intents", this::receive);
            server.start();
        }

        /** Create and start one endpoint, converting checked startup errors into test failures. */
        static IntentExecutionEndpoint start() {
            try {
                return new IntentExecutionEndpoint();
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to start the E2E execution endpoint", exception);
            }
        }

        /** Attach started Flink and MySQL containers before an intent may be delivered. */
        void configure(ExecutionEnvironment environment) {
            this.environment = environment;
        }

        /** Initialize every configured physical table through a bounded Flink job. */
        void initializeTables() throws Exception {
            ExecutionEnvironment activeEnvironment = environment;
            if (activeEnvironment == null) {
                throw new IllegalStateException("E2E execution environment is not configured");
            }
            List<String> command = flinkCommand(false);
            addArgument(command, "job-type", "INIT");
            addArgument(command, "warehouse", activeEnvironment.warehouse());
            org.testcontainers.containers.Container.ExecResult result =
                    activeEnvironment.jobManager().execInContainer(command.toArray(String[]::new));
            assertThat(result.getExitCode())
                    .as("Paimon initialization output: %s%s", result.getStdout(), result.getStderr())
                    .isZero();
        }

        /** Return the HTTP destination configured into both Lakehouse Flow publishers. */
        String url() {
            return "http://127.0.0.1:" + server.getAddress().getPort() + "/intents";
        }

        /** Handle one at-least-once delivery and execute each intent key only once. */
        private void receive(HttpExchange exchange) throws IOException {
            if (!"POST".equals(exchange.getRequestMethod())) {
                exchange.sendResponseHeaders(405, -1);
                exchange.close();
                return;
            }
            try {
                byte[] body = exchange.getRequestBody().readAllBytes();
                JsonNode payload = OBJECT_MAPPER.readTree(body);
                String intentKey = requiredText(payload, "intentKey");
                deliveredPayloads.putIfAbsent(intentKey, payload.deepCopy());
                submissions.computeIfAbsent(intentKey, ignored -> CompletableFuture.runAsync(
                        () -> submit(payload), executor));
                exchange.sendResponseHeaders(202, -1);
            } catch (RuntimeException exception) {
                exchange.sendResponseHeaders(500, -1);
            } finally {
                exchange.close();
            }
        }

        /** Submit one control or data-processing instruction to the real Flink cluster. */
        private void submit(JsonNode payload) {
            String intentKey = requiredText(payload, "intentKey");
            try {
                ExecutionEnvironment activeEnvironment = environment;
                if (activeEnvironment == null) {
                    throw new IllegalStateException("E2E execution environment is not configured");
                }
                if (consumeSuppression(payload)) {
                    return;
                }
                String intentKind = requiredText(payload, "intentKind");
                if ("JOB_CONTROL".equals(intentKind)) {
                    submitControl(payload, activeEnvironment);
                } else if ("ODS_REPLAY".equals(jobType(payload))) {
                    submitStreamingReplay(payload, activeEnvironment);
                } else {
                    requireSuccessfulExecution(
                            intentKey,
                            activeEnvironment.jobManager().execInContainer(
                                    batchCommand(payload, activeEnvironment).toArray(String[]::new)));
                }
                successfulJobTypes.add(jobType(payload));
            } catch (Exception exception) {
                failures.put(intentKey, exception);
                throw new IllegalStateException("Unable to execute intent " + intentKey, exception);
            }
        }

        /** Fence and cancel the previous stream generation before submitting its replacement. */
        private void submitControl(JsonNode payload, ExecutionEnvironment environment) throws Exception {
            String writerJobKey = requiredText(payload.path("writer"), "writerJobKey");
            String operationType = requiredText(payload.path("control"), "operationType");
            String restorePath = null;
            if ("RESTART_JOB".equals(operationType)) {
                restorePath = suspendedWriterCheckpoints.remove(writerJobKey);
                if (restorePath == null) {
                    restorePath = cancelActiveWriter(writerJobKey, environment);
                }
            }
            org.testcontainers.containers.Container.ExecResult result = environment.jobManager()
                    .execInContainer(controlCommand(payload, environment, restorePath).toArray(String[]::new));
            requireSuccessfulExecution(requiredText(payload, "intentKey"), result);
            String jobId = extractJobId(result);
            awaitLatestCheckpoint(jobId, environment);
            activeWriterJobs.put(writerJobKey, jobId);
        }

        /** Suspend the streaming writer, replay one date, and request a new controlled generation. */
        private void submitStreamingReplay(JsonNode payload, ExecutionEnvironment environment) throws Exception {
            String writerJobKey = requiredText(payload.path("writer"), "writerJobKey");
            String restorePath = cancelActiveWriter(writerJobKey, environment);
            suspendedWriterCheckpoints.put(writerJobKey, restorePath);
            requireSuccessfulExecution(
                    requiredText(payload, "intentKey"),
                    environment.jobManager().execInContainer(
                            batchCommand(payload, environment).toArray(String[]::new)));
            requestStreamRestart(payload, environment);
        }

        /** Ask Lakehouse Flow to allocate the next stream epoch after a bounded replay. */
        private void requestStreamRestart(JsonNode payload, ExecutionEnvironment environment) throws Exception {
            String writerJobKey = requiredText(payload.path("writer"), "writerJobKey");
            String intentKey = requiredText(payload, "intentKey");
            byte[] body = OBJECT_MAPPER.writeValueAsBytes(Map.of(
                    "requestKey", "e2e-restart-after-replay:" + intentKey,
                    "requestedBy", "e2e-platform",
                    "reason", "resume streaming writer after bounded backfill replay"));
            HttpRequest request = HttpRequest.newBuilder(environment.lakehouseFlowRestUri().resolve(
                            "api/v1/writer-jobs/" + writerJobKey + "/restart"))
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofByteArray(body))
                    .build();
            HttpResponse<String> response = HTTP_CLIENT.send(
                    request,
                    HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
            if (response.statusCode() < 200 || response.statusCode() >= 300) {
                throw new IllegalStateException(
                        "Controlled stream restart failed with " + response.statusCode() + ": " + response.body());
            }
        }

        /** Wait until the submitted stream has established a durable external checkpoint. */
        private String awaitLatestCheckpoint(String jobId, ExecutionEnvironment environment) throws Exception {
            long deadline = System.nanoTime() + STREAM_START_TIMEOUT.toNanos();
            String lastResponse = "";
            while (System.nanoTime() < deadline) {
                HttpRequest request = HttpRequest.newBuilder(
                                environment.jobManagerRestUri().resolve("/jobs/" + jobId + "/checkpoints"))
                        .GET()
                        .build();
                HttpResponse<String> response = HTTP_CLIENT.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                lastResponse = response.body();
                if (response.statusCode() == 200) {
                    String externalPath = OBJECT_MAPPER.readTree(lastResponse)
                            .path("latest")
                            .path("completed")
                            .path("external_path")
                            .asText();
                    if (!externalPath.isBlank()) {
                        return externalPath;
                    }
                }
                TimeUnit.MILLISECONDS.sleep(200);
            }
            throw new IllegalStateException(
                    "Flink job " + jobId + " produced no external checkpoint: " + lastResponse);
        }

        /** Return a failed active writer's Flink exception for E2E diagnostics only. */
        private String activeWriterFailure() throws Exception {
            ExecutionEnvironment activeEnvironment = environment;
            if (activeEnvironment == null) {
                return null;
            }
            for (Map.Entry<String, String> writerJob : activeWriterJobs.entrySet()) {
                HttpRequest request = HttpRequest.newBuilder(activeEnvironment.jobManagerRestUri().resolve(
                                "/jobs/" + writerJob.getValue() + "/exceptions"))
                        .GET()
                        .build();
                HttpResponse<String> response = HTTP_CLIENT.send(
                        request,
                        HttpResponse.BodyHandlers.ofString(StandardCharsets.UTF_8));
                if (response.statusCode() != 200) {
                    continue;
                }
                String rootException = OBJECT_MAPPER.readTree(response.body())
                        .path("root-exception")
                        .asText();
                if (!rootException.isBlank()) {
                    return writerJob.getKey() + " (" + writerJob.getValue() + "): " + rootException;
                }
            }
            return null;
        }

        /** Cancel the old generation and return its retained final source checkpoint. */
        private String cancelActiveWriter(String writerJobKey, ExecutionEnvironment environment) throws Exception {
            String jobId = activeWriterJobs.remove(writerJobKey);
            if (jobId == null) {
                throw new IllegalStateException("No active Flink job is tracked for restart: " + writerJobKey);
            }
            org.testcontainers.containers.Container.ExecResult running = environment.jobManager().execInContainer(
                    "flink", "list", "-r", "-m", FLINK_JOB_MANAGER_ALIAS + ":8081");
            requireSuccessfulExecution("list-running-jobs", running);
            if (running.getStdout().contains(jobId)) {
                org.testcontainers.containers.Container.ExecResult cancelled = environment.jobManager().execInContainer(
                        "flink", "cancel", "-m", FLINK_JOB_MANAGER_ALIAS + ":8081", jobId);
                requireSuccessfulExecution("cancel-writer:" + writerJobKey, cancelled);
            }
            return awaitLatestCheckpoint(jobId, environment);
        }

        /** Reject a nonzero Flink CLI response with its complete diagnostic output. */
        private void requireSuccessfulExecution(
                String operation,
                org.testcontainers.containers.Container.ExecResult result) {
            if (result.getExitCode() != 0) {
                throw new IllegalStateException("Flink operation failed for " + operation + ": "
                        + result.getStdout() + result.getStderr());
            }
        }

        /** Parse the detached Flink job identifier returned by the command-line client. */
        private String extractJobId(org.testcontainers.containers.Container.ExecResult result) {
            Matcher matcher = FLINK_JOB_ID.matcher(result.getStdout() + result.getStderr());
            if (!matcher.find()) {
                throw new IllegalStateException("Flink submission returned no JobID: "
                        + result.getStdout() + result.getStderr());
            }
            return matcher.group(1);
        }

        /** Build the detached ODS CDC submission from a platform job-control intent. */
        private List<String> controlCommand(
                JsonNode payload,
                ExecutionEnvironment environment,
                String restorePath) {
            JsonNode writer = payload.path("writer");
            JsonNode requiredProperties = payload.path("snapshotObservation").path("requiredWriterProperties");
            List<String> command = flinkCommand(true, restorePath);
            addArgument(command, "job-type", "ODS");
            addCommonArguments(
                    command,
                    payload,
                    writer,
                    requiredProperties,
                    environment,
                    "JOB_CONTROL",
                    requiredText(writer, "processingMode"));
            addMySqlArguments(command, environment);
            addArgument(command, "mysql-server-id", "5400");
            addArgument(command, "checkpoint-interval-ms", "500");
            addArgument(command, "checkpoint-storage", environment.warehouse() + "flink-checkpoints");
            return command;
        }

        /** Build one attached bounded batch submission from a data-processing intent. */
        private List<String> batchCommand(JsonNode payload, ExecutionEnvironment environment) {
            JsonNode writer = payload.path("writer");
            JsonNode requiredProperties = payload.path("snapshotEvidence").path("requiredSnapshotProperties");
            List<String> command = flinkCommand(false);
            addArgument(command, "job-type", jobType(payload));
            addCommonArguments(
                    command,
                    payload,
                    writer,
                    requiredProperties,
                    environment,
                    "DATA_PROCESSING",
                    requiredText(payload.path("processing"), "processingMode"));
            if ("ODS_REPLAY".equals(jobType(payload))) {
                addMySqlArguments(command, environment);
                addArgument(command, "biz-date", requiredText(payload.path("schedule"), "bizDate"));
            } else {
                JsonNode inputs = payload.path("processing").path("inputSnapshotVector");
                String sourceTable = inputs.isArray() && !inputs.isEmpty()
                        ? physicalTable(requiredText(inputs.get(0), "assetKey"))
                        : actionEntrySourceTable(payload);
                addArgument(command, "source-table", sourceTable);
            }
            return command;
        }

        /** Resolve the configured input for an explicit action entry that bypasses parent evidence. */
        private String actionEntrySourceTable(JsonNode payload) {
            return switch (jobType(payload)) {
                case "DWD" -> "ods.orders";
                case "DWS" -> "dwd.order_detail";
                case "ADS" -> "dws.daily_gmv";
                default -> throw new IllegalArgumentException(
                        "Batch intent has no frozen input snapshot vector: " + requiredText(payload, "intentKey"));
            };
        }

        /** Add the shared MySQL connection arguments used by CDC and bounded replay jobs. */
        private void addMySqlArguments(List<String> command, ExecutionEnvironment environment) {
            addArgument(command, "mysql-host", environment.mysqlHost());
            addArgument(command, "mysql-port", "3306");
            addArgument(command, "mysql-database", environment.mysqlDatabase());
            addArgument(command, "mysql-table", "orders");
            addArgument(command, "mysql-username", environment.mysqlUsername());
            addArgument(command, "mysql-password", environment.mysqlPassword());
        }

        /** Build the invariant prefix for detached or attached Flink CLI submissions. */
        private List<String> flinkCommand(boolean detached) {
            return flinkCommand(detached, null);
        }

        /** Build a Flink CLI submission that optionally restores operator state from a checkpoint. */
        private List<String> flinkCommand(boolean detached, String restorePath) {
            List<String> command = new ArrayList<>(List.of(
                    "flink", "run"));
            if (detached) {
                command.add("-d");
            }
            if (restorePath != null) {
                Collections.addAll(command, "-s", restorePath);
            }
            Collections.addAll(
                    command,
                    "-m", FLINK_JOB_MANAGER_ALIAS + ":8081",
                    JOB_JAR_IN_CONTAINER);
            return command;
        }

        /** Add writer identity, Paimon target, and attribution properties shared by all jobs. */
        private void addCommonArguments(
                List<String> command,
                JsonNode payload,
                JsonNode writer,
                JsonNode requiredProperties,
                ExecutionEnvironment environment,
                String intentKind,
                String processingMode) {
            addArgument(command, "warehouse", environment.warehouse());
            String tableAssetKey = requiredText(writer, "tableAssetKey");
            addArgument(command, "target-table", physicalTable(tableAssetKey));
            addArgument(command, "table-asset-key", tableAssetKey);
            addArgument(command, "writer-job-key", requiredText(writer, "writerJobKey"));
            addArgument(command, "writer-epoch", requiredText(writer, "writerEpoch"));
            addArgument(command, "intent-key", requiredText(payload, "intentKey"));
            addArgument(command, "intent-kind", intentKind);
            addArgument(command, "processing-mode", processingMode);
            addArgument(command, "snapshot-properties", encodeProperties(requiredProperties));
            addArgument(command, "fence-jdbc-url", environment.fenceJdbcUrl());
            addArgument(command, "fence-jdbc-username", environment.fenceJdbcUsername());
            addArgument(command, "fence-jdbc-password", environment.fenceJdbcPassword());
        }

        /** Append one key/value pair to a Flink job command. */
        private void addArgument(List<String> command, String key, String value) {
            command.add("--" + key);
            command.add(value);
        }

        /** Resolve the E2E transformation from the engine-neutral task code. */
        private String jobType(JsonNode payload) {
            if ("JOB_CONTROL".equals(payload.path("intentKind").asText())) {
                return "ODS";
            }
            return switch (requiredText(payload.path("definition"), "taskCode")) {
                case "ods-orders" -> "ODS_REPLAY";
                case "dwd-order-detail" -> "DWD";
                case "dws-daily-gmv" -> "DWS";
                case "ads-gmv" -> "ADS";
                default -> throw new IllegalArgumentException(
                        "Unknown E2E task code: " + payload.path("definition").path("taskCode"));
            };
        }

        /** Convert catalog.database.table[.partition] into database.table for the Flink job. */
        private String physicalTable(String assetKey) {
            String[] parts = assetKey.split("\\.", 4);
            if (parts.length < 3) {
                throw new IllegalArgumentException("Invalid table asset key: " + assetKey);
            }
            return parts[1] + "." + parts[2];
        }

        /** Serialize required Paimon snapshot properties as URL-safe Base64 JSON. */
        private String encodeProperties(JsonNode properties) {
            if (!properties.isObject()) {
                throw new IllegalArgumentException("Intent has no required snapshot properties");
            }
            try {
                return Base64.getUrlEncoder().withoutPadding().encodeToString(
                        OBJECT_MAPPER.writeValueAsBytes(properties));
            } catch (IOException exception) {
                throw new IllegalStateException("Unable to encode snapshot properties", exception);
            }
        }

        /** Require one textual or numeric JSON field and normalize it to text. */
        private String requiredText(JsonNode object, String field) {
            JsonNode value = object.path(field);
            if (value.isMissingNode() || value.isNull() || value.asText().isBlank()) {
                throw new IllegalArgumentException("Missing intent field: " + field);
            }
            return value.asText();
        }

        /** Wait until the asynchronous Flink CLI has accepted or completed one intent. */
        void awaitSuccess(String intentKey, Duration timeout) throws Exception {
            await().atMost(timeout).until(() -> submissions.containsKey(intentKey));
            submissions.get(intentKey).get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }

        /** Wait until the platform has resumed the named streaming writer after replay. */
        void awaitStreamingWriter(String writerJobKey, Duration timeout) {
            await().atMost(timeout).until(() -> activeWriterJobs.containsKey(writerJobKey));
        }

        /** Suppress one matching snapshot commit while still acknowledging its HTTP delivery. */
        void suppressSnapshot(String triggerEventId, String taskCode) {
            suppressedInstructions.add(new SuppressedInstruction(triggerEventId, taskCode));
        }

        /** Consume one configured no-snapshot fault at the execution boundary. */
        private boolean consumeSuppression(JsonNode payload) {
            if ("JOB_CONTROL".equals(payload.path("intentKind").asText())) {
                return false;
            }
            return suppressedInstructions.remove(new SuppressedInstruction(
                    requiredText(payload.path("schedule"), "triggerEventId"),
                    requiredText(payload.path("definition"), "taskCode")));
        }

        /** Run a real Paimon commit probe and require the superseded epoch to be fenced. */
        void verifyStaleEpochRejected(String staleIntentKey) throws Exception {
            ExecutionEnvironment activeEnvironment = environment;
            if (activeEnvironment == null) {
                throw new IllegalStateException("E2E execution environment is not configured");
            }
            JsonNode stalePayload = deliveredPayloads.get(staleIntentKey);
            if (stalePayload == null) {
                throw new IllegalArgumentException("No delivered payload found for " + staleIntentKey);
            }
            JsonNode writer = stalePayload.path("writer");
            JsonNode requiredProperties = stalePayload.path("snapshotObservation")
                    .path("requiredWriterProperties");
            List<String> command = flinkCommand(false);
            addArgument(command, "job-type", "FENCE_PROBE");
            addCommonArguments(
                    command,
                    stalePayload,
                    writer,
                    requiredProperties,
                    activeEnvironment,
                    "JOB_CONTROL",
                    requiredText(writer, "processingMode"));
            org.testcontainers.containers.Container.ExecResult result = activeEnvironment.jobManager()
                    .execInContainer(command.toArray(String[]::new));
            String output = result.getStdout() + result.getStderr();
            assertThat(result.getExitCode())
                    .as("A superseded writer epoch must not publish: %s", output)
                    .isNotZero();
            assertThat(output).contains("is fenced: current writer epoch is 2");
        }

        /** Return successfully submitted job kinds without exposing them to Lakehouse Flow. */
        Set<String> successfulJobTypes() {
            return Set.copyOf(successfulJobTypes);
        }

        /** Return asynchronous execution failures for high-signal timeout diagnostics. */
        Map<String, Throwable> failures() {
            return Map.copyOf(failures);
        }

        /** Verify the terminal ADS value through an isolated bounded Flink container job. */
        void verifyGmv(LocalDate businessDate, long expectedGmvCents) throws Exception {
            ExecutionEnvironment activeEnvironment = environment;
            if (activeEnvironment == null) {
                throw new IllegalStateException("E2E execution environment is not configured");
            }
            List<String> command = flinkCommand(false);
            addArgument(command, "job-type", "VERIFY");
            addArgument(command, "warehouse", activeEnvironment.warehouse());
            addArgument(command, "target-table", "ads.gmv");
            addArgument(command, "expected-date", businessDate.toString());
            addArgument(command, "expected-gmv-cents", Long.toString(expectedGmvCents));
            org.testcontainers.containers.Container.ExecResult result =
                    activeEnvironment.jobManager().execInContainer(command.toArray(String[]::new));
            assertThat(result.getExitCode())
                    .as("ADS verification output: %s%s", result.getStdout(), result.getStderr())
                    .isZero();
            assertThat(result.getStdout())
                    .contains("LAKEHOUSE_FLOW_E2E_GMV_VERIFIED=" + expectedGmvCents);
        }

        /** Stop accepting intents and interrupt any remaining test execution work. */
        @Override
        public void close() {
            server.stop(0);
            executor.shutdownNow();
        }

        /** Test-only selector for one acknowledged instruction that publishes no snapshot. */
        private record SuppressedInstruction(String triggerEventId, String taskCode) {
        }
    }
}
