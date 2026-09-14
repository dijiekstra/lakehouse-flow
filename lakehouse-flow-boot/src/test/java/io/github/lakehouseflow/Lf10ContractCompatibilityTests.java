package io.github.lakehouseflow;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.lakehouseflow.api.dto.ActionRequests;
import io.github.lakehouseflow.api.dto.CreateFlowPlanRequest;
import io.github.lakehouseflow.api.dto.CreateFlowPlanVersionRequest;
import io.github.lakehouseflow.api.dto.CreateJobControlIntentRequest;
import io.github.lakehouseflow.api.dto.CreateScheduleNodeRequest;
import io.github.lakehouseflow.api.dto.CreateWriterJobBindingRequest;
import io.github.lakehouseflow.api.dto.PublishFlowPlanVersionRequest;
import io.github.lakehouseflow.common.JobControlIntentContract;
import io.github.lakehouseflow.common.SchedulingIntentContract;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.method.HandlerMethod;
import org.springframework.web.servlet.mvc.method.RequestMappingInfo;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

import java.io.IOException;
import java.io.InputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Locks the LF-1.0 REST, outbound payload, and migration compatibility baseline.
 */
@SpringBootTest(classes = LakehouseFlowApplication.class)
@ActiveProfiles("test")
class Lf10ContractCompatibilityTests {

    private static final String CONTRACT_BASE = "contracts/lakehouse-flow/lf-1.0/";

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    @Qualifier("requestMappingHandlerMapping")
    private RequestMappingHandlerMapping handlerMapping;

    /** Verify outbound schemas use the code versions and preserve additive-field compatibility. */
    @Test
    void outboundSchemasMatchFrozenVersionsAndAllowAdditiveFields() throws IOException {
        JsonNode scheduling = readJson(CONTRACT_BASE + "scheduling-intent-1.3.schema.json");
        JsonNode jobControl = readJson(CONTRACT_BASE + "job-control-intent-1.0.schema.json");

        assertEquals(
                SchedulingIntentContract.CONTRACT_VERSION,
                scheduling.at("/properties/contractVersion/const").asText());
        assertEquals(
                JobControlIntentContract.CONTRACT_VERSION,
                jobControl.at("/properties/contractVersion/const").asText());
        assertEquals(
                SchedulingIntentContract.INTENT_KIND,
                scheduling.at("/properties/intentKind/const").asText());
        assertEquals(
                JobControlIntentContract.INTENT_KIND,
                jobControl.at("/properties/intentKind/const").asText());
        assertEveryObjectAllowsAdditionalProperties(scheduling, "scheduling-intent");
        assertEveryObjectAllowsAdditionalProperties(jobControl, "job-control-intent");
    }

    /** Verify every application controller method and path remains in the frozen v1 surface. */
    @Test
    void restApiSurfaceMatchesFrozenManifest() throws IOException {
        JsonNode manifest = readJson(CONTRACT_BASE + "rest-api-v1.surface.json");
        assertTrue(manifest.path("operations").isArray());
        Set<String> expected = toManifestOperations(manifest.path("operations"));
        Set<String> actual = handlerMapping.getHandlerMethods().entrySet().stream()
                .filter(entry -> isLakehouseFlowApi(entry.getValue()))
                .flatMap(entry -> operationKeys(entry.getKey()).stream())
                .collect(Collectors.toUnmodifiableSet());

        assertEquals("1.0", manifest.path("contractVersion").asText());
        assertEquals("ADDITIVE_ONLY", manifest.path("compatibilityMode").asText());
        assertEquals(expected, actual);
    }

    /** Verify all frozen request DTOs explicitly tolerate future additive JSON fields. */
    @Test
    void requestContractsIgnoreUnknownAdditiveFields() throws IOException {
        for (Class<?> requestType : requestTypes()) {
            JsonIgnoreProperties policy = requestType.getAnnotation(JsonIgnoreProperties.class);
            assertNotNull(policy, requestType.getName() + " must declare its unknown-field policy");
            assertTrue(policy.ignoreUnknown(), requestType.getName() + " must ignore additive fields");
            assertNotNull(objectMapper.readValue("{\"futureField\":true}", requestType));
        }
    }

    /** Verify applied V1-V23 SQL files remain byte-for-byte immutable. */
    @Test
    void migrationBaselineRemainsImmutable() throws IOException, NoSuchAlgorithmException {
        for (Map.Entry<String, String> migration : frozenMigrationHashes().entrySet()) {
            assertEquals(
                    migration.getValue(),
                    sha256("db/migration/" + migration.getKey()),
                    migration.getKey() + " changed; add a forward migration instead");
        }
    }

    /** Read one versioned contract artifact from the application classpath. */
    private JsonNode readJson(String path) throws IOException {
        try (InputStream input = new ClassPathResource(path).getInputStream()) {
            return objectMapper.readTree(input);
        }
    }

    /** Require every object schema to retain additive-field compatibility. */
    private void assertEveryObjectAllowsAdditionalProperties(JsonNode node, String path) {
        if (node.isObject() && declaresObjectType(node)) {
            assertTrue(
                    node.path("additionalProperties").asBoolean(false),
                    path + " must set additionalProperties=true");
        }
        if (node.isContainerNode()) {
            node.fields().forEachRemaining(entry ->
                    assertEveryObjectAllowsAdditionalProperties(entry.getValue(), path + "/" + entry.getKey()));
        }
    }

    /** Check whether a schema node declares object as one of its accepted types. */
    private boolean declaresObjectType(JsonNode node) {
        JsonNode type = node.path("type");
        if (type.isTextual()) {
            return "object".equals(type.asText());
        }
        if (!type.isArray()) {
            return false;
        }
        for (JsonNode acceptedType : type) {
            if ("object".equals(acceptedType.asText())) {
                return true;
            }
        }
        return false;
    }

    /** Convert the checked-in REST manifest into comparable method and path keys. */
    private Set<String> toManifestOperations(JsonNode operations) {
        return java.util.stream.StreamSupport.stream(operations.spliterator(), false)
                .map(operation -> operation.path("method").asText() + " " + operation.path("path").asText())
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Check whether a Spring handler belongs to the public Lakehouse Flow API package. */
    private boolean isLakehouseFlowApi(HandlerMethod handlerMethod) {
        return "io.github.lakehouseflow.api".equals(handlerMethod.getBeanType().getPackageName());
    }

    /** Expand one Spring mapping into stable method and path keys. */
    private Set<String> operationKeys(RequestMappingInfo mapping) {
        Set<String> paths = Objects.requireNonNull(mapping.getPathPatternsCondition()).getPatternValues();
        Set<RequestMethod> methods = mapping.getMethodsCondition().getMethods();
        return paths.stream()
                .flatMap(path -> methods.stream().map(method -> method.name() + " " + path))
                .collect(Collectors.toUnmodifiableSet());
    }

    /** Return every LF-1.0 JSON request body type. */
    private List<Class<?>> requestTypes() {
        return List.of(
                CreateFlowPlanRequest.class,
                CreateFlowPlanVersionRequest.class,
                CreateScheduleNodeRequest.class,
                PublishFlowPlanVersionRequest.class,
                CreateWriterJobBindingRequest.class,
                CreateJobControlIntentRequest.class,
                ActionRequests.RerunWorkflowRequest.class,
                ActionRequests.RerunNodeRequest.class,
                ActionRequests.BackfillWorkflowRequest.class,
                ActionRequests.BackfillNodeRequest.class,
                ActionRequests.RecoverBackfillRequest.class,
                ActionRequests.BackfillBatchActionRequest.class,
                ActionRequests.WorkflowInstanceActionRequest.class,
                ActionRequests.TaskInstanceActionRequest.class);
    }

    /** Calculate the SHA-256 of one classpath migration using Java 17 HexFormat. */
    private String sha256(String path) throws IOException, NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (InputStream input = new ClassPathResource(path).getInputStream()) {
            return HexFormat.of().formatHex(digest.digest(input.readAllBytes()));
        }
    }

    /** Return the immutable LF-1.0 migration baseline through V23. */
    private Map<String, String> frozenMigrationHashes() {
        return Map.ofEntries(
                Map.entry("V1.0__init_schema.sql", "e34412c6b9de0b19ef46dc1f6a9b41b12682ab6698caae536eca668246e43ef2"),
                Map.entry("V2.0__phase2_tables.sql", "0258bf81370ac6358b5cc323905b048931eb1e1bbd741624258ae738ccb426f6"),
                Map.entry("V3.0__scheduling_action.sql", "9d86aa3a7540947e30219d757511dad607377e46450a8e5df86e7d0cf5b4194d"),
                Map.entry("V4.0__flow_plan_node.sql", "b8f142b021379f7ef3e37695c8a292bfb9767ee805a1c1050b2e43cd8efca5c9"),
                Map.entry("V5.0__scheduling_action_node_targets.sql", "d90b5123885b451c67fd9cac68ba317051a113f065c171d585d627b9ecc64b06"),
                Map.entry("V6.0__instance_definition_links.sql", "91a2f20afa073d56a7ed70c62318d8fedb39ed8bf6e86cbed6bbac84e151b357"),
                Map.entry("V7.0__backfill_batch_item.sql", "41d38df329bd80167071799ddddafb06893ad7ac9635a3f51485648298d885fa"),
                Map.entry("V8.0__intent_claim_and_versions.sql", "f68da28568ac7773a3c044498834bb28cd396387c13b5265feaca2d81d0c0a28"),
                Map.entry("V9.0__backfill_progression_policy.sql", "85174a2e468045541f7686c87cbdfb31e6e3155c233520e63146a5b3dbb69074"),
                Map.entry("V10.0__backfill_recovery_lineage.sql", "bc52920833bddff082a00db254d95e552df889d9f5a67eae199ada1e7b3f5762"),
                Map.entry("V11.0__backfill_skip_evidence.sql", "499808651fd8b5134a3f829fa9a6ae7b20a44213ce09da7bd932f886bf9f6066"),
                Map.entry("V12.0__unified_backfill_scope.sql", "a3498a6e82e051ca1958a02e0865c32ab9ebe2f181d5aee464ce0c593d3268c0"),
                Map.entry("V13.0__scheduling_action_query_index.sql", "2203d1c9d6db54bee540c09317eb71423d497fb18bbd81ac8d4591f6918caeb8"),
                Map.entry("V14.0__backfill_recovery_strategy.sql", "8bcf726abe97a23410df77504da53037b83c7664aad97282385488d9976aa95d"),
                Map.entry("V15.0__task_intent_claim_lease.sql", "fec737ba00e4fab83f2cd6000189ebe2c550b4145fc511fa0378df4b6ca72a31"),
                Map.entry("V16.0__scheduling_intent_outbox.sql", "13fd404c19f6ef9100e2f637fb6a4f232b193dde0bebe39c0337ce88eb619ecc"),
                Map.entry("V17.0__scheduling_intent_contract.sql", "9541d4d0c7894232e74fbaacdc829480f52d42b7eae06a80d1ef09ee463b299d"),
                Map.entry("V18.0__scheduling_target_admission.sql", "d49b820e3ef0c6ca8118bd39d4398f02f505f0f5fdff02289d2f75301280dd8b"),
                Map.entry("V19.0__scheduling_intent_delivery_reliability.sql", "7c4e83a6869473926e95ab461261b6780738450ea145afcfd766fafddeec4823"),
                Map.entry("V20.0__split_observed_and_data_snapshot_state.sql", "802531f32706b163fc2fa3610552776e271edc3083b5d140ed6d1a209dec2d8e"),
                Map.entry("V21.0__source_health_and_mixed_dag.sql", "39d02f137b5dc9a46aa0e4e6205ece24a3a9405336e3ff1605a25792914cfe5d"),
                Map.entry("V22.0__writer_job_control.sql", "290d96f6d5e8d679d06f796a9bd742b596332158b82584f85ec21c1771fc37ec"),
                Map.entry("V23.0__task_source_evidence.sql", "72fb97d19194437838f7db10c6dc8aa7a5b61af2e6e5074978abcc0a2e0485ea"));
    }
}
