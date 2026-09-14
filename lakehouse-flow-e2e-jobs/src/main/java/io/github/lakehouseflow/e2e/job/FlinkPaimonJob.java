package io.github.lakehouseflow.e2e.job;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import io.github.lakehouseflow.writer.paimon.FlinkPaimonWriterAdapter;
import io.github.lakehouseflow.writer.paimon.JdbcWriterEpochFence;
import io.github.lakehouseflow.writer.paimon.WriterCommitContext;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.util.CloseableIterator;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.fs.Path;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;

import java.io.Serializable;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Test-only Flink entry point for the complete order-to-GMV Paimon pipeline.
 *
 * <p>The artifact is submitted to a real Flink Testcontainers cluster by the E2E HTTP
 * consumer. It deliberately lives outside production modules: Lakehouse Flow publishes
 * scheduling instructions, while this process interprets those instructions as executable
 * Flink work for system verification.
 */
public final class FlinkPaimonJob {

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final String ODS = "ODS";
    private static final String DWD = "DWD";
    private static final String DWS = "DWS";
    private static final String ADS = "ADS";
    private static final String INIT = "INIT";
    private static final String VERIFY = "VERIFY";
    private static final String FENCE_PROBE = "FENCE_PROBE";

    /** Prevent construction of the command-line entry point. */
    private FlinkPaimonJob() {
    }

    /**
     * Dispatch one streaming or batch Flink job from the immutable E2E instruction arguments.
     *
     * @param rawArguments command-line key/value arguments supplied by the test executor
     * @throws Exception when Flink execution or the Paimon commit fails
     */
    public static void main(String[] rawArguments) throws Exception {
        JobArguments arguments = JobArguments.parse(rawArguments);
        switch (arguments.required("job-type").toUpperCase(java.util.Locale.ROOT)) {
            case INIT -> initializeTables(arguments);
            case ODS -> runOdsCdc(arguments);
            case DWD -> runDwdBatch(arguments);
            case DWS -> runDwsBatch(arguments);
            case ADS -> runAdsBatch(arguments);
            case VERIFY -> verifyAds(arguments);
            case FENCE_PROBE -> runFenceProbe(arguments);
            default -> throw new IllegalArgumentException(
                    "Unsupported E2E job type: " + arguments.required("job-type"));
        }
    }

    /**
     * Provision the four physical Paimon tables before snapshot source scanning begins.
     *
     * @param arguments warehouse location supplied by the E2E environment
     * @throws Exception when catalog initialization or Flink execution fails
     */
    private static void initializeTables(JobArguments arguments) throws Exception {
        String warehouse = arguments.required("warehouse");
        PaimonWarehouse.ensureTable(warehouse, "ods.orders", TableKind.ODS);
        PaimonWarehouse.ensureTable(warehouse, "dwd.order_detail", TableKind.DWD);
        PaimonWarehouse.ensureTable(warehouse, "dws.daily_gmv", TableKind.DWS);
        PaimonWarehouse.ensureTable(warehouse, "ads.gmv", TableKind.ADS);

        StreamExecutionEnvironment environment = batchEnvironment();
        environment.fromData(1)
                .name("paimon-table-initialization-marker")
                .print();
        environment.execute("lakehouse-flow-e2e-paimon-table-initialization");
    }

    /**
     * Run the unbounded MySQL CDC source and commit ODS data only from completed checkpoints.
     *
     * @param arguments validated ODS job arguments
     * @throws Exception when the source or checkpoint sink fails
     */
    private static void runOdsCdc(JobArguments arguments) throws Exception {
        String targetTable = arguments.required("target-table");
        PaimonWarehouse.ensureTable(arguments.required("warehouse"), targetTable, TableKind.ODS);

        MySqlSource<String> source = MySqlSource.<String>builder()
                .hostname(arguments.required("mysql-host"))
                .port(arguments.integer("mysql-port"))
                .databaseList(arguments.required("mysql-database"))
                .tableList(arguments.required("mysql-database") + "." + arguments.required("mysql-table"))
                .username(arguments.required("mysql-username"))
                .password(arguments.required("mysql-password"))
                .serverId(arguments.required("mysql-server-id"))
                .serverTimeZone("UTC")
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema())
                .build();

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        environment.setParallelism(1);
        configureRecordSerializers(environment);
        environment.enableCheckpointing(
                arguments.longValue("checkpoint-interval-ms"),
                CheckpointingMode.EXACTLY_ONCE);
        environment.getCheckpointConfig().setCheckpointTimeout(Duration.ofMinutes(1).toMillis());
        environment.getCheckpointConfig().setCheckpointStorage(arguments.required("checkpoint-storage"));
        environment.getCheckpointConfig().enableExternalizedCheckpoints(
                ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        environment.fromSource(source, WatermarkStrategy.noWatermarks(), "mysql-orders-cdc")
                .map(FlinkPaimonJob::parseCdcOrder)
                .returns(TypeInformation.of(OrderRecord.class))
                .addSink(new CheckpointPaimonOrderSink(arguments))
                .name("paimon-ods-checkpoint-commit")
                .setParallelism(1);
        environment.execute("lakehouse-flow-e2e-ods-orders-cdc");
    }

    /**
     * Execute the bounded ODS-to-DWD cleansing job and commit its attributed Paimon snapshot.
     *
     * @param arguments immutable scheduling-intent arguments
     * @throws Exception when Flink execution or Paimon commit fails
     */
    private static void runDwdBatch(JobArguments arguments) throws Exception {
        List<OrderRecord> orders = PaimonWarehouse.readOrders(
                arguments.required("warehouse"), arguments.required("source-table"));
        StreamExecutionEnvironment environment = batchEnvironment();
        DataStream<DetailRecord> details = environment.fromCollection(orders)
                .filter(order -> "PAID".equals(order.status()) && order.amountCents() > 0)
                .map(order -> new DetailRecord(
                        order.orderId(), order.userId(), order.amountCents(), order.orderDate()))
                .returns(TypeInformation.of(DetailRecord.class));
        List<DetailRecord> result = collect(details);
        PaimonWarehouse.commitDetails(arguments, result);
    }

    /**
     * Execute the bounded DWD-to-DWS daily aggregation and commit its attributed snapshot.
     *
     * @param arguments immutable scheduling-intent arguments
     * @throws Exception when Flink execution or Paimon commit fails
     */
    private static void runDwsBatch(JobArguments arguments) throws Exception {
        List<DetailRecord> details = PaimonWarehouse.readDetails(
                arguments.required("warehouse"), arguments.required("source-table"));
        StreamExecutionEnvironment environment = batchEnvironment();
        DataStream<DailyGmvRecord> daily = environment.fromCollection(details)
                .map(detail -> new DailyGmvRecord(detail.orderDate(), 1L, detail.amountCents()))
                .returns(TypeInformation.of(DailyGmvRecord.class))
                .keyBy(DailyGmvRecord::orderDate)
                .reduce((left, right) -> new DailyGmvRecord(
                        left.orderDate(),
                        left.orderCount() + right.orderCount(),
                        left.gmvCents() + right.gmvCents()));
        List<DailyGmvRecord> result = collect(daily);
        PaimonWarehouse.commitDailyGmv(arguments, result);
    }

    /**
     * Execute the bounded DWS-to-ADS publication job and commit the final GMV snapshot.
     *
     * @param arguments immutable scheduling-intent arguments
     * @throws Exception when Flink execution or Paimon commit fails
     */
    private static void runAdsBatch(JobArguments arguments) throws Exception {
        List<DailyGmvRecord> daily = PaimonWarehouse.readDailyGmv(
                arguments.required("warehouse"), arguments.required("source-table"));
        StreamExecutionEnvironment environment = batchEnvironment();
        DataStream<GmvMetricRecord> metrics = environment.fromCollection(daily)
                .map(value -> new GmvMetricRecord(value.orderDate(), value.gmvCents()))
                .returns(TypeInformation.of(GmvMetricRecord.class));
        List<GmvMetricRecord> result = collect(metrics);
        PaimonWarehouse.commitGmvMetric(arguments, result);
    }

    /**
     * Read the final ADS table through a bounded Flink job and fail on an incorrect GMV value.
     *
     * @param arguments expected business date and GMV assertion arguments
     * @throws Exception when the table cannot be read or its value differs
     */
    private static void verifyAds(JobArguments arguments) throws Exception {
        List<GmvMetricRecord> metrics = PaimonWarehouse.readGmvMetrics(
                arguments.required("warehouse"), arguments.required("target-table"));
        StreamExecutionEnvironment environment = batchEnvironment();
        DataStream<GmvMetricRecord> matching = environment
                .fromCollection(metrics, TypeInformation.of(GmvMetricRecord.class))
                .filter(metric -> metric.metricDate().equals(arguments.required("expected-date")))
                .returns(TypeInformation.of(GmvMetricRecord.class));
        List<GmvMetricRecord> result = collect(matching);
        GmvMetricRecord expected = new GmvMetricRecord(
                arguments.required("expected-date"),
                arguments.longValue("expected-gmv-cents"));
        if (!result.equals(List.of(expected))) {
            throw new IllegalStateException("Unexpected ADS GMV: expected " + expected + " but got " + result);
        }
        System.out.println("LAKEHOUSE_FLOW_E2E_GMV_VERIFIED=" + expected.gmvCents());
    }

    /**
     * Attempt one real Paimon commit using a superseded writer epoch.
     *
     * @param arguments stale writer coordinates supplied by the E2E execution plane
     * @throws Exception always when fencing works, before a Paimon snapshot is published
     */
    private static void runFenceProbe(JobArguments arguments) throws Exception {
        try (FlinkPaimonWriterAdapter adapter = openWriterAdapter(arguments)) {
            adapter.write(PaimonWarehouse.orderRow(new OrderRecord(
                    9_999_999L,
                    9_999_999L,
                    1L,
                    "2099-12-31",
                    "PAID",
                    false)));
            List<CommitMessage> messages = adapter.prepareBatch();
            adapter.commit(
                    FlinkPaimonWriterAdapter.stableCommitIdentifier(
                            arguments.required("intent-key") + ":stale-probe"),
                    messages);
        }
        throw new IllegalStateException("Stale writer epoch unexpectedly published a Paimon snapshot");
    }

    /** Open the reusable Paimon writer adapter with a JDBC-backed epoch fence. */
    private static FlinkPaimonWriterAdapter openWriterAdapter(JobArguments arguments) throws Exception {
        Options catalogOptions = new Options();
        catalogOptions.set("warehouse", arguments.required("warehouse"));
        catalogOptions.set("metastore", "filesystem");
        WriterCommitContext context = new WriterCommitContext(
                arguments.required("table-asset-key"),
                arguments.required("writer-job-key"),
                arguments.longValue("writer-epoch"),
                arguments.required("intent-key"),
                WriterCommitContext.IntentKind.valueOf(arguments.required("intent-kind")),
                arguments.required("processing-mode"),
                arguments.snapshotProperties());
        JdbcWriterEpochFence fence = new JdbcWriterEpochFence(
                arguments.required("fence-jdbc-url"),
                arguments.required("fence-jdbc-username"),
                arguments.required("fence-jdbc-password"));
        return FlinkPaimonWriterAdapter.open(
                catalogOptions,
                arguments.required("target-table"),
                context,
                fence);
    }

    /** Create a deterministic single-parallelism Flink batch environment. */
    private static StreamExecutionEnvironment batchEnvironment() {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        environment.setParallelism(1);
        configureRecordSerializers(environment);
        return environment;
    }

    /**
     * Register field-level serializers because Flink 1.20 Kryo cannot reflectively mutate final
     * record components on Java 17.
     *
     * @param environment environment that transports E2E data-plane records
     */
    private static void configureRecordSerializers(StreamExecutionEnvironment environment) {
        environment.getConfig().registerTypeWithKryoSerializer(OrderRecord.class, OrderRecordSerializer.class);
        environment.getConfig().registerTypeWithKryoSerializer(DetailRecord.class, DetailRecordSerializer.class);
        environment.getConfig().registerTypeWithKryoSerializer(DailyGmvRecord.class, DailyGmvRecordSerializer.class);
        environment.getConfig().registerTypeWithKryoSerializer(GmvMetricRecord.class, GmvMetricRecordSerializer.class);
    }

    /** Collect a bounded Flink result before the driver publishes one atomic Paimon snapshot. */
    private static <T> List<T> collect(DataStream<T> stream) throws Exception {
        List<T> values = new ArrayList<>();
        try (CloseableIterator<T> iterator = stream.executeAndCollect()) {
            iterator.forEachRemaining(values::add);
        }
        return List.copyOf(values);
    }

    /** Convert one Debezium JSON envelope into an insert or delete order record. */
    private static OrderRecord parseCdcOrder(String json) throws Exception {
        JsonNode envelope = OBJECT_MAPPER.readTree(json);
        boolean deleted = "d".equals(envelope.path("op").asText());
        JsonNode row = deleted ? envelope.path("before") : envelope.path("after");
        if (row.isMissingNode() || row.isNull()) {
            throw new IllegalArgumentException("CDC envelope has no row image: " + json);
        }
        return new OrderRecord(
                row.path("order_id").asLong(),
                row.path("user_id").asLong(),
                row.path("amount_cents").asLong(),
                row.path("order_date").asText(),
                row.path("status").asText(),
                deleted);
    }

    /** Serializable order image transferred through the Flink data plane. */
    record OrderRecord(
            long orderId,
            long userId,
            long amountCents,
            String orderDate,
            String status,
            boolean deleted) implements Serializable {
    }

    /** Serializable cleansed order detail transferred through the Flink data plane. */
    record DetailRecord(long orderId, long userId, long amountCents, String orderDate)
            implements Serializable {
    }

    /** Serializable date-level aggregate transferred through the Flink data plane. */
    record DailyGmvRecord(String orderDate, long orderCount, long gmvCents)
            implements Serializable {
    }

    /** Serializable ADS metric transferred through the Flink data plane. */
    record GmvMetricRecord(String metricDate, long gmvCents) implements Serializable {
    }

    /** Field-level Kryo serializer for immutable ODS order records. */
    public static final class OrderRecordSerializer extends Serializer<OrderRecord> {

        /** Serialize one order without reflective access to record components. */
        @Override
        public void write(Kryo kryo, Output output, OrderRecord value) {
            output.writeLong(value.orderId());
            output.writeLong(value.userId());
            output.writeLong(value.amountCents());
            output.writeString(value.orderDate());
            output.writeString(value.status());
            output.writeBoolean(value.deleted());
        }

        /** Reconstruct one immutable order from its serialized fields. */
        @Override
        public OrderRecord read(Kryo kryo, Input input, Class<OrderRecord> type) {
            return new OrderRecord(
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readString(),
                    input.readString(),
                    input.readBoolean());
        }
    }

    /** Field-level Kryo serializer for immutable DWD detail records. */
    public static final class DetailRecordSerializer extends Serializer<DetailRecord> {

        /** Serialize one cleansed order detail. */
        @Override
        public void write(Kryo kryo, Output output, DetailRecord value) {
            output.writeLong(value.orderId());
            output.writeLong(value.userId());
            output.writeLong(value.amountCents());
            output.writeString(value.orderDate());
        }

        /** Reconstruct one cleansed order detail. */
        @Override
        public DetailRecord read(Kryo kryo, Input input, Class<DetailRecord> type) {
            return new DetailRecord(
                    input.readLong(),
                    input.readLong(),
                    input.readLong(),
                    input.readString());
        }
    }

    /** Field-level Kryo serializer for immutable DWS aggregates. */
    public static final class DailyGmvRecordSerializer extends Serializer<DailyGmvRecord> {

        /** Serialize one daily GMV aggregate. */
        @Override
        public void write(Kryo kryo, Output output, DailyGmvRecord value) {
            output.writeString(value.orderDate());
            output.writeLong(value.orderCount());
            output.writeLong(value.gmvCents());
        }

        /** Reconstruct one daily GMV aggregate. */
        @Override
        public DailyGmvRecord read(Kryo kryo, Input input, Class<DailyGmvRecord> type) {
            return new DailyGmvRecord(input.readString(), input.readLong(), input.readLong());
        }
    }

    /** Field-level Kryo serializer for immutable ADS metrics. */
    public static final class GmvMetricRecordSerializer extends Serializer<GmvMetricRecord> {

        /** Serialize one published GMV metric. */
        @Override
        public void write(Kryo kryo, Output output, GmvMetricRecord value) {
            output.writeString(value.metricDate());
            output.writeLong(value.gmvCents());
        }

        /** Reconstruct one published GMV metric. */
        @Override
        public GmvMetricRecord read(Kryo kryo, Input input, Class<GmvMetricRecord> type) {
            return new GmvMetricRecord(input.readString(), input.readLong());
        }
    }

    /** Supported Paimon schemas used by the four E2E nodes. */
    enum TableKind {
        ODS,
        DWD,
        DWS,
        ADS
    }

    /**
     * Minimal key/value parser for the isolated Flink job process.
     *
     * @param values immutable command-line values
     */
    record JobArguments(Map<String, String> values) implements Serializable {

        /** Defensively copy parsed arguments. */
        JobArguments {
            values = Map.copyOf(values);
        }

        /** Parse alternating {@code --key value} arguments. */
        static JobArguments parse(String[] arguments) {
            if (arguments == null || arguments.length == 0 || arguments.length % 2 != 0) {
                throw new IllegalArgumentException("E2E job arguments must be --key value pairs");
            }
            Map<String, String> values = new LinkedHashMap<>();
            for (int index = 0; index < arguments.length; index += 2) {
                String key = arguments[index];
                if (!key.startsWith("--") || key.length() == 2) {
                    throw new IllegalArgumentException("Invalid E2E job argument key: " + key);
                }
                values.put(key.substring(2), arguments[index + 1]);
            }
            return new JobArguments(values);
        }

        /** Return one required nonblank value. */
        String required(String key) {
            String value = values.get(key);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing E2E job argument: " + key);
            }
            return value;
        }

        /** Parse one required integer value. */
        int integer(String key) {
            return Integer.parseInt(required(key));
        }

        /** Parse one required long value. */
        long longValue(String key) {
            return Long.parseLong(required(key));
        }

        /** Decode the immutable snapshot property map transported as URL-safe Base64 JSON. */
        @SuppressWarnings("unchecked")
        Map<String, String> snapshotProperties() throws Exception {
            byte[] json = Base64.getUrlDecoder().decode(required("snapshot-properties"));
            return Map.copyOf(OBJECT_MAPPER.readValue(json, Map.class));
        }
    }

    /**
     * Paimon sink that prepares files at a checkpoint barrier and publishes them only after
     * Flink reports that checkpoint complete.
     */
    static final class CheckpointPaimonOrderSink extends RichSinkFunction<OrderRecord>
            implements CheckpointedFunction, CheckpointListener {

        private final JobArguments arguments;

        private transient FlinkPaimonWriterAdapter writerAdapter;
        private transient NavigableMap<Long, List<CommitMessage>> pendingCommits;

        /** Create one checkpoint-bound Paimon sink for a fenced writer generation. */
        CheckpointPaimonOrderSink(JobArguments arguments) {
            this.arguments = arguments;
        }

        /** Open the reusable fenced writer adapter inside the Flink task container. */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            writerAdapter = openWriterAdapter(arguments);
            pendingCommits = new TreeMap<>();
        }

        /** Write one CDC row image into the current Paimon checkpoint transaction. */
        @Override
        public void invoke(OrderRecord value, Context context) throws Exception {
            writerAdapter.write(PaimonWarehouse.orderRow(value));
        }

        /** Prepare Paimon files under the current Flink checkpoint identifier. */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            List<CommitMessage> messages = writerAdapter.prepareCheckpoint(context.getCheckpointId());
            if (!messages.isEmpty()) {
                pendingCommits.put(context.getCheckpointId(), messages);
            }
        }

        /** Initialize current-generation commit bookkeeping after fresh start or source recovery. */
        @Override
        public void initializeState(FunctionInitializationContext context) {
            pendingCommits = new TreeMap<>();
        }

        /** Publish all prepared transactions covered by one completed Flink checkpoint. */
        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            List<Long> completed = new ArrayList<>(pendingCommits.headMap(checkpointId, true).keySet());
            for (Long completedId : completed) {
                writerAdapter.commit(completedId, pendingCommits.remove(completedId));
            }
        }

        /** Abort files prepared only for a checkpoint that Flink explicitly discarded. */
        @Override
        public void notifyCheckpointAborted(long checkpointId) {
            List<CommitMessage> messages = pendingCommits.remove(checkpointId);
            writerAdapter.abort(messages);
        }

        /** Close native Paimon resources when Flink disposes this writer generation. */
        @Override
        public void close() throws Exception {
            try {
                if (writerAdapter != null) {
                    writerAdapter.close();
                }
            } finally {
                super.close();
            }
        }
    }

    /** Native Paimon table helper shared by streaming and bounded Flink jobs. */
    static final class PaimonWarehouse {

        /** Prevent construction of the static warehouse helper. */
        private PaimonWarehouse() {
        }

        /** Create a table when the current E2E job is the first writer for it. */
        static void ensureTable(String warehouse, String tableName, TableKind kind) throws Exception {
            QualifiedTable table = QualifiedTable.parse(tableName);
            try (Catalog catalog = openCatalog(warehouse)) {
                catalog.createDatabase(table.database(), true);
                catalog.createTable(
                        Identifier.create(table.database(), table.table()),
                        schema(kind),
                        true);
            }
        }

        /** Load one physical Paimon table and reject unexpected table implementations. */
        static FileStoreTable loadTable(String warehouse, String tableName) throws Exception {
            QualifiedTable table = QualifiedTable.parse(tableName);
            Catalog catalog = openCatalog(warehouse);
            Table loaded = catalog.getTable(Identifier.create(table.database(), table.table()));
            if (loaded instanceof FileStoreTable fileStoreTable) {
                return fileStoreTable;
            }
            catalog.close();
            throw new IllegalStateException("Not a Paimon FileStoreTable: " + tableName);
        }

        /** Read the current ODS table into detached serializable order records. */
        static List<OrderRecord> readOrders(String warehouse, String tableName) throws Exception {
            return read(warehouse, tableName, row -> new OrderRecord(
                    row.getLong(0),
                    row.getLong(1),
                    row.getLong(2),
                    row.getString(3).toString(),
                    row.getString(4).toString(),
                    false));
        }

        /** Read the current DWD table into detached serializable detail records. */
        static List<DetailRecord> readDetails(String warehouse, String tableName) throws Exception {
            return read(warehouse, tableName, row -> new DetailRecord(
                    row.getLong(0),
                    row.getLong(1),
                    row.getLong(2),
                    row.getString(3).toString()));
        }

        /** Read the current DWS table into detached serializable daily aggregates. */
        static List<DailyGmvRecord> readDailyGmv(String warehouse, String tableName) throws Exception {
            return read(warehouse, tableName, row -> new DailyGmvRecord(
                    row.getString(0).toString(),
                    row.getLong(2),
                    row.getLong(3)));
        }

        /** Read the final ADS GMV values for E2E assertions. */
        static List<GmvMetricRecord> readGmvMetrics(String warehouse, String tableName) throws Exception {
            return read(warehouse, tableName, row -> new GmvMetricRecord(
                    row.getString(0).toString(),
                    row.getLong(2)));
        }

        /** Commit cleansed details and exact scheduling-intent properties. */
        static void commitDetails(JobArguments arguments, List<DetailRecord> values) throws Exception {
            commitRows(arguments, TableKind.DWD, values.stream()
                    .map(value -> GenericRow.of(
                            value.orderId(),
                            value.userId(),
                            value.amountCents(),
                            BinaryString.fromString(value.orderDate())))
                    .toList());
        }

        /** Commit daily aggregates and exact scheduling-intent properties. */
        static void commitDailyGmv(JobArguments arguments, List<DailyGmvRecord> values) throws Exception {
            commitRows(arguments, TableKind.DWS, values.stream()
                    .map(value -> GenericRow.of(
                            BinaryString.fromString(value.orderDate()),
                            BinaryString.fromString("GMV"),
                            value.orderCount(),
                            value.gmvCents()))
                    .toList());
        }

        /** Commit final ADS metrics and exact scheduling-intent properties. */
        static void commitGmvMetric(JobArguments arguments, List<GmvMetricRecord> values) throws Exception {
            commitRows(arguments, TableKind.ADS, values.stream()
                    .map(value -> GenericRow.of(
                            BinaryString.fromString(value.metricDate()),
                            BinaryString.fromString("GMV"),
                            value.gmvCents()))
                    .toList());
        }

        /** Convert one CDC order into the native Paimon row representation. */
        static GenericRow orderRow(OrderRecord order) {
            return GenericRow.ofKind(
                    order.deleted() ? RowKind.DELETE : RowKind.INSERT,
                    order.orderId(),
                    order.userId(),
                    order.amountCents(),
                    BinaryString.fromString(order.orderDate()),
                    BinaryString.fromString(order.status()));
        }

        /** Commit a bounded Flink result to one target table. */
        private static void commitRows(
                JobArguments arguments,
                TableKind kind,
                List<GenericRow> rows) throws Exception {
            String warehouse = arguments.required("warehouse");
            String targetTable = arguments.required("target-table");
            ensureTable(warehouse, targetTable, kind);
            try (FlinkPaimonWriterAdapter writerAdapter = openWriterAdapter(arguments)) {
                for (GenericRow row : rows) {
                    writerAdapter.write(row);
                }
                List<CommitMessage> messages = writerAdapter.prepareBatch();
                if (messages.isEmpty()) {
                    throw new IllegalStateException("Batch job produced no Paimon commit messages");
                }
                writerAdapter.commit(
                        FlinkPaimonWriterAdapter.stableCommitIdentifier(arguments.required("intent-key")),
                        messages);
            }
        }

        /** Read all visible rows from the latest Paimon snapshot. */
        private static <T> List<T> read(
                String warehouse,
                String tableName,
                RowMapper<T> mapper) throws Exception {
            FileStoreTable table = loadTable(warehouse, tableName);
            ReadBuilder readBuilder = table.newReadBuilder();
            List<T> values = new ArrayList<>();
            try (RecordReader<InternalRow> reader = readBuilder.newRead()
                    .createReader(readBuilder.newScan().plan())) {
                reader.forEachRemaining(row -> values.add(mapper.map(row)));
            }
            return List.copyOf(values);
        }

        /** Open a filesystem catalog shared by the host scanner and Flink containers. */
        private static Catalog openCatalog(String warehouse) {
            Options options = new Options();
            options.set("warehouse", warehouse);
            options.set("metastore", "filesystem");
            return CatalogFactory.createCatalog(CatalogContext.create(options));
        }

        /** Build the fixed schema for one E2E lakehouse layer. */
        private static Schema schema(TableKind kind) {
            Schema.Builder builder = Schema.newBuilder()
                    .option("bucket", "1")
                    .option("write-only", "true")
                    .option("snapshot.time-retained", "24 h")
                    .partitionKeys("dt");
            return switch (kind) {
                case ODS -> builder
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("amount_cents", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .column("status", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .build();
                case DWD -> builder
                        .column("order_id", DataTypes.BIGINT())
                        .column("user_id", DataTypes.BIGINT())
                        .column("amount_cents", DataTypes.BIGINT())
                        .column("dt", DataTypes.STRING())
                        .primaryKey("order_id", "dt")
                        .build();
                case DWS -> builder
                        .column("dt", DataTypes.STRING())
                        .column("metric_key", DataTypes.STRING())
                        .column("order_count", DataTypes.BIGINT())
                        .column("gmv_cents", DataTypes.BIGINT())
                        .primaryKey("dt", "metric_key")
                        .build();
                case ADS -> builder
                        .column("dt", DataTypes.STRING())
                        .column("metric_key", DataTypes.STRING())
                        .column("gmv_cents", DataTypes.BIGINT())
                        .primaryKey("dt", "metric_key")
                        .build();
            };
        }


        /** Map a native Paimon row into a detached Java record. */
        @FunctionalInterface
        private interface RowMapper<T> {

            /** Convert one potentially reused native row immediately. */
            T map(InternalRow row);
        }
    }

    /**
     * Database and table pair parsed from the scheduler's table asset key without its catalog.
     *
     * @param database Paimon database
     * @param table Paimon table
     */
    record QualifiedTable(String database, String table) {

        /** Parse {@code database.table} and reject partition-scoped values. */
        static QualifiedTable parse(String value) {
            String[] parts = value == null ? new String[0] : value.split("\\.", -1);
            if (parts.length != 2 || parts[0].isBlank() || parts[1].isBlank()) {
                throw new IllegalArgumentException("Expected database.table but got: " + value);
            }
            return new QualifiedTable(parts[0], parts[1]);
        }
    }
}
