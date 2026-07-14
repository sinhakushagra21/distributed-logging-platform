package com.observability;

import com.observability.enrich.EnrichmentFunction;
import com.observability.metrics.MetricsAggregator;
import com.observability.metrics.MetricsWindowFunction;
import com.observability.model.EnrichedLog;
import com.observability.model.ServiceMetrics;
import com.observability.parse.LogParseFunction;
import com.observability.sink.ElasticsearchSinkFactory;
import com.observability.sink.LogToAvro;
import com.observability.sink.ParquetSinkFactory;
import com.observability.sink.TimescaleSinkFactory;
import com.observability.sink.AvroSchemas;
import com.observability.util.BytesDeserializationSchema;

import org.apache.avro.Schema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.formats.avro.typeutils.GenericRecordAvroTypeInfo;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * The core stream-processing job. Topology:
 *
 * <pre>
 *   Kafka "logs" (Protobuf bytes)
 *        │
 *        ▼  parse + validate  ──(malformed)──► side output ──► Kafka "logs.dlq"
 *   EnrichedLog
 *        │  enrich (status_class, processing ts)
 *        ▼  filter (drop most DEBUG; keep INFO/WARN/ERROR)
 *   assign event-time watermarks (bounded out-of-orderness 5s)
 *        ├───────────────► Elasticsearch  (enriched logs, daily index)
 *        ├──► map→Avro ───► S3/MinIO       (hourly Parquet archive)
 *        ├─ keyBy(service) ─ TUMBLING 1m  ─ aggregate ─► TimescaleDB (metrics)
 *        └─ keyBy(service) ─ SLIDING 1m/10s ─ aggregate ─► TimescaleDB (moving)
 * </pre>
 *
 * Fault tolerance: checkpointing is enabled in EXACTLY_ONCE mode. Flink's Kafka
 * source replays from committed offsets on recovery, operator state (window
 * accumulators) is restored from the last checkpoint, and the transactional
 * pieces (Parquet files roll on checkpoint; ES/JDBC sinks are made idempotent
 * via stable ids / upserts) give effectively-once results end to end.
 */
public class StreamingJob {

    /** Late events (arriving after the watermark + allowed lateness) go here. */
    private static final OutputTag<EnrichedLog> LATE =
            new OutputTag<EnrichedLog>("late-logs") {};

    public static void main(String[] args) throws Exception {
        // ---- config (env with sensible local defaults) ----
        String bootstrap = env("KAFKA_BOOTSTRAP", "kafka:9092");
        String inTopic = env("KAFKA_TOPIC", "logs");
        String dlqTopic = env("KAFKA_DLQ_TOPIC", "logs.dlq");
        String esHost = env("ES_HOST", "elasticsearch");
        int esPort = Integer.parseInt(env("ES_PORT", "9200"));
        String tsUrl = env("TIMESCALE_URL", "jdbc:postgresql://timescaledb:5432/metrics");
        String tsUser = env("TIMESCALE_USER", "postgres");
        String tsPass = env("TIMESCALE_PASSWORD", "postgres");
        String parquetPath = env("PARQUET_PATH", "s3a://logs-archive/enriched");
        String ckptDir = env("CHECKPOINT_DIR", "file:///tmp/flink-checkpoints");
        double debugKeep = Double.parseDouble(env("DEBUG_SAMPLE_KEEP", "0.1"));
        // Per-sink toggles: let the job run on a memory-constrained machine with
        // only a subset of backends (e.g. metrics-only). Default: everything on.
        boolean enableEs = Boolean.parseBoolean(env("ENABLE_ES", "true"));
        boolean enableParquet = Boolean.parseBoolean(env("ENABLE_PARQUET", "true"));
        boolean enableTimescale = Boolean.parseBoolean(env("ENABLE_TIMESCALE", "true"));

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // ---- checkpointing / fault tolerance ----
        env.enableCheckpointing(30_000, CheckpointingMode.EXACTLY_ONCE);
        CheckpointConfig ckpt = env.getCheckpointConfig();
        ckpt.setMinPauseBetweenCheckpoints(5_000);
        ckpt.setCheckpointTimeout(120_000);
        ckpt.setMaxConcurrentCheckpoints(1);
        ckpt.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        ckpt.setCheckpointStorage(ckptDir);
        // NB: restart strategy uses common.time.Time, distinct from the
        // windowing Time imported for the window assigners below.
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                3, org.apache.flink.api.common.time.Time.seconds(10)));

        // ---- source: raw Protobuf bytes off Kafka ----
        KafkaSource<byte[]> source = KafkaSource.<byte[]>builder()
                .setBootstrapServers(bootstrap)
                .setTopics(inTopic)
                .setGroupId("flink-log-processor")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new BytesDeserializationSchema())
                .build();

        DataStream<byte[]> raw = env.fromSource(
                source, WatermarkStrategy.noWatermarks(), "kafka-logs-source");

        // ---- parse + validate (malformed -> dead-letter) ----
        SingleOutputStreamOperator<EnrichedLog> parsed =
                raw.process(new LogParseFunction()).name("parse-validate");

        // dead-letter records -> Kafka logs.dlq
        KafkaSink<String> dlqSink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrap)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(dlqTopic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build())
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        parsed.getSideOutput(LogParseFunction.DEAD_LETTER)
                .sinkTo(dlqSink).name("dead-letter-sink");

        // ---- enrich ----
        SingleOutputStreamOperator<EnrichedLog> enriched =
                parsed.map(new EnrichmentFunction()).name("enrich");

        // ---- filter: keep all non-DEBUG; sample DEBUG ----
        SingleOutputStreamOperator<EnrichedLog> filtered = enriched.filter(e -> {
            if (!"DEBUG".equalsIgnoreCase(e.level)) {
                return true;
            }
            return ThreadLocalRandom.current().nextDouble() < debugKeep;
        }).name("filter-debug-sample");

        // ---- event-time watermarks ----
        // Bounded out-of-orderness: we assume logs can arrive up to 5s late
        // relative to the max event time seen; the watermark = maxSeen - 5s.
        // withIdleness keeps windows progressing if a partition goes quiet.
        SingleOutputStreamOperator<EnrichedLog> timed = filtered.assignTimestampsAndWatermarks(
                WatermarkStrategy.<EnrichedLog>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.eventTimeMs)
                        .withIdleness(Duration.ofSeconds(15))
        ).name("assign-watermarks");

        // ---- SINK 1: enriched logs -> Elasticsearch ----
        if (enableEs) {
            timed.sinkTo(ElasticsearchSinkFactory.build(esHost, esPort)).name("es-sink");
        }

        // ---- SINK 2: enriched logs -> S3/MinIO Parquet ----
        if (enableParquet) {
            // Tag the GenericRecord stream with an explicit Avro type so Flink
            // uses AvroSerializer (not Kryo) to copy/serialize it. Kryo cannot
            // reconstruct Avro's Schema (it holds immutable collections) and
            // throws UnsupportedOperationException — this is the fix for that.
            Schema avroSchema = new Schema.Parser().parse(AvroSchemas.ENRICHED_LOG_JSON);
            timed.map(new LogToAvro())
                    .returns(new GenericRecordAvroTypeInfo(avroSchema))
                    .name("to-avro")
                    .sinkTo(ParquetSinkFactory.build(parquetPath)).name("parquet-sink");
        }

        // ---- SINK 3a: TUMBLING 1-min per-service metrics -> TimescaleDB ----
        SingleOutputStreamOperator<ServiceMetrics> tumbling = timed
                .keyBy(e -> e.serviceName)
                .window(TumblingEventTimeWindows.of(Time.minutes(1)))
                .allowedLateness(Time.seconds(30))
                .sideOutputLateData(LATE)
                .aggregate(new MetricsAggregator(), new MetricsWindowFunction("tumbling_1m"))
                .name("tumbling-1m");
        if (enableTimescale) {
            tumbling.addSink(TimescaleSinkFactory.build(tsUrl, tsUser, tsPass)).name("ts-tumbling");
        }

        // ---- SINK 3b: SLIDING 1-min / 10-sec moving error-rate -> TimescaleDB --
        SingleOutputStreamOperator<ServiceMetrics> sliding = timed
                .keyBy(e -> e.serviceName)
                .window(SlidingEventTimeWindows.of(Time.minutes(1), Time.seconds(10)))
                .aggregate(new MetricsAggregator(), new MetricsWindowFunction("sliding_1m_10s"))
                .name("sliding-1m-10s");
        if (enableTimescale) {
            sliding.addSink(TimescaleSinkFactory.build(tsUrl, tsUser, tsPass)).name("ts-sliding");
        }

        // ---- late data (past watermark + lateness): send to DLQ for audit ----
        tumbling.getSideOutput(LATE)
                .map(e -> "{\"reason\":\"late_event\",\"service\":\"" + e.serviceName
                        + "\",\"cid\":\"" + e.correlationId + "\",\"event_time_ms\":"
                        + e.eventTimeMs + "}")
                .sinkTo(dlqSink).name("late-to-dlq");

        env.execute("distributed-log-processor");
    }

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isEmpty()) ? def : v;
    }
}
