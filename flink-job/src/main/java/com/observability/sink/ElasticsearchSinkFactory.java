package com.observability.sink;

import com.observability.model.EnrichedLog;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.elasticsearch.sink.ElasticsearchSink;
import org.apache.flink.connector.elasticsearch.sink.FlushBackoffType;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;

/**
 * Builds the Elasticsearch sink for enriched logs.
 *
 * <p>Design notes:
 *   - <b>Daily indices</b> ({@code logs-yyyy.MM.dd}): time-based indices make
 *     retention trivial (drop an old index) and keep each index small enough to
 *     search fast. The index is derived from the record's EVENT time, so a late
 *     log still lands in the correct day.
 *   - <b>Deterministic document id</b>: id = correlationId:service:eventTime:hash.
 *     Flink's ES sink is at-least-once (it may re-send on recovery); a stable id
 *     makes those retries idempotent (same id overwrites rather than duplicates).
 *   - <b>Bulk flushing</b>: batches index requests for throughput, with backoff
 *     retries so transient ES pressure doesn't drop data.
 */
public final class ElasticsearchSinkFactory {

    private static final DateTimeFormatter INDEX_DAY =
            DateTimeFormatter.ofPattern("yyyy.MM.dd").withZone(ZoneOffset.UTC);

    private ElasticsearchSinkFactory() {
    }

    public static ElasticsearchSink<EnrichedLog> build(String host, int port) {
        return new Elasticsearch7SinkBuilder<EnrichedLog>()
                .setHosts(new HttpHost(host, port, "http"))
                .setBulkFlushMaxActions(500)
                .setBulkFlushInterval(2000)
                .setBulkFlushBackoffStrategy(FlushBackoffType.EXPONENTIAL, 3, 1000)
                .setEmitter((log, ctx, indexer) -> indexer.add(toRequest(log)))
                .build();
    }

    private static IndexRequest toRequest(EnrichedLog e) {
        String index = "logs-" + INDEX_DAY.format(Instant.ofEpochMilli(e.eventTimeMs));
        String id = e.correlationId + ":" + e.serviceName + ":" + e.eventTimeMs
                + ":" + Math.abs((e.message == null ? "" : e.message).hashCode());

        Map<String, Object> doc = new HashMap<>();
        doc.put("@timestamp", e.timestamp);          // ES/Kibana convention
        doc.put("event_time_ms", e.eventTimeMs);
        doc.put("service_name", e.serviceName);
        doc.put("level", e.level);
        doc.put("correlation_id", e.correlationId);
        doc.put("message", e.message);
        doc.put("status_code", e.statusCode);
        doc.put("status_class", e.statusClass);
        doc.put("latency_ms", e.latencyMs);
        doc.put("region", e.region);
        doc.put("endpoint", e.endpoint);
        doc.put("user_id", e.userId);
        doc.put("host", e.host);
        doc.put("processing_ts_ms", e.processingTsMs);
        if (e.extra != null && !e.extra.isEmpty()) {
            doc.put("extra", e.extra);
        }

        // source(Map) lets the ES client infer content type, avoiding a direct
        // XContentType dependency (its package moved across ES 7.x versions).
        return new IndexRequest(index).id(id).source(doc);
    }
}
