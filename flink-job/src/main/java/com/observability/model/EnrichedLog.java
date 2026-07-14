package com.observability.model;

import java.util.HashMap;
import java.util.Map;

/**
 * A validated, enriched log record — the canonical internal representation that
 * flows between Flink operators and out to the sinks.
 *
 * <p>This is a Flink POJO: a public no-arg constructor plus public fields, which
 * lets Flink use its fast built-in serializer (instead of falling back to Kryo)
 * for network shuffles and state. We convert the raw Protobuf {@code LogEvent}
 * into this type immediately after parsing so the (Kryo-unfriendly) protobuf
 * object never travels between operators.
 */
public class EnrichedLog {

    // ---- core ----
    public String timestamp;        // original ISO8601 string
    public long eventTimeMs;        // parsed epoch millis (used for event-time)
    public String serviceName;
    public String level;
    public String correlationId;
    public String message;

    // ---- context ----
    public int statusCode;
    public double latencyMs;
    public String region;
    public String endpoint;
    public String userId;

    // ---- metadata ----
    public String host;
    public String collector;
    public long ingestTsMs;         // when the shipper produced to Kafka

    // ---- derived by the enrichment step ----
    public String statusClass;      // "2xx".."5xx" or "none"
    public long processingTsMs;     // when Flink processed it (proc-time)

    public Map<String, String> extra = new HashMap<>();

    public EnrichedLog() {
    }

    public boolean isError() {
        return "ERROR".equalsIgnoreCase(level);
    }

    public boolean isWarn() {
        return "WARN".equalsIgnoreCase(level) || "WARNING".equalsIgnoreCase(level);
    }

    @Override
    public String toString() {
        return "EnrichedLog{" + serviceName + " " + level + " cid=" + correlationId
                + " status=" + statusCode + " lat=" + latencyMs + "}";
    }
}
