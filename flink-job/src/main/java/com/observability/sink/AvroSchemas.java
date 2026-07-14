package com.observability.sink;

/** Avro schema (as JSON) for the Parquet archival files written to S3/MinIO. */
public final class AvroSchemas {

    private AvroSchemas() {
    }

    public static final String ENRICHED_LOG_JSON =
            "{"
            + "\"type\":\"record\",\"name\":\"EnrichedLog\","
            + "\"namespace\":\"com.observability.avro\",\"fields\":["
            + "{\"name\":\"timestamp\",\"type\":\"string\"},"
            + "{\"name\":\"event_time_ms\",\"type\":\"long\"},"
            + "{\"name\":\"service_name\",\"type\":\"string\"},"
            + "{\"name\":\"level\",\"type\":\"string\"},"
            + "{\"name\":\"correlation_id\",\"type\":\"string\"},"
            + "{\"name\":\"message\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"status_code\",\"type\":\"int\"},"
            + "{\"name\":\"status_class\",\"type\":\"string\"},"
            + "{\"name\":\"latency_ms\",\"type\":\"double\"},"
            + "{\"name\":\"region\",\"type\":\"string\"},"
            + "{\"name\":\"endpoint\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"user_id\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"host\",\"type\":[\"null\",\"string\"],\"default\":null},"
            + "{\"name\":\"processing_ts_ms\",\"type\":\"long\"},"
            + "{\"name\":\"extra_json\",\"type\":[\"null\",\"string\"],\"default\":null}"
            + "]}";
}
