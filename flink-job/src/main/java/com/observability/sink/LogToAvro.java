package com.observability.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.observability.model.EnrichedLog;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * Converts an {@link EnrichedLog} POJO into an Avro {@link GenericRecord} for the
 * Parquet sink. The Avro {@link Schema} and Jackson mapper are not serializable,
 * so we build them lazily in {@link #open} (per task instance) rather than
 * shipping them with the function.
 */
public class LogToAvro extends RichMapFunction<EnrichedLog, GenericRecord> {

    private transient Schema schema;
    private transient ObjectMapper mapper;

    @Override
    public void open(Configuration parameters) {
        this.schema = new Schema.Parser().parse(AvroSchemas.ENRICHED_LOG_JSON);
        this.mapper = new ObjectMapper();
    }

    @Override
    public GenericRecord map(EnrichedLog e) throws Exception {
        GenericRecord r = new GenericData.Record(schema);
        r.put("timestamp", e.timestamp);
        r.put("event_time_ms", e.eventTimeMs);
        r.put("service_name", e.serviceName);
        r.put("level", e.level);
        r.put("correlation_id", e.correlationId);
        r.put("message", e.message);
        r.put("status_code", e.statusCode);
        r.put("status_class", e.statusClass);
        r.put("latency_ms", e.latencyMs);
        r.put("region", e.region);
        r.put("endpoint", e.endpoint);
        r.put("user_id", e.userId);
        r.put("host", e.host);
        r.put("processing_ts_ms", e.processingTsMs);
        // Flatten the free-form map into a JSON string column so the Parquet
        // schema stays flat and query engines (DuckDB/Trino) can read it easily.
        r.put("extra_json", e.extra == null || e.extra.isEmpty()
                ? null : mapper.writeValueAsString(e.extra));
        return r;
    }
}
