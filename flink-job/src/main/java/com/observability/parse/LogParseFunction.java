package com.observability.parse;

import com.observability.model.EnrichedLog;
import com.observability.proto.LogEvent;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Instant;
import java.util.Set;

/**
 * Step 1+2 of the topology: parse Protobuf and validate.
 *
 * <p>Valid records are emitted downstream as {@link EnrichedLog} (a Flink POJO;
 * we convert here so the raw protobuf object never crosses the network).
 * Anything that fails to deserialize or fails validation is routed to the
 * {@link #DEAD_LETTER} side output — the "dead-letter" pattern — so a single
 * malformed record can never crash the job or silently vanish. A downstream
 * Kafka sink writes the side output to the {@code logs.dlq} topic for later
 * inspection/replay.
 */
public class LogParseFunction extends ProcessFunction<byte[], EnrichedLog> {

    /** Side-output channel for records we cannot process. */
    public static final OutputTag<String> DEAD_LETTER =
            new OutputTag<String>("dead-letter") {};

    private static final Set<String> VALID_LEVELS =
            Set.of("DEBUG", "INFO", "WARN", "WARNING", "ERROR");

    @Override
    public void processElement(byte[] value, Context ctx, Collector<EnrichedLog> out) {
        LogEvent evt;
        try {
            evt = LogEvent.parseFrom(value);
        } catch (InvalidProtocolBufferException e) {
            ctx.output(DEAD_LETTER, dlq("protobuf_parse_error", e.getMessage(), value.length));
            return;
        }

        // --- validation: required fields must be present and sane ---
        if (isBlank(evt.getCorrelationId())) {
            ctx.output(DEAD_LETTER, dlq("missing_correlation_id", evt.toString(), value.length));
            return;
        }
        if (isBlank(evt.getServiceName())) {
            ctx.output(DEAD_LETTER, dlq("missing_service_name", evt.toString(), value.length));
            return;
        }
        if (!VALID_LEVELS.contains(evt.getLevel().toUpperCase())) {
            ctx.output(DEAD_LETTER, dlq("invalid_level:" + evt.getLevel(), evt.toString(), value.length));
            return;
        }

        long eventTimeMs;
        try {
            // Our timestamps are ISO8601 with a trailing Z; Instant.parse handles
            // nanosecond precision. A bad timestamp is a validation failure.
            eventTimeMs = Instant.parse(evt.getTimestamp()).toEpochMilli();
        } catch (Exception e) {
            ctx.output(DEAD_LETTER, dlq("bad_timestamp:" + evt.getTimestamp(), evt.toString(), value.length));
            return;
        }

        out.collect(toEnriched(evt, eventTimeMs));
    }

    private static EnrichedLog toEnriched(LogEvent evt, long eventTimeMs) {
        EnrichedLog e = new EnrichedLog();
        e.timestamp = evt.getTimestamp();
        e.eventTimeMs = eventTimeMs;
        e.serviceName = evt.getServiceName();
        e.level = evt.getLevel().toUpperCase();
        e.correlationId = evt.getCorrelationId();
        e.message = evt.getMessage();
        e.statusCode = evt.getStatusCode();
        e.latencyMs = evt.getLatencyMs();
        e.region = evt.getRegion();
        e.endpoint = evt.getEndpoint();
        e.userId = evt.getUserId();
        e.host = evt.getHost();
        e.collector = evt.getCollector();
        e.ingestTsMs = evt.getIngestTsMs();
        e.extra.putAll(evt.getExtraMap());
        return e;
    }

    private static boolean isBlank(String s) {
        return s == null || s.isEmpty();
    }

    /** Compact JSON payload written to the dead-letter topic. */
    private static String dlq(String reason, String detail, int rawLen) {
        String safeDetail = detail == null ? "" : detail.replace("\"", "'").replace("\n", " ");
        if (safeDetail.length() > 500) {
            safeDetail = safeDetail.substring(0, 500);
        }
        return "{\"reason\":\"" + reason + "\",\"raw_len\":" + rawLen
                + ",\"detail\":\"" + safeDetail + "\"}";
    }
}
