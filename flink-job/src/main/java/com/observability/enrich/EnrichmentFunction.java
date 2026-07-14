package com.observability.enrich;

import com.observability.model.EnrichedLog;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Step 3: enrichment. Adds derived fields that make downstream querying and
 * dashboards easier, without going back to the source:
 *   - status_class: bucket the HTTP status code (2xx/3xx/4xx/5xx) so Kibana/
 *     Grafana can filter "all client errors" without range queries.
 *   - processing_ts: wall-clock time Flink handled the record (processing-time),
 *     useful for measuring end-to-end lag against event-time.
 *
 * Region is already present from the source; in a real system this is where you
 * might join a lookup table (e.g. datacenter -> geo) using Flink state or a
 * broadcast stream.
 */
public class EnrichmentFunction implements MapFunction<EnrichedLog, EnrichedLog> {

    @Override
    public EnrichedLog map(EnrichedLog e) {
        e.statusClass = classify(e.statusCode);
        e.processingTsMs = System.currentTimeMillis();
        if (e.region == null || e.region.isEmpty()) {
            e.region = "unknown";
        }
        return e;
    }

    private static String classify(int status) {
        if (status >= 200 && status < 300) return "2xx";
        if (status >= 300 && status < 400) return "3xx";
        if (status >= 400 && status < 500) return "4xx";
        if (status >= 500 && status < 600) return "5xx";
        return "none";
    }
}
