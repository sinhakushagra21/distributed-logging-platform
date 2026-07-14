package com.observability.sink;

import com.observability.model.ServiceMetrics;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Timestamp;

/**
 * Builds the TimescaleDB sink for windowed metrics.
 *
 * <p>TimescaleDB is Postgres with a time-series "hypertable", so we use Flink's
 * plain JDBC sink. Writes are batched (throughput) with bounded retries. The
 * target table is created by Stage 4's init.sql as a hypertable partitioned on
 * window_start.
 *
 * <p>The INSERT is idempotent via {@code ON CONFLICT ... DO UPDATE} against a
 * unique (service, window_type, window_start) key, so if Flink replays a window
 * after recovery we overwrite rather than double-count.
 */
public final class TimescaleSinkFactory {

    private static final String UPSERT_SQL =
            "INSERT INTO service_metrics "
            + "(window_start, window_end, service_name, window_type, count, "
            + " error_count, warn_count, error_rate, avg_latency_ms, max_latency_ms) "
            + "VALUES (?,?,?,?,?,?,?,?,?,?) "
            + "ON CONFLICT (service_name, window_type, window_start) DO UPDATE SET "
            + " window_end = EXCLUDED.window_end, count = EXCLUDED.count, "
            + " error_count = EXCLUDED.error_count, warn_count = EXCLUDED.warn_count, "
            + " error_rate = EXCLUDED.error_rate, avg_latency_ms = EXCLUDED.avg_latency_ms, "
            + " max_latency_ms = EXCLUDED.max_latency_ms";

    private TimescaleSinkFactory() {
    }

    public static SinkFunction<ServiceMetrics> build(String url, String user, String password) {
        return JdbcSink.sink(
                UPSERT_SQL,
                (ps, m) -> {
                    ps.setTimestamp(1, new Timestamp(m.windowStartMs));
                    ps.setTimestamp(2, new Timestamp(m.windowEndMs));
                    ps.setString(3, m.serviceName);
                    ps.setString(4, m.windowType);
                    ps.setLong(5, m.count);
                    ps.setLong(6, m.errorCount);
                    ps.setLong(7, m.warnCount);
                    ps.setDouble(8, m.errorRate);
                    ps.setDouble(9, m.avgLatencyMs);
                    ps.setDouble(10, m.maxLatencyMs);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(50)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl(url)
                        .withDriverName("org.postgresql.Driver")
                        .withUsername(user)
                        .withPassword(password)
                        .build());
    }
}
