package com.observability.model;

/**
 * Aggregated per-service metrics for one window. This is what gets written to
 * TimescaleDB (one row per service per window) and drives the Grafana panels.
 */
public class ServiceMetrics {

    public long windowStartMs;
    public long windowEndMs;
    public String serviceName;
    public String windowType;   // "tumbling_1m" or "sliding_1m_10s"

    public long count;          // total records in the window
    public long errorCount;
    public long warnCount;
    public double errorRate;    // errorCount / count
    public double avgLatencyMs;
    public double maxLatencyMs;

    public ServiceMetrics() {
    }

    @Override
    public String toString() {
        return "ServiceMetrics{" + serviceName + " " + windowType
                + " [" + windowStartMs + "," + windowEndMs + ") count=" + count
                + " errRate=" + String.format("%.3f", errorRate)
                + " avgLat=" + String.format("%.1f", avgLatencyMs) + "}";
    }
}
