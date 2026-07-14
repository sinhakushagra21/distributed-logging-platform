package com.observability.metrics;

/**
 * Mutable running totals for one window+service. Kept tiny and POJO-serializable
 * because it lives in Flink managed state and is checkpointed.
 */
public class MetricsAccumulator {
    public long count;
    public long errorCount;
    public long warnCount;
    public double latencySum;
    public double latencyMax;

    public MetricsAccumulator() {
    }
}
