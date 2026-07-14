package com.observability.metrics;

import com.observability.model.EnrichedLog;
import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental aggregation of logs into running metrics.
 *
 * <p>An {@link AggregateFunction} folds each record into a small accumulator as
 * it arrives, so Flink never has to buffer the whole window in memory — only the
 * accumulator is stored in state. This is far more scalable than a
 * {@code ProcessWindowFunction} that iterates all elements at window close, and
 * it is what keeps windowed aggregation cheap under high throughput.
 */
public class MetricsAggregator
        implements AggregateFunction<EnrichedLog, MetricsAccumulator, MetricsAccumulator> {

    @Override
    public MetricsAccumulator createAccumulator() {
        return new MetricsAccumulator();
    }

    @Override
    public MetricsAccumulator add(EnrichedLog e, MetricsAccumulator acc) {
        acc.count++;
        if (e.isError()) acc.errorCount++;
        if (e.isWarn()) acc.warnCount++;
        acc.latencySum += e.latencyMs;
        acc.latencyMax = Math.max(acc.latencyMax, e.latencyMs);
        return acc;
    }

    @Override
    public MetricsAccumulator getResult(MetricsAccumulator acc) {
        return acc;
    }

    @Override
    public MetricsAccumulator merge(MetricsAccumulator a, MetricsAccumulator b) {
        // Needed for session/merging windows and for some window optimizations.
        MetricsAccumulator m = new MetricsAccumulator();
        m.count = a.count + b.count;
        m.errorCount = a.errorCount + b.errorCount;
        m.warnCount = a.warnCount + b.warnCount;
        m.latencySum = a.latencySum + b.latencySum;
        m.latencyMax = Math.max(a.latencyMax, b.latencyMax);
        return m;
    }
}
