package com.observability.metrics;

import com.observability.model.ServiceMetrics;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Runs once per window+key at window close. It receives the single, already
 * folded {@link MetricsAccumulator} from {@link MetricsAggregator} and stamps it
 * with window metadata (start/end, service, window type) to produce the final
 * {@link ServiceMetrics} row. Because the heavy lifting happened incrementally,
 * this function is O(1) per window.
 */
public class MetricsWindowFunction
        extends ProcessWindowFunction<MetricsAccumulator, ServiceMetrics, String, TimeWindow> {

    private final String windowType;

    public MetricsWindowFunction(String windowType) {
        this.windowType = windowType;
    }

    @Override
    public void process(String serviceName, Context ctx,
                        Iterable<MetricsAccumulator> accs, Collector<ServiceMetrics> out) {
        MetricsAccumulator acc = accs.iterator().next();  // exactly one (aggregated)
        ServiceMetrics m = new ServiceMetrics();
        m.windowStartMs = ctx.window().getStart();
        m.windowEndMs = ctx.window().getEnd();
        m.serviceName = serviceName;
        m.windowType = windowType;
        m.count = acc.count;
        m.errorCount = acc.errorCount;
        m.warnCount = acc.warnCount;
        m.errorRate = acc.count == 0 ? 0.0 : (double) acc.errorCount / acc.count;
        m.avgLatencyMs = acc.count == 0 ? 0.0 : acc.latencySum / acc.count;
        m.maxLatencyMs = acc.latencyMax;
        out.collect(m);
    }
}
