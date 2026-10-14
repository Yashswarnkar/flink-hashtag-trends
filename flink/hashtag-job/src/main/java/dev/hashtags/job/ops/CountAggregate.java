package dev.hashtags.job.ops;

import org.apache.flink.api.common.functions.AggregateFunction;

/**
 * Incremental count: each window keeps a single {@code Long} per key, not the list of elements.
 * Flink calls {@link #add} as elements arrive and {@link #getResult} when the window fires.
 */
public class CountAggregate implements AggregateFunction<String, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
    }

    @Override
    public Long add(String tag, Long acc) {
        return acc + 1;
    }

    @Override
    public Long getResult(Long acc) {
        return acc;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}
