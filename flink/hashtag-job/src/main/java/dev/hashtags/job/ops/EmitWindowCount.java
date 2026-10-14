package dev.hashtags.job.ops;

import dev.hashtags.job.model.WindowCount;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/** Attaches the key and window bounds to the pre-aggregated count when a window fires. */
public class EmitWindowCount extends ProcessWindowFunction<Long, WindowCount, String, TimeWindow> {

    @Override
    public void process(String tag, Context ctx, Iterable<Long> counts, Collector<WindowCount> out) {
        // With an AggregateFunction in front, the iterable holds exactly one pre-aggregated value.
        long count = counts.iterator().next();
        out.collect(new WindowCount(tag, ctx.window().getStart(), ctx.window().getEnd(), count));
    }
}
