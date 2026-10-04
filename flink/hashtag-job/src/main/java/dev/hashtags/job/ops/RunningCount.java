package dev.hashtags.job.ops;

import dev.hashtags.job.model.TagCount;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * Keeps one counter per hashtag in Flink keyed state.
 *
 * <p>{@code getState} returns a handle scoped to the <em>current key</em>: inside
 * {@link #processElement} for tag "flink", {@code count.value()} reads flink's counter only.
 * The subtask owning a key group is the only place in the cluster that holds those counters.
 */
public class RunningCount extends KeyedProcessFunction<String, String, TagCount> {

    private transient ValueState<Long> count;

    @Override
    public void open(OpenContext openContext) {
        count = getRuntimeContext().getState(new ValueStateDescriptor<>("count", Types.LONG));
    }

    @Override
    public void processElement(String tag, Context ctx, Collector<TagCount> out) throws Exception {
        Long current = count.value();
        long next = (current == null ? 0 : current) + 1;
        count.update(next);
        out.collect(new TagCount(tag, next));
    }
}
