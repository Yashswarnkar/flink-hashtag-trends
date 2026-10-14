package dev.hashtags.job.ops;

import dev.hashtags.job.model.LateTag;

import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/** Records how far behind the event-time clock each late hashtag occurrence was. */
public class TagLateness extends ProcessFunction<String, LateTag> {

    @Override
    public void processElement(String tag, Context ctx, Collector<LateTag> out) {
        out.collect(new LateTag(tag, ctx.timestamp(), ctx.timerService().currentWatermark()));
    }
}
