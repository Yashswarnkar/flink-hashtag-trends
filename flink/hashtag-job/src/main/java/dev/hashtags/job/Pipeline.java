package dev.hashtags.job;

import dev.hashtags.job.model.LateTag;
import dev.hashtags.job.model.Post;
import dev.hashtags.job.model.TagCount;
import dev.hashtags.job.model.WindowCount;
import dev.hashtags.job.ops.CountAggregate;
import dev.hashtags.job.ops.EmitWindowCount;
import dev.hashtags.job.ops.ExtractHashtags;
import dev.hashtags.job.ops.RunningCount;
import dev.hashtags.job.ops.TagLateness;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.util.OutputTag;

/**
 * The topology between source and sinks, kept separate so tests can feed it from memory.
 *
 * <pre>
 *                                  ┌─keyBy(tag)─► running-count ─────────────► totals
 *   posts (timestamped) ─► extract ┤
 *                                  └─keyBy(tag)─► window-count (sliding) ────► windows
 *                                                   │ late side output
 *                                                   ├──────────► late-tags ──► late
 *                                                   └─keyBy(tag)─► late-count ► late totals
 * </pre>
 *
 * Every stateful operator gets a stable {@code uid}: Flink uses it to match state in a
 * checkpoint/savepoint to an operator when the job is restarted or changed (phase 3/6).
 */
public final class Pipeline {

    /** Tags that arrived after all their windows were closed (past allowed lateness). */
    static final OutputTag<String> LATE_TAGS = new OutputTag<>("late-tags", Types.STRING);

    public record Outputs(
            DataStream<TagCount> totals,
            DataStream<WindowCount> windows,
            DataStream<LateTag> late,
            DataStream<TagCount> lateTotals) {}

    private Pipeline() {}

    /** {@code posts} must already carry event-time timestamps and watermarks. */
    public static Outputs build(DataStream<Post> posts, WindowSettings window) {
        DataStream<String> tags = posts
                .flatMap(new ExtractHashtags()).name("extract-hashtags").uid("extract-hashtags");

        // Phase 1 branch: all-time totals, no notion of time.
        DataStream<TagCount> totals = tags
                .keyBy(tag -> tag)
                .process(new RunningCount()).name("running-count").uid("running-count");

        // Phase 2 branch: per-window counts in event time.
        SingleOutputStreamOperator<WindowCount> windows = tags
                .keyBy(tag -> tag)
                .window(SlidingEventTimeWindows.of(window.size(), window.slide()))
                .allowedLateness(window.allowedLateness())
                .sideOutputLateData(LATE_TAGS)
                .aggregate(new CountAggregate(), new EmitWindowCount())
                .name("window-count").uid("window-count");

        DataStream<String> lateTags = windows.getSideOutput(LATE_TAGS);
        DataStream<LateTag> late = lateTags
                .process(new TagLateness()).name("late-tags").uid("late-tags");
        DataStream<TagCount> lateTotals = lateTags
                .keyBy(tag -> tag)
                .process(new RunningCount()).name("late-count").uid("late-count");

        return new Outputs(totals, windows, late, lateTotals);
    }
}
