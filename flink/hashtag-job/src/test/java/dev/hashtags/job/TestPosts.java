package dev.hashtags.job;

import dev.hashtags.job.model.Post;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;

import java.time.Duration;

/** Helpers for building deterministic event-time test input. */
final class TestPosts {

    private TestPosts() {}

    static Post post(String text, long eventTime) {
        return new Post("p-" + eventTime + "-" + text.hashCode(), "u-1", text, "en", eventTime);
    }

    /**
     * Same watermark formula as {@code forBoundedOutOfOrderness} (max timestamp − bound − 1), but
     * emitted after <em>every</em> event instead of every 200 ms, so tests know exactly which
     * watermark each record meets.
     */
    static WatermarkStrategy<Post> watermarkPerEvent(Duration bound) {
        return WatermarkStrategy.<Post>forGenerator(ctx -> new PerEvent(bound.toMillis()))
                .withTimestampAssigner((post, ignored) -> post.eventTime());
    }

    private static final class PerEvent implements WatermarkGenerator<Post> {
        private final long bound;
        private long max = Long.MIN_VALUE;

        PerEvent(long bound) {
            this.bound = bound;
        }

        @Override
        public void onEvent(Post post, long timestamp, WatermarkOutput out) {
            max = Math.max(max, timestamp);
            out.emitWatermark(new Watermark(max - bound - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput out) {}
    }
}
