package dev.hashtags.job;

import static dev.hashtags.job.TestPosts.post;
import static dev.hashtags.job.TestPosts.watermarkPerEvent;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;

import dev.hashtags.job.model.LateTag;
import dev.hashtags.job.model.Post;
import dev.hashtags.job.model.TagCount;
import dev.hashtags.job.model.WindowCount;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Runs the real topology on an in-process Flink cluster (2 TaskManagers × 2 slots). */
class PipelineTest {

    @RegisterExtension
    static final MiniClusterExtension FLINK = new MiniClusterExtension(
            new MiniClusterResourceConfiguration.Builder()
                    .setNumberTaskManagers(2)
                    .setNumberSlotsPerTaskManager(2)
                    .build());

    /** 10 s windows every 5 s, so each event belongs to exactly two windows. */
    private static final Duration SIZE = Duration.ofSeconds(10);
    private static final Duration SLIDE = Duration.ofSeconds(5);

    @Test
    void totalsCountEveryHashtagAcrossParallelSubtasks() throws Exception {
        StreamExecutionEnvironment env = strictEnv(4);
        List<Post> posts = List.of(
                post("loving #Flink and #redis", 1000),
                post("#flink #flink #flink", 2000),   // distinct per post → counts once
                post("no tags here", 3000),
                post("#Redis rocks, #kafka too", 4000),
                post("#flink", 5000));

        Run run = run(env, posts, Duration.ZERO, Duration.ZERO);

        Map<String, Long> finalCounts = new HashMap<>();
        run.totals.forEach(u -> finalCounts.merge(u.tag(), u.count(), Math::max));
        assertThat(finalCounts).containsExactlyInAnyOrderEntriesOf(
                Map.of("flink", 3L, "redis", 2L, "kafka", 1L));
        // Each tag lives on exactly one subtask, so its updates count up 1, 2, 3 with no gaps.
        assertThat(run.totals.stream().filter(u -> u.tag().equals("flink")).map(TagCount::count))
                .containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    /**
     * Timeline (watermark = max event time − 1 ms, bound 0):
     * <pre>
     *  a@1s a@2s b@6s | a@11s → watermark 10.999s closes windows ending at 5s and 10s
     *                 | a@3s  → both its windows ([-5,5) and [0,10)) are closed → LATE
     *                 | a@12s
     *  end of input   → final watermark closes everything else
     * </pre>
     */
    @Test
    void windowsFireOnWatermarkAndLateDataGoesToSideOutput() throws Exception {
        List<Post> posts = List.of(
                post("#a", 1000), post("#a", 2000), post("#b", 6000),
                post("#a", 11000), post("#a", 3000), post("#a", 12000));

        Run run = run(strictEnv(1), posts, Duration.ZERO, Duration.ZERO);

        assertThat(run.windows)
                .extracting(WindowCount::tag, WindowCount::windowStart, WindowCount::windowEnd, WindowCount::count)
                .containsExactlyInAnyOrder(
                        tuple("a", -5000L, 5000L, 2L),
                        tuple("a", 0L, 10000L, 2L),      // a@3s did NOT make it in
                        tuple("a", 5000L, 15000L, 2L),
                        tuple("a", 10000L, 20000L, 2L),
                        tuple("b", 0L, 10000L, 1L),
                        tuple("b", 5000L, 15000L, 1L));
        assertThat(run.late).containsExactly(new LateTag("a", 3000, 10999));
        assertThat(run.lateTotals).containsExactly(new TagCount("a", 1));
    }

    /** Same input, but windows stay open 5 s past their end: a@3s updates [0,10) and it fires again. */
    @Test
    void allowedLatenessReFiresWindowWithCorrectedCount() throws Exception {
        List<Post> posts = List.of(
                post("#a", 1000), post("#a", 2000), post("#b", 6000),
                post("#a", 11000), post("#a", 3000), post("#a", 12000));

        Run run = run(strictEnv(1), posts, Duration.ZERO, Duration.ofSeconds(5));

        assertThat(run.windows.stream().filter(w -> w.tag().equals("a") && w.windowEnd() == 10000))
                .extracting(WindowCount::count)
                .containsExactly(2L, 3L);                 // first firing, then the correction
        // [-5,5) was already past end + lateness (4.999s + 5s ≤ 10.999s): a@3s is dropped there,
        // but because one of its windows accepted it, it is not reported as late.
        assertThat(run.windows.stream().filter(w -> w.tag().equals("a") && w.windowEnd() == 5000))
                .extracting(WindowCount::count)
                .containsExactly(2L);
        assertThat(run.late).isEmpty();
    }

    /** Out-of-orderness bound of 5 s: a@3s arriving after a@6s is NOT late (watermark only 0.999s). */
    @Test
    void outOfOrderEventsWithinBoundAreCounted() throws Exception {
        List<Post> posts = List.of(post("#a", 1000), post("#a", 6000), post("#a", 3000));

        Run run = run(strictEnv(1), posts, Duration.ofSeconds(5), Duration.ZERO);

        assertThat(run.windows.stream().filter(w -> w.windowEnd() == 10000))
                .extracting(WindowCount::count)
                .containsExactly(3L);
        assertThat(run.late).isEmpty();
    }

    // ---------------------------------------------------------------------------------------

    private record Run(List<TagCount> totals, List<WindowCount> windows, List<LateTag> late,
                       List<TagCount> lateTotals) {}

    private static Run run(StreamExecutionEnvironment env, List<Post> posts,
                           Duration outOfOrderness, Duration allowedLateness) throws Exception {
        Pipeline.Outputs out = Pipeline.build(
                env.fromData(posts).assignTimestampsAndWatermarks(watermarkPerEvent(outOfOrderness)),
                new WindowSettings(SIZE, SLIDE, allowedLateness));

        CloseableIterator<TagCount> totals = out.totals().collectAsync();
        CloseableIterator<WindowCount> windows = out.windows().collectAsync();
        CloseableIterator<LateTag> late = out.late().collectAsync();
        CloseableIterator<TagCount> lateTotals = out.lateTotals().collectAsync();
        env.executeAsync("pipeline-test");

        return new Run(drain(totals), drain(windows), drain(late), drain(lateTotals));
    }

    private static <T> List<T> drain(CloseableIterator<T> it) throws Exception {
        List<T> list = new ArrayList<>();
        try (it) {
            it.forEachRemaining(list::add);
        }
        return list;
    }

    private static StreamExecutionEnvironment strictEnv(int parallelism) {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.GENERIC_TYPES, false); // same strictness as the real job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(parallelism);
        return env;
    }
}
