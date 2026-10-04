package dev.hashtags.job;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hashtags.job.model.TagCount;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

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

    @Test
    void countsEveryHashtagAcrossParallelSubtasks() throws Exception {
        Configuration conf = new Configuration();
        conf.set(PipelineOptions.GENERIC_TYPES, false); // same strictness as the real job
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(4);

        List<String> posts = List.of(
                post("p1", "loving #Flink and #redis"),
                post("p2", "#flink #flink #flink"),        // distinct per post → counts once
                "{broken json",                            // dropped, not fatal
                post("p3", "no tags here"),
                post("p4", "#Redis rocks, #kafka too"),
                post("p5", "#flink"));

        List<TagCount> updates = new ArrayList<>();
        try (CloseableIterator<TagCount> it =
                Pipeline.runningTotals(env.fromData(posts)).executeAndCollect()) {
            it.forEachRemaining(updates::add);
        }

        Map<String, Long> finalCounts = new HashMap<>();
        updates.forEach(u -> finalCounts.merge(u.tag(), u.count(), Math::max));
        assertThat(finalCounts).containsExactlyInAnyOrderEntriesOf(
                Map.of("flink", 3L, "redis", 2L, "kafka", 1L));

        // Each tag lives on exactly one subtask, so its updates count up 1, 2, 3 with no gaps.
        assertThat(updates.stream().filter(u -> u.tag().equals("flink")).map(TagCount::count))
                .containsExactlyInAnyOrder(1L, 2L, 3L);
    }

    private static String post(String id, String text) {
        return """
                {"post_id":"%s","user_id":"u-1","text":"%s","lang":"en","event_time":0}"""
                .formatted(id, text);
    }
}
