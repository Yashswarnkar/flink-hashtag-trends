package dev.hashtags.job.ops;

import dev.hashtags.job.model.Post;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.util.Optional;

/** JSON → {@link Post}. Malformed records are dropped and counted, never fail the job. */
public class ParsePost extends RichFlatMapFunction<String, Post> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient Counter malformed;

    @Override
    public void open(OpenContext openContext) {
        malformed = getRuntimeContext().getMetricGroup().counter("malformedPosts");
    }

    @Override
    public void flatMap(String json, Collector<Post> out) {
        parse(json).ifPresentOrElse(out::collect, malformed::inc);
    }

    public static Optional<Post> parse(String json) {
        try {
            Post post = MAPPER.readValue(json, Post.class);
            return post.text() == null ? Optional.empty() : Optional.of(post);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
