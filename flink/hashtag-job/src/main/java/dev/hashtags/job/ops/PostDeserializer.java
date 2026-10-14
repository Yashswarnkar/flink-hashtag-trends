package dev.hashtags.job.ops;

import dev.hashtags.job.model.Post;

import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.metrics.Counter;
import org.apache.flink.util.Collector;

import java.util.Optional;

/**
 * Kafka bytes → {@link Post}, inside the source.
 *
 * <p>Parsing happens here (not in a later operator) because the source needs each post's
 * {@code event_time} to assign timestamps and generate watermarks <em>per Kafka partition</em>.
 * Malformed records are skipped and counted; they never fail the job.
 */
public class PostDeserializer implements DeserializationSchema<Post> {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private transient Counter malformed;

    @Override
    public void open(InitializationContext context) {
        malformed = context.getMetricGroup().counter("malformedPosts");
    }

    @Override
    public void deserialize(byte[] message, Collector<Post> out) {
        parse(message).ifPresentOrElse(out::collect, () -> {
            if (malformed != null) {
                malformed.inc();
            }
        });
    }

    @Override
    public Post deserialize(byte[] message) {
        return parse(message).orElse(null);
    }

    @Override
    public boolean isEndOfStream(Post next) {
        return false;
    }

    @Override
    public TypeInformation<Post> getProducedType() {
        return TypeInformation.of(Post.class);
    }

    public static Optional<Post> parse(byte[] json) {
        try {
            Post post = MAPPER.readValue(json, Post.class);
            return post.text() == null || post.eventTime() <= 0 ? Optional.empty() : Optional.of(post);
        } catch (Exception e) {
            return Optional.empty();
        }
    }
}
