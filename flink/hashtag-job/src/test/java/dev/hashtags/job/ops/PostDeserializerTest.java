package dev.hashtags.job.ops;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import dev.hashtags.job.model.Post;

import org.junit.jupiter.api.Test;

class PostDeserializerTest {

    @Test
    void parsesGeneratorJsonAndIgnoresUnknownFields() {
        String json = """
                {"post_id":"p1","user_id":"u-1","text":"hi #flink","lang":"en",
                 "event_time":1759550000000,"extra":true}""";

        assertThat(PostDeserializer.parse(json.getBytes(UTF_8)))
                .contains(new Post("p1", "u-1", "hi #flink", "en", 1759550000000L));
    }

    @Test
    void rejectsMalformedJsonMissingTextOrMissingEventTime() {
        assertThat(PostDeserializer.parse("{not json".getBytes(UTF_8))).isEmpty();
        assertThat(PostDeserializer.parse("{\"post_id\":\"p1\",\"event_time\":5}".getBytes(UTF_8))).isEmpty();
        assertThat(PostDeserializer.parse("{\"text\":\"#a\"}".getBytes(UTF_8))).isEmpty();
    }
}
