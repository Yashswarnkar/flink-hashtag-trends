package dev.hashtags.job.ops;

import static org.assertj.core.api.Assertions.assertThat;

import dev.hashtags.job.model.Post;

import org.junit.jupiter.api.Test;

class ParsePostTest {

    @Test
    void parsesGeneratorJsonAndIgnoresUnknownFields() {
        String json = """
                {"post_id":"p1","user_id":"u-1","text":"hi #flink","lang":"en",
                 "event_time":1759550000000,"extra":true}""";

        assertThat(ParsePost.parse(json))
                .contains(new Post("p1", "u-1", "hi #flink", "en", 1759550000000L));
    }

    @Test
    void rejectsMalformedJsonAndMissingText() {
        assertThat(ParsePost.parse("{not json")).isEmpty();
        assertThat(ParsePost.parse("{\"post_id\":\"p1\"}")).isEmpty();
    }
}
