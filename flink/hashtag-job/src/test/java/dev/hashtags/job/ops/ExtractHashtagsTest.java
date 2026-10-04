package dev.hashtags.job.ops;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class ExtractHashtagsTest {

    @Test
    void extractsLowercasedDistinctTagsInOrder() {
        assertThat(ExtractHashtags.extract("loving #Flink and #redis, #FLINK again"))
                .containsExactly("flink", "redis");
    }

    @Test
    void stopsAtPunctuationAndKeepsUnicodeAndUnderscore() {
        assertThat(ExtractHashtags.extract("#café! #stream_processing. #2026"))
                .containsExactly("café", "stream_processing", "2026");
    }

    @Test
    void ignoresHashInsideWordsAndBareHash() {
        assertThat(ExtractHashtags.extract("issue a#b and # alone and C#")).isEmpty();
    }

    @Test
    void noTags() {
        assertThat(ExtractHashtags.extract("just text")).isEmpty();
    }
}
