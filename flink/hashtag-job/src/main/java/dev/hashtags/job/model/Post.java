package dev.hashtags.job.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/** One social-media post as produced by the generator into Kafka topic {@code posts}. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Post(
        @JsonProperty("post_id") String postId,
        @JsonProperty("user_id") String userId,
        @JsonProperty("text") String text,
        @JsonProperty("lang") String lang,
        @JsonProperty("event_time") long eventTime) {}
