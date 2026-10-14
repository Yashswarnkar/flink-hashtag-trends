package dev.hashtags.job;

import java.io.Serializable;
import java.time.Duration;

/** Sliding event-time window: every {@code slide}, count the last {@code size} of event time. */
public record WindowSettings(Duration size, Duration slide, Duration allowedLateness)
        implements Serializable {

    /** Short name used in Redis keys, e.g. {@code "60s"}. */
    public String label() {
        return size.toSeconds() + "s";
    }
}
