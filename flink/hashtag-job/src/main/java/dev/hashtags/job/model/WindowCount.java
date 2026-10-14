package dev.hashtags.job.model;

/** Number of posts using {@code tag} with event time in [windowStart, windowEnd). */
public record WindowCount(String tag, long windowStart, long windowEnd, long count) {}
