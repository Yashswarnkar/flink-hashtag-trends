package dev.hashtags.job.model;

/** Running total for one hashtag, emitted every time it changes. */
public record TagCount(String tag, long count) {}
