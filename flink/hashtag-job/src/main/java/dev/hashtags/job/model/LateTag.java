package dev.hashtags.job.model;

/**
 * A hashtag occurrence that arrived after every window it belongs to was already closed for good.
 *
 * @param eventTime when the post was written
 * @param watermark the operator's event-time clock when it arrived; {@code watermark - eventTime}
 *     is how far behind it was
 */
public record LateTag(String tag, long eventTime, long watermark) {}
