package dev.hashtags.redis;

import java.util.Map;

/**
 * A single Redis mutation, described independently of any client library.
 *
 * <p>Writes are meant to be idempotent (absolute values, never INCR) so that replaying records
 * after a failure overwrites results instead of double counting. {@link Xadd} is the exception:
 * it appends, so it suits logs where a duplicate entry after a replay is acceptable.
 */
public sealed interface RedisWrite
        permits RedisWrite.Hset, RedisWrite.Zadd, RedisWrite.Expire, RedisWrite.Xadd {

    /** {@code HSET key field value}. */
    record Hset(String key, String field, String value) implements RedisWrite {}

    /** {@code ZADD key score member}: sets (not adds to) the member's score. */
    record Zadd(String key, double score, String member) implements RedisWrite {}

    /** {@code EXPIRE key seconds}. */
    record Expire(String key, long seconds) implements RedisWrite {}

    /** {@code XADD key MAXLEN ~ maxLen * field value ...}: append to a capped stream. */
    record Xadd(String key, long maxLen, Map<String, String> fields) implements RedisWrite {}
}
