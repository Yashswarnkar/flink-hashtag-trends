package dev.hashtags.redis;

/**
 * A single Redis mutation, described independently of any client library.
 *
 * <p>Writes are meant to be idempotent (absolute values, never INCR) so that replaying records
 * after a failure overwrites results instead of double counting.
 */
public sealed interface RedisWrite permits RedisWrite.Hset {

    /** {@code HSET key field value}. */
    record Hset(String key, String field, String value) implements RedisWrite {}
}
