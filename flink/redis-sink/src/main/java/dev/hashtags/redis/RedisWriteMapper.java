package dev.hashtags.redis;

import java.io.Serializable;
import java.util.List;

/** Turns one stream element into the Redis writes that persist it. Shipped to every subtask. */
@FunctionalInterface
public interface RedisWriteMapper<T> extends Serializable {

    List<RedisWrite> map(T element);
}
