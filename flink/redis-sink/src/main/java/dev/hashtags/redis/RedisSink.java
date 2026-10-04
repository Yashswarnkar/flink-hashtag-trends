package dev.hashtags.redis;

import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.connector.sink2.WriterInitContext;

/**
 * Minimal Redis sink on Flink's Sink v2 API.
 *
 * <p>The {@code Sink} object is created on the client, serialized and shipped with the job graph;
 * each parallel subtask then calls {@link #createWriter} to get its own {@link RedisSinkWriter}
 * with its own connection.
 *
 * <p>Phase 1 version: one synchronous round trip per write and no buffering. Delivery is
 * at-least-once only in combination with checkpoints, and correctness relies on idempotent writes.
 */
public class RedisSink<T> implements Sink<T> {

    private final RedisConnectionConfig connection;
    private final RedisWriteMapper<T> mapper;

    public RedisSink(RedisConnectionConfig connection, RedisWriteMapper<T> mapper) {
        this.connection = connection;
        this.mapper = mapper;
    }

    @Override
    public SinkWriter<T> createWriter(WriterInitContext context) {
        return new RedisSinkWriter<>(
                connection, mapper, context.metricGroup().getNumRecordsSendCounter());
    }
}
