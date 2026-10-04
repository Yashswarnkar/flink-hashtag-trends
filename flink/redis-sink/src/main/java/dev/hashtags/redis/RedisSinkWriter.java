package dev.hashtags.redis;

import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;

import redis.clients.jedis.Jedis;

/** One instance per sink subtask; owns a single Redis connection. */
class RedisSinkWriter<T> implements SinkWriter<T> {

    private final Jedis jedis;
    private final RedisWriteMapper<T> mapper;
    private final Counter writesSent;

    RedisSinkWriter(RedisConnectionConfig connection, RedisWriteMapper<T> mapper, Counter writesSent) {
        this.jedis = new Jedis(connection.host(), connection.port());
        this.mapper = mapper;
        this.writesSent = writesSent;
    }

    @Override
    public void write(T element, Context context) {
        for (RedisWrite write : mapper.map(element)) {
            execute(write);
            writesSent.inc();
        }
    }

    private void execute(RedisWrite write) {
        if (write instanceof RedisWrite.Hset hset) {
            jedis.hset(hset.key(), hset.field(), hset.value());
        } else {
            throw new IllegalArgumentException("Unsupported Redis write: " + write);
        }
    }

    /** Called on checkpoints and at end of input. Every write is already synchronous. */
    @Override
    public void flush(boolean endOfInput) {}

    @Override
    public void close() {
        jedis.close();
    }
}
