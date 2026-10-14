package dev.hashtags.job;

import org.apache.flink.util.TimeUtils;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * Job settings from {@code --key value} program arguments, with defaults for the compose stack.
 * Durations use Flink's syntax: {@code 500ms}, {@code 10s}, {@code 2min}.
 */
public record JobConfig(
        String bootstrapServers,
        String topic,
        String groupId,
        String redisHost,
        int redisPort,
        WindowSettings window,
        Duration outOfOrderness,
        Duration idleness) {

    public static JobConfig fromArgs(String[] args) {
        Map<String, String> m = new HashMap<>();
        for (int i = 0; i < args.length; i++) {
            if (!args[i].startsWith("--") || i + 1 >= args.length) {
                throw new IllegalArgumentException("Expected '--key value' pairs, got: " + args[i]);
            }
            m.put(args[i].substring(2), args[++i]);
        }
        return new JobConfig(
                m.getOrDefault("bootstrap-servers", "kafka:9092"),
                m.getOrDefault("topic", "posts"),
                m.getOrDefault("group-id", "hashtag-trends"),
                m.getOrDefault("redis-host", "redis"),
                Integer.parseInt(m.getOrDefault("redis-port", "6379")),
                new WindowSettings(
                        duration(m, "window-size", "60s"),
                        duration(m, "window-slide", "10s"),
                        duration(m, "allowed-lateness", "10s")),
                duration(m, "out-of-orderness", "5s"),
                // 0 disables idleness detection: an empty partition then stalls event time forever.
                duration(m, "idleness", "30s"));
    }

    private static Duration duration(Map<String, String> m, String key, String fallback) {
        return TimeUtils.parseDuration(m.getOrDefault(key, fallback));
    }
}
