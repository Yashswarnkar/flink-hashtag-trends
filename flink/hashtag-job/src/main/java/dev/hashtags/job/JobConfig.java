package dev.hashtags.job;

import java.util.HashMap;
import java.util.Map;

/** Job settings from {@code --key value} program arguments, with defaults for the compose stack. */
public record JobConfig(
        String bootstrapServers,
        String topic,
        String groupId,
        String redisHost,
        int redisPort) {

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
                Integer.parseInt(m.getOrDefault("redis-port", "6379")));
    }
}
