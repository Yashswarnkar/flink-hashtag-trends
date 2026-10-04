package dev.hashtags.redis;

import java.io.Serializable;

public record RedisConnectionConfig(String host, int port) implements Serializable {}
