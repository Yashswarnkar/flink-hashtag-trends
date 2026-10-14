package dev.hashtags.job;

import dev.hashtags.job.model.LateTag;
import dev.hashtags.job.model.TagCount;
import dev.hashtags.job.model.WindowCount;
import dev.hashtags.redis.RedisWrite;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** How each result type is stored in Redis. Keys are documented in docs/PLAN.md §4. */
final class RedisMappings {

    static final String TOTALS_KEY = "hashtag:totals";
    static final String LATE_STATS_KEY = "stats:late";
    static final String LATE_EVENTS_KEY = "late:events";
    static final long WINDOW_TTL_SECONDS = 3600;
    static final long LATE_EVENTS_MAX_LEN = 1000;

    private RedisMappings() {}

    static List<RedisWrite> totals(TagCount tc) {
        // Absolute value, not INCR: replays overwrite instead of double counting.
        return List.of(new RedisWrite.Hset(TOTALS_KEY, tc.tag(), Long.toString(tc.count())));
    }

    static List<RedisWrite> lateTotals(TagCount tc) {
        return List.of(new RedisWrite.Hset(LATE_STATS_KEY, tc.tag(), Long.toString(tc.count())));
    }

    /**
     * {@code trending:60s:<windowEndMs>} holds tag → count for one window; a window that fires
     * again (late data within allowed lateness) simply overwrites the tag's score.
     * {@code trending:60s:windows} indexes window ends so readers can find the newest one.
     */
    static List<RedisWrite> window(WindowCount wc, String label) {
        String key = windowKey(label, wc.windowEnd());
        return List.of(
                new RedisWrite.Zadd(key, wc.count(), wc.tag()),
                new RedisWrite.Expire(key, WINDOW_TTL_SECONDS),
                new RedisWrite.Zadd(windowIndexKey(label), wc.windowEnd(), Long.toString(wc.windowEnd())));
    }

    static List<RedisWrite> late(LateTag lt) {
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("tag", lt.tag());
        fields.put("event_time", Long.toString(lt.eventTime()));
        fields.put("watermark", Long.toString(lt.watermark()));
        fields.put("behind_ms", Long.toString(lt.watermark() - lt.eventTime()));
        return List.of(new RedisWrite.Xadd(LATE_EVENTS_KEY, LATE_EVENTS_MAX_LEN, fields));
    }

    static String windowKey(String label, long windowEnd) {
        return "trending:" + label + ":" + windowEnd;
    }

    static String windowIndexKey(String label) {
        return "trending:" + label + ":windows";
    }
}
