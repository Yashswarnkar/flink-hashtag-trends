package dev.hashtags.job;

import dev.hashtags.job.model.TagCount;
import dev.hashtags.job.ops.ExtractHashtags;
import dev.hashtags.job.ops.ParsePost;
import dev.hashtags.job.ops.RunningCount;

import org.apache.flink.streaming.api.datastream.DataStream;

/**
 * The topology between source and sink, kept separate so tests can feed it from memory.
 *
 * <pre>
 *   raw JSON ─► parse ─► extract tags ─╴keyBy(tag)╶─► running count ─► (sink)
 *   └──────── chained into the source task ───────┘   └── chained with the sink ──┘
 * </pre>
 *
 * Every stateful operator gets a stable {@code uid}: Flink uses it to match state in a
 * checkpoint/savepoint to an operator when the job is restarted or changed (phase 3/6).
 */
public final class Pipeline {

    private Pipeline() {}

    public static DataStream<TagCount> runningTotals(DataStream<String> rawPosts) {
        return rawPosts
                .flatMap(new ParsePost()).name("parse-post").uid("parse-post")
                .flatMap(new ExtractHashtags()).name("extract-hashtags").uid("extract-hashtags")
                .keyBy(tag -> tag)
                .process(new RunningCount()).name("running-count").uid("running-count");
    }
}
