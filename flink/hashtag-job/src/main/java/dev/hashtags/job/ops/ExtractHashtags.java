package dev.hashtags.job.ops;

import dev.hashtags.job.model.Post;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Emits each distinct hashtag of a post, lower-cased, without the '#'. */
public class ExtractHashtags implements FlatMapFunction<Post, String> {

    // '#' not preceded by a word character (so "a#b" is not a tag), followed by letters/digits/_.
    private static final Pattern HASHTAG = Pattern.compile("(?<![\\p{L}\\p{N}_])#([\\p{L}\\p{N}_]+)");

    @Override
    public void flatMap(Post post, Collector<String> out) {
        extract(post.text()).forEach(out::collect);
    }

    public static Set<String> extract(String text) {
        Set<String> tags = new LinkedHashSet<>();
        Matcher m = HASHTAG.matcher(text);
        while (m.find()) {
            tags.add(m.group(1).toLowerCase(Locale.ROOT));
        }
        return tags;
    }
}
