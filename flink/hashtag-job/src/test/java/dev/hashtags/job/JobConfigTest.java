package dev.hashtags.job;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

import java.time.Duration;

class JobConfigTest {

    @Test
    void defaultsMatchTheComposeStack() {
        JobConfig cfg = JobConfig.fromArgs(new String[0]);

        assertThat(cfg.bootstrapServers()).isEqualTo("kafka:9092");
        assertThat(cfg.window()).isEqualTo(new WindowSettings(
                Duration.ofSeconds(60), Duration.ofSeconds(10), Duration.ofSeconds(10)));
        assertThat(cfg.window().label()).isEqualTo("60s");
        assertThat(cfg.outOfOrderness()).isEqualTo(Duration.ofSeconds(5));
        assertThat(cfg.idleness()).isEqualTo(Duration.ofSeconds(30));
    }

    @Test
    void parsesFlinkDurationsAndZeroIdleness() {
        JobConfig cfg = JobConfig.fromArgs(new String[] {
            "--window-size", "2min", "--window-slide", "500ms", "--idleness", "0"});

        assertThat(cfg.window().size()).isEqualTo(Duration.ofMinutes(2));
        assertThat(cfg.window().slide()).isEqualTo(Duration.ofMillis(500));
        assertThat(cfg.idleness()).isZero();
    }

    @Test
    void rejectsDanglingKey() {
        assertThatThrownBy(() -> JobConfig.fromArgs(new String[] {"--topic"}))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
