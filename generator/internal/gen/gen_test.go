package gen

import (
	"strings"
	"testing"
	"time"
)

func newTestGen(t *testing.T) *Generator {
	t.Helper()
	g, err := New(Config{Tags: 100, Zipf: 1.3, Users: 50, Seed: 42})
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestTagsAppearInTextAndAreDistinct(t *testing.T) {
	g := newTestGen(t)
	for range 1000 {
		post, tags := g.Next()
		seen := map[string]bool{}
		for _, tag := range tags {
			if seen[tag] {
				t.Fatalf("duplicate tag %q in %v", tag, tags)
			}
			seen[tag] = true
			if !strings.Contains(strings.ToLower(post.Text), "#"+tag) {
				t.Fatalf("tag %q missing from text %q", tag, post.Text)
			}
		}
		if len(tags) > 3 {
			t.Fatalf("too many tags: %v", tags)
		}
	}
}

func TestDistributionIsSkewed(t *testing.T) {
	g := newTestGen(t)
	counts := map[string]int{}
	for range 20000 {
		_, tags := g.Next()
		for _, tag := range tags {
			counts[tag]++
		}
	}
	head, tail := counts["flink"], counts[Vocabulary(100)[99]]
	if head < 10*max(tail, 1) {
		t.Fatalf("expected strong skew, head=%d tail=%d", head, tail)
	}
}

func TestSameSeedSameStream(t *testing.T) {
	a, b := newTestGen(t), newTestGen(t)
	for range 100 {
		pa, _ := a.Next()
		pb, _ := b.Next()
		if pa.Text != pb.Text || pa.UserID != pb.UserID {
			t.Fatalf("streams diverged: %+v vs %+v", pa, pb)
		}
	}
}

func TestRejectsBadConfig(t *testing.T) {
	if _, err := New(Config{Tags: 10, Zipf: 1.0, Users: 1}); err == nil {
		t.Fatal("expected error for zipf <= 1")
	}
}

func TestEventTimeDelays(t *testing.T) {
	now := time.UnixMilli(1_000_000_000)
	g, err := New(Config{Tags: 10, Zipf: 1.3, Users: 5, Seed: 7,
		OutOfOrder: 3 * time.Second, LateFraction: 0.1, Late: 30 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	g.now = func() time.Time { return now }

	late := 0
	for range 10000 {
		p, _ := g.Next()
		behind := now.UnixMilli() - p.EventTime
		switch {
		case behind < 0:
			t.Fatalf("event time in the future: %d ms", behind)
		case behind >= 30_000:
			late++
			if behind > 34_000 { // 3s + 30s + 1s jitter, +1ms for millisecond truncation
				t.Fatalf("late post too far behind: %d ms", behind)
			}
		case behind > 3_000: // +1ms tolerance: event_time is truncated to whole milliseconds
			t.Fatalf("on-time post outside out-of-order bound: %d ms", behind)
		}
	}
	if late < 800 || late > 1200 {
		t.Fatalf("expected ~10%% late posts, got %d of 10000", late)
	}
}

func TestNoDelaysByDefault(t *testing.T) {
	g := newTestGen(t)
	now := time.UnixMilli(5_000)
	g.now = func() time.Time { return now }
	if p, _ := g.Next(); p.EventTime != 5_000 {
		t.Fatalf("expected event time == now, got %d", p.EventTime)
	}
}
