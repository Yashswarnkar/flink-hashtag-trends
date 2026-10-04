package gen

import (
	"strings"
	"testing"
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
