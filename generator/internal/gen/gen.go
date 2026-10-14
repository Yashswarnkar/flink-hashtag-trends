// Package gen produces synthetic social-media posts with a skewed hashtag distribution.
package gen

import (
	"encoding/hex"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"
)

// Post is the JSON value written to Kafka topic "posts". Keep in sync with the Flink job's Post record.
type Post struct {
	PostID    string `json:"post_id"`
	UserID    string `json:"user_id"`
	Text      string `json:"text"`
	Lang      string `json:"lang"`
	EventTime int64  `json:"event_time"`
}

// Config controls the shape of generated traffic.
type Config struct {
	Tags  int     // vocabulary size
	Zipf  float64 // skew exponent s > 1; higher = a few tags dominate
	Users int
	Seed  uint64

	// Event-time disorder. Every post's event_time is "now" minus a delay:
	//   - OutOfOrder: uniform random delay in [0, OutOfOrder) (network jitter, client batching)
	//   - LateFraction of posts additionally get Late + jitter (a phone that was offline)
	OutOfOrder   time.Duration
	LateFraction float64
	Late         time.Duration
}

// Generator is not safe for concurrent use.
type Generator struct {
	rng   *rand.Rand
	zipf  *rand.Zipf
	vocab []string
	users int
	cfg   Config
	now   func() time.Time
}

var popular = []string{
	"flink", "kafka", "redis", "golang", "java", "ai", "llm", "rust", "worldcup", "cricket",
	"music", "movies", "travel", "food", "coffee", "startup", "devops", "kubernetes", "cloud",
	"opensource", "gaming", "fitness", "books", "photography", "climate", "space", "crypto",
	"design", "python", "streaming", "datascience", "monday", "weekend", "news", "memes",
}

var filler = strings.Fields("today just saw really think about this new love the and so much " +
	"what a day anyone else here we go finally can't wait")

var langs = []string{"en", "en", "en", "es", "hi", "pt"}

func New(cfg Config) (*Generator, error) {
	if cfg.Tags < 1 || cfg.Users < 1 {
		return nil, fmt.Errorf("tags and users must be >= 1")
	}
	if cfg.Zipf <= 1 {
		return nil, fmt.Errorf("zipf exponent must be > 1, got %v", cfg.Zipf)
	}
	if cfg.LateFraction < 0 || cfg.LateFraction > 1 || cfg.OutOfOrder < 0 || cfg.Late < 0 {
		return nil, fmt.Errorf("late fraction must be in [0,1] and delays must be >= 0")
	}
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	return &Generator{
		rng:   rng,
		zipf:  rand.NewZipf(rng, cfg.Zipf, 1, uint64(cfg.Tags-1)),
		vocab: Vocabulary(cfg.Tags),
		users: cfg.Users,
		cfg:   cfg,
		now:   time.Now,
	}, nil
}

// Vocabulary returns n tags: well-known ones first (they get the Zipf head), then synthetic ones.
func Vocabulary(n int) []string {
	v := make([]string, n)
	for i := range v {
		if i < len(popular) {
			v[i] = popular[i]
		} else {
			v[i] = fmt.Sprintf("topic%04d", i)
		}
	}
	return v
}

// Next returns a post and the distinct, lower-case tags it contains (what Flink should count).
func (g *Generator) Next() (Post, []string) {
	tags := g.pickTags(g.tagsPerPost())

	words := make([]string, 0, 8)
	for range 3 + g.rng.IntN(4) {
		words = append(words, filler[g.rng.IntN(len(filler))])
	}
	for _, t := range tags {
		// Mixed case on purpose: the job must normalise "#Flink" and "#flink" to one tag.
		if g.rng.IntN(4) == 0 {
			t = strings.ToUpper(t[:1]) + t[1:]
		}
		words = append(words, "#"+t)
	}
	g.rng.Shuffle(len(words), func(i, j int) { words[i], words[j] = words[j], words[i] })

	var id [8]byte
	for i := range id {
		id[i] = byte(g.rng.UintN(256))
	}
	return Post{
		PostID:    hex.EncodeToString(id[:]),
		UserID:    fmt.Sprintf("u-%d", g.rng.IntN(g.users)),
		Text:      strings.Join(words, " "),
		Lang:      langs[g.rng.IntN(len(langs))],
		EventTime: g.now().Add(-g.delay()).UnixMilli(),
	}, tags
}

// delay is how far in the past this post's event time lies relative to when it is sent.
func (g *Generator) delay() time.Duration {
	var d time.Duration
	if g.cfg.OutOfOrder > 0 {
		d = time.Duration(g.rng.Int64N(int64(g.cfg.OutOfOrder)))
	}
	if g.cfg.LateFraction > 0 && g.rng.Float64() < g.cfg.LateFraction {
		d += g.cfg.Late + time.Duration(g.rng.Int64N(int64(time.Second)))
	}
	return d
}

// 0 tags: 10%, 1: 45%, 2: 30%, 3: 15%.
func (g *Generator) tagsPerPost() int {
	switch p := g.rng.IntN(100); {
	case p < 10:
		return 0
	case p < 55:
		return 1
	case p < 85:
		return 2
	default:
		return 3
	}
}

func (g *Generator) pickTags(n int) []string {
	n = min(n, len(g.vocab))
	seen := make(map[string]bool, n)
	tags := make([]string, 0, n)
	for len(tags) < n {
		t := g.vocab[g.zipf.Uint64()]
		if !seen[t] {
			seen[t] = true
			tags = append(tags, t)
		}
	}
	return tags
}
