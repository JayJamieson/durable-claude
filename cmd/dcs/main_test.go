package main

import (
	"strings"
	"testing"

	"github.com/JayJamieson/durable-claude/internal/session"
)

func TestSanitizeSegment(t *testing.T) {
	cases := map[string]string{
		"-home-ubuntu-scratch-durable-claude": "home-ubuntu-scratch-durable-claude",
		"/Users/jay/Code/My Project":          "users-jay-code-my-project",
		"529128A1-9FEC-432E":                  "529128a1-9fec-432e",
		"a??b":                                "a-b",
		"":                                    "unnamed",
		"???":                                 "unnamed",
		"keep.dots_and_underscores":           "keep.dots_and_underscores",
	}
	for in, want := range cases {
		if got := sanitizeSegment(in); got != want {
			t.Errorf("sanitizeSegment(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestStreamPath(t *testing.T) {
	c := &commonFlags{prefix: "/claude"}
	s := session.Session{Project: "-home-ubuntu-scratch-durable-claude", ID: "529128a1-9fec-432e-b80a-bb7c1e7b1f46"}
	want := "/claude/home-ubuntu-scratch-durable-claude/529128a1-9fec-432e-b80a-bb7c1e7b1f46"
	if got := c.streamPath(s); got != want {
		t.Errorf("got %q, want %q", got, want)
	}

	c.prefix = "sessions/"
	if got := c.streamPath(s); !strings.HasPrefix(got, "/sessions/home-ubuntu") {
		t.Errorf("prefix not normalised: %q", got)
	}

	c.stream = "custom/path"
	if got := c.streamPath(s); got != "/custom/path" {
		t.Errorf("explicit -stream should win: %q", got)
	}
}

func TestStreamPathFallsBackToCwd(t *testing.T) {
	c := &commonFlags{prefix: "/claude"}
	s := session.Session{Project: "???", Cwd: "/work/thing", ID: "abc"}
	if got := c.streamPath(s); got != "/claude/work-thing/abc" {
		t.Errorf("got %q", got)
	}
}

func TestViewerURL(t *testing.T) {
	c := &commonFlags{ui: "https://example.github.io/durable-claude"}
	got := c.viewerURL("https://yasdb.fly.dev", "/claude/p/s", "start")
	for _, want := range []string{
		"https://example.github.io/durable-claude/?",
		"server=https%3A%2F%2Fyasdb.fly.dev",
		"stream=%2Fclaude%2Fp%2Fs",
		"from=start",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("viewer URL %q is missing %q", got, want)
		}
	}
	if (&commonFlags{}).viewerURL("s", "p", "start") != "" {
		t.Error("no -ui configured should yield no URL")
	}
}

func TestHumanBytes(t *testing.T) {
	cases := map[int64]string{0: "0B", 999: "999B", 1024: "1.0KB", 1536: "1.5KB", 1 << 20: "1.0MB", 1 << 30: "1.0GB"}
	for in, want := range cases {
		if got := humanBytes(in); got != want {
			t.Errorf("humanBytes(%d) = %q, want %q", in, got, want)
		}
	}
}

func TestBatcherFlushesOnThresholds(t *testing.T) {
	var batches [][]byte
	b := &batcher{maxEvents: 2, maxBytes: 1 << 20, send: func(p []byte, n int) error {
		batches = append(batches, append([]byte(nil), p...))
		return nil
	}}
	for i := 0; i < 5; i++ {
		if err := b.Add(session.Event{Seq: i, Kind: session.KindUser, Text: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(batches) != 2 {
		t.Fatalf("got %d batches, want 2 auto-flushes at 2 events each", len(batches))
	}
	if err := b.Flush(); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 3 {
		t.Fatalf("got %d batches after the final flush, want 3", len(batches))
	}
	// Every batch must be a JSON array, since the server flattens arrays.
	for _, raw := range batches {
		if raw[0] != '[' {
			t.Fatalf("batch is not a JSON array: %s", raw)
		}
	}
	if err := b.Flush(); err != nil || len(batches) != 3 {
		t.Fatal("flushing an empty batcher should be a no-op")
	}
}

func TestBatcherReportsSendErrors(t *testing.T) {
	b := &batcher{maxEvents: 1, maxBytes: 1 << 20, send: func([]byte, int) error {
		return errSend
	}}
	if err := b.Add(session.Event{Kind: session.KindUser, Text: "x"}); err == nil {
		t.Fatal("expected the send error to surface")
	}
	// The failure is sticky, so the tail stops rather than silently dropping events.
	if err := b.Add(session.Event{Kind: session.KindUser, Text: "y"}); err == nil {
		t.Fatal("expected the error to persist")
	}
}

var errSend = errTest("send failed")

type errTest string

func (e errTest) Error() string { return string(e) }
