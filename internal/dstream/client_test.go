package dstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

func TestOffsetAt(t *testing.T) {
	cases := []struct {
		sample string
		n      int
		want   string
		ok     bool
	}{
		{"0000000000000003_0000000000000000", 2, "0000000000000002_0000000000000000", true},
		{"0000000000000003_0000000000000000", 0, "0000000000000000_0000000000000000", true},
		{"12_0", 7, "07_0", true},
		{"opaque-token", 1, "", false},
		{"", 1, "", false},
		{"0000000000000003_0000000000000000", -1, "", false},
	}
	for _, c := range cases {
		got, ok := OffsetAt(c.sample, c.n)
		if ok != c.ok || got != c.want {
			t.Errorf("OffsetAt(%q, %d) = %q,%v; want %q,%v", c.sample, c.n, got, ok, c.want, c.ok)
		}
	}
}

func TestInfoRecords(t *testing.T) {
	if got := (Info{NextOffset: "0000000000000042_0000000000000000"}).Records(); got != 42 {
		t.Errorf("got %d, want 42", got)
	}
	if got := (Info{NextOffset: "opaque"}).Records(); got != -1 {
		t.Errorf("got %d, want -1 for an undecodable offset", got)
	}
}

// fakeServer is a minimal Durable Streams server: enough of the protocol to
// exercise the client without depending on a live yasdb.
type fakeServer struct {
	mu      sync.Mutex
	records []json.RawMessage
	closed  bool
	fail    int // fail this many appends with 503 before succeeding
	appends int
}

func (f *fakeServer) offset(n int) string { return fmt.Sprintf("%016d_%016d", n, 0) }

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()

	switch r.Method {
	case http.MethodPut:
		w.Header().Set("Stream-Next-Offset", f.offset(len(f.records)))
		w.WriteHeader(http.StatusCreated)

	case http.MethodHead:
		w.Header().Set("Stream-Next-Offset", f.offset(len(f.records)))
		w.Header().Set("Content-Type", "application/json")
		if f.closed {
			w.Header().Set("Stream-Closed", "true")
		}
		w.WriteHeader(http.StatusOK)

	case http.MethodPost:
		if r.Header.Get("Stream-Closed") == "true" {
			f.closed = true
			w.WriteHeader(http.StatusNoContent)
			return
		}
		f.appends++
		if f.appends <= f.fail {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		var batch []json.RawMessage
		if err := json.NewDecoder(r.Body).Decode(&batch); err != nil {
			var one json.RawMessage
			w.WriteHeader(http.StatusBadRequest)
			_ = one
			return
		}
		f.records = append(f.records, batch...)
		w.Header().Set("Stream-Next-Offset", f.offset(len(f.records)))
		w.WriteHeader(http.StatusNoContent)

	case http.MethodGet:
		start := 0
		if off := r.URL.Query().Get("offset"); off != "" && off != "-1" {
			if off == "now" {
				start = len(f.records)
			} else {
				fmt.Sscanf(off, "%d_", &start)
			}
		}
		if start > len(f.records) {
			start = len(f.records)
		}
		if r.URL.Query().Get("live") == "sse" {
			f.serveSSE(w, f.records[start:])
			return
		}
		w.Header().Set("Stream-Next-Offset", f.offset(len(f.records)))
		w.Header().Set("Stream-Up-To-Date", "true")
		if f.closed {
			w.Header().Set("Stream-Closed", "true")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(f.records[start:])

	case http.MethodDelete:
		f.records = nil
		f.closed = false
		w.WriteHeader(http.StatusNoContent)
	}
}

func (f *fakeServer) serveSSE(w http.ResponseWriter, records []json.RawMessage) {
	w.Header().Set("Content-Type", "text/event-stream")
	w.WriteHeader(http.StatusOK)
	if len(records) > 0 {
		body, _ := json.Marshal(records)
		// Split the payload across data: lines, as the protocol permits.
		fmt.Fprintf(w, "event: data\ndata: %s\ndata: %s\n\n", body[:1], body[1:])
	}
	ctrl := Control{StreamNextOffset: f.offset(len(f.records)), StreamCursor: "c1", UpToDate: true}
	ctrl.StreamClosed = true // end the tail so the test terminates
	body, _ := json.Marshal(ctrl)
	fmt.Fprintf(w, "event: control\ndata: %s\n\n", body)
}

func newTestClient(t *testing.T) (*Client, *fakeServer) {
	t.Helper()
	fake := &fakeServer{}
	srv := httptest.NewServer(fake)
	t.Cleanup(srv.Close)
	c, err := New(srv.URL, "test-producer")
	if err != nil {
		t.Fatal(err)
	}
	return c, fake
}

func TestAppendAndRead(t *testing.T) {
	ctx := context.Background()
	c, fake := newTestClient(t)

	created, err := c.Create(ctx, "/s", "application/json")
	if err != nil || !created {
		t.Fatalf("create: created=%v err=%v", created, err)
	}
	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"seq":0},{"seq":1}]`)); err != nil {
		t.Fatal(err)
	}
	if len(fake.records) != 2 {
		t.Fatalf("server holds %d records, want 2 (the array should be flattened)", len(fake.records))
	}

	records, _, _, err := c.ReadAll(ctx, "/s", OffsetStart)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 2 || !strings.Contains(string(records[1]), `"seq":1`) {
		t.Fatalf("got %v", records)
	}
}

func TestAppendRetriesWithStableProducerSeq(t *testing.T) {
	ctx := context.Background()
	c, fake := newTestClient(t)
	fake.fail = 2 // two 503s, then success
	c.Retries = 3

	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"a":1}]`)); err != nil {
		t.Fatalf("append should survive transient failures: %v", err)
	}
	if len(fake.records) != 1 {
		t.Fatalf("got %d records, want 1 (retries must not duplicate)", len(fake.records))
	}
}

func TestAppendGivesUpAfterRetries(t *testing.T) {
	c, fake := newTestClient(t)
	fake.fail = 99
	c.Retries = 1
	if _, err := c.AppendJSON(context.Background(), "/s", []byte(`[{"a":1}]`)); err == nil {
		t.Fatal("expected an error once retries are exhausted")
	}
}

func TestHeadAndLastRecord(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestClient(t)

	info, err := c.Head(ctx, "/s")
	if err != nil {
		t.Fatal(err)
	}
	if info.Records() != 0 {
		t.Fatalf("empty stream reports %d records", info.Records())
	}
	if last, err := c.LastRecord(ctx, "/s", info); err != nil || last != nil {
		t.Fatalf("empty stream: last=%s err=%v", last, err)
	}

	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"seq":0},{"seq":1},{"seq":2}]`)); err != nil {
		t.Fatal(err)
	}
	info, _ = c.Head(ctx, "/s")
	if info.Records() != 3 {
		t.Fatalf("got %d records, want 3", info.Records())
	}
	last, err := c.LastRecord(ctx, "/s", info)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(last), `"seq":2`) {
		t.Fatalf("got last record %s, want seq 2", last)
	}
}

// LastRecord must still find the tail when offsets cannot be decoded, which is
// the portable path for a server that does not use yasdb's offset encoding.
func TestLastRecordFallsBackToScanning(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestClient(t)
	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"seq":0},{"seq":1}]`)); err != nil {
		t.Fatal(err)
	}
	last, err := c.LastRecord(ctx, "/s", Info{Exists: true, NextOffset: "opaque-token"})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(last), `"seq":1`) {
		t.Fatalf("got %s, want seq 1", last)
	}
}

func TestCountRecords(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestClient(t)
	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"a":1},{"a":2},{"a":3}]`)); err != nil {
		t.Fatal(err)
	}
	n, err := c.CountRecords(ctx, "/s")
	if err != nil || n != 3 {
		t.Fatalf("got %d, %v; want 3", n, err)
	}
}

func TestTailParsesMultiLineDataEvents(t *testing.T) {
	ctx := context.Background()
	c, _ := newTestClient(t)
	if _, err := c.AppendJSON(ctx, "/s", []byte(`[{"seq":0},{"seq":1}]`)); err != nil {
		t.Fatal(err)
	}

	var got []string
	var controls []Control
	err := c.Tail(ctx, "/s", OffsetStart,
		func(raw json.RawMessage) error { got = append(got, string(raw)); return nil },
		func(ctrl Control) { controls = append(controls, ctrl) })
	if !errors.Is(err, ErrStreamClosed) {
		t.Fatalf("got err %v, want ErrStreamClosed", err)
	}
	if len(got) != 2 || !strings.Contains(got[1], `"seq":1`) {
		t.Fatalf("got %v", got)
	}
	if len(controls) != 1 || controls[0].StreamCursor != "c1" {
		t.Fatalf("control events: %+v", controls)
	}
}

func TestCloseAndDelete(t *testing.T) {
	ctx := context.Background()
	c, fake := newTestClient(t)
	if err := c.CloseStream(ctx, "/s"); err != nil {
		t.Fatal(err)
	}
	if !fake.closed {
		t.Fatal("stream should be closed")
	}
	info, _ := c.Head(ctx, "/s")
	if !info.Closed {
		t.Fatal("HEAD should report the stream closed")
	}
	if err := c.Delete(ctx, "/s"); err != nil {
		t.Fatal(err)
	}
}

func TestNewRejectsBadServer(t *testing.T) {
	if _, err := New("://nope", ""); err == nil {
		t.Fatal("expected an error for a malformed URL")
	}
	c, err := New("yasdb.fly.dev", "")
	if err != nil {
		t.Fatal(err)
	}
	if got := c.URL("/a/b", nil); got != "https://yasdb.fly.dev/a/b" {
		t.Fatalf("got %q, want a https:// URL", got)
	}
}
