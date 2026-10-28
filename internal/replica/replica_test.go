package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/JayJamieson/durable-claude/internal/dstream"
)

const (
	testSession = "529128a1-9fec-432e-b80a-bb7c1e7b1f46"
	testProject = "-home-you-code-thing"
)

// fakeStream is one in-memory application/json stream. Like yasdb it flattens a
// posted array into one record per element and re-encodes each record.
type fakeStream struct {
	records []json.RawMessage
	posts   []int // body size of every append
}

func (f *fakeStream) Create(context.Context, string, string) (bool, error) {
	return len(f.records) == 0, nil
}

func (f *fakeStream) Head(context.Context, string) (dstream.Info, error) {
	return dstream.Info{Exists: true, NextOffset: fmt.Sprintf("%016d_%016d", len(f.records), 0)}, nil
}

func (f *fakeStream) LastRecord(context.Context, string, dstream.Info) (json.RawMessage, error) {
	if len(f.records) == 0 {
		return nil, nil
	}
	return f.records[len(f.records)-1], nil
}

func (f *fakeStream) AppendJSON(_ context.Context, _ string, body []byte) (string, error) {
	f.posts = append(f.posts, len(body))
	var items []json.RawMessage
	if err := json.Unmarshal(body, &items); err != nil {
		items = []json.RawMessage{body}
	}
	for _, it := range items {
		var buf bytes.Buffer
		if err := json.Compact(&buf, it); err != nil {
			return "", err
		}
		f.records = append(f.records, buf.Bytes())
	}
	return "", nil
}

func writeFile(t *testing.T, path, data string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(data); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	b, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal(err)
	}
	return string(b)
}

func newPusher(t *testing.T, s *fakeStream, file string) *Pusher {
	t.Helper()
	p := NewPusher(s, "/claude/p/s/jsonl", file, Header{Session: testSession, Project: testProject, Title: "t"})
	if err := p.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	return p
}

func pass(t *testing.T, p *Pusher) int64 {
	t.Helper()
	n, err := p.Pass(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// assemble applies records into dir and flushes, returning the bytes written.
func assemble(t *testing.T, dir string, records []json.RawMessage) (*Assembler, int64) {
	t.Helper()
	a := NewAssembler(dir, testSession)
	for _, r := range records {
		if err := a.Apply(r); err != nil {
			t.Fatal(err)
		}
	}
	n, err := a.Flush()
	if err != nil {
		t.Fatal(err)
	}
	return a, n
}

func hostFile(dir string) string { return filepath.Join(dir, testProject, testSession+".jsonl") }

func rec(t *testing.T, v any) json.RawMessage {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func header() Header {
	return Header{V: Version, Kind: Kind, Session: testSession, Project: testProject}
}

// transcript returns n lines of JSON-ish text with multi-byte runes, so chunk
// boundaries land mid-rune as well as mid-line.
func transcript(n int) string {
	var b strings.Builder
	for i := range n {
		fmt.Fprintf(&b, `{"type":"user","n":%d,"text":"héllo ✓ %s"}`+"\n", i, strings.Repeat("x", i%300))
	}
	return b.String()
}

func TestPushThenAssembleRoundTrips(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	body := transcript(20000)
	writeFile(t, file, body+`{"partial":`)

	s := &fakeStream{}
	p := newPusher(t, s, file)
	if n := pass(t, p); n != int64(len(body)) {
		t.Fatalf("sent %d bytes, want %d (through the last newline)", n, len(body))
	}
	if len(s.posts) < 3 {
		t.Fatalf("got %d appends for %d bytes, want the body split across several", len(s.posts), len(body))
	}
	for _, size := range s.posts {
		if size > 2<<20 {
			t.Errorf("an append body is %d bytes, want it well under the server's ~3 MiB limit", size)
		}
	}

	dir := t.TempDir()
	_, n := assemble(t, dir, s.records)
	if got := readFile(t, hostFile(dir)); got != body {
		t.Fatalf("host file is %d bytes, want the %d pushed bytes exactly", len(got), len(body))
	}
	if n != int64(len(body)) {
		t.Errorf("Flush reported %d bytes, want %d", n, len(body))
	}
}

func TestPartialLineWaitsForItsNewline(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	writeFile(t, file, "one\ntw")
	s := &fakeStream{}
	p := newPusher(t, s, file)

	if n := pass(t, p); n != 4 {
		t.Fatalf("sent %d bytes, want only the complete line", n)
	}
	if n := pass(t, p); n != 0 {
		t.Fatalf("a pass with only a partial line sent %d bytes", n)
	}
	writeFile(t, file, "o\n")
	if n := pass(t, p); n != 4 {
		t.Fatalf("sent %d bytes, want the completed line", n)
	}

	dir := t.TempDir()
	assemble(t, dir, s.records)
	if got := readFile(t, hostFile(dir)); got != "one\ntwo\n" {
		t.Fatalf("host file = %q", got)
	}
}

func TestRestartedPusherResumesFromTheStream(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	writeFile(t, file, transcript(50))
	s := &fakeStream{}
	pass(t, newPusher(t, s, file))
	before := len(s.records)

	p := newPusher(t, s, file)
	if n := pass(t, p); n != 0 || len(s.records) != before {
		t.Fatalf("restart re-sent %d bytes in %d new records", n, len(s.records)-before)
	}
	writeFile(t, file, "more\n")
	if n := pass(t, p); n != 5 {
		t.Fatalf("sent %d bytes, want just the new line", n)
	}

	dir := t.TempDir()
	assemble(t, dir, s.records)
	if got, want := readFile(t, hostFile(dir)), readFile(t, file); got != want {
		t.Fatalf("host file differs from the guest after a restart:\n got %d bytes\nwant %d bytes", len(got), len(want))
	}
}

func TestStartRejectsAStreamOfOtherContent(t *testing.T) {
	tmp := t.TempDir()
	mine, theirs := filepath.Join(tmp, "mine.jsonl"), filepath.Join(tmp, "theirs.jsonl")
	writeFile(t, theirs, "theirs\n")
	writeFile(t, mine, "mine!!\nand more\n")
	s := &fakeStream{}
	pass(t, newPusher(t, s, theirs))

	err := NewPusher(s, "/claude/p/s/jsonl", mine, Header{Session: testSession, Project: testProject}).
		Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "/claude/p/s/jsonl") {
		t.Fatalf("Start = %v, want an error naming the stream", err)
	}

	other := &fakeStream{}
	newPusher(t, other, mine) // header only
	err = NewPusher(other, "/x/jsonl", mine, Header{Session: "00000000-0000-0000-0000-000000000000", Project: testProject}).
		Start(context.Background())
	if err == nil {
		t.Fatal("Start accepted a stream whose header names another session")
	}
}

func TestLongLineIsSplitAndWrittenWhole(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	long := strings.Repeat("y", 3*chunksPerPost*MaxChunk) + "\n"
	writeFile(t, file, "short\n"+long)
	s := &fakeStream{}
	pass(t, newPusher(t, s, file))
	if len(s.posts) < 4 {
		t.Fatalf("got %d appends, want the long line split across several", len(s.posts))
	}

	dir := t.TempDir()
	a := NewAssembler(dir, testSession)
	for i, r := range s.records {
		if err := a.Apply(r); err != nil {
			t.Fatal(err)
		}
		if _, err := a.Flush(); err != nil {
			t.Fatal(err)
		}
		got := readFile(t, hostFile(dir))
		if i < len(s.records)-1 && got != "short\n" && got != "" {
			t.Fatalf("after record %d the host file holds %d bytes of a partial line", i, len(got))
		}
	}
	if got := readFile(t, hostFile(dir)); got != "short\n"+long {
		t.Fatalf("host file is %d bytes, want %d", len(got), len("short\n"+long))
	}
}

func TestDuplicateAndOverlappingRecordsAssembleOnce(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	writeFile(t, file, transcript(200))
	s := &fakeStream{}
	first, second := newPusher(t, s, file), newPusher(t, s, file)
	pass(t, first)
	writeFile(t, file, transcript(30))
	pass(t, second) // everything again, from offset 0
	pass(t, first)  // just the new tail

	dir := t.TempDir()
	assemble(t, dir, s.records)
	if got, want := readFile(t, hostFile(dir)), readFile(t, file); got != want {
		t.Fatalf("two pushers assembled to %d bytes, want %d", len(got), len(want))
	}

	dir = t.TempDir()
	assemble(t, dir, []json.RawMessage{
		rec(t, header()),
		rec(t, Chunk{Off: 0, Data: []byte("abc\nde")}),
		rec(t, Chunk{Off: 2, Data: []byte("c\ndef\n")}),
		rec(t, Chunk{Off: 0, Data: []byte("abc\n")}),
		rec(t, header()),
		rec(t, Chunk{Off: 8, Data: []byte("g\n")}),
	})
	if got := readFile(t, hostFile(dir)); got != "abc\ndef\ng\n" {
		t.Fatalf("host file = %q", got)
	}
}

func TestGapIsAnError(t *testing.T) {
	dir := t.TempDir()
	a, _ := assemble(t, dir, []json.RawMessage{rec(t, header()), rec(t, Chunk{Off: 0, Data: []byte("a\n")})})

	err := a.Apply(rec(t, Chunk{Off: 5, Data: []byte("late\n")}))
	if err == nil || errors.Is(err, ErrDiverged) {
		t.Fatalf("Apply = %v, want a gap error", err)
	}
	if _, err := a.Flush(); err == nil {
		t.Fatal("Flush after a gap succeeded")
	}
	if got := readFile(t, hostFile(dir)); got != "a\n" {
		t.Fatalf("host file = %q, want nothing written past the gap", got)
	}
}

func TestHostResumeDiverges(t *testing.T) {
	dir := t.TempDir()
	a, _ := assemble(t, dir, []json.RawMessage{rec(t, header()), rec(t, Chunk{Off: 0, Data: []byte("a\n")})})
	writeFile(t, hostFile(dir), "resumed on the host\n")

	if err := a.Apply(rec(t, Chunk{Off: 2, Data: []byte("b\n")})); err != nil {
		t.Fatal(err)
	}
	if _, err := a.Flush(); !errors.Is(err, ErrDiverged) {
		t.Fatalf("Flush = %v, want ErrDiverged", err)
	}
	if got := readFile(t, hostFile(dir)); got != "a\nresumed on the host\n" {
		t.Fatalf("host file = %q, want it left as is", got)
	}

	dir = t.TempDir()
	writeFile(t, hostFile(dir), "zzz\n")
	b := NewAssembler(dir, testSession)
	if err := b.Apply(rec(t, header())); err != nil {
		t.Fatal(err)
	}
	if err := b.Apply(rec(t, Chunk{Off: 0, Data: []byte("abc\nmore\n")})); !errors.Is(err, ErrDiverged) {
		t.Fatalf("Apply = %v, want ErrDiverged for a local prefix that differs", err)
	}
	if _, err := b.Flush(); !errors.Is(err, ErrDiverged) {
		t.Fatalf("Flush = %v, want the divergence to stick", err)
	}
	if got := readFile(t, hostFile(dir)); got != "zzz\n" {
		t.Fatalf("host file = %q, want it left as is", got)
	}
}

func TestRestartWritesOnlyNewBytes(t *testing.T) {
	file := filepath.Join(t.TempDir(), "guest.jsonl")
	writeFile(t, file, transcript(100))
	s := &fakeStream{}
	p := newPusher(t, s, file)
	pass(t, p)
	dir := t.TempDir()
	assemble(t, dir, s.records)

	writeFile(t, file, "new one\nnew two\n")
	pass(t, p)
	a, n := assemble(t, dir, s.records)
	if n != int64(len("new one\nnew two\n")) {
		t.Fatalf("a fresh assembler wrote %d bytes, want only the 16 new ones", n)
	}
	if got, want := readFile(t, hostFile(dir)), readFile(t, file); got != want {
		t.Fatalf("host file is %d bytes, want %d", len(got), len(want))
	}
	if a.Ahead() {
		t.Error("Ahead after a full catch-up")
	}

	writeFile(t, hostFile(dir), "typed on the host\n")
	a, n = assemble(t, dir, s.records)
	if n != 0 || !a.Ahead() {
		t.Fatalf("wrote %d, Ahead %v; want nothing written and the local copy reported ahead", n, a.Ahead())
	}
}

func TestHeaderValidation(t *testing.T) {
	cases := map[string]Header{
		"non-UUID session":   {Session: "not-a-uuid", Project: testProject},
		"upper-case UUID":    {Session: strings.ToUpper(testSession), Project: testProject},
		"dot-dot project":    {Session: testSession, Project: ".."},
		"slash in project":   {Session: testSession, Project: "a/b"},
		"hidden project":     {Session: testSession, Project: ".x"},
		"empty project":      {Session: testSession, Project: ""},
		"other session":      {Session: "00000000-0000-0000-0000-000000000000", Project: testProject},
		"traversal in UUID":  {Session: "../../../../etc/passwd", Project: testProject},
		"backslash project":  {Session: testSession, Project: `a\b`},
		"overlong project":   {Session: testSession, Project: strings.Repeat("p", 256)},
		"whitespace project": {Session: testSession, Project: "a b"},
	}
	for name, h := range cases {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			dir := filepath.Join(root, "projects")
			h.V, h.Kind = Version, Kind
			a := NewAssembler(dir, testSession)
			if err := a.Apply(rec(t, h)); err == nil {
				t.Fatal("header accepted")
			}
			a.Apply(rec(t, Chunk{Off: 0, Data: []byte("x\n")}))
			a.Flush()
			var written []string
			filepath.Walk(root, func(path string, fi os.FileInfo, err error) error {
				if path != root {
					written = append(written, path)
				}
				return nil
			})
			if len(written) > 0 {
				t.Fatalf("files written for a rejected header: %v", written)
			}
		})
	}
}

func TestStreamMustStartWithAHeader(t *testing.T) {
	a := NewAssembler(t.TempDir(), testSession)
	if err := a.Apply(rec(t, Chunk{Off: 0, Data: []byte("x\n")})); err == nil {
		t.Fatal("a chunk before the header was accepted")
	}
}
