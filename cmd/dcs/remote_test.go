package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/JayJamieson/durable-claude/internal/dstream"
)

const (
	sessA = "529128a1-9fec-432e-b80a-bb7c1e7b1f46"
	sessB = "52aa7c3e-1d2f-4b5a-9c8d-0e1f2a3b4c5d"

	vmFolder  = "-home-ubuntu--herdr-worktrees-data-x"
	macFolder = "-Users-jay--herdr-worktrees-data-x"
	vmPath    = "/claude/home-ubuntu--herdr-worktrees-data-x/" + sessA
	macPath   = "/claude/users-jay--herdr-worktrees-data-x/" + sessA
	otherPath = "/claude/home-ubuntu--herdr-worktrees-data-x/" + sessB
)

func TestGroupBySession(t *testing.T) {
	cases := []struct {
		name  string
		paths []string
		want  map[string][]string
	}{
		{
			name:  "viewer and replica under two project paths",
			paths: []string{vmPath, vmPath + "/jsonl", macPath, macPath + "/jsonl"},
			want:  map[string][]string{sessA: {vmPath, vmPath + "/jsonl", macPath, macPath + "/jsonl"}},
		},
		{
			name:  "another id stays apart",
			paths: []string{vmPath, otherPath},
			want:  map[string][]string{sessA: {vmPath}, sessB: {otherPath}},
		},
		{
			name:  "replica only",
			paths: []string{otherPath + "/jsonl"},
			want:  map[string][]string{sessB: {otherPath + "/jsonl"}},
		},
		{
			name:  "a stream that holds no session",
			paths: []string{"/claude/notes", "/claude/notes/jsonl", vmPath},
			want:  map[string][]string{sessA: {vmPath}},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var streams []dstream.StreamSummary
			for _, p := range c.paths {
				streams = append(streams, dstream.StreamSummary{Path: p})
			}
			got := map[string][]string{}
			for id, group := range groupBySession(streams) {
				for _, s := range group {
					got[id] = append(got[id], s.Path)
				}
			}
			if !reflect.DeepEqual(got, c.want) {
				t.Errorf("got %v\nwant %v", got, c.want)
			}
		})
	}
}

// A session saved into the host worktree's folder still has its streams under
// the VM's folder name, which only the server knows.
func TestRemoveFindsStreamsTheVMPushed(t *testing.T) {
	srv := newFakeServer(t, vmPath, vmPath+"/jsonl", otherPath, otherPath+"/jsonl")
	dir := projectsDir(t, macFolder)

	_, stderr, err := dcs(t, "", "rm", sessA, "-y", "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("rm: %v\n%s", err, stderr)
	}
	if got, want := srv.paths(), []string{otherPath, otherPath + "/jsonl"}; !slices.Equal(got, want) {
		t.Errorf("server holds %v, want only the other session's %v", got, want)
	}
}

func TestRemoveDeletesEveryProjectPath(t *testing.T) {
	all := []string{vmPath, vmPath + "/jsonl", macPath, macPath + "/jsonl"}
	srv := newFakeServer(t, append([]string{otherPath}, all...)...)
	dir := projectsDir(t, macFolder)

	_, stderr, err := dcs(t, "y\n", "rm", sessA, "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("rm: %v\n%s", err, stderr)
	}
	prompt := strings.Index(stderr, "Delete the stream(s) above?")
	for _, p := range all {
		if i := strings.Index(stderr, p+" ("); i < 0 || i > prompt {
			t.Errorf("rm did not show %s before the prompt:\n%s", p, stderr)
		}
	}
	if got := srv.paths(); !slices.Equal(got, []string{otherPath}) {
		t.Errorf("server holds %v, want only %s", got, otherPath)
	}
}

func TestRemoveWithNothingOnTheServer(t *testing.T) {
	srv := newFakeServer(t, otherPath)
	dir := projectsDir(t, macFolder)

	_, stderr, err := dcs(t, "", "rm", sessA, "-y", "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("rm: %v", err)
	}
	if !strings.Contains(stderr, "no such stream: "+macPath) {
		t.Errorf("stderr = %q, want no such stream: %s", stderr, macPath)
	}
	if got := srv.paths(); !slices.Equal(got, []string{otherPath}) {
		t.Errorf("server holds %v, want %s untouched", got, otherPath)
	}
}

func TestCatAndURLAskForStreamWhenSeveralPathsHoldTheSession(t *testing.T) {
	srv := newFakeServer(t, vmPath, vmPath+"/jsonl", macPath, macPath+"/jsonl")
	dir := projectsDir(t, macFolder)

	for _, args := range [][]string{{"url", sessA}, {"cat", sessA, "-follow=false"}} {
		_, _, err := dcs(t, "", append(args, "-dir", dir, "-server", srv.URL)...)
		if err == nil {
			t.Fatalf("%s: want an error naming both paths", args[0])
		}
		for _, want := range []string{vmPath, macPath, "-stream <path>"} {
			if !strings.Contains(err.Error(), want) {
				t.Errorf("%s error %q does not mention %q", args[0], err, want)
			}
		}
	}

	listed := srv.listings()
	stdout, _, err := dcs(t, "", "url", sessA, "-stream", vmPath, "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("url -stream: %v", err)
	}
	if !strings.Contains(stdout, "stream="+url.QueryEscape(vmPath)) {
		t.Errorf("url -stream printed %q, want the viewer link for %s", stdout, vmPath)
	}
	if srv.listings() != listed {
		t.Error("url -stream listed the server; an explicit -stream needs no listing")
	}
}

func TestServerOnlySession(t *testing.T) {
	srv := newFakeServer(t, vmPath, vmPath+"/jsonl", otherPath+"/jsonl")
	empty := t.TempDir()
	missing := filepath.Join(t.TempDir(), "missing")

	for _, dir := range []string{empty, missing} {
		stdout, stderr, err := dcs(t, "", "url", "5291", "-dir", dir, "-server", srv.URL)
		if err != nil {
			t.Fatalf("url with -dir %s: %v\n%s", dir, err, stderr)
		}
		if !strings.Contains(stdout, "stream="+url.QueryEscape(vmPath)) {
			t.Errorf("url printed %q, want the viewer link for %s", stdout, vmPath)
		}
	}

	_, _, err := dcs(t, "", "url", "52", "-dir", empty, "-server", srv.URL)
	if err == nil || !strings.Contains(err.Error(), sessA) || !strings.Contains(err.Error(), sessB) {
		t.Errorf("an id prefix of two server sessions: err = %v, want both ids", err)
	}
	_, _, err = dcs(t, "", "url", "ffff", "-dir", empty, "-server", srv.URL)
	if err == nil || !strings.Contains(err.Error(), "no session on the server matches") {
		t.Errorf("an unknown id: err = %v", err)
	}

	if _, stderr, err := dcs(t, "", "rm", sessA[:8], "-y", "-dir", missing, "-server", srv.URL); err != nil {
		t.Fatalf("rm: %v\n%s", err, stderr)
	}
	if got := srv.paths(); !slices.Equal(got, []string{otherPath + "/jsonl"}) {
		t.Errorf("server holds %v, want only %s", got, otherPath+"/jsonl")
	}

	// One session is left on the server, so only the selector rules keep an
	// empty selector from naming it.
	for _, selector := range []string{"", "latest"} {
		if _, _, err := dcs(t, "", "rm", selector, "-y", "-dir", empty, "-server", srv.URL); err == nil {
			t.Errorf("rm %q with nothing on disk: want an error", selector)
		}
	}
	if got := srv.paths(); !slices.Equal(got, []string{otherPath + "/jsonl"}) {
		t.Errorf("rm with no session on disk deleted from the server: it holds %v", got)
	}
}

// The server holds streams that are not sessions, such as /claude/notes. A
// selector that names no session must never reach them.
func TestSelectorNeverNamesOtherStreams(t *testing.T) {
	all := []string{vmPath, vmPath + "/jsonl", "/claude/notes", "/claude/notes/jsonl"}
	srv := newFakeServer(t, all...)
	empty := t.TempDir()

	for _, args := range [][]string{{"rm", "no", "-y"}, {"save", "no"}} {
		_, _, err := dcs(t, "", append(args, "-dir", empty, "-server", srv.URL)...)
		if err == nil || !strings.Contains(err.Error(), "no session on the server matches") {
			t.Errorf("%s no: err = %v, want no session on the server matches", args[0], err)
		}
	}
	if got := srv.paths(); !slices.Equal(got, all) {
		t.Errorf("server holds %v, want %v untouched", got, all)
	}
}

// Saving before and after the host worktree exists leaves two local copies of
// one session. An id prefix still names one session.
func TestTwoLocalCopiesAreOneSession(t *testing.T) {
	srv := newFakeServer(t, vmPath, vmPath+"/jsonl", otherPath)
	dir := projectsDir(t, vmFolder, macFolder)

	_, stderr, err := dcs(t, "", "rm", sessA[:8], "-y", "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("rm: %v\n%s", err, stderr)
	}
	if got := srv.paths(); !slices.Equal(got, []string{otherPath}) {
		t.Errorf("server holds %v, want only %s", got, otherPath)
	}
}

func TestSaveAsksForStreamWhenSeveralPathsHoldTheSession(t *testing.T) {
	setHome(t, false)
	srv := newFakeServer(t)
	srv.putReplica(vmPath, vmFolder)
	srv.putReplica(macPath, macFolder)
	dir := t.TempDir()

	_, _, err := dcs(t, "", "save", sessA, "-dir", dir, "-server", srv.URL)
	if err == nil {
		t.Fatal("save: want an error naming both paths")
	}
	for _, want := range []string{vmPath + "\n", macPath + "\n", "-stream <path>"} {
		if !strings.Contains(err.Error()+"\n", want) {
			t.Errorf("save error %q does not mention %q", err, want)
		}
	}

	stdout, stderr, err := dcs(t, "", "save", sessA, "-stream", vmPath, "-dir", dir, "-server", srv.URL)
	if err != nil {
		t.Fatalf("save -stream: %v\n%s", err, stderr)
	}
	if !strings.HasPrefix(stdout, "saved 529128a1") {
		t.Errorf("save -stream printed %q", stdout)
	}
	if _, err := os.Stat(filepath.Join(dir, vmFolder, sessA+".jsonl")); err != nil {
		t.Errorf("save -stream did not write the VM's copy: %v", err)
	}
}

func TestSaveWarnsAboutAnotherLocalCopy(t *testing.T) {
	for _, earlier := range []bool{false, true} {
		t.Run(fmt.Sprintf("earlier copy %v", earlier), func(t *testing.T) {
			setHome(t, true)
			srv := newFakeServer(t)
			srv.putReplica(vmPath, vmFolder)
			dir := t.TempDir()
			old := filepath.Join(dir, vmFolder, sessA+".jsonl")
			if earlier {
				projectsAt(t, dir, vmFolder)
			}

			stdout, stderr, err := dcs(t, "", "save", sessA, "-dir", dir, "-server", srv.URL)
			if err != nil {
				t.Fatalf("save: %v\n%s", err, stderr)
			}
			if !strings.HasPrefix(stdout, "saved 529128a1") {
				t.Fatalf("save printed %q", stdout)
			}
			warned := strings.Contains(stderr, "  "+old+"\n") &&
				strings.Contains(stderr, "claude --resume "+sessA+" can open any copy")
			if warned != earlier {
				t.Errorf("warned = %v, want %v; stderr:\n%s", warned, earlier, stderr)
			}
			if _, err := os.Stat(old); (err == nil) != earlier {
				t.Errorf("save moved or deleted %s: %v", old, err)
			}
		})
	}
}

// dcs runs the CLI as a user would and returns what it printed.
func dcs(t *testing.T, stdin string, args ...string) (stdout, stderr string, err error) {
	t.Helper()
	t.Setenv("DCS_PREFIX", "")

	files := make([]*os.File, 3)
	for i := range files {
		f, err := os.CreateTemp(t.TempDir(), "std")
		if err != nil {
			t.Fatal(err)
		}
		defer f.Close()
		files[i] = f
	}
	if _, err := files[0].WriteString(stdin); err != nil {
		t.Fatal(err)
	}
	files[0].Seek(0, 0)

	argv, in, out, errOut := os.Args, os.Stdin, os.Stdout, os.Stderr
	os.Args = append([]string{"dcs"}, args...)
	os.Stdin, os.Stdout, os.Stderr = files[0], files[1], files[2]
	err = run()
	os.Args, os.Stdin, os.Stdout, os.Stderr = argv, in, out, errOut

	printed, _ := os.ReadFile(files[1].Name())
	warned, _ := os.ReadFile(files[2].Name())
	return string(printed), string(warned), err
}

// setHome points HOME at a temp dir, so dcs save maps the VM's cwd onto it.
// With worktree, the host worktree ~/.herdr/worktrees/data-x exists there.
func setHome(t *testing.T, worktree bool) {
	t.Helper()
	home := t.TempDir()
	if worktree {
		if err := os.MkdirAll(filepath.Join(home, ".herdr", "worktrees", "data-x"), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	t.Setenv("HOME", home)
}

// projectsDir makes a projects dir that holds sessA in each folder.
func projectsDir(t *testing.T, folders ...string) string {
	t.Helper()
	dir := t.TempDir()
	projectsAt(t, dir, folders...)
	return dir
}

func projectsAt(t *testing.T, dir string, folders ...string) {
	t.Helper()
	for _, f := range folders {
		if err := os.MkdirAll(filepath.Join(dir, f), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, f, sessA+".jsonl"), []byte(transcript), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

const transcript = `{"type":"user","cwd":"/home/ubuntu/.herdr/worktrees/data-x","message":{"content":"hi"}}` + "\n"

// fakeServer serves the parts of yasdb that the host commands use. They are
// the admin listing, HEAD, DELETE and catch-up reads.
type fakeServer struct {
	*httptest.Server
	mu      sync.Mutex
	streams map[string][]json.RawMessage
	listed  int
}

func newFakeServer(t *testing.T, paths ...string) *fakeServer {
	t.Helper()
	f := &fakeServer{streams: map[string][]json.RawMessage{}}
	for _, p := range paths {
		f.streams[p] = []json.RawMessage{json.RawMessage(`{"kind":"user","text":"hi"}`)}
	}
	f.Server = httptest.NewServer(f)
	t.Cleanup(f.Close)
	return f
}

// putReplica stores the replica stream that dcs push writes for sessA from a
// VM whose home is /home/ubuntu.
func (f *fakeServer) putReplica(viewer, project string) {
	header, _ := json.Marshal(map[string]any{
		"v": 1, "kind": "jsonl", "session": sessA, "project": project,
		"cwd": "/home/ubuntu/.herdr/worktrees/data-x", "home": "/home/ubuntu",
	})
	chunk := fmt.Sprintf(`{"off":0,"data":%q}`, base64.StdEncoding.EncodeToString([]byte(transcript)))
	f.mu.Lock()
	defer f.mu.Unlock()
	f.streams[viewer] = []json.RawMessage{json.RawMessage(`{"kind":"header"}`)}
	f.streams[viewer+"/jsonl"] = []json.RawMessage{header, json.RawMessage(chunk)}
}

func (f *fakeServer) paths() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Sorted(maps.Keys(f.streams))
}

func (f *fakeServer) listings() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listed
}

func offset(n int) string { return fmt.Sprintf("%016d_%016d", n, 0) }

func (f *fakeServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if r.URL.Path == "/__admin/streams" && r.Method == http.MethodGet {
		f.list(w, r)
		return
	}
	records, ok := f.streams[r.URL.Path]
	if !ok {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	switch r.Method {
	case http.MethodHead:
		w.Header().Set("Stream-Next-Offset", offset(len(records)))
		w.WriteHeader(http.StatusOK)
	case http.MethodDelete:
		delete(f.streams, r.URL.Path)
		w.WriteHeader(http.StatusNoContent)
	case http.MethodGet:
		if r.URL.Query().Get("live") != "" {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		start := 0
		if off := r.URL.Query().Get("offset"); off != dstream.OffsetStart {
			fmt.Sscanf(off, "%d_", &start)
		}
		w.Header().Set("Stream-Next-Offset", offset(len(records)))
		json.NewEncoder(w).Encode(records[min(start, len(records)):])
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// list pages through the streams in path order. The cursor is exclusive, so a
// page holds only the paths after it.
func (f *fakeServer) list(w http.ResponseWriter, r *http.Request) {
	f.listed++
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	cursor := r.URL.Query().Get("cursor")
	var page []dstream.StreamSummary
	next := ""
	for _, p := range slices.Sorted(maps.Keys(f.streams)) {
		if p <= cursor {
			continue
		}
		if len(page) == limit {
			next = page[len(page)-1].Path
			break
		}
		page = append(page, dstream.StreamSummary{Path: p, Records: len(f.streams[p])})
	}
	json.NewEncoder(w).Encode(map[string]any{"streams": page, "nextCursor": next})
}
