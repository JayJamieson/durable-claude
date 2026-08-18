package session

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// projectFixture writes a tiny transcript that looks like a real one.
func projectFixture(t *testing.T, root, project, id, title, prompt string, modified time.Time) string {
	t.Helper()
	dir := filepath.Join(root, project)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, id+".jsonl")
	lines := `{"type":"mode","mode":"normal"}
{"type":"user","cwd":"/work/` + project + `","gitBranch":"main","message":{"role":"user","content":"` + prompt + `"}}
{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"sure"}]}}
{"type":"ai-title","aiTitle":"` + title + `"}
`
	if err := os.WriteFile(path, []byte(lines), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, modified, modified); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestDiscoverReadsTitlesAndStats(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	projectFixture(t, root, "-work-alpha", "11111111-aaaa-bbbb-cccc-000000000001", "Alpha work", "do alpha", now.Add(-time.Hour))
	projectFixture(t, root, "-work-beta", "22222222-aaaa-bbbb-cccc-000000000002", "Beta work", "do beta", now)

	projects, err := Discover(root, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 2 {
		t.Fatalf("got %d projects, want 2", len(projects))
	}
	// Most recent activity first.
	if projects[0].Slug != "-work-beta" {
		t.Fatalf("got %q first, want the most recently touched project", projects[0].Slug)
	}
	s := projects[0].Sessions[0]
	if s.Title != "Beta work" {
		t.Errorf("title = %q", s.Title)
	}
	if s.FirstPrompt != "do beta" {
		t.Errorf("first prompt = %q", s.FirstPrompt)
	}
	if s.Messages != 2 {
		t.Errorf("messages = %d, want 2", s.Messages)
	}
	if s.Cwd != "/work/-work-beta" {
		t.Errorf("cwd = %q, want the value recorded in the transcript", s.Cwd)
	}
	if s.GitBranch != "main" {
		t.Errorf("branch = %q", s.GitBranch)
	}
	if s.Short() != "22222222" {
		t.Errorf("short id = %q", s.Short())
	}
}

func TestDiscoverFastSkipsFileContents(t *testing.T) {
	root := t.TempDir()
	projectFixture(t, root, "-work-alpha", "11111111-aaaa-bbbb-cccc-000000000001", "Alpha", "hi", time.Now())
	projects, err := Discover(root, true)
	if err != nil {
		t.Fatal(err)
	}
	s := projects[0].Sessions[0]
	if s.Title != "" || s.Messages != 0 {
		t.Fatalf("fast mode should not read the transcript: %+v", s)
	}
	if s.Cwd != "" || projects[0].Cwd != "/work/alpha" {
		t.Fatalf("fast mode should fall back to the decoded slug, got %q", projects[0].Cwd)
	}
}

func TestResolveSelectors(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	alpha := projectFixture(t, root, "-work-alpha", "11111111-aaaa-bbbb-cccc-000000000001", "Alpha work", "do alpha", now.Add(-time.Hour))
	beta := projectFixture(t, root, "-work-beta", "22222222-aaaa-bbbb-cccc-000000000002", "Beta work", "do beta", now)

	cases := map[string]string{
		"":                                     beta, // newest
		"latest":                               beta,
		"11111111":                             alpha,
		"11111111-aaaa-bbbb-cccc-000000000001": alpha,
		"alpha":                                alpha,
		"Beta work":                            beta,
		alpha:                                  alpha, // explicit path
	}
	for selector, want := range cases {
		got, err := Resolve(root, selector, false)
		if err != nil {
			t.Errorf("Resolve(%q): %v", selector, err)
			continue
		}
		if got.File != want {
			t.Errorf("Resolve(%q) = %s, want %s", selector, got.File, want)
		}
	}

	if _, err := Resolve(root, "nothing-matches-this", false); err == nil {
		t.Error("expected an error for an unmatched selector")
	}
}

func TestResolvePrefersNewestWithinOneProject(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	projectFixture(t, root, "-work-alpha", "aaaaaaaa-0000-0000-0000-000000000001", "Older", "one", now.Add(-2*time.Hour))
	newest := projectFixture(t, root, "-work-alpha", "bbbbbbbb-0000-0000-0000-000000000002", "Newer", "two", now)

	got, err := Resolve(root, "alpha", false)
	if err != nil {
		t.Fatal(err)
	}
	if got.File != newest {
		t.Fatalf("got %s, want the newest session in the project", got.File)
	}
}

func TestResolveReportsAmbiguityAcrossProjects(t *testing.T) {
	root := t.TempDir()
	now := time.Now()
	projectFixture(t, root, "-work-shared-one", "aaaaaaaa-0000-0000-0000-000000000001", "One", "x", now)
	projectFixture(t, root, "-work-shared-two", "bbbbbbbb-0000-0000-0000-000000000002", "Two", "x", now)

	_, err := Resolve(root, "shared", false)
	var amb *Ambiguous
	if err == nil {
		t.Fatal("expected an ambiguity error")
	}
	if !asAmbiguousErr(err, &amb) || len(amb.Matches) != 2 {
		t.Fatalf("got %v, want an *Ambiguous naming both projects", err)
	}
}

func asAmbiguousErr(err error, target **Ambiguous) bool {
	a, ok := err.(*Ambiguous)
	if ok {
		*target = a
	}
	return ok
}

func TestLabelFallsBackFromTitleToPrompt(t *testing.T) {
	s := Session{ID: "abc-def", FirstPrompt: "first line\nsecond line"}
	if s.Label() != "first line" {
		t.Errorf("got %q", s.Label())
	}
	s.Title = "A title"
	if s.Label() != "A title" {
		t.Errorf("got %q", s.Label())
	}
	if (Session{ID: "abc-def"}).Label() != "abc-def" {
		t.Error("bare session should fall back to its id")
	}
}

func TestDecodeSlug(t *testing.T) {
	// The encoding is lossy: a directory whose own name contains "-" is
	// indistinguishable from a separator, so this is best-effort only and a
	// session's recorded cwd is preferred wherever one exists.
	if got := decodeSlug("-home-ubuntu-scratch-durable-claude"); got != "/home/ubuntu/scratch/durable/claude" {
		t.Errorf("got %q", got)
	}
	if got := decodeSlug("plain"); got != "plain" {
		t.Errorf("got %q", got)
	}
}

func TestDiscoverIgnoresDirsWithoutTranscripts(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "-empty"), 0o755); err != nil {
		t.Fatal(err)
	}
	projects, err := Discover(root, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(projects) != 0 {
		t.Fatalf("got %d projects, want 0", len(projects))
	}
}
