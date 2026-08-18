// Package session discovers Claude Code chat logs on disk and turns their
// JSONL records into a compact, render-friendly event shape.
package session

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// DefaultDir is where Claude Code keeps its per-project chat logs.
func DefaultDir() string {
	home, err := os.UserHomeDir()
	if err != nil {
		return ".claude/projects"
	}
	return filepath.Join(home, ".claude", "projects")
}

// Project is one directory under the projects root: all sessions for one cwd.
type Project struct {
	Slug     string    `json:"slug"`
	Dir      string    `json:"dir"`
	Cwd      string    `json:"cwd"`
	Sessions []Session `json:"sessions"`
}

// Session is a single chat transcript (one .jsonl file).
type Session struct {
	ID          string    `json:"id"`
	File        string    `json:"file"`
	Project     string    `json:"project"`
	Cwd         string    `json:"cwd"`
	GitBranch   string    `json:"gitBranch,omitempty"`
	Title       string    `json:"title,omitempty"`
	FirstPrompt string    `json:"firstPrompt,omitempty"`
	Modified    time.Time `json:"modified"`
	Size        int64     `json:"size"`
	Lines       int       `json:"lines"`
	Messages    int       `json:"messages"`
}

// Label is a short human-readable name for the session.
func (s Session) Label() string {
	if s.Title != "" {
		return s.Title
	}
	if s.FirstPrompt != "" {
		return firstLine(s.FirstPrompt, 72)
	}
	return s.ID
}

// Short is the leading segment of the session UUID, enough to identify it.
func (s Session) Short() string {
	if i := strings.IndexByte(s.ID, '-'); i > 0 {
		return s.ID[:i]
	}
	if len(s.ID) > 8 {
		return s.ID[:8]
	}
	return s.ID
}

// Discover walks root and returns every project that holds at least one
// session, newest activity first. Scanning file contents for titles and
// prompts is skipped when fast is set.
func Discover(root string, fast bool) ([]Project, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("read projects dir: %w", err)
	}
	var projects []Project
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		files, err := filepath.Glob(filepath.Join(dir, "*.jsonl"))
		if err != nil || len(files) == 0 {
			continue
		}
		p := Project{Slug: e.Name(), Dir: dir, Cwd: decodeSlug(e.Name())}
		for _, f := range files {
			s, err := describe(f, p.Slug, fast)
			if err != nil {
				continue
			}
			if s.Cwd != "" {
				p.Cwd = s.Cwd
			}
			p.Sessions = append(p.Sessions, s)
		}
		if len(p.Sessions) == 0 {
			continue
		}
		sort.Slice(p.Sessions, func(i, j int) bool {
			return p.Sessions[i].Modified.After(p.Sessions[j].Modified)
		})
		projects = append(projects, p)
	}
	sort.Slice(projects, func(i, j int) bool {
		return projects[i].Sessions[0].Modified.After(projects[j].Sessions[0].Modified)
	})
	return projects, nil
}

// AllSessions flattens Discover's result, newest first.
func AllSessions(root string, fast bool) ([]Session, error) {
	projects, err := Discover(root, fast)
	if err != nil {
		return nil, err
	}
	var out []Session
	for _, p := range projects {
		out = append(out, p.Sessions...)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Modified.After(out[j].Modified) })
	return out, nil
}

// ErrNotFound is returned when a selector matches no session.
var ErrNotFound = errors.New("no matching session")

// Ambiguous reports a selector that matched more than one session.
type Ambiguous struct{ Matches []Session }

func (a *Ambiguous) Error() string {
	names := make([]string, 0, len(a.Matches))
	for _, m := range a.Matches {
		names = append(names, m.Short()+" ("+m.Project+")")
	}
	return "selector is ambiguous, matches: " + strings.Join(names, ", ")
}

// Resolve turns a user-supplied selector into one session. The selector may be
// a path to a .jsonl file, "latest", a session id or unique id prefix, or a
// substring of the project slug, cwd or title.
func Resolve(root, selector string, fast bool) (Session, error) {
	if strings.HasSuffix(selector, ".jsonl") {
		abs, err := filepath.Abs(selector)
		if err != nil {
			return Session{}, err
		}
		return describe(abs, filepath.Base(filepath.Dir(abs)), fast)
	}
	sessions, err := AllSessions(root, fast)
	if err != nil {
		return Session{}, err
	}
	if len(sessions) == 0 {
		return Session{}, fmt.Errorf("%w: no sessions under %s", ErrNotFound, root)
	}
	if selector == "" || selector == "latest" {
		return sessions[0], nil
	}
	sel := strings.ToLower(selector)
	for _, s := range sessions { // exact id wins outright
		if s.ID == selector {
			return s, nil
		}
	}
	var byID, byOther []Session
	for _, s := range sessions {
		switch {
		case strings.HasPrefix(s.ID, selector):
			byID = append(byID, s)
		case strings.Contains(strings.ToLower(s.Project), sel),
			strings.Contains(strings.ToLower(s.Cwd), sel),
			strings.Contains(strings.ToLower(s.Title), sel):
			byOther = append(byOther, s)
		}
	}
	for _, group := range [][]Session{byID, byOther} {
		switch len(group) {
		case 0:
			continue
		case 1:
			return group[0], nil
		default:
			// Several sessions in the same project is normal; take the newest.
			same := true
			for _, s := range group[1:] {
				if s.Project != group[0].Project {
					same = false
					break
				}
			}
			if same && len(byID) == 0 {
				return group[0], nil
			}
			return Session{}, &Ambiguous{Matches: group}
		}
	}
	return Session{}, fmt.Errorf("%w for %q", ErrNotFound, selector)
}

// describe reads a session file's stats and, unless fast is set, its title,
// first prompt and message count.
func describe(path, project string, fast bool) (Session, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return Session{}, err
	}
	s := Session{
		ID:       strings.TrimSuffix(filepath.Base(path), ".jsonl"),
		File:     path,
		Project:  project,
		Modified: fi.ModTime(),
		Size:     fi.Size(),
	}
	if fast {
		return s, nil
	}
	f, err := os.Open(path)
	if err != nil {
		return s, err
	}
	defer f.Close()

	// Only the handful of fields the listing needs, so big transcripts stay cheap.
	var head struct {
		Type      string `json:"type"`
		AITitle   string `json:"aiTitle"`
		Cwd       string `json:"cwd"`
		GitBranch string `json:"gitBranch"`
		Message   struct {
			Content json.RawMessage `json:"content"`
		} `json:"message"`
	}
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 256*1024), maxLineBytes)
	for sc.Scan() {
		line := sc.Bytes()
		if len(line) == 0 {
			continue
		}
		s.Lines++
		head = struct {
			Type      string `json:"type"`
			AITitle   string `json:"aiTitle"`
			Cwd       string `json:"cwd"`
			GitBranch string `json:"gitBranch"`
			Message   struct {
				Content json.RawMessage `json:"content"`
			} `json:"message"`
		}{}
		if err := json.Unmarshal(line, &head); err != nil {
			continue
		}
		if head.Cwd != "" {
			s.Cwd = head.Cwd
		}
		if head.GitBranch != "" {
			s.GitBranch = head.GitBranch
		}
		switch head.Type {
		case "ai-title":
			if head.AITitle != "" {
				s.Title = head.AITitle
			}
		case "user", "assistant":
			s.Messages++
			if s.FirstPrompt == "" && head.Type == "user" {
				var str string
				if json.Unmarshal(head.Message.Content, &str) == nil && str != "" {
					s.FirstPrompt = str
				}
			}
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return s, nil // a partially written tail is expected on a live session
	}
	return s, nil
}

// decodeSlug turns Claude Code's path-encoded directory name back into a
// best-effort filesystem path. Directory names that themselves contain "-" are
// indistinguishable from separators, so a session's recorded cwd wins when one
// is available.
func decodeSlug(slug string) string {
	if !strings.HasPrefix(slug, "-") {
		return slug
	}
	return "/" + strings.ReplaceAll(strings.TrimPrefix(slug, "-"), "-", "/")
}

func firstLine(s string, max int) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	r := []rune(s)
	if len(r) > max {
		return string(r[:max-1]) + "…"
	}
	return s
}
