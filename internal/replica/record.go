// Package replica copies a Claude Code transcript byte for byte through a
// durable stream, so a session recorded on one machine can be resumed with
// `claude --resume <id>` on another.
//
// The stream holds a Header, then Chunks. Each Chunk carries the offset of its
// first byte in the transcript, so the file is exactly the bytes the chunks
// cover, in offset order. Duplicates from a retry or a second pusher hold the
// same bytes and are harmless; a gap or a mismatch is an error.
package replica

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Version is the replica record schema version.
const Version = 1

// Kind marks a header record.
const Kind = "jsonl"

// MaxChunk is the most transcript bytes one record carries. Base64 inflates it
// by a third, and chunksPerPost of them stay well under the server's ~3 MiB
// body limit.
const MaxChunk = 256 << 10

const chunksPerPost = 4

// Header is record 0 of a replica stream: which transcript the chunks rebuild.
type Header struct {
	V       int    `json:"v"`
	Kind    string `json:"kind"`
	Session string `json:"session"`
	Project string `json:"project"` // the pusher's directory name under ~/.claude/projects
	Cwd     string `json:"cwd,omitempty"`
	Home    string `json:"home,omitempty"` // the pusher's home directory
	Title   string `json:"title,omitempty"`
}

// Ref names the transcript a header describes.
func (h Header) Ref() Ref { return Ref{Project: h.Project, Session: h.Session} }

// Claude Code shortens longer names and adds a hash dcs cannot reproduce.
const maxProjectName = 200

var nonAlnum = regexp.MustCompile(`[^A-Za-z0-9]`)

// localProject is the directory name under the local projects dir. It maps Cwd
// from the pusher's home onto home and returns the name Claude Code gives that
// path. It returns the pusher's Project when it cannot map Cwd to a local
// directory.
func (h Header) localProject(home string) string {
	if home == "" || h.Home == "" || h.Cwd == "" {
		return h.Project
	}
	if h.Cwd != h.Home && !strings.HasPrefix(h.Cwd, h.Home+"/") {
		return h.Project
	}
	local := filepath.Join(home, strings.TrimPrefix(h.Cwd, h.Home))
	if fi, err := os.Stat(local); err != nil || !fi.IsDir() {
		return h.Project
	}
	name := nonAlnum.ReplaceAllString(local, "-")
	if len(name) > maxProjectName {
		return h.Project
	}
	return name
}

// Chunk is a run of transcript bytes starting at Off. Data is a []byte so it
// travels as base64: a JSON object would be re-encoded by the server and lose
// the transcript's exact bytes.
type Chunk struct {
	Off  int64  `json:"off"`
	Data []byte `json:"data"`
}

// Chunks splits b, which starts at offset off, into records of at most MaxChunk.
func Chunks(off int64, b []byte) []Chunk {
	var out []Chunk
	for len(b) > 0 {
		n := min(len(b), MaxChunk)
		out = append(out, Chunk{Off: off, Data: b[:n]})
		off += int64(n)
		b = b[n:]
	}
	return out
}

// decode parses one stream record into either a header or a chunk.
func decode(raw json.RawMessage) (*Header, *Chunk, error) {
	var r struct {
		Header
		Off  *int64 `json:"off"`
		Data []byte `json:"data"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return nil, nil, fmt.Errorf("decode replica record: %w", err)
	}
	switch {
	case r.Kind == Kind:
		if r.V != Version {
			return nil, nil, fmt.Errorf("replica header version %d, want %d", r.V, Version)
		}
		return &r.Header, nil, nil
	case r.Off != nil && *r.Off >= 0:
		return nil, &Chunk{Off: *r.Off, Data: r.Data}, nil
	default:
		return nil, nil, fmt.Errorf("not a replica record: %.80s", raw)
	}
}

// Ref names one transcript: <project>/<session>.jsonl under a projects dir.
type Ref struct {
	Project string // the project directory, such as -home-you-code-thing
	Session string // the session UUID
}

var (
	projectRE = regexp.MustCompile(`^[A-Za-z0-9._-]{1,255}$`)
	sessionRE = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

// Validate reports whether the ref is safe to join onto a projects dir. Replica
// records come from a server anyone can write to.
func (r Ref) Validate() error {
	if !projectRE.MatchString(r.Project) || r.Project[0] == '.' {
		return errors.New("project must be a plain directory name")
	}
	if !IsSessionID(r.Session) {
		return errors.New("session must be a lowercase UUID")
	}
	return nil
}

// IsSessionID reports whether s has the form of a Claude Code session id, a
// lowercase UUID.
func IsSessionID(s string) bool { return sessionRE.MatchString(s) }
