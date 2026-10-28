package replica

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// ErrDiverged means the local file holds bytes the stream does not, most likely
// because the session was resumed on this machine. Nothing is written after it.
var ErrDiverged = errors.New("local copy has changes the stream doesn't")

// Assembler rebuilds a transcript under a projects dir from replica records. It
// only ever appends: bytes already in the local file are compared against the
// stream, never rewritten, so a session resumed locally is left alone.
type Assembler struct {
	dir     string
	home    string // this machine's home directory, or "" to keep the pusher's project
	session string // the session the stream path names

	header  *Header
	file    string
	size    int64  // bytes the local file should hold: what was there, plus what we appended
	end     int64  // bytes of the transcript the stream has covered so far
	pending []byte // stream bytes [size, end), waiting for a newline
	err     error  // sticky: a diverged or broken stream stays that way
}

// NewAssembler writes the transcript of session under dir. If the pusher's cwd
// maps to a directory under home, the transcript goes in the project directory
// that Claude Code uses for it. An empty home keeps the pusher's project
// directory.
func NewAssembler(dir, home, session string) *Assembler {
	return &Assembler{dir: dir, home: home, session: session}
}

// File is the local transcript path, known once the header has been applied.
func (a *Assembler) File() string { return a.file }

// Size is how many bytes the local transcript holds.
func (a *Assembler) Size() int64 { return a.size }

// Ahead reports whether the local file holds bytes the stream has not covered
// yet. That is fine if the stream later catches up with the same bytes.
func (a *Assembler) Ahead() bool { return a.header != nil && a.size > a.end }

// Apply takes the next record in stream order.
func (a *Assembler) Apply(raw json.RawMessage) error {
	if a.err != nil {
		return a.err
	}
	a.err = a.apply(raw)
	return a.err
}

func (a *Assembler) apply(raw json.RawMessage) error {
	h, c, err := decode(raw)
	if err != nil {
		return err
	}
	if h != nil {
		return a.applyHeader(*h)
	}
	if a.header == nil {
		return errors.New("replica stream does not start with a header")
	}
	return a.applyChunk(*c)
}

func (a *Assembler) applyHeader(h Header) error {
	if a.header != nil {
		// Two pushers racing on an empty stream can both write one.
		if h.Ref() != a.header.Ref() {
			return fmt.Errorf("replica stream has a second header for %s/%s", h.Project, h.Session)
		}
		return nil
	}
	if err := h.Ref().Validate(); err != nil {
		return fmt.Errorf("replica header: %w", err)
	}
	if h.Session != a.session {
		return fmt.Errorf("replica header names session %s, but the stream is for %s", h.Session, a.session)
	}
	a.header = &h
	a.file = filepath.Join(a.dir, h.localProject(a.home), h.Session+".jsonl")
	fi, err := os.Stat(a.file)
	switch {
	case err == nil:
		a.size = fi.Size()
	case errors.Is(err, os.ErrNotExist):
		a.size = 0
	default:
		return err
	}
	return nil
}

func (a *Assembler) applyChunk(c Chunk) error {
	if c.Off > a.end {
		return fmt.Errorf("replica stream skips bytes %d to %d", a.end, c.Off)
	}
	known := max(a.end, a.size)
	stop := c.Off + int64(len(c.Data))
	if overlap := min(stop, known) - c.Off; overlap > 0 {
		have, err := a.read(c.Off, overlap)
		if err != nil {
			return err
		}
		if !bytes.Equal(have, c.Data[:overlap]) {
			return fmt.Errorf("%w: %s differs from the stream at bytes %d to %d",
				ErrDiverged, a.file, c.Off, c.Off+overlap)
		}
	}
	if stop > known {
		a.pending = append(a.pending, c.Data[known-c.Off:]...)
	}
	a.end = max(a.end, stop)
	return nil
}

// read returns the n bytes at off that the local file holds or will hold.
func (a *Assembler) read(off, n int64) ([]byte, error) {
	out := make([]byte, 0, n)
	if off < a.size {
		k := min(n, a.size-off)
		buf := make([]byte, k)
		f, err := os.Open(a.file)
		if err != nil {
			return nil, err
		}
		_, err = f.ReadAt(buf, off)
		f.Close()
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%w: %s is shorter than expected", ErrDiverged, a.file)
		}
		if err != nil {
			return nil, err
		}
		out = append(out, buf...)
		off, n = off+k, n-k
	}
	if n > 0 {
		out = append(out, a.pending[off-a.size:off-a.size+n]...)
	}
	return out, nil
}

// Flush appends pending bytes through the last newline to the local file and
// reports how many it wrote. The file must still be exactly the size this
// Assembler left it at; anything else means someone else wrote to it.
func (a *Assembler) Flush() (int64, error) {
	if a.err != nil {
		return 0, a.err
	}
	if a.header == nil {
		return 0, nil
	}
	var actual int64
	fi, err := os.Stat(a.file)
	switch {
	case err == nil:
		actual = fi.Size()
	case !errors.Is(err, os.ErrNotExist):
		return 0, err
	}
	if actual != a.size {
		a.err = fmt.Errorf("%w: %s is %d bytes, expected %d", ErrDiverged, a.file, actual, a.size)
		return 0, a.err
	}
	n := int64(bytes.LastIndexByte(a.pending, '\n') + 1)
	if n == 0 {
		return 0, nil
	}
	if err := os.MkdirAll(filepath.Dir(a.file), 0o755); err != nil {
		return 0, err
	}
	f, err := os.OpenFile(a.file, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return 0, err
	}
	w, err := f.Write(a.pending[:n])
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	a.size += int64(w)
	a.pending = a.pending[w:]
	return int64(w), err
}
