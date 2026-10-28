package replica

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"github.com/JayJamieson/durable-claude/internal/dstream"
)

// Client is the part of *dstream.Client a Pusher uses.
type Client interface {
	Create(ctx context.Context, path, contentType string) (bool, error)
	Head(ctx context.Context, path string) (dstream.Info, error)
	LastRecord(ctx context.Context, path string, info dstream.Info) (json.RawMessage, error)
	AppendJSON(ctx context.Context, path string, body []byte) (string, error)
}

// Pusher appends one local transcript to a replica stream, complete lines at a
// time. The server is its only state: Start learns from the stream's last record
// how much of the file it already holds.
type Pusher struct {
	client Client
	path   string // the replica stream
	file   string // the local transcript
	header Header
	sent   int64 // bytes of file the stream covers
}

// NewPusher returns a Pusher that replicates file to the stream at path.
func NewPusher(client Client, path, file string, header Header) *Pusher {
	header.V, header.Kind = Version, Kind
	return &Pusher{client: client, path: path, file: file, header: header}
}

// Sent is how many bytes of the file the stream holds.
func (p *Pusher) Sent() int64 { return p.sent }

// Start creates the stream if needed and works out where to resume.
func (p *Pusher) Start(ctx context.Context) error {
	if _, err := p.client.Create(ctx, p.path, "application/json"); err != nil {
		return err
	}
	info, err := p.client.Head(ctx, p.path)
	if err != nil {
		return err
	}
	if info.Closed {
		return fmt.Errorf("replica stream %s is closed; re-run with -reset to start it over", p.path)
	}
	last, err := p.client.LastRecord(ctx, p.path, info)
	if err != nil {
		return err
	}
	if len(last) == 0 {
		body, err := json.Marshal(p.header)
		if err != nil {
			return err
		}
		_, err = p.client.AppendJSON(ctx, p.path, body)
		p.sent = 0
		return err
	}
	h, c, err := decode(last)
	if err != nil {
		return fmt.Errorf("replica stream %s: %w", p.path, err)
	}
	if h != nil {
		if h.Session != p.header.Session {
			return fmt.Errorf("replica stream %s belongs to session %s; re-run with -reset to start it over",
				p.path, h.Session)
		}
		p.sent = 0
		return nil
	}
	if !p.fileHas(*c) {
		return fmt.Errorf("replica stream %s holds different content than %s; re-run with -reset to start it over",
			p.path, p.file)
	}
	p.sent = c.Off + int64(len(c.Data))
	return nil
}

// fileHas reports whether the local file holds c's bytes at c's offset.
func (p *Pusher) fileHas(c Chunk) bool {
	f, err := os.Open(p.file)
	if err != nil {
		return false
	}
	defer f.Close()
	got := make([]byte, len(c.Data))
	if _, err := f.ReadAt(got, c.Off); err != nil {
		return false
	}
	return bytes.Equal(got, c.Data)
}

// Pass sends every complete line written since the last pass and reports how
// many bytes it sent. A trailing partial line waits for the next pass.
func (p *Pusher) Pass(ctx context.Context) (int64, error) {
	f, err := os.Open(p.file)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return 0, err
	}
	size := fi.Size()
	if size < p.sent {
		return 0, fmt.Errorf("%s shrank to %d bytes, but the replica holds %d", p.file, size, p.sent)
	}
	var total int64
	for p.sent < size {
		body, err := completeLines(f, p.sent, size)
		if err != nil {
			return total, err
		}
		if len(body) == 0 {
			break
		}
		chunks := Chunks(p.sent, body)
		for len(chunks) > 0 {
			batch := chunks[:min(len(chunks), chunksPerPost)]
			chunks = chunks[len(batch):]
			payload, err := json.Marshal(batch)
			if err != nil {
				return total, err
			}
			if _, err := p.client.AppendJSON(ctx, p.path, payload); err != nil {
				return total, err
			}
			for _, c := range batch {
				p.sent += int64(len(c.Data))
				total += int64(len(c.Data))
			}
		}
	}
	return total, nil
}

// readSize is how much of the file completeLines reads at a time.
const readSize = 8 << 20

// completeLines reads f from offset up to end and returns everything through
// the last newline, reading further when a single line is longer than readSize.
// It returns nothing while the only line left is still being written.
func completeLines(f *os.File, offset, end int64) ([]byte, error) {
	buf := make([]byte, min(end-offset, readSize))
	n, err := f.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	buf = buf[:n]
	for {
		if i := bytes.LastIndexByte(buf, '\n'); i >= 0 {
			return buf[:i+1], nil
		}
		if len(buf) < readSize {
			return nil, nil
		}
		more := make([]byte, readSize)
		n, err := f.ReadAt(more, offset+int64(len(buf)))
		if err != nil && err != io.EOF {
			return nil, err
		}
		if n == 0 {
			return nil, nil
		}
		buf = append(buf, more[:n]...)
	}
}
