// Package tailer reads a growing line-delimited file, optionally following it
// for new writes the way `tail -f` does.
package tailer

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"
)

// Handler receives one complete line. lineNo is 1-based within the file. The
// slice is only valid for the duration of the call. Returning an error stops
// the tail.
type Handler func(line []byte, lineNo int) error

// Options configures a Tail run.
type Options struct {
	FromEnd     bool          // start at the current end of file, skipping history
	Follow      bool          // keep watching for appends after reaching EOF
	Poll        time.Duration // how often to re-check for new bytes
	MaxLine     int           // drop any line longer than this (0 = 8 MiB)
	WaitForFile time.Duration // how long to wait for the file to appear (0 = fail fast)
	// CaughtUp fires the first time the reader reaches end of file, after any
	// backlog has been handed to the Handler.
	CaughtUp func(lineNo int)
}

// Tail streams path to fn. With Follow set it blocks until ctx is cancelled or
// fn returns an error; otherwise it returns at end of file.
func Tail(ctx context.Context, path string, opt Options, fn Handler) error {
	if opt.Poll <= 0 {
		opt.Poll = 250 * time.Millisecond
	}
	if opt.MaxLine <= 0 {
		opt.MaxLine = 8 << 20
	}

	f, err := open(ctx, path, opt.WaitForFile, opt.Poll)
	if err != nil {
		return err
	}
	defer func() { f.Close() }()

	var offset int64
	lineNo := 0
	if opt.FromEnd {
		// Count the existing lines so numbering stays consistent with the file,
		// then resume from the byte we stopped at rather than a stale stat size.
		lineNo, offset, err = countLines(f, opt.MaxLine)
		if err != nil {
			return err
		}
	}

	var pending []byte // bytes after the last newline, not yet a complete line
	var fingerprint []byte
	caughtUp := false
	buf := make([]byte, 64*1024)

	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		fi, err := f.Stat()
		if err != nil {
			return fmt.Errorf("stat %s: %w", path, err)
		}

		// A session file can be replaced or rewritten underneath us. Shrinking
		// is the obvious sign, but a rewrite can also land on a larger file, so
		// the head of the file is fingerprinted and re-checked as well.
		restart := fi.Size() < offset
		if !restart && fingerprint != nil {
			if changed, err := headChanged(f, fi.Size(), fingerprint); err != nil {
				return err
			} else if changed {
				restart = true
			}
		}
		if !restart && offset > 0 {
			if replaced, err := replacedAtPath(path, fi); err == nil && replaced {
				restart = true
			}
		}
		if restart {
			reopened, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("reopen %s: %w", path, err)
			}
			f.Close()
			f = reopened
			offset, lineNo, pending, fingerprint = 0, 0, nil, nil
		}

		progressed := false
		for {
			atStart := offset == 0
			n, err := f.ReadAt(buf, offset)
			if n > 0 {
				// Fingerprint the head from the very bytes being consumed. Reading
				// it back on a later pass would leave a window in which a rewrite
				// is recorded as the original.
				if atStart && fingerprint == nil {
					size := n
					if size > fingerprintSize {
						size = fingerprintSize
					}
					fingerprint = append([]byte(nil), buf[:size]...)
				}
				offset += int64(n)
				progressed = true
				chunk := buf[:n]
				for {
					i := bytes.IndexByte(chunk, '\n')
					if i < 0 {
						pending = append(pending, chunk...)
						if len(pending) > opt.MaxLine {
							pending = pending[:0] // oversized record: skip it
						}
						break
					}
					line := chunk[:i]
					chunk = chunk[i+1:]
					lineNo++
					if len(pending) > 0 {
						pending = append(pending, line...)
						line = pending
					}
					if len(line) > 0 && len(line) <= opt.MaxLine {
						if err := fn(line, lineNo); err != nil {
							return err
						}
					}
					pending = pending[:0]
				}
			}
			if err != nil {
				if errors.Is(err, io.EOF) {
					break
				}
				return fmt.Errorf("read %s: %w", path, err)
			}
		}

		_ = progressed
		if !caughtUp {
			// Reached end of file for the first time: the backlog is fully delivered.
			caughtUp = true
			if opt.CaughtUp != nil {
				opt.CaughtUp(lineNo)
			}
		}
		if !opt.Follow {
			return nil
		}

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opt.Poll):
		}
	}
}

// fingerprintSize is how much of the file head identifies its contents.
const fingerprintSize = 256

// headChanged reports whether the head of the file still matches what was read
// from it. A mismatch means the file was rewritten rather than appended to.
func headChanged(f *os.File, size int64, stored []byte) (bool, error) {
	if size < int64(len(stored)) {
		return true, nil
	}
	head := make([]byte, len(stored))
	if _, err := f.ReadAt(head, 0); err != nil && !errors.Is(err, io.EOF) {
		return false, err
	}
	return !bytes.Equal(head, stored), nil
}

// replacedAtPath reports whether the path now resolves to a different file than
// the one held open, which is what log rotation looks like.
func replacedAtPath(path string, open os.FileInfo) (bool, error) {
	current, err := os.Stat(path)
	if err != nil {
		return false, err
	}
	return !os.SameFile(current, open), nil
}

func open(ctx context.Context, path string, wait, poll time.Duration) (*os.File, error) {
	deadline := time.Now().Add(wait)
	for {
		f, err := os.Open(path)
		if err == nil {
			return f, nil
		}
		if !errors.Is(err, os.ErrNotExist) || time.Now().After(deadline) {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(poll):
		}
	}
}

// countLines walks the whole file to learn how many complete lines it holds and
// where the last one ends, so a --from end tail numbers lines like the file does.
func countLines(f *os.File, maxLine int) (lines int, offset int64, err error) {
	buf := make([]byte, 256*1024)
	var pos int64
	for {
		n, rerr := f.ReadAt(buf, pos)
		chunk := buf[:n]
		for {
			i := bytes.IndexByte(chunk, '\n')
			if i < 0 {
				break
			}
			lines++
			offset = pos + int64(n-len(chunk)) + int64(i) + 1
			chunk = chunk[i+1:]
		}
		pos += int64(n)
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				return lines, offset, nil
			}
			return lines, offset, rerr
		}
	}
}
