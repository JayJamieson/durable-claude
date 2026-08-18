// Package dstream is a small client for a Durable Streams server
// (https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md).
// It covers the operations this tool needs: create, append, head, delete,
// catch-up read and SSE live tail.
package dstream

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// Sentinel offsets defined by the protocol.
const (
	OffsetStart = "-1"
	OffsetNow   = "now"
)

// Client talks to one Durable Streams server.
type Client struct {
	base *url.URL
	http *http.Client

	// Idempotent producer identity, so a retried append is deduplicated by the
	// server instead of writing the batch twice.
	producerID string
	epoch      int64
	seq        atomic.Int64

	// Retries bounds how many times a failed append is retried.
	Retries int
}

// New returns a client for a server base URL such as https://yasdb.fly.dev.
func New(server, producerID string) (*Client, error) {
	if !strings.Contains(server, "://") {
		server = "https://" + server
	}
	u, err := url.Parse(strings.TrimRight(server, "/"))
	if err != nil {
		return nil, fmt.Errorf("bad server url %q: %w", server, err)
	}
	if u.Host == "" {
		return nil, fmt.Errorf("bad server url %q: no host", server)
	}
	return &Client{
		base:       u,
		http:       &http.Client{Timeout: 60 * time.Second},
		producerID: producerID,
		epoch:      time.Now().Unix(),
		Retries:    4,
	}, nil
}

// Server returns the base URL as a string.
func (c *Client) Server() string { return c.base.String() }

// URL renders the absolute URL of a stream path, with optional query values.
func (c *Client) URL(path string, q url.Values) string {
	u := *c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/" + strings.TrimLeft(path, "/")
	if len(q) > 0 {
		u.RawQuery = q.Encode()
	}
	return u.String()
}

// Info is a stream's current state, as reported by HEAD.
type Info struct {
	Exists      bool
	Closed      bool
	ContentType string
	NextOffset  string
}

// Records is the number of records the stream holds, derived from the offset
// token. Offsets are opaque in the protocol; yasdb encodes them as
// "<record>_<subrecord>", so this reports -1 when the shape is unrecognized and
// the caller should fall back to CountRecords.
func (i Info) Records() int {
	head, _, ok := strings.Cut(i.NextOffset, "_")
	if !ok {
		return -1
	}
	n, err := strconv.Atoi(head)
	if err != nil {
		return -1
	}
	return n
}

// Head reports whether a stream exists and where its tail is.
func (c *Client) Head(ctx context.Context, path string) (Info, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, c.URL(path, nil), nil)
	if err != nil {
		return Info{}, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return Info{}, err
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusNotFound, http.StatusGone:
		return Info{Exists: false}, nil
	case http.StatusOK:
		return Info{
			Exists:      true,
			Closed:      resp.Header.Get("Stream-Closed") == "true",
			ContentType: resp.Header.Get("Content-Type"),
			NextOffset:  resp.Header.Get("Stream-Next-Offset"),
		}, nil
	default:
		return Info{}, statusError("HEAD", path, resp)
	}
}

// Create makes the stream if it is absent. It reports whether the stream was
// newly created; an existing stream with a matching content type is not an error.
func (c *Client) Create(ctx context.Context, path, contentType string) (bool, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, c.URL(path, nil), nil)
	if err != nil {
		return false, err
	}
	req.Header.Set("Content-Type", contentType)
	resp, err := c.http.Do(req)
	if err != nil {
		return false, err
	}
	defer drain(resp)

	switch resp.StatusCode {
	case http.StatusCreated:
		return true, nil
	case http.StatusOK:
		return false, nil
	case http.StatusConflict:
		return false, fmt.Errorf("stream %s already exists with a different configuration "+
			"(content type or closed state); use --reset to recreate it", path)
	default:
		return false, statusError("PUT", path, resp)
	}
}

// Delete removes a stream.
func (c *Client) Delete(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, c.URL(path, nil), nil)
	if err != nil {
		return err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK ||
		resp.StatusCode == http.StatusNotFound {
		return nil
	}
	return statusError("DELETE", path, resp)
}

// CloseStream appends nothing and marks the stream complete (EOF for readers).
func (c *Client) CloseStream(ctx context.Context, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL(path, nil), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Stream-Closed", "true")
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer drain(resp)
	if resp.StatusCode == http.StatusNoContent || resp.StatusCode == http.StatusOK {
		return nil
	}
	return statusError("POST(close)", path, resp)
}

// AppendJSON posts a JSON body. A top-level array is flattened by the server
// into one record per element. It returns the stream's new tail offset.
func (c *Client) AppendJSON(ctx context.Context, path string, body []byte) (string, error) {
	seq := c.seq.Add(1) - 1
	var lastErr error
	for attempt := 0; attempt <= c.Retries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(1<<uint(attempt-1)) * 250 * time.Millisecond
			select {
			case <-ctx.Done():
				return "", ctx.Err()
			case <-time.After(delay):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL(path, nil), bytes.NewReader(body))
		if err != nil {
			return "", err
		}
		req.Header.Set("Content-Type", "application/json")
		if c.producerID != "" {
			// Same identity on every retry, so the server drops the duplicate.
			req.Header.Set("Producer-Id", c.producerID)
			req.Header.Set("Producer-Epoch", strconv.FormatInt(c.epoch, 10))
			req.Header.Set("Producer-Seq", strconv.FormatInt(seq, 10))
		}
		resp, err := c.http.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		next := resp.Header.Get("Stream-Next-Offset")
		status := resp.StatusCode
		drain(resp)

		switch {
		case status == http.StatusNoContent || status == http.StatusOK:
			return next, nil
		case status == http.StatusNotFound:
			return "", fmt.Errorf("stream %s does not exist", path)
		case status >= 500 || status == http.StatusTooManyRequests:
			lastErr = fmt.Errorf("append %s: %s", path, http.StatusText(status))
		default:
			return "", fmt.Errorf("append %s: unexpected status %d", path, status)
		}
	}
	return "", fmt.Errorf("append %s failed after %d attempts: %w", path, c.Retries+1, lastErr)
}

// ReadAll fetches a catch-up chunk from offset and returns the records it holds
// plus the next offset to read from.
func (c *Client) ReadAll(ctx context.Context, path, offset string) ([]json.RawMessage, string, bool, error) {
	q := url.Values{"offset": {offset}}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL(path, q), nil)
	if err != nil {
		return nil, "", false, err
	}
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, "", false, err
	}
	defer drain(resp)
	if resp.StatusCode != http.StatusOK {
		return nil, "", false, statusError("GET", path, resp)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if err != nil {
		return nil, "", false, err
	}
	var records []json.RawMessage
	if len(bytes.TrimSpace(body)) > 0 {
		if err := json.Unmarshal(body, &records); err != nil {
			return nil, "", false, fmt.Errorf("decode %s: %w", path, err)
		}
	}
	return records, resp.Header.Get("Stream-Next-Offset"),
		resp.Header.Get("Stream-Closed") == "true", nil
}

// CountRecords walks a stream from the start and counts its records. It is the
// protocol-portable fallback for Info.Records.
func (c *Client) CountRecords(ctx context.Context, path string) (int, error) {
	offset, total := OffsetStart, 0
	for {
		records, next, closed, err := c.ReadAll(ctx, path, offset)
		if err != nil {
			return 0, err
		}
		total += len(records)
		if next == "" || next == offset || closed {
			return total, nil
		}
		offset = next
	}
}

// OffsetAt builds an offset token addressing record index n, inferred from the
// shape of a known-good offset. Offsets are opaque in the protocol; yasdb
// encodes them as zero-padded "<record>_<subrecord>", and ok is false for any
// other encoding so callers can fall back to a full scan.
func OffsetAt(sample string, n int) (string, bool) {
	head, tail, found := strings.Cut(sample, "_")
	if !found || head == "" || tail == "" || n < 0 {
		return "", false
	}
	if _, err := strconv.Atoi(head); err != nil {
		return "", false
	}
	if _, err := strconv.Atoi(tail); err != nil {
		return "", false
	}
	return fmt.Sprintf("%0*d_%s", len(head), n, strings.Repeat("0", len(tail))), true
}

// LastRecord returns the final record of a stream, or nil when it is empty. It
// seeks straight to the tail when the server's offsets are decodable, and walks
// the stream otherwise.
func (c *Client) LastRecord(ctx context.Context, path string, info Info) (json.RawMessage, error) {
	n := info.Records()
	if n == 0 {
		return nil, nil
	}
	if n > 0 {
		if offset, ok := OffsetAt(info.NextOffset, n-1); ok {
			records, _, _, err := c.ReadAll(ctx, path, offset)
			if err == nil && len(records) > 0 {
				return records[len(records)-1], nil
			}
			if err != nil {
				return nil, err
			}
		}
	}
	var last json.RawMessage
	offset := OffsetStart
	for {
		records, next, closed, err := c.ReadAll(ctx, path, offset)
		if err != nil {
			return nil, err
		}
		if len(records) > 0 {
			last = records[len(records)-1]
		}
		if next == "" || next == offset || closed {
			return last, nil
		}
		offset = next
	}
}

// Control is the payload of an SSE control event.
type Control struct {
	StreamNextOffset string `json:"streamNextOffset"`
	StreamCursor     string `json:"streamCursor"`
	UpToDate         bool   `json:"upToDate"`
	StreamClosed     bool   `json:"streamClosed"`
}

// ErrStreamClosed is returned by Tail when the stream reaches EOF.
var ErrStreamClosed = errors.New("stream closed")

// Tail live-tails a stream over SSE from offset, invoking onRecord for each
// record. It reconnects on a dropped connection using the last control offset,
// which is what the protocol asks of clients, and returns ErrStreamClosed at EOF.
func (c *Client) Tail(ctx context.Context, path, offset string, onRecord func(json.RawMessage) error, onControl func(Control)) error {
	cursor := ""
	client := &http.Client{} // no timeout: an SSE body is long-lived
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		q := url.Values{"offset": {offset}, "live": {"sse"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.URL(path, q), nil)
		if err != nil {
			return err
		}
		req.Header.Set("Accept", "text/event-stream")
		resp, err := client.Do(req)
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(time.Second):
			}
			continue
		}
		if resp.StatusCode != http.StatusOK {
			err := statusError("GET(sse)", path, resp)
			drain(resp)
			return err
		}

		closed, err := readSSE(resp.Body, &offset, &cursor, onRecord, onControl)
		resp.Body.Close()
		if err != nil && ctx.Err() == nil && !errors.Is(err, io.EOF) {
			return err
		}
		if closed {
			return ErrStreamClosed
		}
		if ctx.Err() != nil {
			return nil
		}
		// The server hangs up every ~60s for CDN collapsing; reconnect from the
		// last control offset rather than replaying what we already saw.
	}
}

// readSSE parses one SSE response body. It advances offset/cursor as control
// events arrive and reports whether the stream reached EOF.
func readSSE(body io.Reader, offset, cursor *string, onRecord func(json.RawMessage) error, onControl func(Control)) (bool, error) {
	sc := bufio.NewScanner(body)
	sc.Buffer(make([]byte, 0, 256*1024), 64<<20)

	event := ""
	var data []string
	dispatch := func() error {
		if len(data) == 0 {
			event = ""
			return nil
		}
		payload := strings.Join(data, "")
		data = data[:0]
		name := event
		event = ""
		switch name {
		case "data":
			var records []json.RawMessage
			if err := json.Unmarshal([]byte(payload), &records); err != nil {
				// A non-array data event is a single record.
				records = []json.RawMessage{json.RawMessage(payload)}
			}
			for _, r := range records {
				if onRecord != nil {
					if err := onRecord(r); err != nil {
						return err
					}
				}
			}
		case "control":
			var ctrl Control
			if err := json.Unmarshal([]byte(payload), &ctrl); err != nil {
				return nil
			}
			if ctrl.StreamNextOffset != "" {
				*offset = ctrl.StreamNextOffset
			}
			*cursor = ctrl.StreamCursor
			if onControl != nil {
				onControl(ctrl)
			}
			if ctrl.StreamClosed {
				return errEOF
			}
		}
		return nil
	}

	for sc.Scan() {
		line := sc.Text()
		switch {
		case line == "":
			if err := dispatch(); err != nil {
				if errors.Is(err, errEOF) {
					return true, nil
				}
				return false, err
			}
		case strings.HasPrefix(line, ":"):
			// comment / keep-alive
		case strings.HasPrefix(line, "event:"):
			event = strings.TrimSpace(strings.TrimPrefix(line, "event:"))
		case strings.HasPrefix(line, "data:"):
			data = append(data, strings.TrimPrefix(strings.TrimPrefix(line, "data:"), " "))
		}
	}
	if err := dispatch(); err != nil {
		if errors.Is(err, errEOF) {
			return true, nil
		}
		return false, err
	}
	return false, sc.Err()
}

var errEOF = errors.New("sse: stream closed")

// StreamSummary is one entry of the server's admin stream listing.
type StreamSummary struct {
	Path        string `json:"path"`
	ContentType string `json:"contentType"`
	Closed      bool   `json:"closed"`
	Records     int    `json:"records"`
	IsFork      bool   `json:"isFork"`
	Readers     struct {
		LongPoll int `json:"longPoll"`
		SSE      int `json:"sse"`
	} `json:"readers"`
}

// ListStreams returns the streams whose path starts with prefix. The listing is
// ordered by path and its cursor is exclusive, so passing the prefix itself as
// the opening cursor seeks straight to the first match and paging stops as soon
// as the paths move past it — a server with thousands of unrelated streams costs
// one page, not hundreds. This endpoint is a yasdb extension, not part of the
// protocol, so a 404 here is reported as an ordinary error.
func (c *Client) ListStreams(ctx context.Context, prefix string, limit int) ([]StreamSummary, error) {
	var out []StreamSummary
	cursor := prefix
	for pages := 0; pages < 1000; pages++ {
		q := url.Values{"limit": {"200"}}
		if cursor != "" {
			q.Set("cursor", cursor)
		}
		u := *c.base
		u.Path = strings.TrimRight(u.Path, "/") + "/__admin/streams"
		u.RawQuery = q.Encode()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
		if err != nil {
			return nil, err
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode != http.StatusOK {
			err := fmt.Errorf("list streams: unexpected status %d", resp.StatusCode)
			drain(resp)
			return nil, err
		}
		var page struct {
			Streams    []StreamSummary `json:"streams"`
			NextCursor string          `json:"nextCursor"`
		}
		err = json.NewDecoder(resp.Body).Decode(&page)
		drain(resp)
		if err != nil {
			return nil, err
		}
		for _, s := range page.Streams {
			if prefix != "" && !strings.HasPrefix(s.Path, prefix) {
				return out, nil // ordered by path: nothing further can match
			}
			out = append(out, s)
			if limit > 0 && len(out) >= limit {
				return out, nil
			}
		}
		if page.NextCursor == "" || len(page.Streams) == 0 {
			return out, nil
		}
		cursor = page.NextCursor
	}
	return out, nil
}

func statusError(op, path string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
	msg := strings.TrimSpace(string(body))
	if msg != "" {
		return fmt.Errorf("%s %s: %d %s: %s", op, path, resp.StatusCode, http.StatusText(resp.StatusCode), msg)
	}
	return fmt.Errorf("%s %s: %d %s", op, path, resp.StatusCode, http.StatusText(resp.StatusCode))
}

func drain(resp *http.Response) {
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
	resp.Body.Close()
}
