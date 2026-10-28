package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/JayJamieson/durable-claude/internal/replica"
	"github.com/JayJamieson/durable-claude/internal/session"
	"github.com/JayJamieson/durable-claude/internal/tailer"
)

// headerSeq is the sequence number of the header record. Every event that comes
// from the transcript has a non-negative sequence: its ordinal in the file. That
// ordinal is deterministic, so a resumed push can skip exactly what the server
// already holds, no matter how the batches were split last time.
const headerSeq = -1

// pushOptions are the push flags that shape how one session is published.
type pushOptions struct {
	from        string
	follow      bool
	reset       bool
	closeAtEnd  bool
	dryRun      bool
	jsonl       bool
	norm        session.Options
	batchEvents int
	batchBytes  int
	interval    time.Duration
	poll        time.Duration
	wait        time.Duration
}

func cmdPush(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("push", flag.ExitOnError)
	common := addCommon(fs)
	var opt pushOptions
	fs.StringVar(&opt.from, "from", "auto", "where to start: auto | start | end | resume")
	fs.BoolVar(&opt.follow, "follow", true, "keep tailing the file for new writes")
	fs.BoolVar(&opt.reset, "reset", false, "delete and recreate the stream first")
	fs.BoolVar(&opt.closeAtEnd, "close", false, "close the stream (EOF) when the tail stops")
	fs.BoolVar(&opt.norm.Meta, "meta", false, "include bookkeeping records (mode changes, snapshots, queue ops)")
	fs.BoolVar(&opt.norm.Raw, "raw", false, "attach the original JSONL record to every event")
	fs.IntVar(&opt.norm.MaxText, "max-text", 16000, "truncate text fields past this many bytes (0 = unlimited)")
	fs.IntVar(&opt.norm.MaxInput, "max-input", 8000, "truncate tool inputs past this many bytes (0 = unlimited)")
	fs.IntVar(&opt.batchEvents, "batch", 200, "flush after this many buffered events")
	fs.IntVar(&opt.batchBytes, "batch-bytes", 512*1024, "flush after this many buffered bytes")
	fs.DurationVar(&opt.interval, "interval", 200*time.Millisecond, "maximum time an event waits before being flushed")
	fs.DurationVar(&opt.poll, "poll", 250*time.Millisecond, "how often to check the file for new writes")
	fs.DurationVar(&opt.wait, "wait", 0, "wait this long for the transcript file to appear")
	fs.BoolVar(&opt.dryRun, "dry-run", false, "print events instead of sending them")
	fs.BoolVar(&opt.jsonl, "jsonl", true, "also copy the raw transcript to <stream>/jsonl, for dcs save")
	all := fs.Bool("all", false, "publish every session that is created or written to while dcs runs")
	quiet := fs.Bool("quiet", false, "only print errors")
	if err := parseArgs(fs, args); err != nil {
		return err
	}

	logf := func(format string, a ...any) {
		if !*quiet {
			fmt.Fprintf(os.Stderr, format+"\n", a...)
		}
	}
	if *all {
		if arg(fs) != "" || common.stream != "" || opt.reset {
			return errors.New("-all watches every session; it takes no selector, -stream or -reset")
		}
		opt.follow = true
		return pushAll(ctx, common, opt, logf)
	}
	sess, err := selectSession(common, arg(fs))
	if err != nil {
		return err
	}
	return pushSession(ctx, common, sess, opt, logf)
}

// pushAll publishes every session that is created or grows while it runs, each
// in its own goroutine. Sessions untouched since it started stay unpublished.
func pushAll(ctx context.Context, common *commonFlags, opt pushOptions, logf func(string, ...any)) error {
	list := func() ([]session.Session, error) {
		sessions, err := session.AllSessions(common.dir, true)
		if errors.Is(err, os.ErrNotExist) {
			return nil, nil // no session has run on this machine yet
		}
		return sessions, err
	}
	sessions, err := list()
	if err != nil {
		return err
	}
	started := map[string]int64{} // transcript → its size when last started, or at startup
	for _, s := range sessions {
		started[s.File] = s.Size
	}
	logf("%s watching %s; %d existing session(s) wait until they change", dim("    all"), common.dir, len(sessions))

	running := map[string]bool{}
	done := make(chan string)
	var wg sync.WaitGroup
	defer wg.Wait()
	ticker := time.NewTicker(opt.poll)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case file := <-done:
			delete(running, file)
		case <-ticker.C:
			sessions, err := list()
			if err != nil {
				fmt.Fprintf(os.Stderr, "dcs: %v\n", err)
				continue
			}
			for _, s := range sessions {
				if size, ok := started[s.File]; running[s.File] || ok && s.Size <= size {
					continue
				}
				started[s.File] = s.Size
				running[s.File] = true
				wg.Add(1)
				go func() {
					defer wg.Done()
					short := s.Short()
					if full, err := session.Resolve(common.dir, s.File, false); err == nil {
						s = full // the listing skips titles; the stream headers want them
					}
					err := pushSession(ctx, common, s, opt, func(format string, a ...any) {
						logf("%s "+format, append([]any{short}, a...)...)
					})
					if err != nil && ctx.Err() == nil {
						fmt.Fprintf(os.Stderr, "%s %s %v; retrying when it next grows\n", short, dim("  error"), err)
					}
					select {
					case done <- s.File:
					case <-ctx.Done():
					}
				}()
			}
		}
	}
}

// pushSession publishes one session's events to its stream, replaying and then
// following the transcript as opt says.
func pushSession(ctx context.Context, common *commonFlags, sess session.Session, opt pushOptions,
	logf func(string, ...any)) error {
	logf("%s %s", bold("session"), sess.Label())
	logf("%s %s", dim("   file"), sess.File)

	if opt.dryRun {
		return dryRunPush(ctx, sess, opt.norm, opt.from == "end", opt.follow, opt.poll, opt.wait, logf)
	}

	streamPath := common.streamPath(sess)
	client, err := common.client(streamPath)
	if err != nil {
		return err
	}
	logf("%s %s", dim(" stream"), client.URL(streamPath, nil))
	replicaPath := streamPath + "/jsonl"
	if opt.jsonl {
		logf("%s %s", dim("replica"), client.URL(replicaPath, nil))
	}

	if opt.reset {
		if err := client.Delete(ctx, streamPath); err != nil {
			return err
		}
		if opt.jsonl {
			if err := client.Delete(ctx, replicaPath); err != nil {
				return err
			}
		}
		logf("%s deleted the existing stream", dim("  reset"))
	}
	if _, err := client.Create(ctx, streamPath, "application/json"); err != nil {
		return err
	}
	info, err := client.Head(ctx, streamPath)
	if err != nil {
		return err
	}
	if info.Closed {
		return fmt.Errorf("stream %s is closed; re-run with -reset to start it over", streamPath)
	}
	existing := info.Records()
	if existing < 0 { // offsets not decodable on this server: count the hard way
		if existing, err = client.CountRecords(ctx, streamPath); err != nil {
			return err
		}
	}

	// --- decide where in the file to start --------------------------------
	fromEnd := false
	resumeSeq := headerSeq // events at or below this ordinal are already sent
	switch opt.from {
	case "auto", "resume":
		if existing > 0 {
			resumeSeq, err = lastSentSeq(ctx, client, streamPath, info)
			if err != nil {
				return fmt.Errorf("work out where to resume: %w", err)
			}
			logf("%s resuming after %d record(s) on the server", dim("   mode"), existing)
		} else if opt.from == "resume" {
			logf("%s nothing on the server yet, replaying from the start", dim("   mode"))
		} else {
			logf("%s replaying from the start", dim("   mode"))
		}
	case "start":
		if existing > 0 {
			logf("%s replaying from the start on top of %d existing record(s); use -reset to avoid duplicates",
				dim("   mode"), existing)
		} else {
			logf("%s replaying from the start", dim("   mode"))
		}
	case "end":
		fromEnd = true
		logf("%s live only, skipping existing history", dim("   mode"))
	default:
		return fmt.Errorf("-from must be auto, start, end or resume (got %q)", opt.from)
	}
	if existing > 0 {
		logf("%s %d record(s) already on the server", dim(" server"), existing)
	}

	// --- batching ---------------------------------------------------------
	sent := 0
	b := &batcher{
		maxEvents: opt.batchEvents,
		maxBytes:  opt.batchBytes,
		send: func(payload []byte, count int) error {
			if _, err := client.AppendJSON(ctx, streamPath, payload); err != nil {
				return err
			}
			sent += count
			return nil
		},
	}
	stopFlusher := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		ticker := time.NewTicker(opt.interval)
		defer ticker.Stop()
		for {
			select {
			case <-stopFlusher:
				return
			case <-ctx.Done():
				return
			case <-ticker.C:
				b.Flush()
			}
		}
	}()

	// Record 0 of a stream is always a header, so a viewer knows which session
	// it is looking at before the first message arrives.
	if existing == 0 {
		if err := b.Add(headerEvent(sess)); err != nil {
			return err
		}
	}

	// A --from end tail still needs the true ordinal of the first new event, so
	// that a later resume lines up. Counting is a parse-only pass over the file.
	ord := 0
	if fromEnd {
		if ord, err = countEvents(sess.File, opt.norm); err != nil {
			return err
		}
	}

	// The replica runs beside the tail with its own client, so a failure in
	// either stops both and the caller sees it.
	tailCtx, stopTail := context.WithCancel(ctx)
	defer stopTail()
	var replicaDone chan error

	live := false
	filter := &session.Filter{}
	tailErr := tailer.Tail(tailCtx, sess.File, tailer.Options{
		FromEnd: fromEnd, Follow: opt.follow, Poll: opt.poll, WaitForFile: opt.wait,
		CaughtUp: func(int) {
			live = true
			b.Flush()
			if opt.follow {
				logf("%s caught up (%d event(s) sent); following %s", dim("   live"), sent, sess.File)
				if url := common.viewerURL(client.Server(), streamPath, "start"); url != "" {
					logf("%s %s", dim(" viewer"), url)
				}
			}
			// Started here rather than up front, because by now the file exists.
			if opt.jsonl {
				replicaDone = make(chan error, 1)
				go func() {
					err := pushReplica(tailCtx, common, sess, replicaPath, opt, logf)
					if err != nil {
						stopTail()
					}
					replicaDone <- err
				}()
			}
		},
	}, func(line []byte, lineNo int) error {
		for _, e := range session.Normalize(line, lineNo, opt.norm) {
			seq := ord
			ord++
			if seq <= resumeSeq || !filter.Keep(e) {
				continue // already sent, or nothing new to say
			}
			e.Seq = seq
			if err := b.Add(e); err != nil {
				return err
			}
		}
		if live {
			return b.Flush() // once tailing, latency beats batching
		}
		return nil
	})

	close(stopFlusher)
	wg.Wait()
	if err := b.Flush(); err != nil && tailErr == nil {
		tailErr = err
	}
	if replicaDone != nil {
		if tailErr != nil {
			stopTail()
		}
		if err := <-replicaDone; err != nil {
			return fmt.Errorf("replica: %w", err)
		}
	}
	if tailErr != nil && !errors.Is(tailErr, context.Canceled) {
		return tailErr
	}

	if opt.closeAtEnd {
		if err := client.CloseStream(ctx, streamPath); err != nil {
			return err
		}
		logf("%s stream closed (EOF)", dim(" server"))
	}
	logf("%s %d event(s) appended", dim("   done"), sent)
	if url := common.viewerURL(client.Server(), streamPath, "start"); url != "" && !live {
		logf("%s %s", dim(" viewer"), url)
	}
	return nil
}

// pushReplica keeps the stream at path a byte-exact copy of the transcript, for
// dcs save on another machine. Every start resumes from what the server holds,
// so -from applies to the viewer stream only.
func pushReplica(ctx context.Context, common *commonFlags, sess session.Session, path string,
	opt pushOptions, logf func(string, ...any)) error {
	client, err := common.client(path)
	if err != nil {
		return err
	}
	title := sess.Label()
	if title == sess.ID {
		title = ""
	}
	p := replica.NewPusher(client, path, sess.File, replica.Header{
		Session: sess.ID, Project: sess.Project, Cwd: sess.Cwd, Title: title,
	})
	if err := p.Start(ctx); err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	for first := true; ; first = false {
		if _, err := p.Pass(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		if first {
			logf("%s caught up (%s); on the host: %s", dim("replica"), humanBytes(p.Sent()), common.saveCommand(sess.ID))
		}
		if !opt.follow {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opt.poll):
		}
	}
}

// headerEvent describes the session the stream carries.
func headerEvent(s session.Session) session.Event {
	now := time.Now().UTC().Format(time.RFC3339)
	return session.Event{
		V: session.SchemaVersion, Kind: session.KindHeader, Seq: headerSeq,
		Ts: now, Text: s.Label(), Session: s.ID,
		Meta: map[string]any{
			"project": s.Project, "cwd": s.Cwd, "file": s.File,
			"gitBranch": s.GitBranch, "title": s.Title,
			"tool": "dcs " + version, "startedAt": now,
		},
	}
}

// lastSentSeq reads the stream's final record to learn which transcript event it
// came from. An unreadable or foreign tail record resumes from the header, which
// replays rather than skipping — duplicating is safer than losing messages.
func lastSentSeq(ctx context.Context, client interface {
	LastRecord(context.Context, string, dstreamInfo) (json.RawMessage, error)
}, path string, info dstreamInfo) (int, error) {
	raw, err := client.LastRecord(ctx, path, info)
	if err != nil {
		return headerSeq, err
	}
	if len(raw) == 0 {
		return headerSeq, nil
	}
	var e session.Event
	if err := json.Unmarshal(raw, &e); err != nil {
		return headerSeq, nil
	}
	return e.Seq, nil
}

// countEvents parses a transcript without emitting anything, to learn how many
// events precede the point a --from end tail starts at.
func countEvents(path string, opt session.Options) (int, error) {
	n := 0
	err := tailer.Tail(context.Background(), path, tailer.Options{}, func(line []byte, lineNo int) error {
		n += len(session.Normalize(line, lineNo, opt))
		return nil
	})
	return n, err
}

func dryRunPush(ctx context.Context, sess session.Session, opt session.Options,
	fromEnd, follow bool, poll, wait time.Duration, logf func(string, ...any)) error {
	enc := json.NewEncoder(os.Stdout)
	ord := 0
	if fromEnd {
		var err error
		if ord, err = countEvents(sess.File, opt); err != nil {
			return err
		}
	} else if err := enc.Encode(headerEvent(sess)); err != nil {
		return err
	}
	filter := &session.Filter{}
	err := tailer.Tail(ctx, sess.File, tailer.Options{
		FromEnd: fromEnd, Follow: follow, Poll: poll, WaitForFile: wait,
	}, func(line []byte, lineNo int) error {
		for _, e := range session.Normalize(line, lineNo, opt) {
			e.Seq = ord
			ord++
			if !filter.Keep(e) {
				continue
			}
			if err := enc.Encode(e); err != nil {
				return err
			}
		}
		return nil
	})
	logf("%s %d event(s)", dim("  total"), ord)
	return err
}

// selectSession resolves the selector, falling back to an interactive picker
// when one was not supplied and the terminal can ask.
func selectSession(common *commonFlags, selector string) (session.Session, error) {
	if selector == "" && isTTY(os.Stdin) && isTTY(os.Stderr) {
		sessions, err := session.AllSessions(common.dir, false)
		if err != nil {
			return session.Session{}, err
		}
		if len(sessions) == 0 {
			return session.Session{}, fmt.Errorf("no chat sessions found under %s", common.dir)
		}
		return pick(sessions)
	}
	return common.resolve(selector, false)
}

// batcher accumulates events and ships them as one JSON array, which the server
// flattens back into one record per event.
type batcher struct {
	mu        sync.Mutex
	buf       []json.RawMessage
	bytes     int
	maxEvents int
	maxBytes  int
	send      func(payload []byte, count int) error
	err       error
}

// Add buffers one event, flushing when a threshold is crossed.
func (b *batcher) Add(e session.Event) error {
	raw, err := json.Marshal(e)
	if err != nil {
		return err
	}
	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
	b.buf = append(b.buf, raw)
	b.bytes += len(raw) + 1
	full := len(b.buf) >= b.maxEvents || b.bytes >= b.maxBytes
	b.mu.Unlock()
	if full {
		return b.Flush()
	}
	return nil
}

// Flush sends whatever is buffered.
func (b *batcher) Flush() error {
	b.mu.Lock()
	if b.err != nil {
		err := b.err
		b.mu.Unlock()
		return err
	}
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return nil
	}
	batch := b.buf
	b.buf, b.bytes = nil, 0
	b.mu.Unlock()

	payload, err := json.Marshal(batch)
	if err == nil {
		err = b.send(payload, len(batch))
	}
	if err != nil {
		b.mu.Lock()
		b.err = err
		b.mu.Unlock()
	}
	return err
}
