package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/JayJamieson/durable-claude/internal/dstream"
	"github.com/JayJamieson/durable-claude/internal/session"
)

// cmdCat reads a stream back in the terminal, which is the quickest way to
// confirm a push landed without opening the viewer.
func cmdCat(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("cat", flag.ExitOnError)
	common := addCommon(fs)
	from := fs.String("from", "start", "where to read from: start | end")
	follow := fs.Bool("follow", true, "keep tailing after catching up")
	asJSON := fs.Bool("json", false, "print raw stream records, one per line")
	if err := parseArgs(fs, args); err != nil {
		return err
	}

	streamPath, client, err := targetStream(ctx, common, arg(fs))
	if err != nil {
		return err
	}
	offset := dstream.OffsetStart
	if *from == "end" {
		offset = dstream.OffsetNow
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	onRecord := func(raw json.RawMessage) error {
		if *asJSON {
			fmt.Fprintln(out, string(raw))
			return out.Flush()
		}
		var e session.Event
		if err := json.Unmarshal(raw, &e); err != nil {
			fmt.Fprintln(out, dim(string(raw)))
			return out.Flush()
		}
		renderEvent(out, e)
		return out.Flush()
	}

	if !*follow {
		for {
			records, next, closed, err := client.ReadAll(ctx, streamPath, offset)
			if err != nil {
				return err
			}
			for _, r := range records {
				if err := onRecord(r); err != nil {
					return err
				}
			}
			if closed || next == "" || next == offset {
				return nil
			}
			offset = next
		}
	}

	err = client.Tail(ctx, streamPath, offset, onRecord, nil)
	if errors.Is(err, dstream.ErrStreamClosed) {
		fmt.Fprintln(out, dim("— stream closed —"))
		return nil
	}
	return err
}

func cmdURL(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("url", flag.ExitOnError)
	common := addCommon(fs)
	from := fs.String("from", "start", "viewer start mode: start | end")
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	streamPath, client, err := targetStream(ctx, common, arg(fs))
	if err != nil {
		return err
	}
	fmt.Println(common.viewerURL(client.Server(), streamPath, *from))
	fmt.Fprintln(os.Stderr, dim("stream: "+client.URL(streamPath, nil)))
	return nil
}

func cmdRemove(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("rm", flag.ExitOnError)
	common := addCommon(fs)
	yes := fs.Bool("y", false, "do not ask for confirmation")
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	client, err := dstream.New(common.server, "")
	if err != nil {
		return err
	}
	// The raw replica goes too, and so does every project path that holds the
	// session, so removing a session leaves no copy behind.
	var targets []dstream.StreamSummary
	streamPath := ""
	if common.stream != "" {
		streamPath = "/" + strings.TrimLeft(common.stream, "/")
		for _, p := range []string{streamPath, streamPath + "/jsonl"} {
			info, err := client.Head(ctx, p)
			if err != nil {
				return err
			}
			if info.Exists {
				targets = append(targets, dstream.StreamSummary{Path: p, Records: info.Records()})
			}
		}
	} else {
		s, err := findSession(ctx, client, common, arg(fs))
		if err != nil {
			return err
		}
		streamPath, targets = s.derived, s.streams
	}
	if len(targets) == 0 {
		fmt.Fprintln(os.Stderr, "no such stream: "+streamPath)
		return nil
	}
	if !*yes {
		for _, t := range targets {
			fmt.Fprintf(os.Stderr, "  %s (%d records)\n", client.URL(t.Path, nil), t.Records)
		}
		fmt.Fprintf(os.Stderr, "Delete the stream(s) above? [y/N] ")
		line, _ := bufio.NewReader(os.Stdin).ReadString('\n')
		if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(line)), "y") {
			return errors.New("aborted")
		}
	}
	for _, t := range targets {
		if err := client.Delete(ctx, t.Path); err != nil {
			return err
		}
		fmt.Fprintln(os.Stderr, "deleted "+t.Path)
	}
	return nil
}

func cmdStreams(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("streams", flag.ExitOnError)
	common := addCommon(fs)
	all := fs.Bool("all", false, "list every stream, not just those under -prefix")
	asJSON := fs.Bool("json", false, "emit JSON")
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	client, err := dstream.New(common.server, "")
	if err != nil {
		return err
	}
	prefix := "/" + strings.Trim(common.prefix, "/")
	if *all {
		prefix = ""
	}
	streams, err := client.ListStreams(ctx, prefix, 0)
	if err != nil {
		return err
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(streams)
	}
	if len(streams) == 0 {
		fmt.Fprintf(os.Stderr, "no streams under %s on %s\n", prefix, common.server)
		return nil
	}
	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()
	for _, s := range streams {
		flags := []string{}
		if s.Closed {
			flags = append(flags, "closed")
		}
		if n := s.Readers.SSE + s.Readers.LongPoll; n > 0 {
			flags = append(flags, fmt.Sprintf("%d reader(s)", n))
		}
		note := ""
		if len(flags) > 0 {
			note = "  " + dim("("+strings.Join(flags, ", ")+")")
		}
		fmt.Fprintf(out, "%7d  %s%s\n", s.Records, s.Path, note)
	}
	return nil
}

// targetStream resolves a selector to the one viewer stream that cat and url
// read. An explicit -stream skips the on-disk lookup and the server listing.
func targetStream(ctx context.Context, common *commonFlags, selector string) (string, *dstream.Client, error) {
	client, err := dstream.New(common.server, "")
	if err != nil {
		return "", nil, err
	}
	if common.stream != "" {
		return "/" + strings.TrimLeft(common.stream, "/"), client, nil
	}
	s, err := findSession(ctx, client, common, selector)
	if err != nil {
		return "", nil, err
	}
	streamPath, err := s.viewer()
	return streamPath, client, err
}

// renderEvent prints one event in a compact transcript style.
func renderEvent(w *bufio.Writer, e session.Event) {
	switch e.Kind {
	case session.KindHeader:
		fmt.Fprintf(w, "\n%s\n", bold("── "+e.Text+" ──"))
		if e.Meta != nil {
			if cwd, ok := e.Meta["cwd"].(string); ok && cwd != "" {
				fmt.Fprintf(w, "%s\n", dim("   "+cwd))
			}
		}
	case session.KindTitle:
		fmt.Fprintf(w, "%s\n", dim("   title: "+e.Text))
	case session.KindUser:
		fmt.Fprintf(w, "\n%s %s\n", bold("›"), indent(e.Text, "  "))
	case session.KindAssistant:
		fmt.Fprintf(w, "\n%s\n", indent(e.Text, "  "))
	case session.KindThinking:
		fmt.Fprintf(w, "%s\n", dim(indent("✻ "+firstLines(e.Text, 3), "  ")))
	case session.KindToolUse:
		name := "tool"
		if e.Tool != nil && e.Tool.Name != "" {
			name = e.Tool.Name
		}
		fmt.Fprintf(w, "  %s %s\n", bold("⏺ "+name), dim(compactJSON(e.Tool)))
	case session.KindToolResult:
		tag := "  ⎿ "
		if e.Tool != nil && e.Tool.IsError {
			tag = "  ⎿ error: "
		}
		fmt.Fprintf(w, "%s\n", dim(tag+firstLines(e.Text, 2)))
	case session.KindSummary, session.KindSystem, session.KindMeta:
		fmt.Fprintf(w, "%s\n", dim("  · "+firstLines(e.Text, 1)))
	}
}

func compactJSON(t *session.Tool) string {
	if t == nil || len(t.Input) == 0 {
		return ""
	}
	s := strings.Join(strings.Fields(string(t.Input)), " ")
	return truncate(s, 100)
}

func firstLines(s string, n int) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	if len(lines) > n {
		lines = append(lines[:n], fmt.Sprintf("… +%d lines", len(lines)-n))
	}
	for i := range lines {
		lines[i] = truncate(lines[i], 120)
	}
	return strings.Join(lines, "\n    ")
}

func indent(s, pad string) string {
	return strings.ReplaceAll(strings.TrimRight(s, "\n"), "\n", "\n"+pad)
}
