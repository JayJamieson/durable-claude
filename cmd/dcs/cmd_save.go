package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"strings"
	"sync"
	"unicode"

	"github.com/JayJamieson/durable-claude/internal/dstream"
	"github.com/JayJamieson/durable-claude/internal/replica"
)

// remoteSession is one replica stream on the server, as dcs push writes it at
// <prefix>/<project>/<session>/jsonl.
type remoteSession struct {
	path    string
	id      string // the path segment before /jsonl
	records int
	header  replica.Header // read only when a list is shown
}

func (r remoteSession) short() string {
	if i := strings.IndexByte(r.id, '-'); i > 0 {
		return r.id[:i]
	}
	return r.id
}

// cmdSave writes a session that dcs push replicated into the local projects
// dir, so `claude --resume <id>` finds it from any directory.
func cmdSave(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("save", flag.ExitOnError)
	common := addCommon(fs)
	follow := fs.Bool("follow", false, "keep writing as the session grows")
	if err := parseArgs(fs, args); err != nil {
		return err
	}
	client, err := dstream.New(common.server, "")
	if err != nil {
		return err
	}
	remotes, err := listRemoteSessions(ctx, client, common)
	if err != nil {
		return err
	}
	r, err := selectRemote(ctx, client, remotes, arg(fs))
	if err != nil {
		return err
	}
	return saveSession(ctx, client, common.dir, r, *follow)
}

// listRemoteSessions finds the replica streams under -prefix, or under -stream
// when one is given.
func listRemoteSessions(ctx context.Context, client *dstream.Client, common *commonFlags) ([]remoteSession, error) {
	root := common.prefix
	if common.stream != "" {
		root = common.stream
	}
	prefix := "/"
	if p := strings.Trim(root, "/"); p != "" {
		prefix += p + "/"
	}
	streams, err := client.ListStreams(ctx, prefix, 0)
	if err != nil {
		return nil, err
	}
	var out []remoteSession
	for _, s := range streams {
		if strings.HasSuffix(s.Path, "/jsonl") {
			out = append(out, remoteSession{path: s.Path, id: path.Base(path.Dir(s.Path)), records: s.Records})
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("no sessions under %s on %s; run dcs push on the machine that has them", prefix, client.Server())
	}
	return out, nil
}

// selectRemote resolves a selector against the server's sessions: an id or
// unique id prefix, else a substring of the stream path. With no selector it
// asks, or lists them when it cannot.
func selectRemote(ctx context.Context, client *dstream.Client, remotes []remoteSession, selector string) (remoteSession, error) {
	if selector == "" {
		readHeaders(ctx, client, remotes)
		if isTTY(os.Stdin) && isTTY(os.Stderr) {
			i, err := choose("Select a session to save:", len(remotes), func(i int) string {
				return describeRemote(remotes[i])
			})
			if err != nil {
				return remoteSession{}, err
			}
			return remotes[i], nil
		}
		for _, r := range remotes {
			fmt.Println(describeRemote(r))
		}
		return remoteSession{}, errors.New("pass a session id to save one")
	}

	var byID, byPath []remoteSession
	for _, r := range remotes {
		switch {
		case r.id == selector:
			return r, nil
		case strings.HasPrefix(r.id, selector):
			byID = append(byID, r)
		case strings.Contains(r.path, selector):
			byPath = append(byPath, r)
		}
	}
	for _, group := range [][]remoteSession{byID, byPath} {
		switch len(group) {
		case 0:
			continue
		case 1:
			return group[0], nil
		default:
			readHeaders(ctx, client, group)
			fmt.Fprintln(os.Stderr, "dcs: selector matches several sessions:")
			for _, r := range group {
				fmt.Fprintln(os.Stderr, "  "+describeRemote(r))
			}
			return remoteSession{}, errors.New("be more specific (a session id prefix is always unique)")
		}
	}
	return remoteSession{}, fmt.Errorf("no session on the server matches %q", selector)
}

// readHeaders fills in each session's header from record 0 of its stream.
func readHeaders(ctx context.Context, client *dstream.Client, remotes []remoteSession) {
	var wg sync.WaitGroup
	limit := make(chan struct{}, 8)
	for i := range remotes {
		wg.Add(1)
		go func() {
			defer wg.Done()
			limit <- struct{}{}
			defer func() { <-limit }()
			records, _, _, err := client.ReadAll(ctx, remotes[i].path, dstream.OffsetStart)
			if err == nil && len(records) > 0 {
				json.Unmarshal(records[0], &remotes[i].header)
			}
		}()
	}
	wg.Wait()
}

// describeRemote is one line for a picker or list. Everything in it comes from
// a server anyone can write to, so control characters are dropped.
func describeRemote(r remoteSession) string {
	where := r.header.Cwd
	if where == "" {
		where = path.Dir(path.Dir(r.path))
	}
	title := r.header.Title
	if title == "" {
		title = r.id
	}
	return bold(truncate(printable(title), 56)) + "  " +
		dim(fmt.Sprintf("%s · %s · %d records", printable(r.short()), truncate(printable(where), 40), r.records))
}

func printable(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return -1
		}
		return r
	}, s)
}

// saveSession catches the local transcript up with the replica stream and, with
// follow, keeps appending as the stream grows.
func saveSession(ctx context.Context, client *dstream.Client, dir string, r remoteSession, follow bool) error {
	a := replica.NewAssembler(dir, r.id)
	failed := func(err error) error {
		if errors.Is(err, replica.ErrDiverged) {
			return fmt.Errorf("diverged %s: local copy has changes the stream doesn't (resumed here?); left alone", r.short())
		}
		return err
	}

	var written int64
	offset := dstream.OffsetStart
	for {
		records, next, closed, err := client.ReadAll(ctx, r.path, offset)
		if err != nil {
			return err
		}
		for _, rec := range records {
			if err := a.Apply(rec); err != nil {
				return failed(err)
			}
		}
		n, err := a.Flush()
		written += n
		if err != nil {
			return failed(err)
		}
		caughtUp := closed || next == "" || next == offset
		if next != "" {
			offset = next
		}
		if caughtUp {
			break
		}
	}

	switch {
	case a.File() == "":
		fmt.Printf("empty %s: the stream has no header yet\n", r.short())
	case a.Ahead():
		fmt.Printf("ahead %s: local copy is ahead of the stream; left alone\n", r.short())
	case written > 0:
		fmt.Printf("saved %s +%s (%s)  claude --resume %s\n", r.short(), humanBytes(written), humanBytes(a.Size()), r.id)
	default:
		fmt.Printf("up to date %s (%s)  claude --resume %s\n", r.short(), humanBytes(a.Size()), r.id)
	}
	if !follow {
		return nil
	}

	// A control event ends each batch of records, which is when a complete line
	// is most likely to be pending. Tail has no way to return a Flush error, so
	// it is stashed and the tail cancelled.
	tailCtx, stop := context.WithCancel(ctx)
	defer stop()
	var flushErr error
	err := client.Tail(tailCtx, r.path, offset, a.Apply, func(dstream.Control) {
		n, err := a.Flush()
		if err != nil {
			flushErr = err
			stop()
			return
		}
		if n > 0 {
			fmt.Printf("saved %s +%s (%s)\n", r.short(), humanBytes(n), humanBytes(a.Size()))
		}
	})
	if flushErr != nil {
		err = flushErr
	}
	if err != nil {
		return failed(err)
	}
	return nil
}
