package main

import (
	"flag"
	"fmt"
	"net/url"
	"os"
	"strings"

	"github.com/JayJamieson/durable-claude/internal/dstream"
	"github.com/JayJamieson/durable-claude/internal/session"
)

// dstreamInfo is the stream state reported by HEAD.
type dstreamInfo = dstream.Info

// defaultServer is the author's public yasdb deployment.
const defaultServer = "https://yasdb.fly.dev"

// defaultUI is where the static viewer is published.
const defaultUI = "https://jayjamieson.github.io/durable-claude/"

// commonFlags are shared by every command that has to find a session on disk
// and map it to a stream on the server.
type commonFlags struct {
	dir    string
	server string
	stream string
	prefix string
	ui     string
}

func addCommon(fs *flag.FlagSet) *commonFlags {
	c := &commonFlags{}
	fs.StringVar(&c.dir, "dir", env("DCS_DIR", session.DefaultDir()), "Claude projects directory")
	fs.StringVar(&c.server, "server", env("DCS_SERVER", defaultServer), "Durable Streams server base URL")
	fs.StringVar(&c.stream, "stream", "", "explicit stream path (overrides the derived path)")
	fs.StringVar(&c.prefix, "prefix", env("DCS_PREFIX", "/claude"), "path prefix for derived stream paths")
	fs.StringVar(&c.ui, "ui", env("DCS_UI", defaultUI), "viewer base URL")
	return c
}

// parseArgs parses flags that appear before or after the positional selector.
// Go's flag package stops at the first non-flag argument, so "dcs push latest
// -reset" would otherwise drop -reset on the floor.
func parseArgs(fs *flag.FlagSet, args []string) error {
	var flags, positional []string
	for i := 0; i < len(args); i++ {
		a := args[i]
		if a == "--" {
			positional = append(positional, args[i+1:]...)
			break
		}
		if len(a) > 1 && a[0] == '-' {
			flags = append(flags, a)
			name := strings.TrimLeft(a, "-")
			if !strings.Contains(name, "=") {
				if f := fs.Lookup(name); f != nil && !isBoolFlag(f) && i+1 < len(args) {
					i++
					flags = append(flags, args[i])
				}
			}
			continue
		}
		positional = append(positional, a)
	}
	return fs.Parse(append(flags, positional...))
}

func isBoolFlag(f *flag.Flag) bool {
	b, ok := f.Value.(interface{ IsBoolFlag() bool })
	return ok && b.IsBoolFlag()
}

// arg returns the positional selector, if any.
func arg(fs *flag.FlagSet) string {
	if fs.NArg() > 0 {
		return fs.Arg(0)
	}
	return ""
}

// resolve finds the session a selector points at, printing a helpful list when
// the selector is ambiguous.
func (c *commonFlags) resolve(selector string, fast bool) (session.Session, error) {
	s, err := session.Resolve(c.dir, selector, fast)
	var amb *session.Ambiguous
	if err != nil && asAmbiguous(err, &amb) {
		fmt.Fprintln(os.Stderr, "dcs: selector matches several sessions:")
		for _, m := range amb.Matches {
			fmt.Fprintf(os.Stderr, "  %s  %s  %s\n", m.Short(), m.Cwd, m.Label())
		}
		return session.Session{}, fmt.Errorf("be more specific (a session id prefix is always unique)")
	}
	return s, err
}

func asAmbiguous(err error, target **session.Ambiguous) bool {
	for err != nil {
		if a, ok := err.(*session.Ambiguous); ok {
			*target = a
			return true
		}
		u, ok := err.(interface{ Unwrap() error })
		if !ok {
			return false
		}
		err = u.Unwrap()
	}
	return false
}

// streamPath is where a session's events live on the server: the explicit
// -stream flag if given, otherwise <prefix>/<project>/<session-id>.
func (c *commonFlags) streamPath(s session.Session) string {
	if c.stream != "" {
		return "/" + strings.TrimLeft(c.stream, "/")
	}
	project := sanitizeSegment(s.Project)
	if project == "unnamed" && s.Cwd != "" {
		project = sanitizeSegment(s.Cwd)
	}
	prefix := strings.Trim(c.prefix, "/")
	parts := []string{}
	if prefix != "" {
		parts = append(parts, prefix)
	}
	parts = append(parts, project, sanitizeSegment(s.ID))
	return "/" + strings.Join(parts, "/")
}

// client builds a Durable Streams client whose producer identity is stable for
// a given stream path, so retried appends deduplicate on the server.
func (c *commonFlags) client(streamPath string) (*dstream.Client, error) {
	host, _ := os.Hostname()
	return dstream.New(c.server, "dcs-"+sanitizeSegment(host)+"-"+sanitizeSegment(streamPath))
}

// viewerURL renders a deep link into the static viewer for one stream.
func (c *commonFlags) viewerURL(server, streamPath, from string) string {
	if c.ui == "" {
		return ""
	}
	base := c.ui
	if !strings.Contains(base, "?") && !strings.HasSuffix(base, "/") && !strings.HasSuffix(base, ".html") {
		base += "/"
	}
	q := url.Values{
		"server": {server},
		"stream": {streamPath},
	}
	if from != "" {
		q.Set("from", from)
	}
	sep := "?"
	if strings.Contains(base, "?") {
		sep = "&"
	}
	return base + sep + q.Encode()
}
