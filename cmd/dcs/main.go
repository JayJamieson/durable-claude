// Command dcs streams Claude Code chat logs to a Durable Streams server so a
// browser can replay or live-tail them.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

// version is stamped by the release build; see the Makefile.
var version = "dev"

const usage = `dcs — stream Claude Code chats to a durable stream

Usage:
  dcs list [selector]        List projects and chat sessions on disk
  dcs push [selector]        Replay and/or live-tail a session to a stream
  dcs save [session-id]      Write a pushed session into ~/.claude/projects
  dcs cat  [selector]        Read a stream back in the terminal
  dcs url  [selector]        Print the viewer URL for a session
  dcs rm   [selector]        Delete a session's streams from the server
  dcs streams                List streams already on the server
  dcs service install        Run "dcs push -all" from boot (systemd, with sudo)
  dcs version

Selector (all commands):
  omitted        the most recent session, or an interactive picker for push
  latest         the most recently modified session anywhere
  <path>.jsonl   an explicit transcript file
  <session-id>   a full session UUID or a unique prefix of one
  <text>         a substring of the project path or the session title

Common flags:
  -dir     string  Claude projects directory (default ~/.claude/projects, $DCS_DIR)
  -server  string  Durable Streams server (default https://yasdb.fly.dev, $DCS_SERVER)
  -stream  string  Explicit stream path; overrides the derived one
  -prefix  string  Path prefix for derived stream paths (default /claude, $DCS_PREFIX)
  -ui      string  Viewer base URL (default $DCS_UI)

Run "dcs <command> -h" for the flags of a single command.

Examples:
  dcs list
  dcs push latest                    # replay history, then follow live writes
  dcs push -from end -follow         # only what happens from now on
  dcs push my-project -reset         # start the stream over
  dcs push -all                      # publish every session that changes from now on
  dcs cat latest -from start         # read it back without a browser
  dcs save 529128a1 -follow          # on another machine, then: claude --resume <id>
`

func main() {
	if err := run(); err != nil {
		if errors.Is(err, context.Canceled) {
			return
		}
		fmt.Fprintln(os.Stderr, "dcs: "+err.Error())
		os.Exit(1)
	}
}

func run() error {
	if len(os.Args) < 2 {
		fmt.Print(usage)
		return nil
	}
	cmd, args := os.Args[1], os.Args[2:]

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "list", "ls":
		return cmdList(ctx, args)
	case "push", "stream":
		return cmdPush(ctx, args)
	case "cat", "tail":
		return cmdCat(ctx, args)
	case "url", "open":
		return cmdURL(ctx, args)
	case "save":
		return cmdSave(ctx, args)
	case "rm", "delete":
		return cmdRemove(ctx, args)
	case "streams":
		return cmdStreams(ctx, args)
	case "service":
		return cmdService(ctx, args)
	case "version", "-v", "--version":
		fmt.Println("dcs " + version)
		return nil
	case "help", "-h", "--help":
		fmt.Print(usage)
		return nil
	default:
		fmt.Print(usage)
		return fmt.Errorf("unknown command %q", cmd)
	}
}

// env reads a variable with a fallback, so flags can default from the environment.
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// isTTY reports whether f is an interactive terminal.
func isTTY(f *os.File) bool {
	fi, err := f.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

// sanitizeSegment makes one path segment safe to embed in a stream URL.
func sanitizeSegment(s string) string {
	s = strings.Trim(s, "-/ ")
	var b strings.Builder
	lastDash := false
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_':
			b.WriteRune(r)
			lastDash = false
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r + 32)
			lastDash = false
		case r == '-':
			b.WriteRune('-')
			lastDash = false
		default:
			if !lastDash {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "unnamed"
	}
	return out
}

func humanBytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%dB", n)
	}
	div, exp := int64(unit), 0
	for v := n / unit; v >= unit && exp < 3; v /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(n)/float64(div), "KMGT"[exp])
}
