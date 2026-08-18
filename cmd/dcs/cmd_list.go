package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/JayJamieson/durable-claude/internal/session"
)

func cmdList(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("list", flag.ExitOnError)
	common := addCommon(fs)
	asJSON := fs.Bool("json", false, "emit JSON instead of a table")
	fast := fs.Bool("fast", false, "skip reading transcripts (no titles or message counts)")
	limit := fs.Int("n", 0, "show at most this many sessions per project (0 = all)")
	flat := fs.Bool("flat", false, "one line per session, without project grouping")
	if err := parseArgs(fs, args); err != nil {
		return err
	}

	projects, err := session.Discover(common.dir, *fast)
	if err != nil {
		return err
	}
	if len(projects) == 0 {
		return fmt.Errorf("no chat sessions found under %s", common.dir)
	}
	if sel := arg(fs); sel != "" {
		projects = filterProjects(projects, sel)
		if len(projects) == 0 {
			return fmt.Errorf("no sessions match %q", sel)
		}
	}
	if *limit > 0 {
		for i := range projects {
			if len(projects[i].Sessions) > *limit {
				projects[i].Sessions = projects[i].Sessions[:*limit]
			}
		}
	}

	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		return enc.Encode(projects)
	}

	out := bufio.NewWriter(os.Stdout)
	defer out.Flush()

	if *flat {
		for _, p := range projects {
			for _, s := range p.Sessions {
				fmt.Fprintf(out, "%-10s  %-19s  %-40s  %s\n",
					s.Short(), ago(s.Modified), truncate(p.Cwd, 40), truncate(s.Label(), 60))
			}
		}
		return nil
	}

	total := 0
	for _, p := range projects {
		fmt.Fprintf(out, "\n%s\n", bold(p.Cwd))
		fmt.Fprintf(out, "%s\n", dim("  "+p.Slug))
		for _, s := range p.Sessions {
			total++
			meta := ago(s.Modified)
			if !*fast {
				meta += fmt.Sprintf(" · %d msgs", s.Messages)
			}
			meta += " · " + humanBytes(s.Size)
			if s.GitBranch != "" {
				meta += " · " + s.GitBranch
			}
			fmt.Fprintf(out, "  %s  %s\n", bold(s.Short()), truncate(s.Label(), 68))
			fmt.Fprintf(out, "  %s  %s\n", strings.Repeat(" ", len(s.Short())), dim(meta))
		}
	}
	fmt.Fprintf(out, "\n%s\n", dim(fmt.Sprintf("%d session(s) in %d project(s) · dcs push <id> to stream one",
		total, len(projects))))
	return nil
}

func filterProjects(projects []session.Project, sel string) []session.Project {
	sel = strings.ToLower(sel)
	var out []session.Project
	for _, p := range projects {
		if strings.Contains(strings.ToLower(p.Slug), sel) || strings.Contains(strings.ToLower(p.Cwd), sel) {
			out = append(out, p)
			continue
		}
		var kept []session.Session
		for _, s := range p.Sessions {
			if strings.HasPrefix(s.ID, sel) || strings.Contains(strings.ToLower(s.Title), sel) {
				kept = append(kept, s)
			}
		}
		if len(kept) > 0 {
			p.Sessions = kept
			out = append(out, p)
		}
	}
	return out
}

// pick shows a numbered menu of sessions and returns the chosen one.
func pick(sessions []session.Session) (session.Session, error) {
	if len(sessions) == 1 {
		return sessions[0], nil
	}
	max := len(sessions)
	if max > 20 {
		max = 20
	}
	fmt.Fprintln(os.Stderr, "Select a session to stream:")
	for i := 0; i < max; i++ {
		s := sessions[i]
		fmt.Fprintf(os.Stderr, "  %2d) %s  %s\n", i+1, bold(truncate(s.Label(), 56)),
			dim(fmt.Sprintf("%s · %s · %d msgs", ago(s.Modified), truncate(s.Cwd, 34), s.Messages)))
	}
	fmt.Fprintf(os.Stderr, "\nNumber [1]: ")
	line, err := bufio.NewReader(os.Stdin).ReadString('\n')
	if err != nil && strings.TrimSpace(line) == "" {
		return session.Session{}, fmt.Errorf("no selection")
	}
	line = strings.TrimSpace(line)
	if line == "" {
		return sessions[0], nil
	}
	var n int
	if _, err := fmt.Sscanf(line, "%d", &n); err != nil || n < 1 || n > max {
		return session.Session{}, fmt.Errorf("invalid selection %q", line)
	}
	return sessions[n-1], nil
}

func ago(t time.Time) string {
	d := time.Since(t)
	switch {
	case d < time.Minute:
		return "just now"
	case d < time.Hour:
		return fmt.Sprintf("%dm ago", int(d.Minutes()))
	case d < 24*time.Hour:
		return fmt.Sprintf("%dh ago", int(d.Hours()))
	case d < 14*24*time.Hour:
		return fmt.Sprintf("%dd ago", int(d.Hours()/24))
	default:
		return t.Format("2006-01-02")
	}
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	if n < 2 {
		return string(r[:n])
	}
	return string(r[:n-1]) + "…"
}

// colour is disabled when stdout is not a terminal or NO_COLOR is set.
var useColour = isTTY(os.Stdout) && os.Getenv("NO_COLOR") == ""

func bold(s string) string {
	if !useColour {
		return s
	}
	return "\x1b[1m" + s + "\x1b[0m"
}

func dim(s string) string {
	if !useColour {
		return s
	}
	return "\x1b[2m" + s + "\x1b[0m"
}
