package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"slices"
	"strings"

	"github.com/JayJamieson/durable-claude/internal/dstream"
	"github.com/JayJamieson/durable-claude/internal/replica"
	"github.com/JayJamieson/durable-claude/internal/session"
)

// listPrefix turns a root such as -prefix or -stream into a listing prefix.
// The trailing slash keeps /claude from also matching /claude-old.
func listPrefix(root string) string {
	prefix := "/"
	if p := strings.Trim(root, "/"); p != "" {
		prefix += p + "/"
	}
	return prefix
}

// sessionID returns the session that a stream path holds. That is the last
// path segment, after the /jsonl suffix of a replica stream is removed.
func sessionID(streamPath string) string {
	return path.Base(strings.TrimSuffix(streamPath, "/jsonl"))
}

// groupBySession maps each session id to every stream that holds it. Each
// machine that pushes a session publishes it under its own project directory
// name, so one id can sit under several project paths. Streams that hold no
// session, such as /claude/notes, are left out, so no selector can name them.
func groupBySession(streams []dstream.StreamSummary) map[string][]dstream.StreamSummary {
	groups := map[string][]dstream.StreamSummary{}
	for _, s := range streams {
		if id := sessionID(s.Path); replica.IsSessionID(id) {
			groups[id] = append(groups[id], s)
		}
	}
	return groups
}

// sessionStreams is one session and the streams the server holds for it.
type sessionStreams struct {
	id      string
	derived string                  // where push publishes it from this machine; "" when it is not on disk
	streams []dstream.StreamSummary // viewer and replica streams under every project path
}

// findSession resolves a selector to a session and lists the server once for
// its streams. It finds them by id, because dcs save can put a session under a
// different project directory name than the one the pusher used.
func findSession(ctx context.Context, client *dstream.Client, common *commonFlags, selector string) (sessionStreams, error) {
	sess, err := session.Resolve(common.dir, selector, true)
	var amb *session.Ambiguous
	if asAmbiguous(err, &amb) && sameID(amb.Matches) {
		sess, err = amb.Matches[0], nil
	}
	// "", "latest" and a .jsonl path name a file on disk, never a server id.
	serverOnly := (errors.Is(err, session.ErrNotFound) || errors.Is(err, os.ErrNotExist)) &&
		selector != "" && selector != "latest" && !strings.HasSuffix(selector, ".jsonl")
	if err != nil && !serverOnly {
		return sessionStreams{}, explainAmbiguous(err)
	}

	streams, err := client.ListStreams(ctx, listPrefix(common.prefix), 0)
	if err != nil {
		return sessionStreams{}, err
	}
	groups := groupBySession(streams)
	if !serverOnly {
		return sessionStreams{id: sess.ID, derived: common.streamPath(sess), streams: groups[sess.ID]}, nil
	}
	id, err := matchID(groups, selector)
	if err != nil {
		return sessionStreams{}, err
	}
	return sessionStreams{id: id, streams: groups[id]}, nil
}

func sameID(sessions []session.Session) bool {
	for _, s := range sessions[1:] {
		if s.ID != sessions[0].ID {
			return false
		}
	}
	return true
}

// matchID returns the server session id that equals selector. Otherwise it
// returns the one id that starts with selector.
func matchID(groups map[string][]dstream.StreamSummary, selector string) (string, error) {
	if _, ok := groups[selector]; ok {
		return selector, nil
	}
	var ids []string
	for id := range groups {
		if strings.HasPrefix(id, selector) {
			ids = append(ids, id)
		}
	}
	switch len(ids) {
	case 0:
		return "", fmt.Errorf("no session on the server matches %q", selector)
	case 1:
		return ids[0], nil
	}
	slices.Sort(ids)
	return "", fmt.Errorf("%q matches several sessions on the server:\n  %s\nbe more specific",
		selector, strings.Join(ids, "\n  "))
}

// viewer is the one viewer stream that cat and url read. With no stream on
// the server yet, it is the path push will publish to.
func (s sessionStreams) viewer() (string, error) {
	var paths []string
	for _, st := range s.streams {
		if p := strings.TrimSuffix(st.Path, "/jsonl"); !slices.Contains(paths, p) {
			paths = append(paths, p)
		}
	}
	switch len(paths) {
	case 0:
		return s.derived, nil
	case 1:
		return paths[0], nil
	}
	return "", severalPaths(s.id, paths)
}

// severalPaths reports a session that the server holds under more than one
// project path. The id is the same under each, so only -stream can pick one.
func severalPaths(id string, paths []string) error {
	return fmt.Errorf("the server holds session %s under several paths:\n  %s\npass -stream <path> to pick one",
		id, strings.Join(paths, "\n  "))
}
