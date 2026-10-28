# durable-claude

Publish Claude Code chat sessions to a [Durable Streams](https://github.com/durable-streams/durable-streams/blob/main/PROTOCOL.md)
server, and watch them in a browser — replayed from the start, or live-tailed as
you work.

Two pieces:

- **`dcs`** — a Go CLI that lists the transcripts under `~/.claude/projects/`,
  normalizes one into renderable events, and appends them to a stream. It can
  replay a finished session, resume where it left off, or follow a live one the
  way `tail -f` does.
- **`docs/index.html`** — a single static page, no build step, that reads a
  stream back. It works on GitHub Pages and talks to the server directly.

Built against [yasdb](https://github.com/JayJamieson/yasdb), which is what
`https://yasdb.fly.dev` runs.

> **These streams are public.** yasdb has no authentication, and
> `GET /__admin/streams` lists every stream path on the server. Anything you
> push to a public deployment is readable by anyone who can reach it, and chat
> transcripts contain your prompts, file contents, and command output. Push to a
> server you control, or treat what you publish as public.
>
> **The raw replica is the whole transcript.** `dcs push` also writes
> `<stream>/jsonl` for `dcs save`. Unlike the viewer events, it is not
> truncated: it holds every prompt, tool output, and file content. Anyone who can
> reach the server can read it or append to it. Pass `-jsonl=false` to skip it.

## Install

```bash
go install github.com/JayJamieson/durable-claude/cmd/dcs@latest
# or, from a clone:
make build && ./dcs list
```

Go 1.25+. No dependencies outside the standard library.

## Quick start

```bash
dcs list                       # what sessions exist on this machine
dcs push latest                # replay this project's newest session, then follow it
dcs push latest -from end      # publish only what happens from now on
dcs cat latest                 # read the stream back in the terminal
```

`push` prints the stream URL and a deep link into the viewer:

```
session Yasdb Claude chat viewer
   file /home/you/.claude/projects/-home-you-code-thing/529128a1-....jsonl
 stream https://yasdb.fly.dev/claude/home-you-code-thing/529128a1-...
   mode replaying from the start
   live caught up (170 event(s) sent); following ...
 viewer https://jayjamieson.github.io/durable-claude/?from=start&server=...&stream=...
```

Leave it running in a second terminal while you work, and the page updates as
Claude Code writes.

## Selecting a session

Every command takes the same selector:

| Selector | Meaning |
| --- | --- |
| *(omitted)* | the newest session — or an interactive picker, for `push` in a terminal |
| `latest` | the most recently modified session anywhere |
| `529128a1` | a session UUID, or any unique prefix of one |
| `my-project` | a substring of the project path or the session title |
| `path/to/file.jsonl` | an explicit transcript |

A selector that matches several sessions in one project picks the newest. One
that straddles projects is rejected, and the candidates are listed.

`dcs save` matches against the sessions on the server instead: a session id, a
unique id prefix, or a substring of the stream path.

## Commands

```
dcs list [selector]     list projects and sessions      -json -fast -flat -n
dcs push [selector]     publish a session to a stream   see below
dcs save [selector]     write a pushed session locally  -follow -dir
dcs cat  [selector]     read a stream in the terminal   -from start|end -follow -json
dcs url  [selector]     print the viewer deep link      -from start|end
dcs rm   [selector]     delete a session's streams      -y
dcs streams             list streams on the server      -all -json
dcs service <action>    run push -all from boot         install | uninstall | print
```

Shared flags: `-dir` (default `~/.claude/projects`, `$DCS_DIR`), `-server`
(default `https://yasdb.fly.dev`, `$DCS_SERVER`), `-prefix` (default `/claude`,
`$DCS_PREFIX`), `-stream` (an explicit path, overriding the derived one), and
`-ui` (`$DCS_UI`).

### `dcs push`

| Flag | Default | Meaning |
| --- | --- | --- |
| `-from` | `auto` | `auto`: resume if the stream has records, else replay from the start. `start`: replay everything. `end`: publish only new writes. `resume`: always resume. |
| `-follow` | `true` | keep tailing the file after catching up |
| `-reset` | `false` | delete and recreate the stream first |
| `-close` | `false` | close the stream (EOF) when the tail stops |
| `-meta` | `false` | include bookkeeping records — mode changes, snapshots, queue operations |
| `-raw` | `false` | attach the original JSONL record to every event |
| `-max-text` | `16000` | truncate text fields past this many bytes (`0` = unlimited) |
| `-max-input` | `8000` | truncate tool inputs past this many bytes |
| `-batch` / `-batch-bytes` / `-interval` | 200 / 512 KiB / 200ms | flush thresholds; once caught up, every event is flushed immediately |
| `-poll` | `250ms` | how often to check the file for new writes |
| `-wait` | `0` | wait this long for the transcript to appear (useful before starting a session) |
| `-dry-run` | `false` | print events to stdout instead of publishing |
| `-jsonl` | `true` | also copy the raw transcript to `<stream>/jsonl`, for `dcs save` |
| `-all` | `false` | publish every session that starts or grows while `dcs` runs; implies `-follow` |

Resuming is exact rather than approximate: every published event carries the
ordinal of the transcript event it came from, so `push` reads the stream's last
record, learns that ordinal, and skips precisely what the server already holds.
Re-running `dcs push` on a session you pushed an hour ago appends only what has
happened since — no duplicates, no gaps.

## Resume a VM session on your Mac

You can run Claude Code in a VM (the guest) and resume the same session on your
Mac (the host) with `claude --resume <id>`. The session travels through the
Durable Streams server, so the guest and the host never connect to each other.

On the guest, install `dcs`, then install the boot service once:

```bash
go install github.com/JayJamieson/durable-claude/cmd/dcs@latest
sudo "$(command -v dcs)" service install
```

The service is a systemd unit, `dcs-push`, that runs `dcs push -all` as you. It
publishes every session that starts or grows after boot and leaves untouched
sessions unpublished. Put extra push flags after `--`, as in
`service install -- -prefix /vm`. `dcs service print` shows the unit, and
`sudo "$(command -v dcs)" service uninstall` removes it.

On the host:

```bash
dcs save                      # pick one of the sessions on the server
dcs save 529128a1 -follow     # copy it, then keep appending as the guest writes
claude --resume 529128a1-9fec-432e-b80a-bb7c1e7b1f46
```

`dcs save` writes `~/.claude/projects/<project>/<id>.jsonl`. It maps the guest's
working directory from the guest's home to your home, so `/home/you/code/thing`
on the guest becomes `~/code/thing` on the host. If that directory exists on the
host, `<project>` is the project directory that Claude Code uses for it. Then
`claude --continue` in that directory finds the session. If the directory does
not exist, or an older `dcs` started the stream, `<project>` is the guest's
project directory name. `claude --resume <id>` finds the session from any
directory.

Each pushed session has a second stream next to the viewer stream:

```
/claude/home-you-code-thing/529128a1-...          viewer events
/claude/home-you-code-thing/529128a1-.../jsonl    the raw transcript
```

Record 0 of `<stream>/jsonl` is a header that names the session, its project
directory, its working directory, and the guest's home directory. Every later
record is `{"off": <byte offset>, "data": "<base64>"}`
with at most 256 KiB of the file. The data is base64 because yasdb re-encodes
JSON objects, which would change the transcript's bytes. `dcs push` sends
complete lines and resumes from the stream's last record, so a restart leaves no
gap and a duplicate record is harmless.

`dcs save` only appends. It compares the bytes the host file already holds with
the stream and never rewrites them. When you resume on the host, Claude Code
appends to the host file, and the next byte the guest writes no longer matches.
`dcs save` then stops writing to that file and exits non-zero:

```
dcs: diverged 529128a1: local copy has changes the stream doesn't (resumed here?); left alone
```

If the guest has written nothing new since, `dcs save` reports that the local
copy is ahead and leaves it alone. To take the guest's copy again, move the host
file aside and run `dcs save` once more.

## Stream layout

A session lands at `<prefix>/<project>/<session-id>`, for example:

```
/claude/home-you-code-thing/529128a1-9fec-432e-b80a-bb7c1e7b1f46
```

The stream's content type is `application/json`, so a batch posted as an array
is flattened by the server into one record per event. The raw transcript for
`dcs save` lives at `<stream>/jsonl`; see
[Resume a VM session on your Mac](#resume-a-vm-session-on-your-mac).

## Event schema

Record 0 is always a `header` describing the session. Every later record is one
renderable item, since a single assistant turn holds several content blocks.

```jsonc
{
  "v": 1,                       // schema version
  "seq": 42,                    // ordinal of this event in the transcript; -1 for the header
  "line": 193,                  // source line in the .jsonl
  "kind": "tool_use",           // header|user|assistant|thinking|tool_use|tool_result|title|summary|system|meta
  "ts": "2026-08-18T01:33:33.036Z",
  "text": "...",                // message text, thinking, or tool output
  "truncated": true,            // set when -max-text or -max-input clipped the value
  "uuid": "...", "parent": "...",
  "model": "claude-opus-5",
  "usage": { "output": 1076, "thinking": 616 },   // on the first block of a turn
  "tool": { "id": "toolu_...", "name": "Bash", "input": {...}, "isError": false },
  "session": "529128a1-..."
}
```

A `tool_result` carries the `tool.id` of the call it answers, which is how the
viewer nests output under the tool call rather than listing it separately.

Some noise is dropped by default: attachments, file-history snapshots, mode
changes and queue operations (`-meta` keeps them), thinking signatures, and the
`ai-title` record Claude Code rewrites every turn — the title is published once,
and again only when it actually changes.

## The viewer

`docs/index.html` is self-contained: no dependencies, no build, no server of its
own. It takes its configuration from the query string, so links are shareable:

```
index.html?server=https://yasdb.fly.dev&stream=/claude/project/session&from=start&speed=8
```

- **from start** replays the whole stream, then switches to live tailing.
- **from end** skips the history and shows only what arrives next.
- **play** paces the replay by the events' own timestamps — `instant`, `8x`, or
  `1x` — so a session can be watched rather than dumped.
- **Browse** lists the streams on the server. The admin listing is ordered by
  path and its cursor is exclusive, so the picker seeks straight to the prefix
  instead of paging through everything else on the server.
- Thinking, tool calls, tool results and notes toggle independently.

History comes from catch-up `GET ?offset=`, following `Stream-Next-Offset` until
the server reports `Stream-Up-To-Date`. Live tailing is SSE parsed by hand
rather than `EventSource`, because the protocol asks clients to reconnect from
the last control offset and `EventSource` would replay from the original one.

### Publishing to GitHub Pages

`.github/workflows/pages.yml` deploys `docs/` on every push to `main`. Enable it
once under **Settings → Pages → Source → GitHub Actions**. The page then lives at
`https://<user>.github.io/<repo>/`.

Point the CLI at your own copy with `-ui` or `DCS_UI`:

```bash
export DCS_UI=https://yourname.github.io/durable-claude/
```

To work on it locally:

```bash
make serve-ui    # http://localhost:8080
```

The server needs permissive CORS for a cross-origin page to read it; yasdb
already sends `access-control-allow-origin: *` and exposes its `Stream-*`
headers.

## Development

```bash
make build       # ./dcs
make test        # go test ./...
make vet fmt
```

Tests cover the normalizer, the file tailer (partial lines, truncation,
rotation, `-from end` numbering), the stream client (retries, resume, SSE
framing) against an in-process fake server, and the replica round trip (resume,
split lines, gaps, divergence, hostile headers) against an in-memory stream.
Nothing in the suite needs network access.
