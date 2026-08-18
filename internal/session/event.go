package session

import (
	"encoding/json"
	"strings"
)

// maxLineBytes caps a single JSONL record. Claude Code writes some very large
// lines (tool results, attachment payloads); anything past this is skipped
// rather than aborting the scan.
const maxLineBytes = 32 * 1024 * 1024

// SchemaVersion is carried on every event so the viewer can evolve independently.
const SchemaVersion = 1

// Event kinds.
const (
	KindHeader     = "header"
	KindUser       = "user"
	KindAssistant  = "assistant"
	KindThinking   = "thinking"
	KindToolUse    = "tool_use"
	KindToolResult = "tool_result"
	KindTitle      = "title"
	KindSummary    = "summary"
	KindSystem     = "system"
	KindMeta       = "meta"
)

// Event is one renderable item in a chat: a message, a thinking block, a tool
// call, or a tool result. One JSONL record can produce several events, because
// an assistant message holds a list of content blocks.
type Event struct {
	V         int             `json:"v"`
	Seq       int             `json:"seq"`
	Line      int             `json:"line"`
	Kind      string          `json:"kind"`
	Ts        string          `json:"ts,omitempty"`
	Text      string          `json:"text,omitempty"`
	Truncated bool            `json:"truncated,omitempty"`
	UUID      string          `json:"uuid,omitempty"`
	Parent    string          `json:"parent,omitempty"`
	Model     string          `json:"model,omitempty"`
	Sidechain bool            `json:"sidechain,omitempty"`
	Tool      *Tool           `json:"tool,omitempty"`
	Usage     *Usage          `json:"usage,omitempty"`
	Session   string          `json:"session,omitempty"`
	Meta      map[string]any  `json:"meta,omitempty"`
	Raw       json.RawMessage `json:"raw,omitempty"`
}

// Tool carries a tool call and, on a result event, its output.
type Tool struct {
	ID      string          `json:"id,omitempty"`
	Name    string          `json:"name,omitempty"`
	Input   json.RawMessage `json:"input,omitempty"`
	IsError bool            `json:"isError,omitempty"`
}

// Usage is the token accounting for one assistant turn.
type Usage struct {
	Input       int `json:"input,omitempty"`
	Output      int `json:"output,omitempty"`
	CacheRead   int `json:"cacheRead,omitempty"`
	CacheCreate int `json:"cacheCreate,omitempty"`
	Thinking    int `json:"thinking,omitempty"`
}

// Filter drops events that would add nothing to the stream. Claude Code
// rewrites the session's ai-title record on most turns, so an unchanged title
// is published once rather than dozens of times.
type Filter struct{ lastTitle string }

// Keep reports whether an event is worth publishing, and records what it saw.
func (f *Filter) Keep(e Event) bool {
	if e.Kind != KindTitle {
		return true
	}
	if e.Text == f.lastTitle {
		return false
	}
	f.lastTitle = e.Text
	return true
}

// Options controls how much of each record survives normalization.
type Options struct {
	MaxText  int  // truncate any text field past this many bytes (0 = unlimited)
	MaxInput int  // truncate a serialized tool input past this many bytes
	Meta     bool // emit bookkeeping records (mode changes, snapshots, queue ops)
	Raw      bool // attach the untouched source record to every event
}

// DefaultOptions are tuned to keep a live tail small enough to stream comfortably.
func DefaultOptions() Options {
	return Options{MaxText: 16000, MaxInput: 8000}
}

// record mirrors the subset of a Claude Code JSONL line that we render.
type record struct {
	Type        string          `json:"type"`
	UUID        string          `json:"uuid"`
	ParentUUID  string          `json:"parentUuid"`
	Timestamp   string          `json:"timestamp"`
	SessionID   string          `json:"sessionId"`
	IsSidechain bool            `json:"isSidechain"`
	AITitle     string          `json:"aiTitle"`
	Summary     string          `json:"summary"`
	Content     json.RawMessage `json:"content"`
	Operation   string          `json:"operation"`
	Attachment  struct {
		Type string `json:"type"`
	} `json:"attachment"`
	Message struct {
		Role    string          `json:"role"`
		Model   string          `json:"model"`
		Content json.RawMessage `json:"content"`
		Usage   struct {
			InputTokens              int `json:"input_tokens"`
			OutputTokens             int `json:"output_tokens"`
			CacheReadInputTokens     int `json:"cache_read_input_tokens"`
			CacheCreationInputTokens int `json:"cache_creation_input_tokens"`
			OutputTokensDetails      struct {
				ThinkingTokens int `json:"thinking_tokens"`
			} `json:"output_tokens_details"`
		} `json:"usage"`
	} `json:"message"`
}

// block is one entry of a message's content array.
type block struct {
	Type      string          `json:"type"`
	Text      string          `json:"text"`
	Thinking  string          `json:"thinking"`
	ID        string          `json:"id"`
	Name      string          `json:"name"`
	Input     json.RawMessage `json:"input"`
	ToolUseID string          `json:"tool_use_id"`
	IsError   bool            `json:"is_error"`
	Content   json.RawMessage `json:"content"`
}

// Normalize converts one JSONL line into zero or more events. Records that
// carry no renderable content return nil unless Options.Meta is set.
func Normalize(line []byte, lineNo int, opt Options) []Event {
	line = trimSpace(line)
	if len(line) == 0 || line[0] != '{' {
		return nil
	}
	var r record
	if err := json.Unmarshal(line, &r); err != nil {
		return nil
	}

	base := Event{
		V:         SchemaVersion,
		Line:      lineNo,
		Ts:        r.Timestamp,
		UUID:      r.UUID,
		Parent:    r.ParentUUID,
		Sidechain: r.IsSidechain,
		Session:   r.SessionID,
	}
	if opt.Raw {
		base.Raw = json.RawMessage(append([]byte(nil), line...))
	}

	switch r.Type {
	case "user", "assistant":
		return messageEvents(r, base, opt)

	case "ai-title":
		if r.AITitle == "" {
			return nil
		}
		e := base
		e.Kind = KindTitle
		e.Text = r.AITitle
		return []Event{e}

	case "summary":
		if r.Summary == "" {
			return nil
		}
		e := base
		e.Kind = KindSummary
		e.Text, e.Truncated = clip(r.Summary, opt.MaxText)
		return []Event{e}

	case "system":
		var text string
		if json.Unmarshal(r.Content, &text) != nil || text == "" {
			return nil
		}
		e := base
		e.Kind = KindSystem
		e.Text, e.Truncated = clip(text, opt.MaxText)
		return []Event{e}

	default:
		if !opt.Meta {
			return nil
		}
		e := base
		e.Kind = KindMeta
		e.Meta = map[string]any{"type": r.Type}
		if r.Attachment.Type != "" {
			e.Meta["attachment"] = r.Attachment.Type
		}
		if r.Operation != "" {
			e.Meta["operation"] = r.Operation
		}
		var text string
		if len(r.Content) > 0 && json.Unmarshal(r.Content, &text) == nil {
			e.Text, e.Truncated = clip(text, opt.MaxText)
		}
		return []Event{e}
	}
}

func messageEvents(r record, base Event, opt Options) []Event {
	base.Model = r.Message.Model

	// A plain typed prompt arrives as a bare string rather than a block list.
	var text string
	if json.Unmarshal(r.Message.Content, &text) == nil {
		if strings.TrimSpace(text) == "" {
			return nil
		}
		e := base
		e.Kind = KindUser
		if r.Type == "assistant" {
			e.Kind = KindAssistant
		}
		e.Text, e.Truncated = clip(text, opt.MaxText)
		return []Event{e}
	}

	var blocks []block
	if json.Unmarshal(r.Message.Content, &blocks) != nil {
		return nil
	}

	usage := usageOf(r)
	out := make([]Event, 0, len(blocks))
	for _, b := range blocks {
		e := base
		switch b.Type {
		case "text":
			if strings.TrimSpace(b.Text) == "" {
				continue
			}
			e.Kind = KindAssistant
			if r.Type == "user" {
				e.Kind = KindUser
			}
			e.Text, e.Truncated = clip(b.Text, opt.MaxText)

		case "thinking":
			if strings.TrimSpace(b.Thinking) == "" {
				continue
			}
			e.Kind = KindThinking
			e.Text, e.Truncated = clip(b.Thinking, opt.MaxText)

		case "tool_use":
			e.Kind = KindToolUse
			e.Tool = &Tool{ID: b.ID, Name: b.Name, Input: clipJSON(b.Input, opt.MaxInput)}

		case "tool_result":
			e.Kind = KindToolResult
			e.Tool = &Tool{ID: b.ToolUseID, IsError: b.IsError}
			e.Text, e.Truncated = clip(resultText(b.Content), opt.MaxText)

		default:
			if !opt.Meta {
				continue
			}
			e.Kind = KindMeta
			e.Meta = map[string]any{"block": b.Type}
		}

		// Token accounting belongs to the turn, so only the first block carries it.
		if usage != nil && len(out) == 0 {
			e.Usage = usage
		}
		out = append(out, e)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func usageOf(r record) *Usage {
	u := r.Message.Usage
	if u.InputTokens == 0 && u.OutputTokens == 0 && u.CacheReadInputTokens == 0 && u.CacheCreationInputTokens == 0 {
		return nil
	}
	return &Usage{
		Input:       u.InputTokens,
		Output:      u.OutputTokens,
		CacheRead:   u.CacheReadInputTokens,
		CacheCreate: u.CacheCreationInputTokens,
		Thinking:    u.OutputTokensDetails.ThinkingTokens,
	}
}

// resultText flattens a tool result payload, which is either a plain string or
// a list of content blocks.
func resultText(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var s string
	if json.Unmarshal(raw, &s) == nil {
		return s
	}
	var blocks []block
	if json.Unmarshal(raw, &blocks) == nil {
		parts := make([]string, 0, len(blocks))
		for _, b := range blocks {
			switch {
			case b.Text != "":
				parts = append(parts, b.Text)
			case b.Type != "" && b.Type != "text":
				parts = append(parts, "["+b.Type+"]")
			}
		}
		return strings.Join(parts, "\n")
	}
	return string(raw)
}

// clip truncates on a rune boundary and reports whether anything was dropped.
func clip(s string, max int) (string, bool) {
	if max <= 0 || len(s) <= max {
		return s, false
	}
	cut := max
	for cut > 0 && !utf8Start(s[cut]) {
		cut--
	}
	return s[:cut], true
}

func clipJSON(raw json.RawMessage, max int) json.RawMessage {
	if max <= 0 || len(raw) <= max {
		return raw
	}
	// Keep the payload valid JSON: hand back a string describing the overflow.
	var obj map[string]json.RawMessage
	if json.Unmarshal(raw, &obj) == nil {
		for k, v := range obj {
			if len(v) > max {
				s, _ := clip(string(v), max)
				b, _ := json.Marshal(s + "… [truncated]")
				obj[k] = b
			}
		}
		if b, err := json.Marshal(obj); err == nil && len(b) <= max*2 {
			return b
		}
	}
	s, _ := clip(string(raw), max)
	b, _ := json.Marshal(s + "… [truncated]")
	return b
}

func utf8Start(b byte) bool { return b&0xC0 != 0x80 }

func trimSpace(b []byte) []byte {
	for len(b) > 0 && (b[0] == ' ' || b[0] == '\t' || b[0] == '\r' || b[0] == '\n') {
		b = b[1:]
	}
	for len(b) > 0 {
		c := b[len(b)-1]
		if c != ' ' && c != '\t' && c != '\r' && c != '\n' {
			break
		}
		b = b[:len(b)-1]
	}
	return b
}
