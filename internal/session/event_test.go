package session

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNormalizeUserPrompt(t *testing.T) {
	line := []byte(`{"type":"user","uuid":"u1","timestamp":"2026-08-18T01:21:02.305Z",
		"sessionId":"s1","message":{"role":"user","content":"hello there"}}`)
	events := Normalize(line, 4, DefaultOptions())
	if len(events) != 1 {
		t.Fatalf("got %d events, want 1", len(events))
	}
	e := events[0]
	if e.Kind != KindUser || e.Text != "hello there" {
		t.Fatalf("got kind=%q text=%q", e.Kind, e.Text)
	}
	if e.Line != 4 || e.UUID != "u1" || e.Session != "s1" {
		t.Fatalf("metadata not carried: %+v", e)
	}
	if e.V != SchemaVersion {
		t.Fatalf("got schema version %d, want %d", e.V, SchemaVersion)
	}
}

func TestNormalizeAssistantBlocks(t *testing.T) {
	line := []byte(`{"type":"assistant","uuid":"a1","timestamp":"2026-08-18T01:21:12Z",
		"message":{"role":"assistant","model":"claude-opus-5","content":[
			{"type":"thinking","thinking":"weighing it up","signature":"BIGBASE64"},
			{"type":"text","text":"Here is the plan."},
			{"type":"tool_use","id":"t1","name":"Bash","input":{"command":"ls -la"}}],
		"usage":{"input_tokens":2,"output_tokens":1076,
			"output_tokens_details":{"thinking_tokens":616}}}}`)
	events := Normalize(line, 16, DefaultOptions())
	if len(events) != 3 {
		t.Fatalf("got %d events, want 3", len(events))
	}
	if events[0].Kind != KindThinking || events[0].Text != "weighing it up" {
		t.Fatalf("thinking block: %+v", events[0])
	}
	if strings.Contains(string(mustJSON(t, events[0])), "BIGBASE64") {
		t.Error("thinking signature should not be published")
	}
	if events[1].Kind != KindAssistant || events[1].Model != "claude-opus-5" {
		t.Fatalf("text block: %+v", events[1])
	}
	if events[2].Kind != KindToolUse || events[2].Tool == nil ||
		events[2].Tool.Name != "Bash" || events[2].Tool.ID != "t1" {
		t.Fatalf("tool_use block: %+v", events[2])
	}
	// Token accounting belongs to the turn, so only the first block carries it.
	if events[0].Usage == nil || events[0].Usage.Output != 1076 || events[0].Usage.Thinking != 616 {
		t.Fatalf("usage on first block: %+v", events[0].Usage)
	}
	if events[1].Usage != nil || events[2].Usage != nil {
		t.Error("usage should appear once per message, not per block")
	}
}

func TestNormalizeToolResult(t *testing.T) {
	for name, content := range map[string]string{
		"string": `"stdout here"`,
		"blocks": `[{"type":"text","text":"stdout here"}]`,
	} {
		t.Run(name, func(t *testing.T) {
			line := []byte(`{"type":"user","message":{"role":"user","content":[
				{"type":"tool_result","tool_use_id":"t1","is_error":true,"content":` + content + `}]}}`)
			events := Normalize(line, 17, DefaultOptions())
			if len(events) != 1 {
				t.Fatalf("got %d events, want 1", len(events))
			}
			e := events[0]
			if e.Kind != KindToolResult || e.Text != "stdout here" {
				t.Fatalf("got kind=%q text=%q", e.Kind, e.Text)
			}
			if e.Tool == nil || e.Tool.ID != "t1" || !e.Tool.IsError {
				t.Fatalf("tool metadata: %+v", e.Tool)
			}
		})
	}
}

func TestNormalizeSkipsNoiseUnlessAsked(t *testing.T) {
	line := []byte(`{"type":"file-history-snapshot","messageId":"m1","snapshot":{}}`)
	if events := Normalize(line, 3, DefaultOptions()); len(events) != 0 {
		t.Fatalf("got %d events, want 0 without -meta", len(events))
	}
	opt := DefaultOptions()
	opt.Meta = true
	events := Normalize(line, 3, opt)
	if len(events) != 1 || events[0].Kind != KindMeta {
		t.Fatalf("with -meta: %+v", events)
	}
	if events[0].Meta["type"] != "file-history-snapshot" {
		t.Fatalf("meta type not carried: %+v", events[0].Meta)
	}
}

func TestNormalizeSkipsEmptyAndMalformed(t *testing.T) {
	cases := map[string][]byte{
		"blank":         []byte("   "),
		"not json":      []byte("hello"),
		"truncated":     []byte(`{"type":"user","message":{"role`),
		"empty text":    []byte(`{"type":"assistant","message":{"content":[{"type":"text","text":"  "}]}}`),
		"empty think":   []byte(`{"type":"assistant","message":{"content":[{"type":"thinking","thinking":""}]}}`),
		"empty content": []byte(`{"type":"user","message":{"role":"user","content":""}}`),
	}
	for name, line := range cases {
		if events := Normalize(line, 1, DefaultOptions()); len(events) != 0 {
			t.Errorf("%s: got %d events, want 0", name, len(events))
		}
	}
}

func TestClipTruncatesOnRuneBoundary(t *testing.T) {
	text := strings.Repeat("é", 40) // two bytes per rune
	got, truncated := clip(text, 15)
	if !truncated {
		t.Fatal("expected truncation")
	}
	if len(got) > 15 {
		t.Fatalf("got %d bytes, want <= 15", len(got))
	}
	for _, r := range got {
		if r != 'é' {
			t.Fatalf("clip split a rune: %q", got)
		}
	}
}

func TestNormalizeMarksTruncation(t *testing.T) {
	long := strings.Repeat("x", 500)
	line := []byte(`{"type":"user","message":{"role":"user","content":"` + long + `"}}`)
	opt := Options{MaxText: 100}
	events := Normalize(line, 1, opt)
	if len(events) != 1 || !events[0].Truncated || len(events[0].Text) != 100 {
		t.Fatalf("got truncated=%v len=%d", events[0].Truncated, len(events[0].Text))
	}
}

func TestClipJSONStaysValidJSON(t *testing.T) {
	long := strings.Repeat("y", 4000)
	input := json.RawMessage(`{"command":"` + long + `","timeout":5}`)
	out := clipJSON(input, 200)
	var decoded map[string]any
	if err := json.Unmarshal(out, &decoded); err != nil {
		t.Fatalf("clipped tool input is not valid JSON: %v", err)
	}
	if _, ok := decoded["command"]; !ok {
		t.Fatalf("clipped input lost its keys: %s", out)
	}
}

func TestRawOptionCarriesSourceRecord(t *testing.T) {
	line := []byte(`{"type":"user","message":{"role":"user","content":"hi"}}`)
	opt := DefaultOptions()
	opt.Raw = true
	events := Normalize(line, 1, opt)
	if len(events) != 1 || len(events[0].Raw) == 0 {
		t.Fatal("expected the source record to be attached")
	}
	var back map[string]any
	if err := json.Unmarshal(events[0].Raw, &back); err != nil {
		t.Fatalf("raw is not valid JSON: %v", err)
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}
