package tailer

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func write(t *testing.T, path, data string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.WriteString(data); err != nil {
		t.Fatal(err)
	}
	f.Close()
}

func TestTailReadsExistingLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.jsonl")
	write(t, path, "one\ntwo\nthree\n")

	var got []string
	var nums []int
	err := Tail(context.Background(), path, Options{}, func(line []byte, n int) error {
		got = append(got, string(line))
		nums = append(nums, n)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0] != "one" || got[2] != "three" {
		t.Fatalf("got %q", got)
	}
	if nums[0] != 1 || nums[2] != 3 {
		t.Fatalf("line numbers are not 1-based: %v", nums)
	}
}

func TestTailIgnoresIncompleteTrailingLine(t *testing.T) {
	// A live session is written incrementally, so the last line is often partial.
	path := filepath.Join(t.TempDir(), "a.jsonl")
	write(t, path, "done\npartial-no-newline")

	var got []string
	if err := Tail(context.Background(), path, Options{}, func(line []byte, _ int) error {
		got = append(got, string(line))
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "done" {
		t.Fatalf("got %q, want only the complete line", got)
	}
}

func TestTailFollowsAppendsAndCompletesPartialLines(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.jsonl")
	write(t, path, "first\nsec")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var got []string
	caughtUp := make(chan int, 1)

	done := make(chan error, 1)
	go func() {
		done <- Tail(ctx, path, Options{
			Follow: true, Poll: 10 * time.Millisecond,
			CaughtUp: func(n int) { caughtUp <- n },
		}, func(line []byte, _ int) error {
			mu.Lock()
			got = append(got, string(line))
			mu.Unlock()
			return nil
		})
	}()

	select {
	case n := <-caughtUp:
		if n != 1 {
			t.Errorf("caught up reported %d lines, want 1", n)
		}
	case <-ctx.Done():
		t.Fatal("never caught up")
	}

	write(t, path, "ond\nthird\n")
	waitFor(t, ctx, &mu, &got, 3)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	want := []string{"first", "second", "third"}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
}

func TestTailFromEndSkipsHistory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.jsonl")
	write(t, path, "old-1\nold-2\n")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var got []string
	var nums []int
	done := make(chan error, 1)
	go func() {
		done <- Tail(ctx, path, Options{FromEnd: true, Follow: true, Poll: 10 * time.Millisecond},
			func(line []byte, n int) error {
				mu.Lock()
				got = append(got, string(line))
				nums = append(nums, n)
				mu.Unlock()
				return nil
			})
	}()

	time.Sleep(50 * time.Millisecond)
	write(t, path, "new-1\n")
	waitFor(t, ctx, &mu, &got, 1)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 1 || got[0] != "new-1" {
		t.Fatalf("got %q, want only the new line", got)
	}
	if nums[0] != 3 {
		t.Fatalf("line number %d, want 3 so numbering matches the file", nums[0])
	}
}

func TestTailRestartsWhenFileIsTruncated(t *testing.T) {
	path := filepath.Join(t.TempDir(), "a.jsonl")
	write(t, path, "a\nb\n")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var got []string
	done := make(chan error, 1)
	go func() {
		done <- Tail(ctx, path, Options{Follow: true, Poll: 10 * time.Millisecond},
			func(line []byte, _ int) error {
				mu.Lock()
				got = append(got, string(line))
				mu.Unlock()
				return nil
			})
	}()
	waitFor(t, ctx, &mu, &got, 2)

	if err := os.WriteFile(path, []byte("fresh\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, ctx, &mu, &got, 3)
	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	if got[len(got)-1] != "fresh" {
		t.Fatalf("got %q, want the rewritten content at the end", got)
	}
}

func TestTailWaitsForAMissingFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "later.jsonl")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var mu sync.Mutex
	var got []string
	done := make(chan error, 1)
	go func() {
		done <- Tail(ctx, path, Options{
			Follow: true, Poll: 10 * time.Millisecond, WaitForFile: 3 * time.Second,
		}, func(line []byte, _ int) error {
			mu.Lock()
			got = append(got, string(line))
			mu.Unlock()
			return nil
		})
	}()

	time.Sleep(60 * time.Millisecond)
	write(t, path, "arrived\n")
	waitFor(t, ctx, &mu, &got, 1)
	cancel()
	<-done
}

func TestTailFailsFastOnMissingFile(t *testing.T) {
	err := Tail(context.Background(), filepath.Join(t.TempDir(), "nope.jsonl"), Options{}, nil)
	if err == nil {
		t.Fatal("expected an error for a missing file")
	}
}

func waitFor(t *testing.T, ctx context.Context, mu *sync.Mutex, got *[]string, n int) {
	t.Helper()
	for {
		mu.Lock()
		have := len(*got)
		mu.Unlock()
		if have >= n {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %d lines, have %d", n, have)
		case <-time.After(10 * time.Millisecond):
		}
	}
}
