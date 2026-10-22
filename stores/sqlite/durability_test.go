package sqlite

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

// sqliteSynchronous maps SQLite's numeric PRAGMA synchronous result.
var sqliteSynchronous = map[Synchronous]int{SynchronousNormal: 1, SynchronousFull: 2}

func TestWithSynchronousAppliesToEveryPooledConnection(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []Option
		want Synchronous
	}{
		{"default", nil, SynchronousNormal},
		{"normal", []Option{WithSynchronous(SynchronousNormal)}, SynchronousNormal},
		{"full", []Option{WithSynchronous(SynchronousFull)}, SynchronousFull},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store, err := New(filepath.Join(t.TempDir(), "events.db"), tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })

			// Write through the pool first, then drop idle connections so the
			// held ones below include connections opened after construction
			// and after a commit.
			if _, err := store.Append(t.Context(), &eventbus.Event{Type: "t", Data: json.RawMessage(`{}`), Timestamp: time.Now()}); err != nil {
				t.Fatal(err)
			}
			// The writer pool is unbounded by default, so holding three
			// connections at once forces three distinct SQLite connections.
			store.db.SetMaxIdleConns(0)
			store.db.SetMaxIdleConns(2)
			store.db.SetMaxOpenConns(3)

			var connections []*sql.Conn
			for range 3 {
				connection, err := store.db.Conn(t.Context())
				if err != nil {
					t.Fatal(err)
				}
				t.Cleanup(func() { _ = connection.Close() })
				connections = append(connections, connection)
			}
			for i, connection := range connections {
				var got int
				if err := connection.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&got); err != nil {
					t.Fatal(err)
				}
				if got != sqliteSynchronous[tc.want] {
					t.Fatalf("connection %d: synchronous=%d, want %d (%v)", i, got, sqliteSynchronous[tc.want], tc.want)
				}
			}
		})
	}
}

func TestWithSynchronousRejectsUnknownModes(t *testing.T) {
	for _, path := range []string{":memory:", filepath.Join(t.TempDir(), "events.db")} {
		if store, err := New(path, WithSynchronous(Synchronous(7))); err == nil {
			_ = store.Close()
			t.Fatalf("New(%q) accepted an unknown synchronous mode", path)
		} else if !strings.Contains(err.Error(), "unsupported synchronous mode Synchronous(7)") {
			t.Fatalf("unexpected error: %v", err)
		}
	}
}

func TestOpenReadOnlyIgnoresSynchronous(t *testing.T) {
	path, _ := readOnlyFixture(t)
	reader, err := OpenReadOnly(path, WithSynchronous(Synchronous(7)))
	if err != nil {
		t.Fatalf("read-only stores never write and must ignore the durability mode: %v", err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	var got int
	if err := reader.db.QueryRowContext(t.Context(), "PRAGMA synchronous").Scan(&got); err != nil || got != 2 {
		t.Fatalf("read-only connection changed SQLite's default synchronous setting: %d (error %v)", got, err)
	}
}

func TestSynchronousString(t *testing.T) {
	for mode, want := range map[Synchronous]string{
		SynchronousNormal: "NORMAL",
		SynchronousFull:   "FULL",
		Synchronous(-1):   "Synchronous(-1)",
	} {
		if got := mode.String(); got != want {
			t.Errorf("%d.String() = %q, want %q", int(mode), got, want)
		}
	}
}

// durabilityChildEnv carries the database path to the child process that
// TestAcknowledgedAppendsSurviveProcessKill kills mid-write.
const durabilityChildEnv = "EBU_SQLITE_DURABILITY_CHILD_DB"

// TestDurabilityChildAppendsUntilKilled is the child half of the process-kill
// test. It appends forever with SynchronousFull, reporting each acknowledged
// sequence number on stdout only after Append returned.
func TestDurabilityChildAppendsUntilKilled(t *testing.T) {
	path := os.Getenv(durabilityChildEnv)
	if path == "" {
		t.Skip("helper process for the process-kill durability test")
	}
	store, err := New(path, WithSynchronous(SynchronousFull))
	if err != nil {
		fmt.Println("open-error", err)
		os.Exit(2)
	}
	for seq := 1; ; seq++ {
		data, _ := json.Marshal(seq)
		if _, err := store.Append(context.Background(), &eventbus.Event{ID: strconv.Itoa(seq), Type: "seq", Data: data, Timestamp: time.Now()}); err != nil {
			fmt.Println("append-error", err)
			os.Exit(2)
		}
		fmt.Println("acked", seq)
	}
}

// TestAcknowledgedAppendsSurviveProcessKill is a crash-consistency smoke
// test: it kills a SynchronousFull writer without any shutdown and checks
// that every acknowledged append is present and that the recovered log is
// the ordered, gap-free prefix of what the writer appended. It would also pass
// with SynchronousNormal, because a process kill cannot simulate power loss
// (the operating system still holds the written pages); the PRAGMA checks
// above cover the setting that extends the guarantee to power loss.
func TestAcknowledgedAppendsSurviveProcessKill(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns a child process")
	}
	path := filepath.Join(t.TempDir(), "events.db")
	cmd := exec.Command(os.Args[0], "-test.run=^TestDurabilityChildAppendsUntilKilled$", "-test.count=1")
	cmd.Env = append(os.Environ(), durabilityChildEnv+"="+path)
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	killed := false
	t.Cleanup(func() {
		if !killed {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})

	// Fail fast instead of hanging if the child stalls: killing it closes
	// stdout and ends the scan below.
	stall := time.AfterFunc(30*time.Second, func() { _ = cmd.Process.Kill() })
	defer stall.Stop()

	const killAfter = 50
	acked := 0
	scanner := bufio.NewScanner(stdout)
	for acked < killAfter && scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "open-error") || strings.HasPrefix(line, "append-error") {
			t.Fatalf("child: %s", line)
		}
		value, ok := strings.CutPrefix(line, "acked ")
		if !ok {
			continue // test-framework output, e.g. with -v in GOFLAGS
		}
		if acked, err = strconv.Atoi(value); err != nil {
			t.Fatal(err)
		}
	}
	if acked < killAfter {
		t.Fatalf("child stopped after %d acknowledged appends: %v", acked, scanner.Err())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = cmd.Wait()
	killed = true

	store, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	events, _, err := store.Read(t.Context(), eventbus.OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) < acked {
		t.Fatalf("recovered %d events, but %d appends were acknowledged", len(events), acked)
	}
	for i, event := range events {
		want := i + 1
		var seq int
		if err := json.Unmarshal(event.Data, &seq); err != nil || seq != want || event.Offset != formatOffset(int64(want)) {
			t.Fatalf("event %d: seq=%d offset=%s (error %v); the recovered log is not an ordered prefix", i, seq, event.Offset, err)
		}
	}
}
