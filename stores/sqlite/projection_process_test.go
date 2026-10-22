package sqlite

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
)

func killCounterConfig(source eventbus.EventStore, applied *atomic.Int64) ProjectionConfig {
	return ProjectionConfig{Name: "kill-counter", Version: 1, Source: source, BatchSize: 7,
		Setup: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS kill_counts (k TEXT PRIMARY KEY, n INTEGER NOT NULL)`)
			return err
		},
		Apply: func(ctx context.Context, tx *sql.Tx, e *eventbus.StoredEvent) error {
			if applied != nil {
				applied.Add(1)
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO kill_counts VALUES ('all', 1) ON CONFLICT(k) DO UPDATE SET n = n + 1`)
			return err
		}}
}

// Separate SQLiteStore handles (separate pools, as separate processes would
// have) serving one projection on one file (from the pre-release review).
func TestProjectionSeparateHandlesShareOneCheckpoint(t *testing.T) {
	path := filepath.Join(t.TempDir(), "p.db")
	writer, err := New(path)
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	var applied atomic.Int64
	var projections []*Projection
	for range 3 {
		h, err := New(path)
		if err != nil {
			t.Fatal(err)
		}
		defer h.Close()
		p, err := h.OpenProjection(t.Context(), killCounterConfig(h, &applied))
		if err != nil {
			t.Fatal(err)
		}
		projections = append(projections, p)
	}
	const total = 300
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := range total {
			if _, err := writer.Append(context.Background(), &eventbus.Event{Type: "t", Data: json.RawMessage(strconv.Itoa(i)), Timestamp: time.Now().UTC()}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for _, p := range projections {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 60 {
				if _, err := p.CatchUp(context.Background()); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	for _, p := range projections {
		if _, err := p.CatchUp(t.Context()); err != nil {
			t.Fatal(err)
		}
	}
	var n int64
	if err := writer.db.QueryRow(`SELECT n FROM kill_counts WHERE k = 'all'`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != total || applied.Load() != total {
		t.Fatalf("count=%d applied=%d, want %d exactly once", n, applied.Load(), total)
	}
}

const projectionKillChildEnv = "EBU_SQLITE_PROJECTION_KILL_CHILD"

func TestProjectionKillChild(t *testing.T) {
	path := os.Getenv(projectionKillChildEnv)
	if path == "" {
		t.Skip("child")
	}
	store, err := New(path)
	if err != nil {
		os.Exit(3)
	}
	p, err := store.OpenProjection(context.Background(), killCounterConfig(store, nil))
	if err != nil {
		os.Exit(4)
	}
	for i := 0; ; i++ {
		if _, err := store.Append(context.Background(), &eventbus.Event{Type: "t", Data: json.RawMessage(strconv.Itoa(i)), Timestamp: time.Now().UTC()}); err != nil {
			os.Exit(5)
		}
		if i%3 == 0 {
			if _, err := p.CatchUp(context.Background()); err != nil {
				os.Exit(6)
			}
		}
		if i%50 == 0 {
			os.Stdout.WriteString("tick " + strconv.Itoa(i) + "\n")
		}
	}
}

// SIGKILL a child mid CatchUp; after restart the counter equals the number of
// events through the stored checkpoint (exactly once across the crash; from
// the pre-release review).
func TestProjectionExactlyOnceAcrossProcessKill(t *testing.T) {
	if testing.Short() {
		t.Skip("spawns child processes")
	}
	for round := range 3 {
		path := filepath.Join(t.TempDir(), "k.db")
		cmd := exec.Command(os.Args[0], "-test.run=^TestProjectionKillChild$", "-test.count=1")
		cmd.Env = append(os.Environ(), projectionKillChildEnv+"="+path)
		out, _ := cmd.StdoutPipe()
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		stall := time.AfterFunc(30*time.Second, func() { _ = cmd.Process.Kill() })
		sc := bufio.NewScanner(out)
		ticks := 0
		for sc.Scan() {
			if strings.HasPrefix(sc.Text(), "tick ") {
				ticks++
				if ticks >= 4+round {
					break
				}
			}
		}
		stall.Stop()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if ticks < 4+round {
			t.Fatalf("round %d: child stopped after %d ticks", round, ticks)
		}

		store, err := New(path)
		if err != nil {
			t.Fatal(err)
		}
		p, err := store.OpenProjection(t.Context(), killCounterConfig(store, nil))
		if err != nil {
			t.Fatal(err)
		}
		cp := p.Checkpoint()
		var n int64
		_ = store.db.QueryRow(`SELECT COALESCE((SELECT n FROM kill_counts WHERE k='all'),0)`).Scan(&n)
		through := int64(0)
		if cp != eventbus.OffsetOldest {
			through, _ = parseOffset(cp)
		}
		var events int64
		_ = store.db.QueryRow(`SELECT COUNT(*) FROM events WHERE position <= ?`, through).Scan(&events)
		if n != events {
			t.Fatalf("round %d: counter %d but %d events through checkpoint %q", round, n, events, cp)
		}
		if _, err := p.CatchUp(t.Context()); err != nil {
			t.Fatal(err)
		}
		var all int64
		_ = store.db.QueryRow(`SELECT n FROM kill_counts WHERE k='all'`).Scan(&n)
		_ = store.db.QueryRow(`SELECT COUNT(*) FROM events`).Scan(&all)
		if n != all {
			t.Fatalf("round %d: after catch-up counter %d, events %d", round, n, all)
		}
		t.Logf("round %d: checkpoint %s, %d events, exactly once", round, cp, all)
		_ = store.Close()
	}
}
