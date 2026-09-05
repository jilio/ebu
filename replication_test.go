package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func replicationConfig(src, dst EventStore) ReplicatorConfig {
	return ReplicatorConfig{Source: src, Destination: dst, Checkpoints: NewMemoryStore(), ID: "app-backup", Generation: "epoch-1"}
}
func newTestReplicator(t *testing.T, cfg ReplicatorConfig, opts ...MirrorOption) *Replicator {
	t.Helper()
	r, err := NewReplicator(cfg, append([]MirrorOption{MirrorPollInterval(time.Millisecond)}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func startReplicator(t *testing.T, r *Replicator) (context.CancelFunc, <-chan error) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("replicator did not stop")
		}
	})
	return cancel, done
}
func appendReplicationEvent(t *testing.T, s EventStore, id string) Offset {
	t.Helper()
	o, err := s.Append(context.Background(), &Event{ID: id, Type: "replicated", Data: json.RawMessage(`{"value":1}`)})
	if err != nil {
		t.Fatal(err)
	}
	return o
}
func replicationPoint(t *testing.T, r *Replicator, o Offset) ReplicationPosition {
	t.Helper()
	p, err := r.Position(o)
	if err != nil {
		t.Fatal(err)
	}
	return p
}
func TestReplicatorValidation(t *testing.T) {
	src, dst := NewMemoryStore(), NewMemoryStore()
	for _, name := range []string{"source", "destination", "checkpoints", "id", "generation", "invalid ID", "invalid generation", "comparer", "nil option", "bad option", "rewind"} {
		t.Run(name, func(t *testing.T) {
			c := replicationConfig(src, dst)
			var opts []MirrorOption
			switch name {
			case "source":
				c.Source = nil
			case "destination":
				c.Destination = nil
			case "checkpoints":
				c.Checkpoints = nil
			case "id":
				c.ID = ""
			case "generation":
				c.Generation = ""
			case "invalid ID":
				c.ID = string([]byte{0xff})
			case "invalid generation":
				c.Generation = string([]byte{0xfe})
			case "comparer":
				c.Source = &mirrorScriptStore{}
			case "nil option":
				opts = []MirrorOption{nil}
			case "bad option":
				opts = []MirrorOption{MirrorPollInterval(0)}
			case "rewind":
				opts = []MirrorOption{MirrorResetOnSourceRewind()}
			}
			if _, err := NewReplicator(c, opts...); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	r := newTestReplicator(t, replicationConfig(src, dst))
	if _, ready := r.Confirmed(); ready {
		t.Fatal("confirmed before startup")
	}
	for _, o := range []Offset{OffsetNewest} {
		if _, err := r.Position(o); !errors.Is(err, ErrReplicationPosition) {
			t.Fatalf("Position(%q): %v", o, err)
		}
	}
	p := replicationPoint(t, r, OffsetOldest)
	for _, bad := range []ReplicationPosition{{ID: "other", Generation: p.Generation}, {ID: p.ID, Generation: "other"}, {ID: p.ID, Generation: p.Generation, Offset: OffsetNewest}} {
		if err := r.Wait(context.Background(), bad); !errors.Is(err, ErrReplicationPosition) {
			t.Fatalf("foreign position: %v", err)
		}
	}
	// Structurally distinct identities must not share checkpoint keys.
	c1, c2 := replicationConfig(src, dst), replicationConfig(src, dst)
	c1.ID = "a:b"
	c1.Generation = "c"
	c2.ID = "a"
	c2.Generation = "b:c"
	if newTestReplicator(t, c1).checkpointID == newTestReplicator(t, c2).checkpointID {
		t.Fatal("checkpoint identity collision")
	}
}

func TestReplicationBarrierWaitsForWholePrefix(t *testing.T) {
	src, replica := NewMemoryStore(), NewMemoryStore()
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	dst := &mirrorScriptStore{appendFn: func(ctx context.Context, e *Event) (Offset, error) {
		if e.ID == "ordinary" {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		return replica.Append(ctx, e)
	}}
	cfg := replicationConfig(src, dst)
	r := newTestReplicator(t, cfg)
	appendReplicationEvent(t, src, "ordinary")
	important := appendReplicationEvent(t, src, "important")
	target := replicationPoint(t, r, important)
	waiting := make(chan error, 1)
	waitCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { waiting <- r.Wait(waitCtx, target) }() // Waiting before Run is supported.
	startReplicator(t, r)
	lifecycleReceive(t, entered)
	if p, ok := r.Confirmed(); !ok || p.Offset != OffsetOldest {
		t.Fatalf("premature confirmed prefix: %+v, %v", p, ok)
	}
	cancel()
	if err := lifecycleReceive(t, waiting); !errors.Is(err, context.Canceled) {
		t.Fatalf("wait cancellation: %v", err)
	}
	// Canceling one waiter does not cancel copying or unrelated local writes.
	later := appendReplicationEvent(t, src, "later")
	unblock()
	if err := r.Wait(lifecycleContext(t), target); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(lifecycleContext(t), replicationPoint(t, r, later)); err != nil {
		t.Fatal(err)
	}
	got, _, err := replica.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	for i, id := range []string{"ordinary", "important", "later"} {
		if got[i].ID != id {
			t.Fatalf("prefix order: %+v", got)
		}
	}
	// Confirmation survives Run stopping and serialization of the position.
	raw, _ := json.Marshal(target)
	var restored ReplicationPosition
	if err := json.Unmarshal(raw, &restored); err != nil {
		t.Fatal(err)
	}
	if err := r.Wait(lifecycleContext(t), restored); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationCaptureIsFixedAndWaitersIndependent(t *testing.T) {
	src, replica := NewMemoryStore(), NewMemoryStore()
	dst := &mirrorScriptStore{appendFn: func(ctx context.Context, e *Event) (Offset, error) {
		if e.ID == "later" {
			<-ctx.Done()
			return "", ctx.Err()
		}
		return replica.Append(ctx, e)
	}}
	r := newTestReplicator(t, replicationConfig(src, dst))
	appendReplicationEvent(t, src, "included")
	target, err := r.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	appendReplicationEvent(t, src, "later")
	startReplicator(t, r)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := r.Wait(ctx, target); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if n := mirrorCount(t, replica); n != 1 {
		t.Fatalf("replicated %d events", n)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := r.Wait(ctx, target); !errors.Is(err, context.Canceled) {
		t.Fatalf("already canceled wait: %v", err)
	}
}

func TestReplicationAmbiguousAppendRetriesWithoutAcknowledging(t *testing.T) {
	src, replica := NewMemoryStore(), NewMemoryStore()
	var attempts atomic.Int32
	entered, release := make(chan struct{}), make(chan struct{})
	unblock := sync.OnceFunc(func() { close(release) })
	defer unblock()
	dst := &mirrorScriptStore{appendFn: func(ctx context.Context, e *Event) (Offset, error) {
		n := attempts.Add(1)
		if n == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return "", ctx.Err()
			}
		}
		o, err := replica.Append(ctx, e)
		if err != nil {
			return "", err
		}
		if n == 1 {
			return "", errors.New("acknowledgement lost after commit")
		}
		return o, nil
	}}
	r := newTestReplicator(t, replicationConfig(src, dst))
	target := replicationPoint(t, r, appendReplicationEvent(t, src, "stable-id"))
	startReplicator(t, r)
	lifecycleReceive(t, entered)
	if p, _ := r.Confirmed(); p.Offset != OffsetOldest {
		t.Fatal("ambiguous append was confirmed")
	}
	unblock()
	if err := r.Wait(lifecycleContext(t), target); err != nil {
		t.Fatal(err)
	}
	got, _, _ := replica.Read(context.Background(), OffsetOldest, 0)
	if len(got) != 2 || got[0].ID != got[1].ID {
		t.Fatalf("expected at-least-once copies with stable IDs: %+v", got)
	}
}

func TestReplicationCheckpointFailureCannotConfirmOrSkip(t *testing.T) {
	src, dst := NewMemoryStore(), NewMemoryStore()
	offsets := newMirrorLegacyOffsets()
	cfg := replicationConfig(src, dst)
	cfg.Checkpoints = offsets
	failed := make(chan struct{}, 1)
	// Seed a proven checkpoint so a later transient save is retried in place.
	r := newTestReplicator(t, cfg, MirrorOnError(func(error) {
		select {
		case failed <- struct{}{}:
		default:
		}
	}))
	first := appendReplicationEvent(t, src, "first")
	offsets.offsets[r.checkpointID] = first
	appendReplicationEvent(t, dst, "first")
	offsets.savesBeforeErr = 0
	offsets.saveErr = errors.New("checkpoint unavailable")
	second := appendReplicationEvent(t, src, "second")
	third := appendReplicationEvent(t, src, "third")
	startReplicator(t, r)
	lifecycleReceive(t, failed)
	if p, _ := r.Confirmed(); p.Offset != first {
		t.Fatalf("uncheckpointed progress: %+v", p)
	}
	if mirrorCount(t, dst) != 2 {
		t.Fatal("advanced past the uncheckpointed event")
	}
	offsets.mu.Lock()
	offsets.saveErr = nil
	offsets.mu.Unlock()
	if err := r.Wait(lifecycleContext(t), replicationPoint(t, r, third)); err != nil {
		t.Fatal(err)
	}
	if n := mirrorCount(t, dst); n != 3 {
		t.Fatalf("retry re-appended an already acknowledged event: %d", n)
	}
	if err := r.Wait(lifecycleContext(t), replicationPoint(t, r, second)); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationRestartAndGenerationIsolation(t *testing.T) {
	src, dst := NewMemoryStore(), NewMemoryStore()
	cfg := replicationConfig(src, dst)
	r := newTestReplicator(t, cfg)
	target := replicationPoint(t, r, appendReplicationEvent(t, src, "first"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	if err := r.Wait(lifecycleContext(t), target); err != nil {
		t.Fatal(err)
	}
	cancel()
	if err := lifecycleReceive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Run(context.Background()); !errors.Is(err, ErrReplicatorStarted) {
		t.Fatal(err)
	}
	if err := r.Wait(context.Background(), target); err != nil {
		t.Fatalf("confirmed prefix invalidated by stop: %v", err)
	}
	future := replicationPoint(t, r, appendReplicationEvent(t, src, "second"))
	if err := r.Wait(context.Background(), future); !errors.Is(err, ErrReplicationStopped) || !errors.Is(err, context.Canceled) {
		t.Fatalf("stopped wait: %v", err)
	}
	resumed := newTestReplicator(t, cfg)
	startReplicator(t, resumed)
	if err := resumed.Wait(lifecycleContext(t), future); err != nil {
		t.Fatal(err)
	}
	if mirrorCount(t, dst) != 2 {
		t.Fatal("persisted prefix was recopied")
	}
	// A replacement destination gets a fresh epoch/checkpoint and full replay.
	replacement := NewMemoryStore()
	cfg.Destination = replacement
	cfg.Generation = "epoch-2"
	next := newTestReplicator(t, cfg)
	if err := next.Wait(context.Background(), future); !errors.Is(err, ErrReplicationPosition) {
		t.Fatal(err)
	}
	startReplicator(t, next)
	if err := next.Wait(lifecycleContext(t), replicationPoint(t, next, future.Offset)); err != nil {
		t.Fatal(err)
	}
	if mirrorCount(t, replacement) != 2 {
		t.Fatal("new epoch skipped old checkpoint prefix")
	}
}

func TestReplicationStartupFailuresWakeWaiters(t *testing.T) {
	for _, kind := range []string{"load", "newest", "invalid", "rewind", "first save", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			src, dst := NewMemoryStore(), NewMemoryStore()
			cfg := replicationConfig(src, dst)
			offsets := newMirrorLegacyOffsets()
			cfg.Checkpoints = offsets
			realOffset := appendReplicationEvent(t, src, "one")
			r := newTestReplicator(t, cfg)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch kind {
			case "load":
				offsets.loadErr = errors.New("load failed")
			case "newest":
				offsets.offsets[r.checkpointID] = OffsetNewest
			case "invalid":
				offsets.offsets[r.checkpointID] = "bogus"
			case "rewind":
				offsets.offsets[r.checkpointID] = "00000000000000000009"
			case "first save":
				offsets.saveErr = errors.New("cannot save offset")
			case "canceled":
				cancel()
			}
			target := replicationPoint(t, r, realOffset)
			wait := make(chan error, 1)
			go func() { wait <- r.Wait(context.Background(), target) }()
			err := r.Run(ctx)
			if err == nil {
				t.Fatal("expected Run failure")
			}
			if kind == "rewind" && !errors.Is(err, ErrReplicationHistory) {
				t.Fatalf("rewind: %v", err)
			}
			if err := lifecycleReceive(t, wait); !errors.Is(err, ErrReplicationStopped) {
				t.Fatalf("wait: %v", err)
			}
		})
	}
}

func TestReplicationChunkBoundaryNeverConfirmsUnfinishedSuffix(t *testing.T) {
	for _, tailing := range []bool{false, true} {
		t.Run(fmt.Sprint(tailing), func(t *testing.T) {
			// The concrete target "02" falls inside the indivisible 00..03 chunk.
			batch := []*StoredEvent{{ID: "a", Offset: OffsetOldest}, {ID: "b", Offset: OffsetOldest}, {ID: "c", Offset: "03"}}
			read := func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				if from == OffsetNewest {
					return nil, "03", nil
				}
				if from == OffsetOldest {
					return batch, "03", nil
				}
				return nil, from, nil
			}
			compare := func(l, r Offset) (int, error) { return strings.Compare(string(l), string(r)), nil }
			var src EventStore = &mirrorComparerStore{mirrorScriptStore: mirrorScriptStore{readFn: read}, compareFn: compare}
			if tailing {
				src = &mirrorTailerComparerStore{mirrorTailerStore: mirrorTailerStore{mirrorScriptStore: mirrorScriptStore{readFn: read}, tailFn: func(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
					return func(yield func(*StoredEvent, error) bool) {
						if from == OffsetOldest {
							for _, e := range batch {
								if !yield(e, nil) {
									return
								}
							}
						}
						<-ctx.Done()
					}
				}}, compareFn: compare}
			}
			replica := NewMemoryStore()
			entered, release := make(chan struct{}), make(chan struct{})
			unblock := sync.OnceFunc(func() { close(release) })
			defer unblock()
			dst := &mirrorScriptStore{appendFn: func(ctx context.Context, e *Event) (Offset, error) {
				if e.ID == "c" {
					close(entered)
					select {
					case <-release:
					case <-ctx.Done():
						return "", ctx.Err()
					}
				}
				return replica.Append(ctx, e)
			}}
			r := newTestReplicator(t, replicationConfig(src, dst))
			startReplicator(t, r)
			lifecycleReceive(t, entered)
			if p, _ := r.Confirmed(); p.Offset != OffsetOldest {
				t.Fatalf("unfinished chunk confirmed: %+v", p)
			}
			target := replicationPoint(t, r, "02")
			unblock()
			if err := r.Wait(lifecycleContext(t), target); err != nil {
				t.Fatal(err)
			}
			if mirrorCount(t, replica) != 3 {
				t.Fatal("missing chunk suffix")
			}
		})
	}
}

func TestReplicationDoesNotAcknowledgeHistoryReadErrors(t *testing.T) {
	var fail atomic.Bool
	fail.Store(true)
	readFailed := make(chan struct{}, 1)
	src := &mirrorComparerStore{mirrorScriptStore: mirrorScriptStore{readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
		if from == OffsetNewest {
			return nil, "01", nil
		}
		if fail.Load() {
			select {
			case readFailed <- struct{}{}:
			default:
			}
			return nil, from, errors.New("required history is gone")
		}
		if from == OffsetOldest {
			return []*StoredEvent{{Offset: "01", ID: "one"}}, "01", nil
		}
		return nil, from, nil
	}}, compareFn: func(l, r Offset) (int, error) { return strings.Compare(string(l), string(r)), nil }}
	r := newTestReplicator(t, replicationConfig(src, NewMemoryStore()))
	startReplicator(t, r)
	lifecycleReceive(t, readFailed)
	if p, _ := r.Confirmed(); p.Offset != OffsetOldest {
		t.Fatal("history gap was acknowledged")
	}
	fail.Store(false)
	if err := r.Wait(lifecycleContext(t), replicationPoint(t, r, "01")); err != nil {
		t.Fatal(err)
	}
}

func TestReplicationPositionAndCaptureErrors(t *testing.T) {
	failure := errors.New("source unavailable")
	src := &mirrorComparerStore{compareFn: func(l, r Offset) (int, error) {
		if l == "invalid" {
			return 0, failure
		}
		if l == "negative" {
			return -1, nil
		}
		return strings.Compare(string(l), string(r)), nil
	}}
	r := newTestReplicator(t, replicationConfig(src, NewMemoryStore()))
	for _, o := range []Offset{"invalid", "negative"} {
		if _, err := r.Position(o); !errors.Is(err, ErrReplicationPosition) {
			t.Fatalf("invalid position: %v", err)
		}
	}
	for _, kind := range []string{"read error", "cancel during read", "historical events", "symbolic tail"} {
		t.Run(kind, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			src.readFn = func(context.Context, Offset, int) ([]*StoredEvent, Offset, error) {
				switch kind {
				case "read error":
					return nil, "", failure
				case "cancel during read":
					cancel()
					return nil, "01", nil
				case "historical events":
					return []*StoredEvent{{}}, "01", nil
				default:
					return nil, OffsetNewest, nil
				}
			}
			if _, err := r.Capture(ctx); err == nil {
				t.Fatal("Capture accepted a failed/invalid tail")
			}
		})
	}
}

func TestReplicationComparisonFailuresStopConfirmation(t *testing.T) {
	failure := errors.New("cannot order these offsets")
	for _, kind := range []string{"invalid checkpoint", "checkpoint vs tail", "wait target", "advance"} {
		t.Run(kind, func(t *testing.T) {
			var waitComparisons atomic.Int32
			src := &mirrorComparerStore{mirrorScriptStore: mirrorScriptStore{readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) {
				if from == OffsetNewest {
					return nil, "02", nil
				}
				if kind == "advance" {
					return []*StoredEvent{{ID: "one", Offset: "invalid"}}, "invalid", nil
				}
				return nil, from, nil
			}}, compareFn: func(l, r Offset) (int, error) {
				if l == "invalid" || kind == "checkpoint vs tail" && l == "01" && r == "02" || kind == "wait target" && l == OffsetOldest && r == "02" && waitComparisons.Add(1) > 1 {
					return 0, failure
				}
				return strings.Compare(string(l), string(r)), nil
			}}
			cfg := replicationConfig(src, NewMemoryStore())
			offsets := newMirrorLegacyOffsets()
			cfg.Checkpoints = offsets
			r := newTestReplicator(t, cfg)
			if kind == "invalid checkpoint" {
				offsets.offsets[r.checkpointID] = "invalid"
			}
			if kind == "checkpoint vs tail" {
				offsets.offsets[r.checkpointID] = "01"
			}
			if kind == "wait target" {
				startReplicator(t, r)
				mirrorWaitFor(t, func() bool { _, ok := r.Confirmed(); return ok })
				if err := r.Wait(lifecycleContext(t), replicationPoint(t, r, "02")); !errors.Is(err, failure) {
					t.Fatalf("Wait: %v", err)
				}
			} else {
				if err := r.Run(lifecycleContext(t)); !errors.Is(err, failure) {
					t.Fatalf("Run: %v", err)
				}
			}
		})
	}
}

func TestReplicationCancelWhileCheckpointIsUnavailable(t *testing.T) {
	src, dst := NewMemoryStore(), NewMemoryStore()
	cfg := replicationConfig(src, dst)
	offsets := newMirrorLegacyOffsets()
	cfg.Checkpoints = offsets
	failed := make(chan struct{}, 1)
	r := newTestReplicator(t, cfg, MirrorOnError(func(error) {
		select {
		case failed <- struct{}{}:
		default:
		}
	}))
	first := appendReplicationEvent(t, src, "first")
	appendReplicationEvent(t, dst, "first")
	offsets.offsets[r.checkpointID] = first
	offsets.saveErr = errors.New("checkpoint outage")
	target := replicationPoint(t, r, appendReplicationEvent(t, src, "second"))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	lifecycleReceive(t, failed)
	cancel()
	if err := lifecycleReceive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := r.Wait(context.Background(), target); !errors.Is(err, ErrReplicationStopped) {
		t.Fatalf("uncheckpointed target: %v", err)
	}
	// The second record already reached the destination. Restart may redeliver it,
	// but cannot skip it based on the failed checkpoint save.
	offsets.mu.Lock()
	offsets.saveErr = nil
	offsets.mu.Unlock()
	resumed := newTestReplicator(t, cfg)
	startReplicator(t, resumed)
	if err := resumed.Wait(lifecycleContext(t), target); err != nil {
		t.Fatal(err)
	}
	if mirrorCount(t, dst) != 3 {
		t.Fatal("expected safe redelivery after checkpoint failure")
	}
}

type replicationWaitContext struct {
	context.Context
	entered chan struct{}
	once    sync.Once
}

func (c *replicationWaitContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.entered) })
	return c.Context.Done()
}
func TestReplicationWaitDeadlineDoesNotNeedARunner(t *testing.T) {
	r := newTestReplicator(t, replicationConfig(NewMemoryStore(), NewMemoryStore()))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	observed := &replicationWaitContext{Context: ctx, entered: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- r.Wait(observed, replicationPoint(t, r, OffsetOldest)) }()
	lifecycleReceive(t, observed.entered)
	cancel()
	if err := lifecycleReceive(t, done); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestReplicationConfirmsEmptyResumeBoundaryFromTailCapableSource(t *testing.T) {
	// Tail has no way to yield a cursor-only update. A confirmed copier must use
	// Read's nextOffset even for a source that also provides a pushing iterator.
	source := &mirrorTailerComparerStore{mirrorTailerStore: mirrorTailerStore{
		mirrorScriptStore: mirrorScriptStore{readFn: func(_ context.Context, from Offset, _ int) ([]*StoredEvent, Offset, error) { return nil, "01", nil }},
		tailFn: func(ctx context.Context, _ Offset) iter.Seq2[*StoredEvent, error] {
			return func(func(*StoredEvent, error) bool) { <-ctx.Done() }
		},
	}, compareFn: func(l, r Offset) (int, error) { return strings.Compare(string(l), string(r)), nil }}
	r := newTestReplicator(t, replicationConfig(source, NewMemoryStore()))
	point, err := r.Capture(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	startReplicator(t, r)
	ctx := lifecycleContext(t)
	if err := r.Wait(ctx, point); err != nil {
		t.Fatalf("empty boundary was never confirmed: %v", err)
	}
}

func TestReplicationWaitWakesIdlePolling(t *testing.T) {
	memory := NewMemoryStore()
	idle := make(chan struct{})
	var once sync.Once
	source := &mirrorComparerStore{mirrorScriptStore: mirrorScriptStore{readFn: func(ctx context.Context, from Offset, n int) ([]*StoredEvent, Offset, error) {
		events, next, err := memory.Read(ctx, from, n)
		if from != OffsetNewest && len(events) == 0 {
			once.Do(func() { close(idle) })
		}
		return events, next, err
	}}, compareFn: memory.CompareOffsets}
	r := newTestReplicator(t, replicationConfig(source, NewMemoryStore()), MirrorPollInterval(time.Hour))
	startReplicator(t, r)
	lifecycleReceive(t, idle)
	target := replicationPoint(t, r, appendReplicationEvent(t, memory, "wake"))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := r.Wait(ctx, target); err != nil {
		t.Fatalf("waiter did not wake idle copier: %v", err)
	}
}
