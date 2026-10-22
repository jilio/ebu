package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"
)

var retentionEpoch = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

// appendRetentionEvents appends n events stamped one minute apart from
// retentionEpoch, partitioned by the "p" metadata key using partitionOf.
func appendRetentionEvents(t *testing.T, store *MemoryStore, n int, partitionOf func(int) string) []Offset {
	t.Helper()
	offsets := make([]Offset, n)
	for i := range n {
		event := &Event{
			ID:        fmt.Sprintf("e%d", i+1),
			Type:      "retention",
			Data:      json.RawMessage(fmt.Sprint(i + 1)),
			Timestamp: retentionEpoch.Add(time.Duration(i) * time.Minute),
		}
		if partitionOf != nil {
			if p := partitionOf(i); p != "" {
				event.Metadata = map[string]string{"p": p}
			}
		}
		offset, err := store.Append(context.Background(), event)
		if err != nil {
			t.Fatal(err)
		}
		offsets[i] = offset
	}
	return offsets
}

func retainedIDs(t *testing.T, store *MemoryStore) string {
	t.Helper()
	events, _, err := store.Read(context.Background(), OffsetOldest, 0)
	if err != nil {
		t.Fatal(err)
	}
	ids := make([]string, len(events))
	for i, event := range events {
		ids[i] = event.ID
	}
	return strings.Join(ids, ",")
}

func TestOffsetTrimmedError(t *testing.T) {
	plain := &OffsetTrimmedError{Offset: "5", ResumeFrom: "9"}
	if !errors.Is(plain, ErrOffsetTrimmed) {
		t.Fatal("errors.Is(ErrOffsetTrimmed) = false")
	}
	if got := plain.Error(); got != `eventbus: offset "5" precedes the retained log (resume from "9")` {
		t.Fatalf("Error() = %q", got)
	}
	cause := errors.New("410 gone")
	wrapped := fmt.Errorf("read: %w", &OffsetTrimmedError{Offset: "5", Err: cause})
	var trimmed *OffsetTrimmedError
	if !errors.Is(wrapped, ErrOffsetTrimmed) || !errors.Is(wrapped, cause) || !errors.As(wrapped, &trimmed) {
		t.Fatalf("wrapped error does not match: %v", wrapped)
	}
	if !strings.HasSuffix(trimmed.Error(), ": 410 gone") || trimmed.ResumeFrom != OffsetOldest {
		t.Fatalf("Error() = %q, ResumeFrom = %q", trimmed.Error(), trimmed.ResumeFrom)
	}
}

func TestRetentionPolicyValidate(t *testing.T) {
	for _, tc := range []struct {
		policy RetentionPolicy
		want   string
	}{
		{RetentionPolicy{}, ""},
		{RetentionPolicy{Through: "x", OlderThan: retentionEpoch, MaxEvents: 3, PartitionKey: "p", MaxPerPartition: 2}, ""},
		{RetentionPolicy{PartitionKey: "p"}, ""},
		{RetentionPolicy{Through: OffsetNewest}, "symbolic"},
		{RetentionPolicy{MaxEvents: -1}, "MaxEvents must not be negative"},
		{RetentionPolicy{MaxPerPartition: -1, PartitionKey: "p"}, "MaxPerPartition must not be negative"},
		{RetentionPolicy{MaxPerPartition: 1}, "requires PartitionKey"},
	} {
		err := tc.policy.Validate()
		if (tc.want == "") != (err == nil) || (err != nil && !strings.Contains(err.Error(), tc.want)) {
			t.Errorf("Validate(%+v) = %v, want %q", tc.policy, err, tc.want)
		}
	}
}

func TestMemoryStoreTrimPrefixRules(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		policy  func(offsets []Offset) RetentionPolicy
		deleted int64
		through int // 1-based ID of the newest removed event; 0 = none
	}{
		{"zero policy", func([]Offset) RetentionPolicy { return RetentionPolicy{} }, 0, 0},
		{"through an event", func(o []Offset) RetentionPolicy { return RetentionPolicy{Through: o[2]} }, 3, 3},
		{"through beyond the tail", func([]Offset) RetentionPolicy { return RetentionPolicy{Through: "99999999999999999999"} }, 6, 6},
		{"older than", func([]Offset) RetentionPolicy {
			return RetentionPolicy{OlderThan: retentionEpoch.Add(2*time.Minute + time.Second)}
		}, 3, 3},
		{"older than everything", func([]Offset) RetentionPolicy { return RetentionPolicy{OlderThan: retentionEpoch.Add(time.Hour)} }, 6, 6},
		{"max events", func([]Offset) RetentionPolicy { return RetentionPolicy{MaxEvents: 2} }, 4, 4},
		{"max events above size", func([]Offset) RetentionPolicy { return RetentionPolicy{MaxEvents: 10} }, 0, 0},
		{"strictest rule wins", func(o []Offset) RetentionPolicy {
			return RetentionPolicy{Through: o[0], OlderThan: retentionEpoch.Add(90 * time.Second), MaxEvents: 5}
		}, 2, 2},
	} {
		t.Run(tc.name, func(t *testing.T) {
			store := NewMemoryStore()
			offsets := appendRetentionEvents(t, store, 6, nil)
			result, err := store.Trim(ctx, tc.policy(offsets))
			if err != nil {
				t.Fatal(err)
			}
			wantThrough := OffsetOldest
			if tc.through > 0 {
				wantThrough = offsets[tc.through-1]
			}
			if result.Deleted != tc.deleted || result.TrimmedThrough != wantThrough {
				t.Fatalf("Trim = %+v, want deleted %d through %q", result, tc.deleted, wantThrough)
			}
			var want []string
			for i := tc.through + 1; i <= 6; i++ {
				want = append(want, fmt.Sprintf("e%d", i))
			}
			if got := retainedIDs(t, store); got != strings.Join(want, ",") {
				t.Fatalf("retained %q, want %q", got, strings.Join(want, ","))
			}
		})
	}
}

func TestMemoryStoreTrimAgeStopsAtFirstNewerEvent(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for i, ts := range []time.Time{{}, retentionEpoch, retentionEpoch.Add(time.Hour), retentionEpoch} {
		if _, err := store.Append(ctx, &Event{ID: fmt.Sprintf("e%d", i+1), Type: "t", Data: json.RawMessage(`1`), Timestamp: ts}); err != nil {
			t.Fatal(err)
		}
	}
	// e4 is old but follows the newer e3: trimming never punches a hole.
	result, err := store.Trim(ctx, RetentionPolicy{OlderThan: retentionEpoch.Add(time.Minute)})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 2 || retainedIDs(t, store) != "e3,e4" {
		t.Fatalf("Trim = %+v, retained %q", result, retainedIDs(t, store))
	}
}

func TestMemoryStoreTrimPerPartition(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	// Partitions: a b a "" a b "" a
	partitions := []string{"a", "b", "a", "", "a", "b", "", "a"}
	offsets := appendRetentionEvents(t, store, len(partitions), func(i int) string { return partitions[i] })
	result, err := store.Trim(ctx, RetentionPolicy{PartitionKey: "p", MaxPerPartition: 2})
	if err != nil {
		t.Fatal(err)
	}
	// a keeps e5,e8; b keeps e2,e6; the keyless partition keeps e4,e7.
	if result.Deleted != 2 || result.TrimmedThrough != OffsetOldest || retainedIDs(t, store) != "e2,e4,e5,e6,e7,e8" {
		t.Fatalf("Trim = %+v, retained %q", result, retainedIDs(t, store))
	}
	// Partition caps are compaction: readers are not told, offsets are stable.
	events, next, err := store.Read(ctx, offsets[0], 0)
	if err != nil || len(events) != 6 || events[0].Offset != offsets[1] || next != offsets[7] {
		t.Fatalf("Read after partition trim = %d events, next %q, err %v", len(events), next, err)
	}
	// Nothing more to remove.
	if result, err := store.Trim(ctx, RetentionPolicy{PartitionKey: "p", MaxPerPartition: 2}); err != nil || result.Deleted != 0 {
		t.Fatalf("second Trim = %+v, %v", result, err)
	}
	// Prefix rules apply first; partition caps count what remains.
	result, err = store.Trim(ctx, RetentionPolicy{Through: offsets[4], PartitionKey: "p", MaxPerPartition: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Deleted != 3 || result.TrimmedThrough != offsets[4] || retainedIDs(t, store) != "e6,e7,e8" {
		t.Fatalf("Trim = %+v, retained %q", result, retainedIDs(t, store))
	}
}

func TestMemoryStoreReadsReportTrimmedOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	offsets := appendRetentionEvents(t, store, 5, nil)
	result, err := store.Trim(ctx, RetentionPolicy{Through: offsets[2]})
	if err != nil {
		t.Fatal(err)
	}

	_, next, err := store.Read(ctx, offsets[1], 0)
	var trimmed *OffsetTrimmedError
	if !errors.As(err, &trimmed) || !errors.Is(err, ErrOffsetTrimmed) {
		t.Fatalf("Read behind the trimmed prefix: err = %v", err)
	}
	if trimmed.Offset != offsets[1] || trimmed.ResumeFrom != result.TrimmedThrough || next != offsets[1] {
		t.Fatalf("trimmed = %+v, next %q", trimmed, next)
	}
	for _, stream := range []bool{false, true} {
		var events []*StoredEvent
		if stream {
			for event, err := range store.ReadStream(ctx, trimmed.ResumeFrom) {
				if err != nil {
					t.Fatal(err)
				}
				events = append(events, event)
			}
		} else if events, _, err = store.Read(ctx, trimmed.ResumeFrom, 0); err != nil {
			t.Fatal(err)
		}
		if len(events) != 2 || events[0].ID != "e4" {
			t.Fatalf("resume from %q (stream %v) = %d events", trimmed.ResumeFrom, stream, len(events))
		}
	}
	for event, err := range store.ReadStream(ctx, offsets[0]) {
		if event != nil || !errors.Is(err, ErrOffsetTrimmed) {
			t.Fatalf("ReadStream behind the trimmed prefix = %v, %v", event, err)
		}
	}
	if events, _, err := store.Read(ctx, OffsetOldest, 0); err != nil || len(events) != 2 {
		t.Fatalf("Read(OffsetOldest) = %d events, %v", len(events), err)
	}
	if events, tail, err := store.Read(ctx, OffsetNewest, 0); err != nil || len(events) != 0 || tail != offsets[4] {
		t.Fatalf("Read(OffsetNewest) = %d events, %q, %v", len(events), tail, err)
	}

	// The prefix boundary never moves back, and offsets are never reused.
	again, err := store.Trim(ctx, RetentionPolicy{Through: offsets[0]})
	if err != nil || again.Deleted != 0 || again.TrimmedThrough != offsets[2] {
		t.Fatalf("Trim behind the boundary = %+v, %v", again, err)
	}
	offset, err := store.Append(ctx, &Event{Type: "t", Data: json.RawMessage(`6`)})
	if err != nil || offset <= offsets[4] {
		t.Fatalf("Append after Trim = %q, %v", offset, err)
	}

	// Removing everything keeps the boundary for readers that were behind it.
	all, err := store.Trim(ctx, RetentionPolicy{MaxEvents: 0, Through: offset})
	if err != nil || all.Deleted != 3 || all.TrimmedThrough != offset {
		t.Fatalf("Trim everything = %+v, %v", all, err)
	}
	if _, _, err := store.Read(ctx, offsets[3], 0); !errors.Is(err, ErrOffsetTrimmed) {
		t.Fatalf("Read from a removed event = %v", err)
	}
	if events, next, err := store.Read(ctx, offset, 0); err != nil || len(events) != 0 || next != offset {
		t.Fatalf("Read from the boundary = %d, %q, %v", len(events), next, err)
	}
}

func TestMemoryStoreTrimRejections(t *testing.T) {
	store := NewMemoryStore()
	if _, err := store.Trim(context.Background(), RetentionPolicy{MaxEvents: -1}); err == nil {
		t.Fatal("invalid policy accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := store.Trim(ctx, RetentionPolicy{MaxEvents: 1}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Trim with a cancelled context = %v", err)
	}
}

func TestReplayReportsTrimmedOffsets(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	bus := New(WithStore(store))
	type Ping struct{ N int }
	for i := range 4 {
		if err := TryPublish(bus, Ping{N: i}); err != nil {
			t.Fatal(err)
		}
	}
	events, _, _ := store.Read(ctx, OffsetOldest, 0)
	if _, err := store.Trim(ctx, RetentionPolicy{Through: events[2].Offset}); err != nil {
		t.Fatal(err)
	}
	err := bus.Replay(ctx, events[0].Offset, func(*StoredEvent) error { return nil })
	if !errors.Is(err, ErrOffsetTrimmed) {
		t.Fatalf("Replay from a trimmed offset = %v", err)
	}
	replayed := 0
	if err := bus.Replay(ctx, events[2].Offset, func(*StoredEvent) error { replayed++; return nil }); err != nil || replayed != 1 {
		t.Fatalf("Replay from the boundary = %d, %v", replayed, err)
	}
}

func TestMemoryStoreTrimConcurrentWithAppendAndRead(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(3)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = store.Append(ctx, &Event{Type: "t", Data: json.RawMessage(`1`), Metadata: map[string]string{"p": fmt.Sprint(i % 3)}})
		}
	}()
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			_, _ = store.Trim(ctx, RetentionPolicy{MaxEvents: 50, PartitionKey: "p", MaxPerPartition: 10})
		}
	}()
	go func() {
		defer wg.Done()
		from := OffsetOldest
		for {
			select {
			case <-stop:
				return
			default:
			}
			events, next, err := store.Read(ctx, from, 10)
			var trimmed *OffsetTrimmedError
			switch {
			case errors.As(err, &trimmed):
				from = trimmed.ResumeFrom
			case err != nil:
				t.Error(err)
				return
			default:
				for i := 1; i < len(events); i++ {
					if events[i].Offset <= events[i-1].Offset {
						t.Error("offsets out of order")
					}
				}
				from = next
			}
		}
	}()
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()
}
