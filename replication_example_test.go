package eventbus_test

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	eventbus "github.com/jilio/ebu"
)

func ExampleReplicator() {
	// MemoryStore demonstrates the API only. Use an independent durable
	// destination to protect against loss of the source worker.
	local, backup := eventbus.NewMemoryStore(), eventbus.NewMemoryStore()
	r, err := eventbus.NewReplicator(eventbus.ReplicatorConfig{
		Source: local, Destination: backup, Checkpoints: local,
		ID: "documents-to-backup", Generation: "documents-v1-backup-v1",
	}, eventbus.MirrorPollInterval(time.Millisecond))
	if err != nil {
		panic(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	defer func() { cancel(); <-done }()

	// Ordinary writes return after the local append.
	if _, err := local.Append(ctx, &eventbus.Event{ID: "draft-1", Type: "document", Data: json.RawMessage(`{"text":"draft"}`)}); err != nil {
		panic(err)
	}
	// A critical write waits for the whole prefix, including that draft.
	offset, err := local.Append(ctx, &eventbus.Event{ID: "saved-2", Type: "document", Data: json.RawMessage(`{"text":"saved"}`)})
	if err != nil {
		panic(err)
	}
	point, err := r.Position(offset)
	if err != nil {
		panic(err)
	}
	waitCtx, stopWait := context.WithTimeout(ctx, time.Second)
	defer stopWait()
	if err := r.Wait(waitCtx, point); err != nil {
		panic(err)
	}
	events, _, err := backup.Read(ctx, eventbus.OffsetOldest, 0)
	if err != nil {
		panic(err)
	}
	fmt.Println("confirmed events:", len(events))
	// Output: confirmed events: 2
}
