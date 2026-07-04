// Package state implements the Durable Streams State Protocol for ebu.
//
// The State Protocol provides database-style sync semantics on top of durable streams,
// enabling insert, update, and delete operations on typed entities. This package is
// optional and does not modify core ebu behavior.
//
// # Change Messages
//
// Change messages represent mutations to entities with a composite key (type + key):
//
//	// Insert a new user
//	msg, _ := state.Insert("user:123", User{Name: "Alice"})
//
//	// Update a user
//	msg, _ := state.Update("user:123", User{Name: "Alice Smith"})
//
//	// Update with old value for conflict detection
//	msg, _ := state.UpdateWithOldValue("user:123", newUser, oldUser)
//
//	// Delete a user
//	msg, _ := state.Delete[User]("user:123")
//
// Change messages can be configured with options:
//
//	msg, _ := state.Insert("user:123", user,
//	    state.WithTxID("tx-001"),           // Group related changes
//	    state.WithAutoTimestamp(),           // Add current timestamp
//	)
//
// # Control Messages
//
// Control messages manage stream lifecycle:
//
//	state.SnapshotStart("offset")  // Begin snapshot
//	state.SnapshotEnd("offset")    // End snapshot
//	state.Reset("offset")          // Clear and restart
//
// # Publishing to ebu
//
// Change and control messages implement ebu's TypeNamer interface and can be
// published directly:
//
//	bus := eventbus.New(eventbus.WithStore(store))
//
//	msg, _ := state.Insert("user:1", User{Name: "Alice"})
//	eventbus.Publish(bus, msg)
//
// # Materialization
//
// The Materializer processes state protocol messages and builds state:
//
//	// Create materializer
//	mat := state.NewMaterializer(
//	    state.WithOnReset(func() { log.Println("State reset") }),
//	)
//
//	// Register typed collections
//	userStore := state.NewMemoryStore[User]()
//	users := state.NewTypedCollection[User](userStore)
//	state.RegisterCollection(mat, users)
//
//	// Replay events through the materializer
//	mat.Replay(ctx, bus, eventbus.OffsetOldest)
//
//	// Access materialized state
//	user, ok, err := users.Get("user:123")
//
// Store methods return errors so durable backends can surface failures; the
// materializer never advances its offset past an event whose store write
// failed. Several collections may share one Store: entities are keyed by
// "entityType/key" and every collection operation is scoped to its own
// prefix.
//
// # Mixed Streams and Routing
//
// Events are routed by their stored Type: messages published through ebu are
// typed "state.ChangeMessage" / "state.ControlMessage" and decoded strictly.
// Events with other type names are detected structurally for interop with
// other State Protocol writers, and anything that does not positively
// identify as a state message — including arbitrary JSON that happens to
// carry a "headers" field — is skipped, so state messages can share a stream
// with regular events, even with strict schema validation enabled.
//
// An undecodable typed message aborts materialization by default so the
// error is never lost; configure WithApplyErrorPolicy(ApplySkip) to report
// such poison messages to WithOnError and continue past them.
//
// # Snapshots and Compaction
//
// For high-churn streams, replaying from the beginning on every start gets
// expensive. When the event store implements eventbus.EventStoreSnapshotter,
// the materializer can persist its collections and resume from the snapshot:
//
//	// On startup: restore the snapshot, then replay only the tail.
//	offset, err := mat.LoadSnapshotFrom(ctx, snapshotter, "users")
//	if err != nil { ... }
//	mat.Replay(ctx, bus, offset)
//
//	// Periodically: persist the current state.
//	if err := mat.SaveSnapshotTo(ctx, snapshotter, "users"); err != nil { ... }
//
//	// Optionally, once the snapshot is durably saved, compact the log:
//	if tr, ok := bus.GetStore().(eventbus.EventStoreTruncator); ok {
//	    tr.TruncateBefore(ctx, offset) // offset the snapshot was saved at
//	}
//
// Truncation is only safe once the snapshot is durably saved and no other
// reader or subscription still needs the truncated prefix.
//
// # Custom Type Names
//
// Entity types can implement TypeNamer for stable, explicit type names:
//
//	type User struct {
//	    Name  string `json:"name"`
//	    Email string `json:"email"`
//	}
//
//	func (u User) StateTypeName() string { return "user" }
//
// This ensures type names remain stable across refactoring and package moves.
//
// # JSON Interoperability
//
// All messages serialize to JSON in a format compatible with the durable-streams
// ecosystem. This enables interoperability with other implementations of the
// State Protocol.
//
// For more information on the State Protocol, see:
// https://github.com/durable-streams/durable-streams
package state
