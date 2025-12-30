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
//	user, ok := users.Get("user:123")
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
