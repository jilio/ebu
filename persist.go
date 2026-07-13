package eventbus

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"maps"
	"reflect"
	"sort"
	"sync"
	"time"
)

// Offset represents an opaque position in an event stream.
// Implementations define the format (e.g., "123", "abc_456", timestamp-based).
//
// Offsets are resumption tokens. The bus treats them as opaque: it normally
// performs only equality checks and passes them back to the store that issued
// them. When a store implements EventStoreOffsetComparer, the bus delegates
// same-store ordering checks to it without interpreting token contents. Treat
// offsets from one store as meaningless to any other store.
type Offset string

const (
	// OffsetOldest represents the beginning of the stream.
	// When passed to Read, returns events from the start.
	OffsetOldest Offset = ""

	// OffsetNewest represents the current end of the stream.
	// Useful for subscribing to only new events.
	//
	// OffsetNewest is a query sentinel, not a durable offset. Every EventStore
	// must resolve it, at call time, to the current concrete tail: Read returns
	// no historical events and returns that resumable tail (never "$" itself)
	// as nextOffset. The empty stream resolves to OffsetOldest. Implementations
	// must use an efficient tail lookup rather than scanning or returning the
	// stream's history. ReadStream likewise yields no events and terminates; use
	// EventStoreTailer when later appends should be followed.
	// Persist the concrete nextOffset returned by Read, not OffsetNewest.
	OffsetNewest Offset = "$"
)

// EventStore defines the core interface for persisting events.
// This is a minimal interface with just 2 methods for basic event storage.
// Additional capabilities are provided through optional interfaces.
type EventStore interface {
	// Append stores an event and returns a store-issued, resumable offset.
	// Offsets need not be numeric or unique per event: multiple events may share
	// one token when the store's smallest resumable unit is a chunk. Resuming
	// after the returned token must never skip an event appended later. A returned
	// error does not prove the event is absent: a remote commit may succeed
	// before its acknowledgement is lost. Implementations should use Event.ID
	// to make internal retries idempotent.
	Append(ctx context.Context, event *Event) (Offset, error)

	// Read returns events starting after the given offset.
	// Use OffsetOldest to read from the beginning. OffsetNewest must be
	// resolved according to its sentinel contract above: return no events and
	// the current concrete tail as nextOffset, without scanning stream history.
	// A positive limit is the requested batch target; zero requests no limit.
	// A store should not exceed a positive limit unless splitting its smallest
	// resumable unit (for example, a chunk with one shared offset) would make
	// nextOffset unsafe. It may return that whole unit to preserve resume-token
	// integrity. If such a unit can advance past an earlier concrete tail token
	// without returning that token as a StoredEvent.Offset or nextOffset, the
	// store must also implement EventStoreOffsetComparer so a bounded consumer
	// can detect the overrun.
	//
	// Every returned StoredEvent.Offset must be prefix-safe to persist
	// immediately after that event is handled: Read(ctx, event.Offset, ...) may
	// redeliver events at or before that point, but must never skip a later event
	// from this result. For a chunk protocol without per-event tokens, assign the
	// read-from/chunk-start token to each non-last member and the chunk-end token
	// only to the last member; assigning the end token to every member can lose
	// the unfinished suffix after a crash. nextOffset must likewise be safe after
	// every returned event has completed.
	// Returns the events, the offset to use for the next read, and any error.
	Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error)
}

// EventStoreOffsetComparer is an optional capability for stores whose opaque
// offsets nevertheless have a store-defined order. CompareOffsets compares two
// concrete offsets issued by the same store and returns a negative value when
// left is before right, zero when equal, and a positive value when left is
// after right. OffsetOldest is a valid concrete boundary; OffsetNewest is a
// symbolic query sentinel and must return an error.
//
// A store must implement this interface when one indivisible Read unit can
// cross a previously issued concrete tail without exposing that exact token as
// either a StoredEvent.Offset or nextOffset. Resumable subscriptions then stop
// immediately after the one unit that crossed their captured tail instead of
// chasing concurrent appends.
type EventStoreOffsetComparer interface {
	CompareOffsets(left, right Offset) (int, error)
}

// EventStoreStreamer is an optional interface for memory-efficient streaming.
// When implemented, the Replay method will automatically use streaming.
//
// Implementation notes:
//   - Database-backed stores should use cursor-based iteration to minimize memory
//   - In-memory stores may need to take a snapshot to avoid holding locks during iteration,
//     trading memory for deadlock safety (see MemoryStore.ReadStream for an example)
type EventStoreStreamer interface {
	// ReadStream returns an iterator yielding events starting after the given offset.
	// Use OffsetOldest to read from the beginning.
	// The iterator checks ctx.Done() before each yield and returns ctx.Err() when cancelled.
	// A yielded error terminates iteration.
	ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error]
}

// SubscriptionStore tracks subscription progress separately from event storage.
// This interface is optional and enables resumable subscriptions.
type SubscriptionStore interface {
	// SaveOffset persists the current offset for a subscription.
	SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error

	// LoadOffset retrieves the last saved offset for a subscription.
	// Returns OffsetOldest if the subscription has no saved offset.
	LoadOffset(ctx context.Context, subscriptionID string) (Offset, error)
}

// SubscriptionStoreLookup is an optional capability that distinguishes a
// missing subscription checkpoint from a checkpoint whose concrete value is
// OffsetOldest. That distinction matters when a durable Follow has an explicit
// first-run starting offset: OffsetOldest is valid for stores whose first
// resume-safe unit starts at the empty token, so it cannot also prove absence.
//
// Implementations should perform the lookup atomically. When found is false,
// offset is ignored and should conventionally be OffsetOldest. A found
// OffsetNewest is invalid because symbolic offsets are not durable resume
// tokens. The bundled MemoryStore and SQLite store implement this interface.
type SubscriptionStoreLookup interface {
	LookupOffset(ctx context.Context, subscriptionID string) (offset Offset, found bool, err error)
}

// EventStoreSnapshotter is an optional interface for stores that can persist a
// materialized projection snapshot, keyed by an id and tagged with the Offset the
// snapshot reflects. It lets a caller compact a high-churn log: save the reduced
// state, then (with EventStoreTruncator) drop the events the snapshot subsumes,
// bounding cold-start replay. The blob is opaque to ebu — callers define its
// encoding. A store that does not implement this interface is simply not
// compactable; callers fall back to a full replay from OffsetOldest.
type EventStoreSnapshotter interface {
	// SaveSnapshot upserts the snapshot for snapshotID, recording that blob
	// reflects the projection state as of (and including) atOffset. atOffset MUST
	// be a real, resumable Offset previously returned by Append/Read for THIS
	// store (never OffsetNewest, never synthetic): a caller resumes with
	// Replay(ctx, atOffset, ...), which reads only events strictly after it.
	SaveSnapshot(ctx context.Context, snapshotID string, atOffset Offset, blob json.RawMessage) error

	// LoadSnapshot returns the last saved snapshot for snapshotID. When none
	// exists it returns (OffsetOldest, nil, nil) — "replay from the beginning",
	// never an error — mirroring LoadOffset's OffsetOldest default.
	LoadSnapshot(ctx context.Context, snapshotID string) (atOffset Offset, blob json.RawMessage, err error)
}

// EventStoreTruncator is an optional interface for log compaction: it deletes
// events at or before a given Offset. It is only safe to call once a snapshot
// covering that Offset is durably saved AND no live reader/subscription still
// needs the truncated prefix. Stores whose Offsets are not deletable positions
// (e.g. a remote append-only broker) MUST NOT implement this interface.
type EventStoreTruncator interface {
	// TruncateBefore deletes every event whose Offset <= beforeOffset and returns
	// the number deleted. beforeOffset == OffsetOldest is a no-op. Idempotent:
	// re-running with the same offset deletes nothing more.
	TruncateBefore(ctx context.Context, beforeOffset Offset) (deleted int64, err error)
}

// Event represents an event to be stored (before it has an offset).
//
// ID, Origin, and Metadata form the event envelope. All three are optional
// at the storage level (`omitempty`), so streams written by earlier ebu
// versions or by external producers decode cleanly with empty values.
type Event struct {
	// ID is a globally unique identifier for the publish that produced this
	// event (a ULID, see NewEventID). The bus assigns it once, before the
	// first Append attempt, so a store that retries a failed append writes
	// the SAME ID for every attempt — consumers can deduplicate at-least-once
	// deliveries on it.
	ID string `json:"id,omitempty"`

	// Origin identifies the bus instance that published the event (each
	// EventBus gets a unique origin at New). On a log shared by several
	// processes it tells "my own event echoed back" apart from a peer's.
	Origin string `json:"origin,omitempty"`

	Type string          `json:"type"`
	Data json.RawMessage `json:"data"`

	// Metadata carries publisher-supplied key/value pairs (correlation IDs,
	// causation IDs, tenant tags, ...). Attach it to the publish context with
	// ContextWithMetadata. The bus never reads or writes keys itself.
	Metadata map[string]string `json:"metadata,omitempty"`

	Timestamp time.Time `json:"timestamp"`
}

// StoredEvent represents an event that has been persisted with an offset.
// ID, Origin, and Metadata mirror the Event envelope; they are empty for
// events written before the envelope existed or by external producers.
type StoredEvent struct {
	// Offset is a store-issued, prefix-safe checkpoint immediately after this
	// event's successful handling; resuming from it may redeliver this event or
	// an earlier indivisible unit, but must not skip any later event.
	Offset    Offset            `json:"offset"`
	ID        string            `json:"id,omitempty"`
	Origin    string            `json:"origin,omitempty"`
	Type      string            `json:"type"`
	Data      json.RawMessage   `json:"data"`
	Metadata  map[string]string `json:"metadata,omitempty"`
	Timestamp time.Time         `json:"timestamp"`
}

// WithStore enables persistence with the given store.
//
// Events are persisted in the publish path, after the before-publish hooks
// and before handlers run. Persistence is best-effort for ordinary Subscribe
// handlers: a failed Append does not prevent their delivery, and the error is
// reported to the PersistenceErrorHandler instead. Log-backed resumable
// subscriptions consume only records visible in EventStore. Because an Append
// error may follow a durable remote commit, that record can still be delivered.
//
// New applies all options before enforcing durable type-name uniqueness, so
// custom options that make typed registrations behave identically on either
// side of WithStore. Applying this option directly to an existing bus panics
// if its provisional typed registrations contain a durable-name collision;
// the bus remains nonpersistent in that case.
func WithStore(store EventStore) Option {
	return func(bus *EventBus) {
		if !bus.configuring && store != nil {
			bus.activatePersistenceTypes()
		}
		bus.store = store
	}
}

// offsetCtxKey is the context key under which the persisted event's offset is
// stored for live, followed, and context-aware replay delivery.
type offsetCtxKey struct{}

// OffsetFromContext returns the persisted offset of the event currently being
// handled. It is available after a successful live persist, in followed
// handlers, and in handlers registered with SubscribeContextWithReplay.
func OffsetFromContext(ctx context.Context) (Offset, bool) {
	offset, ok := ctx.Value(offsetCtxKey{}).(Offset)
	return offset, ok
}

// eventIDCtxKey is the context key under which the persisted event ID is
// stored for live, followed, and context-aware replay delivery.
type eventIDCtxKey struct{}

// EventIDFromContext returns the ID assigned to the event currently being
// handled. It is set for every publish on a bus configured with WithStore —
// including publishes whose Append failed in best-effort mode, since the ID
// identifies the publish, not the storage outcome — and restored for Follow
// and SubscribeContextWithReplay delivery. Context-aware handlers can use it
// for idempotency keys and correlation.
func EventIDFromContext(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(eventIDCtxKey{}).(string)
	return id, ok
}

// metadataCtxKey is the context key under which publisher-supplied event
// metadata travels from PublishContext to persistEvent.
type metadataCtxKey struct{}

// ContextWithMetadata returns a context that attaches metadata to every event
// subsequently published with it on a bus configured with WithStore. The map
// is stored on the persisted event's envelope (Event.Metadata) and surfaces
// through StoredEvent.Metadata, Follow, and SubscribeContextWithReplay.
//
// The map is not copied: callers must not mutate it after publishing.
// Publishing with a context that already carries metadata replaces the whole
// map (no merging). A nil or empty map attaches nothing.
func ContextWithMetadata(ctx context.Context, md map[string]string) context.Context {
	if len(md) == 0 {
		return ctx
	}
	return context.WithValue(ctx, metadataCtxKey{}, md)
}

// MetadataFromContext returns the event metadata attached to ctx with
// ContextWithMetadata, if any. Inside live, followed, or context-aware replay
// handlers it returns the metadata of the event being delivered.
func MetadataFromContext(ctx context.Context) (map[string]string, bool) {
	md, ok := ctx.Value(metadataCtxKey{}).(map[string]string)
	return md, ok
}

// WithSubscriptionStore enables subscription position tracking
func WithSubscriptionStore(store SubscriptionStore) Option {
	return func(bus *EventBus) {
		bus.subscriptionStore = store
	}
}

// persistEvent saves an event to storage. It returns a context that carries
// the event's assigned ID (always) and its offset (on success, see
// OffsetFromContext), plus the persistence error, if any. Every error is
// reported to the PersistenceErrorHandler before being returned; the caller
// decides — based on WithStrictPersistence — whether the error also stops
// delivery.
//
// Concurrency note: the bus does not serialize Append calls; stores must be
// safe for concurrent use (all bundled stores are). Once an Append call is
// attempted, the event type's durable wire-name claim is retained even when
// Append returns an error, because a remote commit may have succeeded before
// its acknowledgement was lost.
func (bus *EventBus) persistEvent(ctx context.Context, eventType reflect.Type, event any) (context.Context, error) {
	// The ID identifies this publish: it is minted before the first Append
	// attempt so retries inside a store write the same ID, and it is attached
	// to ctx even when persistence fails (see EventIDFromContext).
	eventID := NewEventID()
	ctx = context.WithValue(ctx, eventIDCtxKey{}, eventID)

	// Marshal the event first
	data, err := json.Marshal(event)
	if err != nil {
		err = fmt.Errorf("failed to marshal event: %w", err)
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, err)
		}
		return ctx, err
	}

	// Use the same type-derived name as subscriptions and typed upcasts. A
	// TypeNamer value is intentionally evaluated on a fresh zero value so one
	// Go type cannot fragment across instance-dependent wire names.
	typeName := typeNameOf(eventType)
	if actualName := EventType(event); actualName != typeName {
		err = fmt.Errorf("eventbus: EventTypeName for Go type %s depends on instance state (%q for this value, %q for the type); EventTypeName must be a pure, immutable function of the type", eventType, actualName, typeName)
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, err)
		}
		return ctx, err
	}
	typeReservation, err := bus.reservePersistedTypes(persistedTypeSpec{name: typeName, eventType: eventType})
	if err != nil {
		err = fmt.Errorf("failed to reserve persisted event type: %w", err)
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, err)
		}
		return ctx, err
	}
	defer typeReservation.Rollback()

	metadata, _ := MetadataFromContext(ctx)
	toStore := &Event{
		ID:        eventID,
		Origin:    bus.originID,
		Type:      typeName,
		Data:      data,
		Metadata:  metadata,
		Timestamp: time.Now(),
	}

	// Apply timeout if configured. The timeout applies to Append only; the
	// original context is what gets returned to the publish path.
	appendCtx := ctx
	if bus.persistenceTimeout > 0 {
		var cancel context.CancelFunc
		appendCtx, cancel = context.WithTimeout(ctx, bus.persistenceTimeout)
		defer cancel()
	}

	// Observability: Track persistence start
	if bus.observability != nil {
		appendCtx = bus.observability.OnPersistStart(appendCtx, typeName)
	}

	start := time.Now()

	// Once Append is invoked, its error outcome is inherently ambiguous: a
	// remote store may commit the event and then time out while returning the
	// response. Keep the wire-name mapping sticky before making the call so a
	// later publish cannot reinterpret a possibly-durable payload as another Go
	// type. Pre-append validation and observability failures still roll back via
	// the deferred cleanup above.
	typeReservation.Commit()

	// Append the event - the store assigns the offset.
	offset, saveErr := bus.store.Append(appendCtx, toStore)

	// Observability: Track persistence complete
	if bus.observability != nil {
		bus.observability.OnPersistComplete(appendCtx, typeName, time.Since(start), offset, saveErr)
	}

	if saveErr != nil {
		saveErr = fmt.Errorf("failed to save event: %w", saveErr)
		if bus.persistenceErrorHandler != nil {
			bus.persistenceErrorHandler(event, eventType, saveErr)
		}
		return ctx, saveErr
	}

	return context.WithValue(ctx, offsetCtxKey{}, offset), nil
}

// Replay replays events from an offset
func (bus *EventBus) Replay(ctx context.Context, from Offset, handler func(*StoredEvent) error) error {
	if bus.store == nil {
		return fmt.Errorf("replay requires persistence (use WithStore option)")
	}
	if from == OffsetNewest {
		// Replay("$") is an immediate tail boundary, not a moving follower.
		// Resolve it exactly once for parity with ReadStream("$"), then return;
		// continuing from that concrete token could consume an append that races
		// a later fallback Read.
		if err := ctx.Err(); err != nil {
			return err
		}
		events, concrete, err := bus.store.Read(ctx, OffsetNewest, 0)
		if err != nil {
			return fmt.Errorf("resolve replay tail: %w", err)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if len(events) != 0 {
			return fmt.Errorf("resolve replay tail: EventStore.Read(OffsetNewest) returned %d event(s), want none", len(events))
		}
		if concrete == OffsetNewest {
			return fmt.Errorf("resolve replay tail: EventStore.Read(OffsetNewest) returned symbolic offset %q, want a concrete tail", OffsetNewest)
		}
		return nil
	}

	// Use streaming if available for memory efficiency
	if streamer, ok := bus.store.(EventStoreStreamer); ok {
		for event, err := range streamer.ReadStream(ctx, from) {
			if err != nil {
				return fmt.Errorf("stream events: %w", err)
			}
			if err := handler(event); err != nil {
				return fmt.Errorf("handle event at offset %s: %w", event.Offset, err)
			}
		}
		return nil
	}

	// Fallback to Read for stores that don't support streaming
	batchSize := bus.replayBatchSize
	if batchSize <= 0 {
		batchSize = 100 // Default batch size
	}

	offset := from
	for {
		// Check context cancellation before each batch
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		events, nextOffset, err := bus.store.Read(ctx, offset, batchSize)
		if err != nil {
			return fmt.Errorf("read events: %w", err)
		}

		if len(events) == 0 {
			// Zero events does not mean the tail was reached: a store may
			// advance the offset past a stretch it cannot deliver (e.g. a
			// remote chunk whose events were all skipped as undecodable).
			// Only a non-advancing offset marks the end of the stream.
			if nextOffset == offset {
				break
			}
			offset = nextOffset
			continue
		}

		for _, event := range events {
			if err := handler(event); err != nil {
				return fmt.Errorf("handle event at offset %s: %w", event.Offset, err)
			}
		}

		// Protect against infinite loop if offset doesn't advance
		if nextOffset == offset {
			return fmt.Errorf("store returned non-advancing offset %s: possible bug in EventStore.Read implementation", offset)
		}
		offset = nextOffset
	}

	return nil
}

// ReplayWithUpcast replays events from an offset, applying upcasts before
// passing them to handler. An upcast failure aborts replay without delivering
// the original schema as if migration had succeeded.
func (bus *EventBus) ReplayWithUpcast(ctx context.Context, from Offset, handler func(*StoredEvent) error) error {
	return bus.Replay(ctx, from, func(event *StoredEvent) error {
		// Apply upcasts if available
		if bus.upcastRegistry != nil {
			upcastedData, upcastedType, err := bus.upcastRegistry.apply(event.Data, event.Type)
			if err != nil {
				// Delivering the original schema after a configured migration failed
				// would turn corruption or an upcaster contract violation into a
				// successful replay. Leave the caller's offset unchanged instead.
				return err
			}
			// Preserve the complete envelope. A shallow copy intentionally
			// carries ID, Origin, Metadata, and any future StoredEvent fields;
			// only the schema-dependent type and payload are replaced.
			upcastedEvent := *event
			upcastedEvent.Type = upcastedType
			upcastedEvent.Data = upcastedData
			return handler(&upcastedEvent)
		}
		return handler(event)
	})
}

// IsPersistent returns true if persistence is enabled
func (bus *EventBus) IsPersistent() bool {
	return bus.store != nil
}

// GetStore returns the event store (or nil if not persistent)
func (bus *EventBus) GetStore() EventStore {
	return bus.store
}

// typeNamerType is the reflect.Type of the TypeNamer interface.
var typeNamerType = reflect.TypeOf((*TypeNamer)(nil)).Elem()

// typeNameOf returns the persisted type name for an event type, honoring the
// TypeNamer interface the same way EventType does for event values.
func typeNameOf(t reflect.Type) string {
	if t.Implements(typeNamerType) {
		if t.Kind() == reflect.Pointer {
			// Calling a value-receiver method through a typed nil pointer
			// panics before the method body runs. Use a fresh value so pointer
			// event types are as safe here as they are at Publish time.
			return reflect.New(t.Elem()).Interface().(TypeNamer).EventTypeName()
		}
		return reflect.Zero(t).Interface().(TypeNamer).EventTypeName()
	}
	// Do not use a pointer-receiver EventTypeName for a non-pointer T. A value
	// of T does not implement TypeNamer, so EventType(value) persists it under
	// the reflection name. Deriving a custom name here would make replay and
	// Follow look under a name Publish never wrote.
	return t.String()
}

// ReplayErrorPolicy determines how SubscribeWithReplay handles a stored
// event that cannot be decoded into the subscription's event type.
type ReplayErrorPolicy int

const (
	// ReplayAbort stops the replay and returns the decode error (default).
	// The subscription's saved offset does not advance past the failing
	// event, so the next SubscribeWithReplay hits it again. Use this when a
	// decode failure means a bug that must be fixed before proceeding.
	ReplayAbort ReplayErrorPolicy = iota

	// ReplaySkip reports the decode error to the PersistenceErrorHandler
	// (with the *StoredEvent as the event argument) and continues with the
	// next event. The skip is durable: the poison event's offset is saved,
	// so it is not re-scanned and re-reported on later restarts — recover
	// its payload from the reported *StoredEvent if needed.
	//
	// Scope: the policy fires only when a stored event OF THE SUBSCRIBED
	// TYPE fails to decode (malformed or type-incompatible JSON). It does
	// not cover events stored under a different type name — those are
	// always skipped silently, by design, since streams may carry many
	// event types — nor JSON that decodes leniently despite schema drift
	// (unknown fields are dropped, missing fields zero-filled; use upcasts
	// for schema evolution).
	ReplaySkip
)

// WithReplayErrorPolicy sets how SubscribeWithReplay and
// SubscribeContextWithReplay treat stored events that fail to decode in any
// phase, including their log-backed live phase. It has no effect on ordinary
// Subscribe/SubscribeContext delivery. The default is ReplayAbort.
func WithReplayErrorPolicy(policy ReplayErrorPolicy) SubscribeOption {
	return func(h *internalHandler) {
		h.replayErrorPolicy = policy
	}
}

// SubscribeWithReplay subscribes a payload-only handler and replays missed
// events. Use SubscribeContextWithReplay when the handler needs the persisted
// event ID, offset, or metadata for idempotency and correlation.
//
// Both functions require an EventStore and a SubscriptionStore. If the event
// store also implements SubscriptionStore, it is used automatically.
//
// Durable live contract: resumable subscriptions are log consumers, not
// ordinary in-memory callbacks. Every phase drains toward a concrete, bounded
// tail barrier using finite requested batch targets. A store may exceed a
// target only when required to preserve an indivisible resume token. If that
// unit crosses the captured barrier without exposing its exact token, the store
// must implement EventStoreOffsetComparer; the handler can then see at most the
// one crossing unit of concurrent post-barrier events before the drain stops at
// its actual resume token. The handler receives a freshly JSON-decoded value
// plus the exact stored ID, offset, and metadata. Therefore json:"-" fields,
// unexported fields, pointer identity, and other transient properties of the
// published object are intentionally unavailable. An event absent from
// EventStore is not delivered by a resumable subscription; an Append call that
// committed before returning an error may still be delivered. Ordinary
// Subscribe handlers retain the bus's configured best-effort behavior.
//
// Ordering and offsets: one coordinator scans the store in log order and
// saves an event's offset only after successful handler completion. A blocked
// or panicking event therefore prevents a newer checkpoint from leapfrogging
// it. A crash between handling and offset save still redelivers the event, so
// delivery is at-least-once and handlers must remain idempotent.
//
// Context usage: ctx controls setup replay and is visible to historical
// handlers. Live delivery preserves cancellation/deadline behavior from the
// leading publish/follow signal but deliberately strips its arbitrary values;
// envelope accessors always describe the stored event being delivered.
// Coalesced work scheduled after a concurrent or same-type reentrant publish
// uses a background operation context. Under that contention, Publish may
// return before this resumable handler runs; bus.Wait/Shutdown waits for the
// scheduled drain. A live read/handler failure retains that work and retries
// automatically with capped exponential backoff; Clear/ClearAll cancels a
// delayed retry. Ordinary Subscribe handlers retain their configured
// synchronous or asynchronous behavior.
//
// Handoff starts with a concrete, bounded tail barrier. When no local signal
// races that replay, setup also completes the second barrier synchronously. If
// a concurrent or reentrant publish is already pending, setup activates after
// the initial barrier and hands catch-up to the tracked background drain, so a
// hot local producer cannot starve SubscribeWithReplay from returning. In that
// case bus.Wait/Shutdown observes completion. Signals remain behind an inactive
// marker until every setup step succeeds, so a failed setup cannot race live
// user-handler side effects.
//
// SaveOffset failures do not stop delivery; they are reported to the
// PersistenceErrorHandler.
//
// Async and Once are rejected. A resumable subscription can only checkpoint
// safely after synchronous handler completion, and Once has no unambiguous
// durable meaning across process restarts.
//
// Validation: all argument and option validation (nil bus/handler/options,
// empty/previously-used subscription ID, interface event types, filter shape)
// happens before replay, so a validation error has delivered no events and
// saved no offsets. After setup succeeds, its subscription ID remains reserved
// for the EventBus's lifetime (create a new bus to restart it). A setup failure
// releases the ID so the same call can be retried on that bus. Cross-process
// coordination still requires one owner per ID.
//
// Filtering: a WithFilter predicate applies to the replay and catch-up
// passes exactly as it does to live delivery — the handler never sees
// non-matching events, and their offsets are not saved.
//
// Note: If the saved offset is OffsetOldest (""), replay starts from the beginning.
// The OffsetNewest ("$") constant is typically not stored and is only used for live subscriptions.
func SubscribeWithReplay[T any](
	ctx context.Context,
	bus *EventBus,
	subscriptionID string,
	handler Handler[T],
	opts ...SubscribeOption,
) error {
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}
	return subscribeContextWithReplay(ctx, bus, subscriptionID,
		func(_ context.Context, event T) { handler(event) }, opts...)
}

// SubscribeContextWithReplay is SubscribeWithReplay for a context-aware
// handler. During historical and catch-up replay, the context exposes the
// StoredEvent envelope through EventIDFromContext, OffsetFromContext, and
// MetadataFromContext, matching live delivery.
func SubscribeContextWithReplay[T any](
	ctx context.Context,
	bus *EventBus,
	subscriptionID string,
	handler ContextHandler[T],
	opts ...SubscribeOption,
) error {
	if handler == nil {
		return fmt.Errorf("eventbus: handler cannot be nil")
	}
	return subscribeContextWithReplay(ctx, bus, subscriptionID, handler, opts...)
}

func subscribeContextWithReplay[T any](
	ctx context.Context,
	bus *EventBus,
	subscriptionID string,
	handler ContextHandler[T],
	opts ...SubscribeOption,
) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if bus.store == nil {
		return fmt.Errorf("SubscribeWithReplay requires persistence (use WithStore option)")
	}

	// Get subscription store - either explicit or from the event store
	subStore := bus.subscriptionStore
	if subStore == nil {
		if ss, ok := bus.store.(SubscriptionStore); ok {
			subStore = ss
		} else {
			return fmt.Errorf("SubscribeWithReplay requires a SubscriptionStore (use WithSubscriptionStore option or use a store that implements SubscriptionStore)")
		}
	}

	eventType := reflect.TypeOf((*T)(nil)).Elem()

	// Build the real user handler before reserving any external identity. The
	// separate signal handler installed later has no user options: its only job
	// is to wake the ordered log consumer.
	h, err := buildContextHandler(handler, eventType, opts)
	if err != nil {
		return err
	}
	if h.async {
		return fmt.Errorf("eventbus: Async cannot be used with SubscribeWithReplay or SubscribeContextWithReplay: durable offsets require synchronous handler completion")
	}
	if h.once {
		return fmt.Errorf("eventbus: Once cannot be used with SubscribeWithReplay or SubscribeContextWithReplay: one-time delivery has no durable meaning across restarts")
	}
	// typeNameOf may instantiate a TypeNamer. Keep it strictly after
	// buildHandler's interface-type validation so a zero interface value can
	// never reach that assertion.
	typeName := typeNameOf(eventType)
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := bus.reserveReplayID(subscriptionID); err != nil {
		return err
	}
	setupSucceeded := false
	defer func() {
		if !setupSucceeded {
			bus.releaseReplayID(subscriptionID)
		}
	}()

	typeReservation, err := bus.reservePersistedTypes(persistedTypeSpec{name: typeName, eventType: eventType})
	if err != nil {
		return err
	}
	defer typeReservation.Rollback()

	// A failed offset load is not equivalent to a new subscription. Replaying
	// from the beginning in that case could repeat the entire side-effect
	// history and then overwrite the last known checkpoint.
	lastOffset, err := subStore.LoadOffset(ctx, subscriptionID)
	if err != nil {
		return fmt.Errorf("eventbus: load offset for subscription %q: %w", subscriptionID, err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if lastOffset == OffsetNewest {
		return fmt.Errorf("eventbus: load offset for subscription %q: symbolic offset %q is not a durable checkpoint", subscriptionID, OffsetNewest)
	}

	coordinator := &replayCoordinator[T]{
		bus:            bus,
		subStore:       subStore,
		subscriptionID: subscriptionID,
		eventType:      eventType,
		typeName:       typeName,
		userHandler:    h,
		typeClaim:      typeReservation,
		scanOffset:     lastOffset,
		waitCh:         make(chan struct{}),
		done:           make(chan struct{}),
	}

	// The shard contains only an unconditional, silent wake-up handler. The
	// coordinator reads and decodes the durable event representation in store
	// order, then invokes userHandler exactly once through the normal panic,
	// Sequential, and observability boundary.
	marker := &internalHandler{
		handlerType: h.handlerType,
		eventType:   eventType,
		internalDelivery: func(signalCtx context.Context, _ any) error {
			return coordinator.signal(signalCtx)
		},
		suppressObservability: true,
	}
	marker.onRemove = func() {
		bus.unregisterReplayMarker(marker)
		coordinator.deactivate()
	}
	// Register before exposing the shard marker. A concurrent predecessor
	// publish can then wake this inactive coordinator; its fixed setup replay
	// will consume that durable record before activation.
	bus.registerReplayMarker(marker)
	bus.addHandler(eventType, marker)

	// Register the inactive marker before the first barrier-bounded replay. Signals
	// coalesce behind setup without waiting, including repeated same-type
	// reentrant publishes from the resumable handler itself.
	if err := coordinator.drainToCapturedTail(ctx); err != nil {
		bus.removeHandler(eventType, marker)
		return fmt.Errorf("replay events: %w", err)
	}

	// Prepare activation under a fresh tail barrier when setup is uncontended.
	// A signal already pending at handoff defers that second catch-up to the
	// tracked drain instead. Either way the marker remains gated until decoder
	// installation and the durable type claim below both succeed.
	needsCatchUp, err := coordinator.activate(ctx)
	if err != nil {
		bus.removeHandler(eventType, marker)
		return fmt.Errorf("catch-up replay: %w", err)
	}

	if err := completeReplaySetup(bus, eventType, marker, coordinator, typeReservation, needsCatchUp); err != nil {
		bus.removeHandler(eventType, marker)
		return err
	}
	setupSucceeded = true
	return nil
}

// completeReplaySetup linearizes successful setup against Clear/ClearAll.
// Those operations remove markers while holding the same shard lock, so they
// either win before this section (and setup rolls back) or run after the
// subscription is fully active. Setup must never return nil for a marker that
// was already removed.
func completeReplaySetup[T any](
	bus *EventBus,
	eventType reflect.Type,
	marker *internalHandler,
	coordinator *replayCoordinator[T],
	typeReservation *persistedTypeReservation,
	needsCatchUp bool,
) error {
	shard := bus.getShard(eventType)
	shard.mu.Lock()
	markerPresent := false
	for _, registered := range shard.handlers[eventType] {
		if registered == marker {
			markerPresent = true
			break
		}
	}
	if !markerPresent {
		shard.mu.Unlock()
		coordinator.deactivate()
		return fmt.Errorf("eventbus: resumable subscription %q was removed during setup", coordinator.subscriptionID)
	}

	if err := installFollowDecoder[T](bus); err != nil {
		shard.mu.Unlock()
		coordinator.deactivate()
		return fmt.Errorf("eventbus: install follow decoder: %w", err)
	}
	if !coordinator.finalizeActivation(needsCatchUp) {
		shard.mu.Unlock()
		return fmt.Errorf("eventbus: resumable subscription %q was removed during setup", coordinator.subscriptionID)
	}
	typeReservation.Commit()
	shard.mu.Unlock()
	return nil
}

// replayCoordinator turns SubscribeWithReplay into an ordered log consumer.
// scanOffset is the store read cursor and advances only after a whole batch is
// processed; this is deliberately separate from the durable per-event
// checkpoint because some stores assign several events the same resume-safe
// offset.
type replayCoordinator[T any] struct {
	bus            *EventBus
	subStore       SubscriptionStore
	subscriptionID string
	eventType      reflect.Type
	typeName       string
	userHandler    *internalHandler
	typeClaim      *persistedTypeReservation
	scanOffset     Offset

	mu      sync.Mutex
	active  bool
	closed  bool
	running bool
	pending bool
	tracked bool // pending-work token spanning first deferral through drain/drop
	waitCh  chan struct{}
	done    chan struct{}
	// retryWaiting coalesces new signals behind the single delayed retry instead
	// of allocating one timer per publish during a store outage.
	retryWaiting bool

	// retryDelay backs off autonomous retries after a live drain failure. A
	// successful drain resets it. The pending-work token remains held across
	// the timer, so Wait and Shutdown cannot mistake a failed wake-up for
	// completed delivery.
	retryDelay time.Duration
}

const (
	replayRetryInitialDelay = 10 * time.Millisecond
	replayRetryMaxDelay     = time.Second
)

// durableDispatchCtxKey marks a dispatch whose caller must not checkpoint
// until every internal delivery layer (including a coalescing replay
// coordinator) has actually finished.
type durableDispatchCtxKey struct{}

func (c *replayCoordinator[T]) saveOffset(ctx context.Context, event T, offset Offset) {
	if err := c.subStore.SaveOffset(ctx, c.subscriptionID, offset); err != nil && c.bus.persistenceErrorHandler != nil {
		c.bus.persistenceErrorHandler(event, c.eventType,
			fmt.Errorf("failed to save offset for subscription %q: %w", c.subscriptionID, err))
	}
}

func (c *replayCoordinator[T]) processStored(ctx context.Context, stored *StoredEvent) error {
	eventData, eventTypeName := stored.Data, stored.Type
	if c.bus.upcastRegistry != nil {
		var err error
		eventData, eventTypeName, err = c.bus.upcastRegistry.apply(eventData, eventTypeName)
		if err != nil {
			return fmt.Errorf("upcast event at offset %s: %w", stored.Offset, err)
		}
	}
	if eventTypeName != c.typeName {
		return nil
	}

	var event T
	if err := json.Unmarshal(eventData, &event); err != nil {
		if c.userHandler.replayErrorPolicy != ReplaySkip {
			return fmt.Errorf("decode event at offset %s: %w", stored.Offset, err)
		}
		if c.bus.persistenceErrorHandler != nil {
			c.bus.persistenceErrorHandler(stored, c.eventType,
				fmt.Errorf("skipping undecodable event at offset %s for subscription %q: %w", stored.Offset, c.subscriptionID, err))
		}
		var zero T
		skipCtx := contextForStoredEvent(ctx, stored)
		if err := skipCtx.Err(); err != nil {
			return err
		}
		c.saveOffset(skipCtx, zero, stored.Offset)
		return nil
	}

	// A successful decode is durable evidence for this Go type even if a
	// filter rejects it or a later handler/read fails. Keep that identity
	// sticky so another type can never reinterpret the proven history.
	c.typeClaim.Commit()
	if c.userHandler.filter != nil {
		matches, err := callFilter(c.userHandler, event, c.bus.panicHandler)
		if err != nil {
			return err
		}
		if !matches {
			return nil
		}
	}

	deliveryCtx := contextForStoredEvent(ctx, stored)
	if err := callHandlerWithContext(c.userHandler, deliveryCtx, event, c.bus.panicHandler,
		c.bus.observability, eventTypeName, false); err != nil {
		return err
	}
	if err := deliveryCtx.Err(); err != nil {
		return err
	}
	c.saveOffset(deliveryCtx, event, stored.Offset)
	return nil
}

// reachedCapturedTail asks the store to order its own opaque offsets when it
// exposes that optional capability. Equality remains sufficient for stores
// whose Read results always expose the exact captured tail token.
func (c *replayCoordinator[T]) reachedCapturedTail(offset, barrier Offset) (bool, error) {
	if offset == barrier {
		return true, nil
	}
	comparer, ok := c.bus.store.(EventStoreOffsetComparer)
	if !ok {
		return false, nil
	}
	order, err := comparer.CompareOffsets(offset, barrier)
	if err != nil {
		return false, fmt.Errorf("compare replay offset %q with captured tail %q: %w", offset, barrier, err)
	}
	return order >= 0, nil
}

// drainToCapturedTail snapshots the current concrete tail, then issues
// EventStore.Read calls with finite batch targets until that barrier is
// processed. A store may exceed a target to keep an indivisible resume token
// safe; a store-provided offset comparer bounds any overrun to that one crossing
// unit. It never uses ReadStream: a streaming implementation may keep observing
// concurrent appends and starve subscription setup indefinitely.
func (c *replayCoordinator[T]) drainToCapturedTail(ctx context.Context) (err error) {
	// EventStore, upcast, filter, and lifecycle callbacks are extension points.
	// A panic from any of them must become a failed durable attempt rather than
	// unwinding past signal's running/token cleanup and wedging the coordinator.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("resumable subscription %q drain panic: %v", c.subscriptionID, recovered)
		}
	}()

	if err := ctx.Err(); err != nil {
		return err
	}
	if c.isClosed() {
		return nil
	}
	events, barrier, err := c.bus.store.Read(ctx, OffsetNewest, 0)
	if err != nil {
		return fmt.Errorf("capture replay tail: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if barrier == OffsetNewest {
		return fmt.Errorf("capture replay tail: EventStore.Read(OffsetNewest) returned symbolic offset %q; a concrete resumable tail is required", OffsetNewest)
	}
	if len(events) != 0 {
		return fmt.Errorf("capture replay tail: EventStore.Read(OffsetNewest) returned %d event(s), want none", len(events))
	}
	reached, err := c.reachedCapturedTail(c.scanOffset, barrier)
	if err != nil {
		return err
	}
	if reached {
		return nil
	}

	batchSize := c.bus.replayBatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if c.isClosed() {
			return nil
		}
		batchStart := c.scanOffset
		events, next, err := c.bus.store.Read(ctx, batchStart, batchSize)
		if err != nil {
			return fmt.Errorf("read events after %s: %w", batchStart, err)
		}
		if len(events) == 0 {
			if next == batchStart {
				return nil
			}
			c.scanOffset = next
			reached, err := c.reachedCapturedTail(c.scanOffset, barrier)
			if err != nil {
				return err
			}
			if reached {
				return nil
			}
			continue
		}

		for _, stored := range events {
			if err := ctx.Err(); err != nil {
				return err
			}
			if c.isClosed() {
				return nil
			}
			if err := c.processStored(ctx, stored); err != nil {
				// Keep scanOffset at batchStart. Stores with chunk-level offsets
				// can then redeliver the whole batch without skipping a failed
				// sibling that shares the same offset.
				return err
			}
			if err := ctx.Err(); err != nil {
				return err
			}
			reached, err := c.reachedCapturedTail(stored.Offset, barrier)
			if err != nil {
				return err
			}
			if reached {
				// Keep the actual processed, resume-safe token. A comparer may
				// report that an indivisible Read unit crossed the captured tail.
				c.scanOffset = stored.Offset
				return nil
			}
		}
		if next == batchStart {
			return fmt.Errorf("store returned non-advancing offset %s while draining subscription %q", batchStart, c.subscriptionID)
		}
		c.scanOffset = next
		reached, err := c.reachedCapturedTail(c.scanOffset, barrier)
		if err != nil {
			return err
		}
		if reached {
			return nil
		}
	}
}

func (c *replayCoordinator[T]) isClosed() bool {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	return closed
}

// notifyWaitersLocked advances the completion generation. Closing and
// replacing the channel while holding mu gives deactivate and a completing
// leader a single owner for every close, even when Clear races a live drain.
func (c *replayCoordinator[T]) notifyWaitersLocked() {
	close(c.waitCh)
	c.waitCh = make(chan struct{})
}

// activate prepares the live handoff while the coordinator remains gated.
// When setup-time publishes are already pending, their local marker proves a
// second synchronous barrier could chase a hot producer, so final setup hands
// that catch-up to the tracked background drain instead.
func (c *replayCoordinator[T]) activate(ctx context.Context) (needsCatchUp bool, err error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false, fmt.Errorf("resumable subscription %q was removed during setup", c.subscriptionID)
	}
	if c.pending {
		c.mu.Unlock()
		return true, nil
	}
	c.running = true
	c.mu.Unlock()

	err = c.drainToCapturedTail(ctx)
	c.mu.Lock()
	c.running = false
	if c.closed && err == nil {
		err = fmt.Errorf("resumable subscription %q was removed during setup", c.subscriptionID)
	}
	if err != nil {
		c.closed = true
	}
	needsCatchUp = err == nil && c.pending
	finishTracked := c.tracked && err != nil
	if finishTracked {
		c.tracked = false
	}
	if err != nil {
		c.notifyWaitersLocked()
	}
	c.mu.Unlock()
	if finishTracked {
		c.bus.asyncFinished()
	}
	return needsCatchUp, err
}

// finalizeActivation opens the marker only after every remaining setup step
// has succeeded. A pending-work token already bridges to scheduleDrain; create
// one defensively if a synthetic coordinator violates that invariant.
func (c *replayCoordinator[T]) finalizeActivation(needsCatchUp bool) bool {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return false
	}
	c.active = true
	needsCatchUp = needsCatchUp || c.pending
	if needsCatchUp && !c.tracked {
		c.bus.asyncStarted()
		c.tracked = true
	}
	if needsCatchUp {
		// Reserve the goroutine token before waking contenders. One of them may
		// otherwise consume the pending token before scheduleDrain is accounted.
		c.scheduleDrain(0)
	}
	c.notifyWaitersLocked()
	c.mu.Unlock()
	return true
}

func (c *replayCoordinator[T]) deactivate() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	c.active = false
	c.pending = false
	c.retryWaiting = false
	if c.done != nil {
		close(c.done)
	}
	finishTracked := c.tracked && !c.running
	if finishTracked {
		c.tracked = false
	}
	c.notifyWaitersLocked()
	c.mu.Unlock()
	if finishTracked {
		c.bus.asyncFinished()
	}
}

// signal coalesces concurrent and reentrant publishes. The leader drains to a
// captured tail barrier; followers return after recording pending work,
// avoiding a self-deadlock when a resumable handler publishes its own event
// type.
func (c *replayCoordinator[T]) signal(ctx context.Context) error {
	mustWait, _ := ctx.Value(durableDispatchCtxKey{}).(bool)
	for {
		c.mu.Lock()
		if c.closed {
			c.mu.Unlock()
			return nil
		}
		if !c.active || c.running || c.retryWaiting {
			c.pending = true
			if !c.tracked {
				// Register before unlocking: a coalesced publisher may return as
				// soon as mu is released, and Wait must already see its deferred
				// replay work at that point.
				c.bus.asyncStarted()
				c.tracked = true
			}
			if !mustWait {
				c.mu.Unlock()
				return nil
			}
			waitCh := c.waitCh
			c.mu.Unlock()
			select {
			case <-waitCh:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		c.running = true
		c.pending = false
		c.mu.Unlock()
		break
	}

	// Live resumable delivery is defined by the stored envelope, not arbitrary
	// values on whichever publisher happened to lead this drain. Preserve only
	// cancellation/deadline behavior from that operation context.
	err := c.drainToCapturedTail(valueStrippedContext{Context: ctx})
	c.mu.Lock()
	c.running = false
	pending := c.pending
	c.pending = false
	closed := c.closed
	schedule := !closed && (pending || err != nil)
	retryDelay := time.Duration(0)
	if err != nil && !closed {
		if c.retryDelay == 0 {
			c.retryDelay = replayRetryInitialDelay
		} else {
			c.retryDelay = min(c.retryDelay*2, replayRetryMaxDelay)
		}
		retryDelay = c.retryDelay
		c.retryWaiting = true
	} else if err == nil {
		c.retryDelay = 0
		c.retryWaiting = false
	}
	if schedule && !c.tracked {
		// The leading synchronous signal has no token of its own. Once it
		// defers a retry, account for that pending work before it returns.
		c.bus.asyncStarted()
		c.tracked = true
	}
	finishTracked := c.tracked && !schedule
	if finishTracked {
		c.tracked = false
	}
	if schedule {
		// Start accounting while leadership is still closed under mu. The new
		// goroutine can safely block on mu until this completion is published.
		c.scheduleDrain(retryDelay)
	}
	c.notifyWaitersLocked()
	c.mu.Unlock()
	if closed {
		err = nil
	}
	if err != nil && c.bus.persistenceErrorHandler != nil {
		c.bus.persistenceErrorHandler(nil, c.eventType,
			fmt.Errorf("resumable subscription %q live drain: %w", c.subscriptionID, err))
	}
	if finishTracked {
		c.bus.asyncFinished()
	}
	return err
}

func (c *replayCoordinator[T]) scheduleDrain(delay time.Duration) {
	// The pending-work token bridges the gap up to this call. Keep a separate
	// goroutine token as well: a woken durable waiter can win leadership and
	// release the pending token before this goroutine gets scheduled.
	c.bus.asyncStarted()
	go func() {
		defer c.bus.asyncFinished()
		if delay > 0 {
			timer := time.NewTimer(delay)
			defer timer.Stop()
			select {
			case <-timer.C:
			case <-c.done:
				return
			}
			c.mu.Lock()
			c.retryWaiting = false
			c.mu.Unlock()
		}
		_ = c.signal(context.Background())
	}()
}

type valueStrippedContext struct {
	context.Context
}

func (valueStrippedContext) Value(any) any { return nil }

// storedDeliveryContext masks any envelope values carried by the operation
// context before restoring the exact stored event. Without masking, an empty
// field on an older event could accidentally inherit ID/metadata from the
// publisher whose signal happened to lead a coalesced drain.
type storedDeliveryContext struct {
	context.Context
	stored *StoredEvent
}

func (c storedDeliveryContext) Value(key any) any {
	switch key.(type) {
	case offsetCtxKey:
		// OffsetOldest is also a valid resume token on stores whose first
		// chunk assigns chunk-start offsets. Preserve that exact empty Offset
		// while still masking any stale offset carried by the signal context.
		return c.stored.Offset
	case eventIDCtxKey:
		if c.stored.ID == "" {
			return nil
		}
		return c.stored.ID
	case metadataCtxKey:
		if len(c.stored.Metadata) == 0 {
			return nil
		}
		return c.stored.Metadata
	default:
		return c.Context.Value(key)
	}
}

// contextForStoredEvent gives replayed and followed handlers the same envelope
// accessors as handlers on a successful live publish while preserving
// cancellation, deadlines, and unrelated caller values.
func contextForStoredEvent(ctx context.Context, stored *StoredEvent) context.Context {
	return storedDeliveryContext{Context: ctx, stored: stored}
}

// MemoryStore is a simple in-memory implementation of EventStore and
// SubscriptionStore. It owns copies of mutable payload and metadata values:
// callers may reuse an Event after Append or mutate a Read/ReadStream result
// without changing the stored history.
type MemoryStore struct {
	events        []*StoredEvent
	subscriptions map[string]Offset
	nextOffset    int64
	mu            sync.RWMutex
}

// Ensure MemoryStore implements all required interfaces
var _ EventStore = (*MemoryStore)(nil)
var _ EventStoreOffsetComparer = (*MemoryStore)(nil)
var _ EventStoreStreamer = (*MemoryStore)(nil)
var _ SubscriptionStore = (*MemoryStore)(nil)
var _ SubscriptionStoreLookup = (*MemoryStore)(nil)

// NewMemoryStore creates a new in-memory event store
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		events:        make([]*StoredEvent, 0),
		subscriptions: make(map[string]Offset),
	}
}

// cloneStoredEvent transfers ownership of a stored envelope across a
// MemoryStore boundary. StoredEvent itself contains a mutable byte slice and
// map, so copying only its pointer (or struct header) would let callers rewrite
// the store's history after Append or through a Read result.
func cloneStoredEvent(event *StoredEvent) *StoredEvent {
	cloned := *event
	cloned.Data = bytes.Clone(event.Data)
	cloned.Metadata = maps.Clone(event.Metadata)
	return &cloned
}

// Append implements EventStore
func (m *MemoryStore) Append(ctx context.Context, event *Event) (Offset, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.nextOffset++
	// Use zero-padded format for correct lexicographic ordering
	offset := Offset(fmt.Sprintf("%020d", m.nextOffset))

	stored := &StoredEvent{
		Offset:    offset,
		ID:        event.ID,
		Origin:    event.Origin,
		Type:      event.Type,
		Data:      bytes.Clone(event.Data),
		Metadata:  maps.Clone(event.Metadata),
		Timestamp: event.Timestamp,
	}
	m.events = append(m.events, stored)
	return offset, nil
}

// searchAfter returns the index of the first event with offset > from.
// Events are stored in ascending, zero-padded offset order, so binary
// search applies. Caller must hold at least a read lock.
func (m *MemoryStore) searchAfter(from Offset) int {
	if from == OffsetOldest {
		return 0
	}
	if from == OffsetNewest {
		// "$" is not a stored offset ("$" sorts before the zero-padded
		// digits, so the binary search would wrongly return 0 = replay
		// everything); the tail means "after every current event".
		return len(m.events)
	}
	return sort.Search(len(m.events), func(i int) bool {
		return m.events[i].Offset > from
	})
}

// tailOffset returns the offset of the last stored event, or OffsetOldest
// when the store is empty. Caller must hold at least a read lock.
func (m *MemoryStore) tailOffset() Offset {
	if len(m.events) == 0 {
		return OffsetOldest
	}
	return m.events[len(m.events)-1].Offset
}

// Read implements EventStore
func (m *MemoryStore) Read(ctx context.Context, from Offset, limit int) ([]*StoredEvent, Offset, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	start := m.searchAfter(from)
	end := len(m.events)
	if limit > 0 && start+limit < end {
		end = start + limit
	}

	if start == end {
		if from == OffsetNewest {
			// Resolve "$" to a concrete, resumable position: echoing the
			// symbolic offset back would make the caller chase a
			// perpetually moving tail.
			return nil, m.tailOffset(), nil
		}
		return nil, from, nil
	}

	result := make([]*StoredEvent, end-start)
	for i, event := range m.events[start:end] {
		result[i] = cloneStoredEvent(event)
	}
	return result, result[len(result)-1].Offset, nil
}

// CompareOffsets orders two concrete offsets issued by this MemoryStore.
// OffsetNewest is a symbolic query sentinel and cannot be compared.
func (m *MemoryStore) CompareOffsets(left, right Offset) (int, error) {
	if left == OffsetNewest || right == OffsetNewest {
		return 0, fmt.Errorf("memory store cannot compare symbolic offset %q", OffsetNewest)
	}
	switch {
	case left < right:
		return -1, nil
	case left > right:
		return 1, nil
	default:
		return 0, nil
	}
}

// SaveOffset implements SubscriptionStore.
// OffsetNewest is resolved to the current tail at save time, so the stored
// value is always a concrete, resumable position.
func (m *MemoryStore) SaveOffset(ctx context.Context, subscriptionID string, offset Offset) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if offset == OffsetNewest {
		offset = m.tailOffset()
	}
	m.subscriptions[subscriptionID] = offset
	return nil
}

// LookupOffset implements SubscriptionStoreLookup.
func (m *MemoryStore) LookupOffset(ctx context.Context, subscriptionID string) (Offset, bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	offset, ok := m.subscriptions[subscriptionID]
	if !ok {
		return OffsetOldest, false, nil
	}
	return offset, true, nil
}

// LoadOffset implements SubscriptionStore.
func (m *MemoryStore) LoadOffset(ctx context.Context, subscriptionID string) (Offset, error) {
	offset, _, err := m.LookupOffset(ctx, subscriptionID)
	return offset, err
}

// ReadStream implements EventStoreStreamer for memory-efficient event iteration.
// Note: This takes a filtered snapshot of matching events to avoid holding the lock
// during iteration, which could cause deadlocks if handlers call other store methods.
func (m *MemoryStore) ReadStream(ctx context.Context, from Offset) iter.Seq2[*StoredEvent, error] {
	return func(yield func(*StoredEvent, error) bool) {
		// Take a snapshot to avoid holding lock during iteration
		m.mu.RLock()
		start := m.searchAfter(from)
		events := make([]*StoredEvent, len(m.events)-start)
		for i, event := range m.events[start:] {
			events[i] = cloneStoredEvent(event)
		}
		m.mu.RUnlock()

		for _, event := range events {
			// Check context cancellation
			select {
			case <-ctx.Done():
				yield(nil, ctx.Err())
				return
			default:
			}

			if !yield(event, nil) {
				return // Consumer stopped iteration
			}
		}
	}
}
