package eventbus

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sync"
)

// UpcastFunc transforms event data from one version to another.
// It receives the raw JSON data and returns transformed data with the new type name.
type UpcastFunc func(data json.RawMessage) (json.RawMessage, string, error)

// UpcastErrorHandler is called when an upcast operation fails
type UpcastErrorHandler func(eventType string, data json.RawMessage, err error)

// Upcaster represents a transformation from one event type to another
type Upcaster struct {
	FromType string     // Source event type
	ToType   string     // Target event type
	Upcast   UpcastFunc // Transformation function
}

// upcastRegistry manages all registered upcasters.
// Each source type has at most one upcaster: apply follows a single chain
// (v1 -> v2 -> v3), so a second upcaster for the same source type could
// never run and is rejected at registration instead of silently ignored.
type upcastRegistry struct {
	upcasters    map[string]Upcaster // Map from source type to its upcaster
	mu           sync.RWMutex
	errorHandler UpcastErrorHandler
}

// newUpcastRegistry creates a new upcast registry
func newUpcastRegistry() *upcastRegistry {
	return &upcastRegistry{
		upcasters: make(map[string]Upcaster),
	}
}

// register adds an upcaster to the registry
func (r *upcastRegistry) register(fromType, toType string, upcast UpcastFunc) error {
	if fromType == "" || toType == "" {
		return fmt.Errorf("eventbus: upcast types cannot be empty")
	}
	if fromType == toType {
		return fmt.Errorf("eventbus: cannot upcast type to itself")
	}
	if upcast == nil {
		return fmt.Errorf("eventbus: upcast function cannot be nil")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if existing, ok := r.upcasters[fromType]; ok {
		return fmt.Errorf("eventbus: upcaster already registered for type %s (-> %s): each source type has exactly one successor; chain versions (v1 -> v2 -> v3) instead", fromType, existing.ToType)
	}

	// Check for circular dependencies
	if r.wouldCreateCycle(fromType, toType) {
		return fmt.Errorf("eventbus: upcast would create circular dependency")
	}

	r.upcasters[fromType] = Upcaster{
		FromType: fromType,
		ToType:   toType,
		Upcast:   upcast,
	}
	return nil
}

// wouldCreateCycle checks if adding an upcast would create a circular dependency
func (r *upcastRegistry) wouldCreateCycle(fromType, toType string) bool {
	// Check if there's already a path from toType back to fromType
	visited := make(map[string]bool)
	return r.hasCycleDFS(toType, fromType, visited)
}

// hasCycleDFS performs depth-first search to detect cycles
func (r *upcastRegistry) hasCycleDFS(current, target string, visited map[string]bool) bool {
	if current == target {
		return true
	}
	if visited[current] {
		return false
	}
	visited[current] = true

	if upcaster, ok := r.upcasters[current]; ok {
		return r.hasCycleDFS(upcaster.ToType, target, visited)
	}
	return false
}

// apply attempts to apply upcasts to transform data to the latest version.
//
// The registry lock is only held for map lookups, never while a user upcast
// function runs: upcast functions may therefore safely call back into the
// registry (e.g. RegisterUpcast) without deadlocking. A registration that
// races with apply may or may not be observed by the in-flight chain.
func (r *upcastRegistry) apply(data json.RawMessage, eventType string) (json.RawMessage, string, error) {
	currentData := data
	currentType := eventType
	appliedTypes := make(map[string]bool) // Prevent infinite loops

	for {
		// Mark this type as processed
		appliedTypes[currentType] = true

		// Find the next upcaster and snapshot the error handler under the
		// read lock; user code runs after the lock is released.
		r.mu.RLock()
		upcaster, found := r.upcasters[currentType]
		errorHandler := r.errorHandler
		r.mu.RUnlock()

		if !found {
			break // No more upcasts available
		}

		// Check for loops
		if appliedTypes[upcaster.ToType] {
			return data, eventType, fmt.Errorf("eventbus: upcast loop detected")
		}

		// Apply the upcast
		newData, newType, err := upcaster.Upcast(currentData)
		if err != nil {
			if errorHandler != nil {
				errorHandler(currentType, currentData, err)
			}
			return data, eventType, fmt.Errorf("eventbus: upcast failed from %s to %s: %w",
				upcaster.FromType, upcaster.ToType, err)
		}

		currentData = newData
		currentType = newType
	}

	return currentData, currentType, nil
}

// clear removes all registered upcasters
func (r *upcastRegistry) clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.upcasters = make(map[string]Upcaster)
}

// clearType removes all upcasters for a specific source type
func (r *upcastRegistry) clearType(eventType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.upcasters, eventType)
}

// RegisterUpcast registers a type-safe upcast function.
//
// From and To must be concrete types. Interface types are rejected with an
// error: upcasts are keyed by type name, and events are stored under their
// concrete type's name (or TypeNamer name), so an upcast keyed by an
// interface type name could never match a stored event — a silently dead
// registration.
func RegisterUpcast[From any, To any](bus *EventBus, upcast func(From) To) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if upcast == nil {
		return fmt.Errorf("eventbus: upcast function cannot be nil")
	}

	from := reflect.TypeOf((*From)(nil)).Elem()
	to := reflect.TypeOf((*To)(nil)).Elem()
	if from.Kind() == reflect.Interface || to.Kind() == reflect.Interface {
		return fmt.Errorf("eventbus: cannot register upcast between interface types (%s -> %s): events are stored under concrete type names, so an interface-keyed upcast would never match", from, to)
	}

	fromType := from.String()
	toType := to.String()

	upcastFunc := func(data json.RawMessage) (json.RawMessage, string, error) {
		var from From
		if err := json.Unmarshal(data, &from); err != nil {
			return nil, "", fmt.Errorf("unmarshal source: %w", err)
		}

		to := upcast(from)

		newData, err := json.Marshal(to)
		if err != nil {
			return nil, "", fmt.Errorf("marshal target: %w", err)
		}

		return newData, toType, nil
	}

	return bus.upcastRegistry.register(fromType, toType, upcastFunc)
}

// RegisterUpcastFunc registers a raw upcast function for complex transformations
func RegisterUpcastFunc(bus *EventBus, fromType, toType string, upcast UpcastFunc) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	return bus.upcastRegistry.register(fromType, toType, upcast)
}

// WithUpcast adds an upcast function during bus creation.
//
// It panics if the registration is invalid (empty types, self-upcast, nil
// function, or a circular dependency): these are programming errors that
// would otherwise be silently ignored at startup. Use RegisterUpcastFunc if
// you need an error value instead.
func WithUpcast(fromType, toType string, upcast UpcastFunc) Option {
	return func(bus *EventBus) {
		if err := bus.upcastRegistry.register(fromType, toType, upcast); err != nil {
			panic(fmt.Sprintf("eventbus: WithUpcast(%q, %q): %v", fromType, toType, err))
		}
	}
}

// setErrorHandler sets the error handler under the registry lock so it is
// safe to call concurrently with apply.
func (r *upcastRegistry) setErrorHandler(handler UpcastErrorHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.errorHandler = handler
}

// WithUpcastErrorHandler sets the error handler for upcast failures
func WithUpcastErrorHandler(handler UpcastErrorHandler) Option {
	return func(bus *EventBus) {
		bus.upcastRegistry.setErrorHandler(handler)
	}
}

// SetUpcastErrorHandler sets the upcast error handler at runtime.
// Safe to call concurrently with replay/publish.
func (bus *EventBus) SetUpcastErrorHandler(handler UpcastErrorHandler) {
	bus.upcastRegistry.setErrorHandler(handler)
}

// ClearUpcasts removes all registered upcasters
func (bus *EventBus) ClearUpcasts() {
	bus.upcastRegistry.clear()
}

// ClearUpcastsForType removes all upcasters for a specific source type
func (bus *EventBus) ClearUpcastsForType(eventType string) {
	bus.upcastRegistry.clearType(eventType)
}
