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

// upcastRegistry manages all registered upcasters
type upcastRegistry struct {
	upcasters    map[string][]Upcaster // Map from source type to list of upcasters
	mu           sync.RWMutex
	errorHandler UpcastErrorHandler
}

// newUpcastRegistry creates a new upcast registry
func newUpcastRegistry() *upcastRegistry {
	return &upcastRegistry{
		upcasters: make(map[string][]Upcaster),
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

	// Check for circular dependencies
	if r.wouldCreateCycle(fromType, toType) {
		return fmt.Errorf("eventbus: upcast would create circular dependency")
	}

	upcaster := Upcaster{
		FromType: fromType,
		ToType:   toType,
		Upcast:   upcast,
	}

	r.upcasters[fromType] = append(r.upcasters[fromType], upcaster)
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

	for _, upcaster := range r.upcasters[current] {
		if r.hasCycleDFS(upcaster.ToType, target, visited) {
			return true
		}
	}
	return false
}

// apply attempts to apply upcasts to transform data to the latest version
func (r *upcastRegistry) apply(data json.RawMessage, eventType string) (json.RawMessage, string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Check if there are any upcasters for this type
	upcasters, exists := r.upcasters[eventType]
	if !exists || len(upcasters) == 0 {
		return data, eventType, nil // No upcasting needed
	}

	// Apply upcasts in chain
	currentData := data
	currentType := eventType
	appliedTypes := make(map[string]bool) // Prevent infinite loops

	for {
		// Mark this type as processed
		appliedTypes[currentType] = true

		// Find upcasters for current type
		upcasters, exists := r.upcasters[currentType]
		if !exists || len(upcasters) == 0 {
			break // No more upcasts available
		}

		// Apply the first available upcaster (could be enhanced to choose best path)
		upcaster := upcasters[0]

		// Check for loops
		if appliedTypes[upcaster.ToType] {
			return data, eventType, fmt.Errorf("eventbus: upcast loop detected")
		}

		// Apply the upcast
		newData, newType, err := upcaster.Upcast(currentData)
		if err != nil {
			if r.errorHandler != nil {
				r.errorHandler(currentType, currentData, err)
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
	r.upcasters = make(map[string][]Upcaster)
}

// clearType removes all upcasters for a specific source type
func (r *upcastRegistry) clearType(eventType string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.upcasters, eventType)
}

// RegisterUpcast registers a type-safe upcast function
func RegisterUpcast[From any, To any](bus *EventBus, upcast func(From) To) error {
	if bus == nil {
		return fmt.Errorf("eventbus: bus cannot be nil")
	}
	if upcast == nil {
		return fmt.Errorf("eventbus: upcast function cannot be nil")
	}

	fromType := reflect.TypeOf((*From)(nil)).Elem().String()
	toType := reflect.TypeOf((*To)(nil)).Elem().String()

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

// WithUpcast adds an upcast function during bus creation
func WithUpcast(fromType, toType string, upcast UpcastFunc) Option {
	return func(bus *EventBus) {
		bus.upcastRegistry.register(fromType, toType, upcast)
	}
}

// WithUpcastErrorHandler sets the error handler for upcast failures
func WithUpcastErrorHandler(handler UpcastErrorHandler) Option {
	return func(bus *EventBus) {
		bus.upcastRegistry.errorHandler = handler
	}
}

// SetUpcastErrorHandler sets the upcast error handler at runtime
func (bus *EventBus) SetUpcastErrorHandler(handler UpcastErrorHandler) {
	bus.upcastRegistry.errorHandler = handler
}

// ClearUpcasts removes all registered upcasters
func (bus *EventBus) ClearUpcasts() {
	bus.upcastRegistry.clear()
}

// ClearUpcastsForType removes all upcasters for a specific source type
func (bus *EventBus) ClearUpcastsForType(eventType string) {
	bus.upcastRegistry.clearType(eventType)
}
