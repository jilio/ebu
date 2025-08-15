package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sync"
)

// Command represents an intent to change the system state
type Command interface{}

// Query represents a request for information from the system
type Query interface{}

// CommandHandler processes commands and returns events to be published
type CommandHandler[C Command] func(ctx context.Context, cmd C) ([]any, error)

// QueryHandler processes queries and returns the requested data
type QueryHandler[Q Query, R any] func(ctx context.Context, query Q) (R, error)

// Aggregate represents an aggregate root in DDD
type Aggregate interface {
	// GetID returns the aggregate's unique identifier
	GetID() string
	// GetVersion returns the current version of the aggregate
	GetVersion() int64
	// GetUncommittedEvents returns events that haven't been persisted yet
	GetUncommittedEvents() []any
	// MarkEventsAsCommitted clears the uncommitted events
	MarkEventsAsCommitted()
	// LoadFromHistory rebuilds the aggregate from historical events
	LoadFromHistory(events []any) error
}

// BaseAggregate provides common functionality for aggregates
type BaseAggregate struct {
	ID                string
	Version           int64
	uncommittedEvents []any
	mu                sync.RWMutex
}

func (a *BaseAggregate) GetID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ID
}

func (a *BaseAggregate) GetVersion() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Version
}

func (a *BaseAggregate) GetUncommittedEvents() []any {
	a.mu.RLock()
	defer a.mu.RUnlock()
	events := make([]any, len(a.uncommittedEvents))
	copy(events, a.uncommittedEvents)
	return events
}

func (a *BaseAggregate) MarkEventsAsCommitted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.uncommittedEvents = nil
}

// RaiseEvent adds an event to the uncommitted events and applies it
func (a *BaseAggregate) RaiseEvent(event any) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.uncommittedEvents = append(a.uncommittedEvents, event)
	a.Version++
}

// CreateSnapshot creates a JSON snapshot of the aggregate state
// Subclasses should override this to include their specific fields
func (a *BaseAggregate) CreateSnapshot() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	// Create a snapshot with just the base fields
	// Derived aggregates should override to include their state
	snapshot := map[string]interface{}{
		"id":      a.ID,
		"version": a.Version,
	}

	return json.Marshal(snapshot)
}

// RestoreFromSnapshot restores aggregate state from a JSON snapshot
// Subclasses should override this to restore their specific fields
func (a *BaseAggregate) RestoreFromSnapshot(data []byte, version int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var snapshot map[string]interface{}
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	// Restore base fields
	if id, ok := snapshot["id"].(string); ok {
		a.ID = id
	}

	a.Version = version
	a.uncommittedEvents = nil

	return nil
}

// Projection represents a read model that is updated by events
type Projection interface {
	// Handle processes an event to update the projection
	Handle(ctx context.Context, event any) error
	// GetID returns the projection's identifier
	GetID() string
}

// ProjectionBuilder helps build projections from events
type ProjectionBuilder struct {
	bus         *EventBus
	projections map[string]Projection
	mu          sync.RWMutex
}

// NewProjectionBuilder creates a new projection builder
func NewProjectionBuilder(bus *EventBus) *ProjectionBuilder {
	return &ProjectionBuilder{
		bus:         bus,
		projections: make(map[string]Projection),
	}
}

// Register adds a projection and subscribes it to relevant events
func (pb *ProjectionBuilder) Register(projection Projection, eventTypes ...any) error {
	pb.mu.Lock()
	pb.projections[projection.GetID()] = projection
	pb.mu.Unlock()

	// Subscribe to each event type
	for _, eventType := range eventTypes {
		t := reflect.TypeOf(eventType)
		if t == nil {
			return errors.New("nil event type")
		}

		// Subscribe based on the event type
		switch t.Kind() {
		case reflect.Ptr:
			t = t.Elem()
		}

		// Create a handler for this specific event type
		// We need to capture the projection in a closure-safe way
		capturedProjection := projection
		handler := func(ctx context.Context, event any) error {
			return capturedProjection.Handle(ctx, event)
		}

		// Use reflection to create the appropriate subscription
		if err := pb.subscribeToEvent(t, handler); err != nil {
			return fmt.Errorf("failed to subscribe projection to %v: %w", t, err)
		}
	}

	return nil
}

func (pb *ProjectionBuilder) subscribeToEvent(eventType reflect.Type, handler func(context.Context, any) error) error {
	// Check for nil bus to return an error instead of panicking
	if pb.bus == nil {
		return errors.New("bus is nil")
	}

	// Create a wrapper that implements ContextHandler for the specific type
	wrapperFunc := func(ctx context.Context, event any) {
		_ = handler(ctx, event) // Ignore error for now in async handlers
	}

	// Create internal handler directly
	h := &internalHandler{
		handler:        wrapperFunc,
		handlerType:    reflect.TypeOf(wrapperFunc),
		eventType:      eventType,
		acceptsContext: true,
		async:          false, // Synchronous to maintain event order
	}

	pb.bus.mu.Lock()
	defer pb.bus.mu.Unlock()

	pb.bus.handlers[eventType] = append(pb.bus.handlers[eventType], h)
	return nil
}

// Get retrieves a projection by ID
func (pb *ProjectionBuilder) Get(id string) (Projection, bool) {
	pb.mu.RLock()
	defer pb.mu.RUnlock()
	p, ok := pb.projections[id]
	return p, ok
}

// CQRS provides a complete CQRS implementation
type CQRS struct {
	bus               *EventBus
	commandHandlers   map[reflect.Type]any
	queryHandlers     map[reflect.Type]any
	projectionBuilder *ProjectionBuilder
	mu                sync.RWMutex
}

// NewCQRS creates a new CQRS instance
func NewCQRS(bus *EventBus) *CQRS {
	return &CQRS{
		bus:               bus,
		commandHandlers:   make(map[reflect.Type]any),
		queryHandlers:     make(map[reflect.Type]any),
		projectionBuilder: NewProjectionBuilder(bus),
	}
}

// RegisterCommand registers a command handler
func (c *CQRS) RegisterCommand(handler any) error {
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return errors.New("handler must be a function")
	}

	if handlerType.NumIn() != 2 {
		return errors.New("command handler must have exactly 2 parameters (context.Context, Command)")
	}

	if handlerType.NumOut() != 2 {
		return errors.New("command handler must return ([]any, error)")
	}

	cmdType := handlerType.In(1)

	c.mu.Lock()
	c.commandHandlers[cmdType] = handler
	c.mu.Unlock()

	return nil
}

// RegisterQuery registers a query handler
func (c *CQRS) RegisterQuery(handler any) error {
	handlerType := reflect.TypeOf(handler)
	if handlerType.Kind() != reflect.Func {
		return errors.New("handler must be a function")
	}

	if handlerType.NumIn() != 2 {
		return errors.New("query handler must have exactly 2 parameters (context.Context, Query)")
	}

	if handlerType.NumOut() != 2 {
		return errors.New("query handler must return (Result, error)")
	}

	queryType := handlerType.In(1)

	c.mu.Lock()
	c.queryHandlers[queryType] = handler
	c.mu.Unlock()

	return nil
}

// ExecuteCommand executes a command and publishes resulting events
func (c *CQRS) ExecuteCommand(ctx context.Context, cmd Command) error {
	cmdType := reflect.TypeOf(cmd)

	c.mu.RLock()
	handler, ok := c.commandHandlers[cmdType]
	c.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler registered for command type %T", cmd)
	}

	// Call the handler using reflection
	handlerValue := reflect.ValueOf(handler)
	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(cmd),
	})

	// Check for error
	if !results[1].IsNil() {
		return results[1].Interface().(error)
	}

	// Publish events
	if !results[0].IsNil() {
		events := results[0].Interface().([]any)
		for _, event := range events {
			Publish(c.bus, event)
		}
	}

	return nil
}

// ExecuteQuery executes a query and returns the result
func (c *CQRS) ExecuteQuery(ctx context.Context, query Query) (any, error) {
	queryType := reflect.TypeOf(query)

	c.mu.RLock()
	handler, ok := c.queryHandlers[queryType]
	c.mu.RUnlock()

	if !ok {
		return nil, fmt.Errorf("no handler registered for query type %T", query)
	}

	// Call the handler using reflection
	handlerValue := reflect.ValueOf(handler)
	results := handlerValue.Call([]reflect.Value{
		reflect.ValueOf(ctx),
		reflect.ValueOf(query),
	})

	// Check for error
	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	return results[0].Interface(), nil
}

// RegisterProjection registers a projection with the CQRS system
func (c *CQRS) RegisterProjection(projection Projection, eventTypes ...any) error {
	return c.projectionBuilder.Register(projection, eventTypes...)
}

// GetProjection retrieves a projection by ID
func (c *CQRS) GetProjection(id string) (Projection, bool) {
	return c.projectionBuilder.Get(id)
}

// GetBus returns the underlying event bus
func (c *CQRS) GetBus() *EventBus {
	return c.bus
}
