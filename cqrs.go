package eventbus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

// Command represents an intent to change the system state
type Command any

// Query represents a request for information from the system
type Query any

// CommandHandler processes commands and returns events to be published
type CommandHandler[C Command, E any] func(ctx context.Context, cmd C) ([]E, error)

// QueryHandler processes queries and returns the requested data
type QueryHandler[Q Query, R any] func(ctx context.Context, query Q) (R, error)

// Aggregate represents an aggregate root in DDD
type Aggregate[E any] interface {
	// GetID returns the aggregate's unique identifier
	GetID() string
	// GetVersion returns the current version of the aggregate
	GetVersion() int64
	// GetUncommittedEvents returns events that haven't been persisted yet
	GetUncommittedEvents() []E
	// MarkEventsAsCommitted clears the uncommitted events
	MarkEventsAsCommitted()
	// LoadFromHistory rebuilds the aggregate from historical events
	LoadFromHistory(events []E) error
	// Apply handles an event to update the aggregate state
	Apply(event E) error
	// CreateSnapshot creates a snapshot of the aggregate state
	CreateSnapshot() ([]byte, error)
	// RestoreFromSnapshot restores aggregate state from a snapshot
	RestoreFromSnapshot(data []byte, version int64) error
}

// BaseAggregate provides common functionality for aggregates
type BaseAggregate[E any] struct {
	ID                string
	Version           int64
	uncommittedEvents []E
	mu                sync.RWMutex
	// ApplyFunc is called to apply events to the aggregate
	ApplyFunc func(event E) error
}

func (a *BaseAggregate[E]) GetID() string {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.ID
}

func (a *BaseAggregate[E]) GetVersion() int64 {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.Version
}

func (a *BaseAggregate[E]) GetUncommittedEvents() []E {
	a.mu.RLock()
	defer a.mu.RUnlock()
	events := make([]E, len(a.uncommittedEvents))
	copy(events, a.uncommittedEvents)
	return events
}

func (a *BaseAggregate[E]) MarkEventsAsCommitted() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.uncommittedEvents = nil
}

// RaiseEvent adds an event to the uncommitted events and applies it
func (a *BaseAggregate[E]) RaiseEvent(event E) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Apply the event if a handler is set
	if a.ApplyFunc != nil {
		if err := a.ApplyFunc(event); err != nil {
			return err
		}
	}

	a.uncommittedEvents = append(a.uncommittedEvents, event)
	a.Version++
	return nil
}

// Apply handles an event to update the aggregate state
func (a *BaseAggregate[E]) Apply(event E) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.ApplyFunc != nil {
		if err := a.ApplyFunc(event); err != nil {
			return err
		}
	}
	a.Version++
	return nil
}

// LoadFromHistory rebuilds the aggregate from historical events
func (a *BaseAggregate[E]) LoadFromHistory(events []E) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	for _, event := range events {
		if a.ApplyFunc != nil {
			if err := a.ApplyFunc(event); err != nil {
				return err
			}
		}
		a.Version++
	}
	return nil
}

// CreateSnapshot creates a JSON snapshot of the aggregate state
func (a *BaseAggregate[E]) CreateSnapshot() ([]byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	snapshot := map[string]any{
		"id":      a.ID,
		"version": a.Version,
	}

	return json.Marshal(snapshot)
}

// RestoreFromSnapshot restores aggregate state from a JSON snapshot
func (a *BaseAggregate[E]) RestoreFromSnapshot(data []byte, version int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	var snapshot map[string]any
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return fmt.Errorf("failed to unmarshal snapshot: %w", err)
	}

	if id, ok := snapshot["id"].(string); ok {
		a.ID = id
	}

	a.Version = version
	a.uncommittedEvents = nil

	return nil
}

// Projection represents a read model that is updated by events
type Projection[E any] interface {
	// Handle processes an event to update the projection
	Handle(ctx context.Context, event E) error
	// GetID returns the projection's identifier
	GetID() string
}

// CommandBus handles command routing and execution
type CommandBus[C Command, E any] struct {
	handlers map[string]CommandHandler[C, E]
	eventBus *EventBus
	mu       sync.RWMutex
	
	// Options
	preHandler  func(ctx context.Context, cmdType string, cmd C) error
	postHandler func(ctx context.Context, cmdType string, cmd C, events []E, err error)
}

// CommandBusOption configures a CommandBus
type CommandBusOption[C Command, E any] func(*CommandBus[C, E])

// WithCommandPreHandler sets a pre-execution handler for commands
func WithCommandPreHandler[C Command, E any](handler func(ctx context.Context, cmdType string, cmd C) error) CommandBusOption[C, E] {
	return func(cb *CommandBus[C, E]) {
		cb.preHandler = handler
	}
}

// WithCommandPostHandler sets a post-execution handler for commands
func WithCommandPostHandler[C Command, E any](handler func(ctx context.Context, cmdType string, cmd C, events []E, err error)) CommandBusOption[C, E] {
	return func(cb *CommandBus[C, E]) {
		cb.postHandler = handler
	}
}

// NewCommandBus creates a new command bus
func NewCommandBus[C Command, E any](eventBus *EventBus, opts ...CommandBusOption[C, E]) *CommandBus[C, E] {
	cb := &CommandBus[C, E]{
		handlers: make(map[string]CommandHandler[C, E]),
		eventBus: eventBus,
	}
	
	for _, opt := range opts {
		opt(cb)
	}
	
	return cb
}

// Register registers a command handler for a specific command type
func (cb *CommandBus[C, E]) Register(cmdType string, handler CommandHandler[C, E]) {
	cb.mu.Lock()
	defer cb.mu.Unlock()
	cb.handlers[cmdType] = handler
}

// Execute executes a command and publishes resulting events
func (cb *CommandBus[C, E]) Execute(ctx context.Context, cmdType string, cmd C) error {
	cb.mu.RLock()
	handler, ok := cb.handlers[cmdType]
	preHandler := cb.preHandler
	postHandler := cb.postHandler
	cb.mu.RUnlock()

	if !ok {
		return fmt.Errorf("no handler registered for command type %s", cmdType)
	}

	// Pre-handler
	if preHandler != nil {
		if err := preHandler(ctx, cmdType, cmd); err != nil {
			return fmt.Errorf("pre-handler failed: %w", err)
		}
	}

	// Execute command
	events, err := handler(ctx, cmd)
	
	// Post-handler (always called, even on error)
	if postHandler != nil {
		postHandler(ctx, cmdType, cmd, events, err)
	}
	
	if err != nil {
		return err
	}

	// Publish events
	for _, event := range events {
		Publish(cb.eventBus, event)
	}

	return nil
}

// QueryBus handles query routing and execution
type QueryBus[Q Query, R any] struct {
	handlers map[string]QueryHandler[Q, R]
	mu       sync.RWMutex
	
	// Options
	cache        map[string]R
	cacheTTL     time.Duration
	cacheEnabled bool
	queryLogger  func(ctx context.Context, queryType string, query Q, result R, err error)
}

// QueryBusOption configures a QueryBus
type QueryBusOption[Q Query, R any] func(*QueryBus[Q, R])

// WithQueryCache enables caching for query results
func WithQueryCache[Q Query, R any](ttl time.Duration) QueryBusOption[Q, R] {
	return func(qb *QueryBus[Q, R]) {
		qb.cacheEnabled = true
		qb.cacheTTL = ttl
		qb.cache = make(map[string]R)
	}
}

// WithQueryLogger sets a logger for queries
func WithQueryLogger[Q Query, R any](logger func(ctx context.Context, queryType string, query Q, result R, err error)) QueryBusOption[Q, R] {
	return func(qb *QueryBus[Q, R]) {
		qb.queryLogger = logger
	}
}

// NewQueryBus creates a new query bus
func NewQueryBus[Q Query, R any](opts ...QueryBusOption[Q, R]) *QueryBus[Q, R] {
	qb := &QueryBus[Q, R]{
		handlers: make(map[string]QueryHandler[Q, R]),
	}
	
	for _, opt := range opts {
		opt(qb)
	}
	
	return qb
}

// Register registers a query handler for a specific query type
func (qb *QueryBus[Q, R]) Register(queryType string, handler QueryHandler[Q, R]) {
	qb.mu.Lock()
	defer qb.mu.Unlock()
	qb.handlers[queryType] = handler
}

// Execute executes a query and returns the result
func (qb *QueryBus[Q, R]) Execute(ctx context.Context, queryType string, query Q) (R, error) {
	qb.mu.RLock()
	handler, ok := qb.handlers[queryType]
	queryLogger := qb.queryLogger
	qb.mu.RUnlock()

	var zero R
	if !ok {
		return zero, fmt.Errorf("no handler registered for query type %s", queryType)
	}

	// TODO: Implement cache logic if needed
	// For now, just execute the query
	result, err := handler(ctx, query)
	
	// Log the query if logger is set
	if queryLogger != nil {
		queryLogger(ctx, queryType, query, result, err)
	}
	
	return result, err
}

// ProjectionManager manages projections for specific event types
type ProjectionManager[E any] struct {
	projections map[string]Projection[E]
	eventBus    *EventBus
	mu          sync.RWMutex
	
	// Options
	errorHandler func(error, Projection[E], E)
	async        bool
}

// ProjectionManagerOption configures a ProjectionManager
type ProjectionManagerOption[E any] func(*ProjectionManager[E])

// WithProjectionErrorHandler sets a custom error handler for projection errors
func WithProjectionErrorHandler[E any](handler func(error, Projection[E], E)) ProjectionManagerOption[E] {
	return func(pm *ProjectionManager[E]) {
		pm.errorHandler = handler
	}
}

// WithAsyncProjections makes all projections handle events asynchronously
func WithAsyncProjections[E any]() ProjectionManagerOption[E] {
	return func(pm *ProjectionManager[E]) {
		pm.async = true
	}
}

// NewProjectionManager creates a new projection manager
func NewProjectionManager[E any](eventBus *EventBus, opts ...ProjectionManagerOption[E]) *ProjectionManager[E] {
	pm := &ProjectionManager[E]{
		projections: make(map[string]Projection[E]),
		eventBus:    eventBus,
	}
	
	for _, opt := range opts {
		opt(pm)
	}
	
	return pm
}

// Register registers a projection WITHOUT subscribing to events
// You must manually subscribe to specific event types
func (pm *ProjectionManager[E]) Register(projection Projection[E]) error {
	if projection == nil {
		return errors.New("projection cannot be nil")
	}
	
	if projection.GetID() == "" {
		return errors.New("projection ID cannot be empty")
	}
	
	pm.mu.Lock()
	pm.projections[projection.GetID()] = projection
	pm.mu.Unlock()

	// Note: Automatic subscription removed because generic interfaces
	// don't match concrete types in Go's type system.
	// Callers must manually subscribe to specific event types.

	return nil
}

// HandleEvent forwards an event to all registered projections
func (pm *ProjectionManager[E]) HandleEvent(ctx context.Context, event E) error {
	pm.mu.RLock()
	projections := make([]Projection[E], 0, len(pm.projections))
	for _, p := range pm.projections {
		projections = append(projections, p)
	}
	async := pm.async
	errorHandler := pm.errorHandler
	pm.mu.RUnlock()

	handleProjection := func(projection Projection[E]) error {
		if err := projection.Handle(ctx, event); err != nil {
			if errorHandler != nil {
				errorHandler(err, projection, event)
				return nil // Don't propagate error if handler is set
			}
			return err
		}
		return nil
	}

	if async {
		var wg sync.WaitGroup
		for _, projection := range projections {
			wg.Add(1)
			go func(p Projection[E]) {
				defer wg.Done()
				_ = handleProjection(p)
			}(projection)
		}
		wg.Wait()
		return nil
	}

	for _, projection := range projections {
		if err := handleProjection(projection); err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves a projection by ID
func (pm *ProjectionManager[E]) Get(id string) (Projection[E], bool) {
	pm.mu.RLock()
	defer pm.mu.RUnlock()
	p, ok := pm.projections[id]
	return p, ok
}

// AggregateStore provides storage for aggregates
type AggregateStore[A Aggregate[E], E any] interface {
	// Load loads an aggregate by ID
	Load(ctx context.Context, id string) (A, error)
	// Save saves an aggregate
	Save(ctx context.Context, aggregate A) error
}

// MemoryAggregateStore provides in-memory storage for aggregates
type MemoryAggregateStore[A Aggregate[E], E any] struct {
	aggregates map[string]A
	events     map[string][]E
	mu         sync.RWMutex
	factory    func(id string) A
}

// NewMemoryAggregateStore creates a new memory aggregate store
func NewMemoryAggregateStore[A Aggregate[E], E any](factory func(id string) A) *MemoryAggregateStore[A, E] {
	return &MemoryAggregateStore[A, E]{
		aggregates: make(map[string]A),
		events:     make(map[string][]E),
		factory:    factory,
	}
}

// Load loads an aggregate by ID
func (s *MemoryAggregateStore[A, E]) Load(ctx context.Context, id string) (A, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Check if we have it in memory
	if agg, ok := s.aggregates[id]; ok {
		return agg, nil
	}

	// Create new aggregate and load events
	agg := s.factory(id)
	if events, ok := s.events[id]; ok {
		if err := agg.LoadFromHistory(events); err != nil {
			var zero A
			return zero, err
		}
	}

	return agg, nil
}

// Save saves an aggregate
func (s *MemoryAggregateStore[A, E]) Save(ctx context.Context, aggregate A) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	id := aggregate.GetID()

	// Append uncommitted events to history
	uncommitted := aggregate.GetUncommittedEvents()
	s.events[id] = append(s.events[id], uncommitted...)

	// Mark events as committed
	aggregate.MarkEventsAsCommitted()

	// Store aggregate
	s.aggregates[id] = aggregate

	return nil
}

// CommandResult represents the result of a command execution
type CommandResult[E any] struct {
	Events  []E
	Version int64
}

// AggregateCommandHandler creates a command handler for an aggregate
func AggregateCommandHandler[C Command, A Aggregate[E], E any](
	store AggregateStore[A, E],
	factory func(id string) A,
	handle func(ctx context.Context, agg A, cmd C) error,
) CommandHandler[C, E] {
	return func(ctx context.Context, cmd C) ([]E, error) {
		// This is a simplified version - real implementation would need to extract ID from command
		// For now, we'll assume commands have an AggregateID() method via interface
		type aggregateCommand interface {
			AggregateID() string
		}

		cmdWithID, ok := any(cmd).(aggregateCommand)
		if !ok {
			return nil, errors.New("command must implement AggregateID() method")
		}

		id := cmdWithID.AggregateID()

		// Load or create aggregate
		agg, err := store.Load(ctx, id)
		if err != nil {
			// Create new aggregate if not found
			agg = factory(id)
		}

		// Handle command
		if err := handle(ctx, agg, cmd); err != nil {
			return nil, err
		}

		// Get uncommitted events
		events := agg.GetUncommittedEvents()

		// Save aggregate
		if err := store.Save(ctx, agg); err != nil {
			return nil, err
		}

		return events, nil
	}
}

// Helper functions for common CQRS patterns

// SubscribeProjection is a helper to subscribe a projection to specific event types
// Usage: SubscribeProjection(bus, pm, AccountCreated{})
func SubscribeProjection[E any](bus *EventBus, pm *ProjectionManager[E], eventExample E) {
	SubscribeContext(bus, func(ctx context.Context, event E) {
		_ = pm.HandleEvent(ctx, event)
	})
}

// SetupCQRSProjections is a helper to set up multiple projections with event subscriptions
func SetupCQRSProjections[E any](
	bus *EventBus,
	pm *ProjectionManager[E],
	projections []Projection[E],
	eventTypes []E,
) error {
	// Register all projections
	for _, projection := range projections {
		if err := pm.Register(projection); err != nil {
			return fmt.Errorf("failed to register projection %s: %w", projection.GetID(), err)
		}
	}
	
	// Subscribe to all event types
	for _, eventType := range eventTypes {
		SubscribeProjection(bus, pm, eventType)
	}
	
	return nil
}

// CQRSConfig provides a fluent API for setting up CQRS components
type CQRSConfig[C Command, Q Query, E any, R any] struct {
	eventBus   *EventBus
	commandBus *CommandBus[C, E]
	queryBus   *QueryBus[Q, R]
	projection *ProjectionManager[E]
}

// NewCQRSConfig creates a new CQRS configuration
func NewCQRSConfig[C Command, Q Query, E any, R any](eventBus *EventBus) *CQRSConfig[C, Q, E, R] {
	return &CQRSConfig[C, Q, E, R]{
		eventBus: eventBus,
	}
}

// WithCommands sets up the command bus with options
func (c *CQRSConfig[C, Q, E, R]) WithCommands(opts ...CommandBusOption[C, E]) *CQRSConfig[C, Q, E, R] {
	c.commandBus = NewCommandBus(c.eventBus, opts...)
	return c
}

// WithQueries sets up the query bus with options
func (c *CQRSConfig[C, Q, E, R]) WithQueries(opts ...QueryBusOption[Q, R]) *CQRSConfig[C, Q, E, R] {
	c.queryBus = NewQueryBus(opts...)
	return c
}

// WithProjections sets up the projection manager with options
func (c *CQRSConfig[C, Q, E, R]) WithProjections(opts ...ProjectionManagerOption[E]) *CQRSConfig[C, Q, E, R] {
	c.projection = NewProjectionManager(c.eventBus, opts...)
	return c
}

// Build returns the configured CQRS components
func (c *CQRSConfig[C, Q, E, R]) Build() (*CommandBus[C, E], *QueryBus[Q, R], *ProjectionManager[E]) {
	return c.commandBus, c.queryBus, c.projection
}
