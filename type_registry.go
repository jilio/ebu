package eventbus

import (
	"fmt"
	"reflect"
	"sync"
)

// persistedTypeRegistry owns the per-bus mapping between durable wire names
// and Go types. Reservations make multi-step operations transactional: a
// subscription can validate its identity before replay without making that
// identity permanent until durable evidence has been decoded or setup
// succeeds.
type persistedTypeRegistry struct {
	mu     sync.Mutex
	active bool
	byName map[string]*persistedTypeClaim
	byType map[reflect.Type]*persistedTypeClaim

	// Before persistence is enabled, typed registrations are retained without
	// enforcing durable wire-name uniqueness. A purely in-process bus routes by
	// reflect.Type, so equal TypeNamer names are harmless there. If persistence
	// is enabled later, activate validates the complete set atomically before a
	// store becomes observable on the bus.
	deferred        []persistedTypeSpec
	deferredSet     map[persistedTypeSpec]struct{}
	deferredPending map[*persistedTypeReservation]struct{}
}

type persistedTypeClaim struct {
	name      string
	eventType reflect.Type
	pending   int
	sticky    bool
}

type persistedTypeSpec struct {
	name      string
	eventType reflect.Type
}

type persistedTypeReservation struct {
	registry *persistedTypeRegistry
	claims   []*persistedTypeClaim
	specs    []persistedTypeSpec
	once     sync.Once
}

func newPersistedTypeRegistry() *persistedTypeRegistry {
	return &persistedTypeRegistry{
		active:          true,
		byName:          make(map[string]*persistedTypeClaim),
		byType:          make(map[reflect.Type]*persistedTypeClaim),
		deferredSet:     make(map[persistedTypeSpec]struct{}),
		deferredPending: make(map[*persistedTypeReservation]struct{}),
	}
}

// newDeferredPersistedTypeRegistry creates the registry used by EventBus.
// New applies arbitrary Option functions before it knows whether the final
// configuration is persistent, so typed registrations remain provisional
// until all options have run.
func newDeferredPersistedTypeRegistry() *persistedTypeRegistry {
	registry := newPersistedTypeRegistry()
	registry.active = false
	return registry
}

// reserve transactionally records every requested name/type pair. An active
// registry validates the complete request before changing pending counts, so a
// conflicting multi-type operation never leaves partial ownership behind. A
// deferred registry postpones that same atomic validation until activation.
func (r *persistedTypeRegistry) reserve(specs ...persistedTypeSpec) (*persistedTypeReservation, error) {
	// Deduplicate identical endpoints (for example, validation of an invalid
	// self-upcast). Conflicts are checked immediately by an active registry and
	// deferred otherwise, because they matter only if persistence is enabled.
	unique := make([]persistedTypeSpec, 0, len(specs))
	seen := make(map[persistedTypeSpec]struct{}, len(specs))
	for _, spec := range specs {
		if _, ok := seen[spec]; ok {
			continue
		}
		seen[spec] = struct{}{}
		unique = append(unique, spec)
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	reservation := &persistedTypeReservation{registry: r, specs: unique}
	if !r.active {
		r.deferredPending[reservation] = struct{}{}
		return reservation, nil
	}
	if err := r.reserveActiveLocked(reservation); err != nil {
		return nil, err
	}
	return reservation, nil
}

// reserveActiveLocked validates and installs the pending claims for one
// reservation. The caller must hold r.mu. Validation finishes before mutation,
// so a multi-type request is atomic.
func (r *persistedTypeRegistry) reserveActiveLocked(reservation *persistedTypeReservation) error {
	requestNames := make(map[string]reflect.Type, len(reservation.specs))
	requestTypes := make(map[reflect.Type]string, len(reservation.specs))
	for _, spec := range reservation.specs {
		if existing, ok := requestNames[spec.name]; ok && existing != spec.eventType {
			return persistedTypeNameConflict(spec.name, existing, spec.eventType)
		}
		if existing, ok := requestTypes[spec.eventType]; ok && existing != spec.name {
			return persistedTypeIdentityConflict(spec.eventType, existing, spec.name)
		}
		requestNames[spec.name] = spec.eventType
		requestTypes[spec.eventType] = spec.name

		if claim, ok := r.byName[spec.name]; ok && claim.eventType != spec.eventType {
			return persistedTypeNameConflict(spec.name, claim.eventType, spec.eventType)
		}
		if claim, ok := r.byType[spec.eventType]; ok && claim.name != spec.name {
			return persistedTypeIdentityConflict(spec.eventType, claim.name, spec.name)
		}
	}

	reservation.claims = make([]*persistedTypeClaim, 0, len(reservation.specs))
	for _, spec := range reservation.specs {
		claim := r.byName[spec.name]
		if claim == nil {
			claim = &persistedTypeClaim{name: spec.name, eventType: spec.eventType}
			r.byName[spec.name] = claim
			r.byType[spec.eventType] = claim
		}
		claim.pending++
		reservation.claims = append(reservation.claims, claim)
	}
	return nil
}

func persistedTypeNameConflict(name string, existing, requested reflect.Type) error {
	return fmt.Errorf("eventbus: persisted event type name %q is already registered for Go type %v; type %v must use a distinct TypeNamer.EventTypeName", name, existing, requested)
}

func persistedTypeIdentityConflict(eventType reflect.Type, existing, requested string) error {
	return fmt.Errorf("eventbus: Go event type %s is already associated with persisted event type name %q; EventTypeName must be a pure, immutable function of the type (got %q)", eventType, existing, requested)
}

// activate atomically audits every successful provisional registration and
// turns durable-name enforcement on. Pending reservations are included, so a
// concurrent registration cannot slip between the audit and activation.
func (r *persistedTypeRegistry) activate() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.active {
		return nil
	}

	all := make([]persistedTypeSpec, 0, len(r.deferred))
	all = append(all, r.deferred...)
	for reservation := range r.deferredPending {
		all = append(all, reservation.specs...)
	}
	validation := &persistedTypeReservation{registry: r, specs: all}
	if err := r.reserveActiveLocked(validation); err != nil {
		return err
	}

	// reserveActiveLocked represented the entire provisional set as pending.
	// Convert committed registrations to sticky claims, then replace the
	// aggregate pending counts with the original in-flight reservations.
	for _, claim := range validation.claims {
		claim.pending--
	}
	for _, spec := range r.deferred {
		claim := r.byName[spec.name]
		claim.sticky = true
	}
	for reservation := range r.deferredPending {
		reservation.claims = make([]*persistedTypeClaim, 0, len(reservation.specs))
		for _, spec := range reservation.specs {
			claim := r.byName[spec.name]
			claim.pending++
			reservation.claims = append(reservation.claims, claim)
		}
	}

	r.deferred = nil
	r.deferredSet = nil
	r.deferredPending = nil
	r.active = true
	return nil
}

func (r *persistedTypeRegistry) isActive() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.active
}

func (bus *EventBus) activatePersistenceTypes() {
	if err := bus.persistedTypes.activate(); err != nil {
		panic(fmt.Sprintf("eventbus: cannot enable persistence: %v", err))
	}
}

// Commit records a successful registration; on an active registry it makes
// every mapping sticky for the lifetime of the bus. Rollback releases only
// this operation's pending references. Both methods are terminal and
// idempotent, which keeps deferred rollback safe after an early proof-of-type
// commit.
func (r *persistedTypeReservation) Commit() {
	r.finish(true)
}

func (r *persistedTypeReservation) Rollback() {
	r.finish(false)
}

func (r *persistedTypeReservation) finish(commit bool) {
	if r == nil {
		return
	}
	r.once.Do(func() {
		r.registry.mu.Lock()
		defer r.registry.mu.Unlock()
		if !r.registry.active {
			delete(r.registry.deferredPending, r)
			if commit {
				for _, spec := range r.specs {
					if _, exists := r.registry.deferredSet[spec]; exists {
						continue
					}
					r.registry.deferredSet[spec] = struct{}{}
					r.registry.deferred = append(r.registry.deferred, spec)
				}
			}
			return
		}
		for _, claim := range r.claims {
			claim.pending--
			if commit {
				claim.sticky = true
			}
			if claim.pending == 0 && !claim.sticky {
				delete(r.registry.byName, claim.name)
				delete(r.registry.byType, claim.eventType)
			}
		}
	})
}

// reservePersistedTypes retains successful typed registrations provisionally
// on a purely in-process bus. They do not constrain reflect.Type routing, but
// are audited atomically if persistence is enabled later.
func (bus *EventBus) reservePersistedTypes(specs ...persistedTypeSpec) (*persistedTypeReservation, error) {
	return bus.persistedTypes.reserve(specs...)
}

func (bus *EventBus) reserveReplayID(subscriptionID string) error {
	if subscriptionID == "" {
		return fmt.Errorf("eventbus: subscription ID cannot be empty")
	}
	bus.replayMu.Lock()
	defer bus.replayMu.Unlock()
	if _, exists := bus.replayIDs[subscriptionID]; exists {
		return fmt.Errorf("eventbus: durable subscription ID %q was already registered or is actively owned on this bus; it cannot be reused while that ownership remains", subscriptionID)
	}
	bus.replayIDs[subscriptionID] = struct{}{}
	return nil
}

func (bus *EventBus) releaseReplayID(subscriptionID string) {
	bus.replayMu.Lock()
	delete(bus.replayIDs, subscriptionID)
	bus.replayMu.Unlock()
}

func (bus *EventBus) registerReplayMarker(marker *internalHandler) {
	bus.replayMu.Lock()
	if _, exists := bus.replayMarkers[marker]; !exists {
		bus.replayMarkers[marker] = struct{}{}
		bus.replayMarkerCount.Add(1)
	}
	bus.replayMu.Unlock()
}

func (bus *EventBus) unregisterReplayMarker(marker *internalHandler) {
	bus.replayMu.Lock()
	if _, exists := bus.replayMarkers[marker]; exists {
		delete(bus.replayMarkers, marker)
		bus.replayMarkerCount.Add(-1)
	}
	bus.replayMu.Unlock()
}
