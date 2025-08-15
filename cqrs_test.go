package eventbus

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
)

// Test domain events
type AccountEvent interface{}

type AccountCreated struct {
	AccountID string
	Balance   float64
}

type MoneyDeposited struct {
	AccountID string
	Amount    float64
}

type MoneyWithdrawn struct {
	AccountID string
	Amount    float64
}

func (a AccountCreated) AggregateID() string { return a.AccountID }
func (m MoneyDeposited) AggregateID() string { return m.AccountID }
func (m MoneyWithdrawn) AggregateID() string { return m.AccountID }

// Test commands
type CreateAccountCommand struct {
	AccountID      string
	InitialBalance float64
}

func (c CreateAccountCommand) AggregateID() string { return c.AccountID }

type DepositMoneyCommand struct {
	AccountID string
	Amount    float64
}

func (d DepositMoneyCommand) AggregateID() string { return d.AccountID }

type WithdrawMoneyCommand struct {
	AccountID string
	Amount    float64
}

func (w WithdrawMoneyCommand) AggregateID() string { return w.AccountID }

// Test queries
type GetAccountBalanceQuery struct {
	AccountID string
}

type GetAllAccountsQuery struct{}

// Test aggregate
type AccountAggregate struct {
	BaseAggregate[AccountEvent]
	Balance float64
}

// NewAccountAggregate creates a new account aggregate
func NewAccountAggregate(id string) *AccountAggregate {
	agg := &AccountAggregate{
		BaseAggregate: BaseAggregate[AccountEvent]{
			ID: id,
		},
	}

	// Set the apply function
	agg.BaseAggregate.ApplyFunc = func(event AccountEvent) error {
		switch e := event.(type) {
		case AccountCreated:
			agg.Balance = e.Balance
		case MoneyDeposited:
			agg.Balance += e.Amount
		case MoneyWithdrawn:
			agg.Balance -= e.Amount
		}
		return nil
	}

	return agg
}

func (a *AccountAggregate) CreateAccount(id string, initialBalance float64) error {
	if a.ID != "" && a.ID != id {
		return errors.New("account already exists")
	}
	event := AccountCreated{
		AccountID: id,
		Balance:   initialBalance,
	}
	a.ID = id
	return a.RaiseEvent(event)
}

func (a *AccountAggregate) Deposit(amount float64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}
	event := MoneyDeposited{
		AccountID: a.ID,
		Amount:    amount,
	}
	return a.RaiseEvent(event)
}

func (a *AccountAggregate) Withdraw(amount float64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}
	if a.Balance < amount {
		return errors.New("insufficient funds")
	}
	event := MoneyWithdrawn{
		AccountID: a.ID,
		Amount:    amount,
	}
	return a.RaiseEvent(event)
}

// Test projection
type AccountBalanceProjection struct {
	id       string
	balances map[string]float64
	mu       sync.RWMutex
}

func NewAccountBalanceProjection() *AccountBalanceProjection {
	return &AccountBalanceProjection{
		id:       "account-balances",
		balances: make(map[string]float64),
	}
}

func (p *AccountBalanceProjection) GetID() string {
	return p.id
}

func (p *AccountBalanceProjection) Handle(ctx context.Context, event AccountEvent) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch e := event.(type) {
	case AccountCreated:
		p.balances[e.AccountID] = e.Balance
	case MoneyDeposited:
		if _, exists := p.balances[e.AccountID]; exists {
			p.balances[e.AccountID] += e.Amount
		}
	case MoneyWithdrawn:
		if _, exists := p.balances[e.AccountID]; exists {
			p.balances[e.AccountID] -= e.Amount
		}
	}
	return nil
}

func (p *AccountBalanceProjection) GetBalance(accountID string) (float64, bool) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	balance, ok := p.balances[accountID]
	return balance, ok
}

func TestBaseAggregate(t *testing.T) {
	agg := &BaseAggregate[AccountEvent]{
		ID:      "test-123",
		Version: 0,
	}

	// Test GetID
	if id := agg.GetID(); id != "test-123" {
		t.Errorf("Expected ID test-123, got %s", id)
	}

	// Test GetVersion
	if version := agg.GetVersion(); version != 0 {
		t.Errorf("Expected version 0, got %d", version)
	}

	// Test RaiseEvent
	event := AccountCreated{AccountID: "test-123", Balance: 100}
	err := agg.RaiseEvent(event)
	if err != nil {
		t.Fatalf("Failed to raise event: %v", err)
	}

	if version := agg.GetVersion(); version != 1 {
		t.Errorf("Expected version 1 after raising event, got %d", version)
	}

	uncommitted := agg.GetUncommittedEvents()
	if len(uncommitted) != 1 {
		t.Fatalf("Expected 1 uncommitted event, got %d", len(uncommitted))
	}

	// Test MarkEventsAsCommitted
	agg.MarkEventsAsCommitted()
	uncommitted = agg.GetUncommittedEvents()
	if len(uncommitted) != 0 {
		t.Errorf("Expected 0 uncommitted events after marking as committed, got %d", len(uncommitted))
	}
}

func TestAccountAggregate(t *testing.T) {
	agg := NewAccountAggregate("")

	// Test CreateAccount
	err := agg.CreateAccount("acc-1", 1000)
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	if agg.Balance != 1000 {
		t.Errorf("Expected balance 1000, got %f", agg.Balance)
	}

	// Test creating account twice
	err = agg.CreateAccount("acc-2", 500)
	if err == nil {
		t.Error("Expected error when creating account twice")
	}

	// Test Deposit
	err = agg.Deposit(500)
	if err != nil {
		t.Fatalf("Failed to deposit: %v", err)
	}

	if agg.Balance != 1500 {
		t.Errorf("Expected balance 1500, got %f", agg.Balance)
	}

	// Test invalid deposit
	err = agg.Deposit(-100)
	if err == nil {
		t.Error("Expected error for negative deposit")
	}

	// Test Withdraw
	err = agg.Withdraw(300)
	if err != nil {
		t.Fatalf("Failed to withdraw: %v", err)
	}

	if agg.Balance != 1200 {
		t.Errorf("Expected balance 1200, got %f", agg.Balance)
	}

	// Test insufficient funds
	err = agg.Withdraw(2000)
	if err == nil {
		t.Error("Expected error for insufficient funds")
	}

	// Test invalid withdrawal
	err = agg.Withdraw(-50)
	if err == nil {
		t.Error("Expected error for negative withdrawal")
	}

	// Test LoadFromHistory
	agg2 := NewAccountAggregate("acc-1")
	events := []AccountEvent{
		AccountCreated{AccountID: "acc-1", Balance: 1000},
		MoneyDeposited{AccountID: "acc-1", Amount: 500},
		MoneyWithdrawn{AccountID: "acc-1", Amount: 300},
	}

	err = agg2.LoadFromHistory(events)
	if err != nil {
		t.Fatalf("Failed to load from history: %v", err)
	}

	if agg2.Balance != 1200 {
		t.Errorf("Expected balance 1200 after loading from history, got %f", agg2.Balance)
	}

	if agg2.Version != 3 {
		t.Errorf("Expected version 3 after loading 3 events, got %d", agg2.Version)
	}
}

func TestProjectionManager(t *testing.T) {
	bus := New()
	pm := NewProjectionManager[AccountEvent](bus)

	projection := NewAccountBalanceProjection()

	// Test Register
	err := pm.Register(projection)
	if err != nil {
		t.Fatalf("Failed to register projection: %v", err)
	}

	// Manually subscribe to specific event types
	SubscribeContext(bus, func(ctx context.Context, e AccountCreated) {
		_ = pm.HandleEvent(ctx, e)
	})
	SubscribeContext(bus, func(ctx context.Context, e MoneyDeposited) {
		_ = pm.HandleEvent(ctx, e)
	})
	SubscribeContext(bus, func(ctx context.Context, e MoneyWithdrawn) {
		_ = pm.HandleEvent(ctx, e)
	})

	// Test Get
	p, ok := pm.Get("account-balances")
	if !ok {
		t.Fatal("Failed to get projection")
	}

	if p.GetID() != "account-balances" {
		t.Errorf("Expected projection ID account-balances, got %s", p.GetID())
	}

	// Test projection updates via events
	Publish(bus, AccountCreated{AccountID: "acc-1", Balance: 1000})
	Publish(bus, MoneyDeposited{AccountID: "acc-1", Amount: 500})
	Publish(bus, MoneyWithdrawn{AccountID: "acc-1", Amount: 200})

	// Wait for all async handlers to complete
	bus.WaitAsync()

	balance, ok := projection.GetBalance("acc-1")
	if !ok {
		t.Fatal("Account not found in projection")
	}

	if balance != 1300 {
		t.Errorf("Expected balance 1300, got %f", balance)
	}
}

func TestCommandBus(t *testing.T) {
	bus := New()
	cmdBus := NewCommandBus[Command, AccountEvent](bus)

	// In-memory store for demo
	accounts := make(map[string]*AccountAggregate)

	// Register command handler for CreateAccount
	createHandler := func(ctx context.Context, cmd Command) ([]AccountEvent, error) {
		createCmd, ok := cmd.(CreateAccountCommand)
		if !ok {
			return nil, errors.New("invalid command type")
		}

		if _, exists := accounts[createCmd.AccountID]; exists {
			return nil, errors.New("account already exists")
		}

		agg := NewAccountAggregate(createCmd.AccountID)
		err := agg.CreateAccount(createCmd.AccountID, createCmd.InitialBalance)
		if err != nil {
			return nil, err
		}

		accounts[createCmd.AccountID] = agg
		events := agg.GetUncommittedEvents()
		agg.MarkEventsAsCommitted()
		return events, nil
	}

	cmdBus.Register("CreateAccount", createHandler)

	// Register deposit handler
	depositHandler := func(ctx context.Context, cmd Command) ([]AccountEvent, error) {
		depositCmd, ok := cmd.(DepositMoneyCommand)
		if !ok {
			return nil, errors.New("invalid command type")
		}

		agg, exists := accounts[depositCmd.AccountID]
		if !exists {
			return nil, errors.New("account not found")
		}

		err := agg.Deposit(depositCmd.Amount)
		if err != nil {
			return nil, err
		}

		events := agg.GetUncommittedEvents()
		agg.MarkEventsAsCommitted()
		return events, nil
	}

	cmdBus.Register("DepositMoney", depositHandler)

	ctx := context.Background()

	// Test ExecuteCommand - Create Account
	err := cmdBus.Execute(ctx, "CreateAccount", CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 1000,
	})
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	// Test ExecuteCommand - Deposit
	err = cmdBus.Execute(ctx, "DepositMoney", DepositMoneyCommand{
		AccountID: "acc-1",
		Amount:    500,
	})
	if err != nil {
		t.Fatalf("Failed to deposit: %v", err)
	}

	// Verify account state
	agg := accounts["acc-1"]
	if agg.Balance != 1500 {
		t.Errorf("Expected balance 1500, got %f", agg.Balance)
	}

	// Test command not found
	err = cmdBus.Execute(ctx, "UnknownCommand", struct{}{})
	if err == nil {
		t.Error("Expected error for unregistered command")
	}

	// Test duplicate account creation
	err = cmdBus.Execute(ctx, "CreateAccount", CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 500,
	})
	if err == nil {
		t.Error("Expected error when creating duplicate account")
	}
}

func TestQueryBus(t *testing.T) {
	queryBus := NewQueryBus[Query, any]()

	// In-memory store for demo
	accounts := map[string]*AccountAggregate{
		"acc-1": {
			BaseAggregate: BaseAggregate[AccountEvent]{ID: "acc-1"},
			Balance:       1000,
		},
		"acc-2": {
			BaseAggregate: BaseAggregate[AccountEvent]{ID: "acc-2"},
			Balance:       2000,
		},
	}

	// Register query handler for GetAccountBalance
	balanceHandler := func(ctx context.Context, query Query) (any, error) {
		balanceQuery, ok := query.(GetAccountBalanceQuery)
		if !ok {
			return nil, errors.New("invalid query type")
		}

		agg, exists := accounts[balanceQuery.AccountID]
		if !exists {
			return 0.0, errors.New("account not found")
		}
		return agg.Balance, nil
	}

	queryBus.Register("GetAccountBalance", balanceHandler)

	// Register query handler for GetAllAccounts
	allAccountsHandler := func(ctx context.Context, query Query) (any, error) {
		_, ok := query.(GetAllAccountsQuery)
		if !ok {
			return nil, errors.New("invalid query type")
		}

		ids := make([]string, 0, len(accounts))
		for id := range accounts {
			ids = append(ids, id)
		}
		return ids, nil
	}

	queryBus.Register("GetAllAccounts", allAccountsHandler)

	ctx := context.Background()

	// Test ExecuteQuery - Get Balance
	result, err := queryBus.Execute(ctx, "GetAccountBalance", GetAccountBalanceQuery{AccountID: "acc-1"})
	if err != nil {
		t.Fatalf("Failed to get balance: %v", err)
	}

	balance := result.(float64)
	if balance != 1000 {
		t.Errorf("Expected balance 1000, got %f", balance)
	}

	// Test ExecuteQuery - Get All Accounts
	result, err = queryBus.Execute(ctx, "GetAllAccounts", GetAllAccountsQuery{})
	if err != nil {
		t.Fatalf("Failed to get all accounts: %v", err)
	}

	accountIDs := result.([]string)
	if len(accountIDs) != 2 {
		t.Errorf("Expected 2 accounts, got %d", len(accountIDs))
	}

	// Test query not found
	_, err = queryBus.Execute(ctx, "UnknownQuery", struct{}{})
	if err == nil {
		t.Error("Expected error for unregistered query")
	}

	// Test non-existent account
	_, err = queryBus.Execute(ctx, "GetAccountBalance", GetAccountBalanceQuery{AccountID: "acc-999"})
	if err == nil {
		t.Error("Expected error for query on non-existent account")
	}
}

func TestMemoryAggregateStore(t *testing.T) {
	factory := func(id string) *AccountAggregate {
		return NewAccountAggregate(id)
	}

	store := NewMemoryAggregateStore[*AccountAggregate, AccountEvent](factory)
	ctx := context.Background()

	// Test creating and saving new aggregate
	agg := NewAccountAggregate("acc-1")
	err := agg.CreateAccount("acc-1", 1000)
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	err = agg.Deposit(500)
	if err != nil {
		t.Fatalf("Failed to deposit: %v", err)
	}

	err = store.Save(ctx, agg)
	if err != nil {
		t.Fatalf("Failed to save aggregate: %v", err)
	}

	// Test loading aggregate
	loaded, err := store.Load(ctx, "acc-1")
	if err != nil {
		t.Fatalf("Failed to load aggregate: %v", err)
	}

	if loaded.GetID() != "acc-1" {
		t.Errorf("Expected ID acc-1, got %s", loaded.GetID())
	}

	if loaded.Balance != 1500 {
		t.Errorf("Expected balance 1500, got %f", loaded.Balance)
	}

	// Test loading non-existent aggregate
	newAgg, err := store.Load(ctx, "acc-2")
	if err != nil {
		t.Fatalf("Failed to load new aggregate: %v", err)
	}

	if newAgg.GetID() != "acc-2" {
		t.Errorf("Expected ID acc-2, got %s", newAgg.GetID())
	}

	if newAgg.Balance != 0 {
		t.Errorf("Expected balance 0 for new aggregate, got %f", newAgg.Balance)
	}
}

func TestAggregateCommandHandler(t *testing.T) {
	factory := func(id string) *AccountAggregate {
		return NewAccountAggregate(id)
	}

	store := NewMemoryAggregateStore[*AccountAggregate, AccountEvent](factory)

	// Create a command handler for CreateAccount
	handler := AggregateCommandHandler[CreateAccountCommand, *AccountAggregate, AccountEvent](
		store,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd CreateAccountCommand) error {
			return agg.CreateAccount(cmd.AccountID, cmd.InitialBalance)
		},
	)

	ctx := context.Background()

	// Test handling create command
	events, err := handler(ctx, CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 1000,
	})
	if err != nil {
		t.Fatalf("Failed to handle command: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	// Create deposit handler
	depositHandler := AggregateCommandHandler[DepositMoneyCommand, *AccountAggregate, AccountEvent](
		store,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd DepositMoneyCommand) error {
			return agg.Deposit(cmd.Amount)
		},
	)

	// Test handling deposit command
	events, err = depositHandler(ctx, DepositMoneyCommand{
		AccountID: "acc-1",
		Amount:    500,
	})
	if err != nil {
		t.Fatalf("Failed to handle deposit command: %v", err)
	}

	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}

	// Load and verify aggregate state
	agg, err := store.Load(ctx, "acc-1")
	if err != nil {
		t.Fatalf("Failed to load aggregate: %v", err)
	}

	if agg.Balance != 1500 {
		t.Errorf("Expected balance 1500, got %f", agg.Balance)
	}
}

func TestAggregateCommandHandlerWithoutAggregateID(t *testing.T) {
	factory := func(id string) *AccountAggregate {
		return NewAccountAggregate(id)
	}

	store := NewMemoryAggregateStore[*AccountAggregate, AccountEvent](factory)

	// Command without AggregateID method
	type InvalidCommand struct {
		Value string
	}

	handler := AggregateCommandHandler[InvalidCommand, *AccountAggregate, AccountEvent](
		store,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd InvalidCommand) error {
			return nil
		},
	)

	ctx := context.Background()

	// Should fail because command doesn't implement AggregateID
	_, err := handler(ctx, InvalidCommand{Value: "test"})
	if err == nil {
		t.Error("Expected error for command without AggregateID method")
	}
}

func TestBaseAggregateApply(t *testing.T) {
	agg := &BaseAggregate[AccountEvent]{
		ID:      "test-123",
		Version: 0,
	}

	errorFunc := func(event AccountEvent) error {
		return errors.New("apply error")
	}

	agg.ApplyFunc = errorFunc

	// Test Apply with error
	err := agg.Apply(AccountCreated{AccountID: "test", Balance: 100})
	if err == nil {
		t.Error("Expected error from Apply")
	}

	// Test RaiseEvent with error
	err = agg.RaiseEvent(AccountCreated{AccountID: "test", Balance: 100})
	if err == nil {
		t.Error("Expected error from RaiseEvent")
	}

	// Test LoadFromHistory with error
	events := []AccountEvent{
		AccountCreated{AccountID: "test", Balance: 100},
	}
	err = agg.LoadFromHistory(events)
	if err == nil {
		t.Error("Expected error from LoadFromHistory")
	}
}

func TestCreateAndRestoreSnapshot(t *testing.T) {
	agg := NewAccountAggregate("acc-1")

	// Create account and make some changes
	err := agg.CreateAccount("acc-1", 1000)
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	// Create snapshot
	snapshot, err := agg.CreateSnapshot()
	if err != nil {
		t.Fatalf("Failed to create snapshot: %v", err)
	}

	// Create new aggregate and restore from snapshot
	agg2 := NewAccountAggregate("")
	err = agg2.RestoreFromSnapshot(snapshot, 1)
	if err != nil {
		t.Fatalf("Failed to restore from snapshot: %v", err)
	}

	if agg2.GetID() != "acc-1" {
		t.Errorf("Expected ID acc-1, got %s", agg2.GetID())
	}

	if agg2.GetVersion() != 1 {
		t.Errorf("Expected version 1, got %d", agg2.GetVersion())
	}
}

func TestRestoreSnapshotWithInvalidJSON(t *testing.T) {
	agg := NewAccountAggregate("")

	invalidJSON := []byte("not valid json")
	err := agg.RestoreFromSnapshot(invalidJSON, 1)
	if err == nil {
		t.Error("Expected error when restoring from invalid JSON")
	}
}

func TestBaseAggregateApplyWithoutFunc(t *testing.T) {
	// Test Apply without ApplyFunc set
	agg := &BaseAggregate[AccountEvent]{
		ID:      "test-123",
		Version: 0,
	}
	
	// Apply should work without ApplyFunc
	err := agg.Apply(AccountCreated{AccountID: "test", Balance: 100})
	if err != nil {
		t.Errorf("Expected no error from Apply without ApplyFunc, got: %v", err)
	}
	
	if agg.Version != 1 {
		t.Errorf("Expected version 1 after Apply, got %d", agg.Version)
	}
}

func TestProjectionManagerHandleEventWithError(t *testing.T) {
	bus := New()
	pm := NewProjectionManager[AccountEvent](bus)
	
	// Create a projection that returns an error
	projection := &errorProjection{}
	
	err := pm.Register(projection)
	if err != nil {
		t.Fatalf("Failed to register projection: %v", err)
	}
	
	// HandleEvent should return the error from the projection
	ctx := context.Background()
	err = pm.HandleEvent(ctx, AccountCreated{AccountID: "test", Balance: 100})
	if err == nil {
		t.Error("Expected error from HandleEvent when projection returns error")
	}
}

// errorProjection is a test projection that returns errors
type errorProjection struct{}

func (p *errorProjection) GetID() string {
	return "error-projection"
}

func (p *errorProjection) Handle(ctx context.Context, event AccountEvent) error {
	return errors.New("projection error")
}

func TestMemoryAggregateStoreLoadWithHistoryError(t *testing.T) {
	// Create aggregate type that fails LoadFromHistory
	factory := func(id string) *errorAggregate {
		return &errorAggregate{
			BaseAggregate: BaseAggregate[AccountEvent]{
				ID: id,
			},
		}
	}
	
	store := NewMemoryAggregateStore(factory)
	
	// Save some events
	ctx := context.Background()
	
	// Store events directly
	store.mu.Lock()
	store.events["test-1"] = []AccountEvent{
		AccountCreated{AccountID: "test-1", Balance: 100},
	}
	store.mu.Unlock()
	
	// Load should fail due to LoadFromHistory error
	_, err := store.Load(ctx, "test-1")
	if err == nil {
		t.Error("Expected error from Load when LoadFromHistory fails")
	}
}

type errorAggregate struct {
	BaseAggregate[AccountEvent]
}

func (a *errorAggregate) LoadFromHistory(events []AccountEvent) error {
	return errors.New("load from history error")
}

func TestAggregateCommandHandlerWithExistingAggregate(t *testing.T) {
	factory := func(id string) *AccountAggregate {
		return NewAccountAggregate(id)
	}
	
	store := NewMemoryAggregateStore(factory)
	
	// Pre-create and save an aggregate
	ctx := context.Background()
	agg := factory("test-account")
	_ = agg.CreateAccount("test-account", 1000)
	
	err := store.Save(ctx, agg)
	if err != nil {
		t.Fatalf("Failed to save aggregate: %v", err)
	}
	
	// Create handler that deposits money
	handler := AggregateCommandHandler[DepositMoneyCommand](
		store,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd DepositMoneyCommand) error {
			return agg.Deposit(cmd.Amount)
		},
	)
	
	// Execute command on existing aggregate
	cmd := DepositMoneyCommand{
		AccountID: "test-account",
		Amount:    500,
	}
	
	events, err := handler(ctx, cmd)
	if err != nil {
		t.Fatalf("Failed to execute command: %v", err)
	}
	
	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}
	
	// Verify aggregate was loaded from store
	loadedAgg, _ := store.Load(ctx, "test-account")
	if loadedAgg.Balance != 1500 {
		t.Errorf("Expected balance 1500, got %f", loadedAgg.Balance)
	}
}

func TestCallHandlerWithContextNonContextHandler(t *testing.T) {
	// This tests the non-context handler path in callHandlerWithContext
	bus := New()
	
	called := false
	handler := func(e TestEvent) {
		called = true
	}
	
	// Subscribe without context
	Subscribe(bus, handler)
	
	// Publish event
	Publish(bus, TestEvent{ID: 1, Value: "test"})
	
	// Wait for handlers
	bus.WaitAsync()
	
	if !called {
		t.Error("Non-context handler was not called")
	}
}

func TestAggregateCommandHandlerErrors(t *testing.T) {
	factory := func(id string) *AccountAggregate {
		return NewAccountAggregate(id)
	}
	
	store := NewMemoryAggregateStore(factory)
	ctx := context.Background()
	
	// Test 1: Command handle returns error
	handler := AggregateCommandHandler[CreateAccountCommand](
		store,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd CreateAccountCommand) error {
			return errors.New("handle error")
		},
	)
	
	cmd := CreateAccountCommand{
		AccountID:      "test-account",
		InitialBalance: 1000,
	}
	
	_, err := handler(ctx, cmd)
	if err == nil || err.Error() != "handle error" {
		t.Errorf("Expected handle error, got %v", err)
	}
	
	// Test 2: Store save returns error
	// Create a store that fails on save
	failingStore := &failingSaveStore{
		MemoryAggregateStore: store,
	}
	
	handler2 := AggregateCommandHandler[CreateAccountCommand](
		failingStore,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd CreateAccountCommand) error {
			return agg.CreateAccount(cmd.AccountID, cmd.InitialBalance)
		},
	)
	
	_, err = handler2(ctx, cmd)
	if err == nil || err.Error() != "save error" {
		t.Errorf("Expected save error, got %v", err)
	}
	
	// Test 3: Store load returns error but aggregate creation succeeds
	// Use a store that always fails on Load to test aggregate creation
	failingLoadStore := &failingLoadStore{
		MemoryAggregateStore: store,
	}
	
	handler3 := AggregateCommandHandler[CreateAccountCommand](
		failingLoadStore,
		factory,
		func(ctx context.Context, agg *AccountAggregate, cmd CreateAccountCommand) error {
			return agg.CreateAccount(cmd.AccountID, cmd.InitialBalance)
		},
	)
	
	cmd2 := CreateAccountCommand{
		AccountID:      "new-account",
		InitialBalance: 500,
	}
	
	events, err := handler3(ctx, cmd2)
	if err != nil {
		t.Fatalf("Expected successful creation for new aggregate, got %v", err)
	}
	
	if len(events) != 1 {
		t.Errorf("Expected 1 event, got %d", len(events))
	}
}

type failingSaveStore struct {
	*MemoryAggregateStore[*AccountAggregate, AccountEvent]
}

func (s *failingSaveStore) Save(ctx context.Context, agg *AccountAggregate) error {
	return errors.New("save error")
}

type failingLoadStore struct {
	*MemoryAggregateStore[*AccountAggregate, AccountEvent]
}

func (s *failingLoadStore) Load(ctx context.Context, id string) (*AccountAggregate, error) {
	return nil, errors.New("load error")
}

func (s *failingLoadStore) Save(ctx context.Context, agg *AccountAggregate) error {
	// Allow save to succeed for this test
	return nil
}

func TestCallHandlerWithContextGenericHandlers(t *testing.T) {
	bus := New()
	
	// Test generic context handler without error
	called1 := false
	handler1 := func(ctx context.Context, event any) {
		called1 = true
	}
	
	// Register handler directly using internalHandler
	bus.mu.Lock()
	eventType := reflect.TypeOf(AccountCreated{})
	if _, exists := bus.handlers[eventType]; !exists {
		bus.handlers[eventType] = []*internalHandler{}
	}
	bus.handlers[eventType] = append(bus.handlers[eventType], &internalHandler{
		handler:        handler1,
		acceptsContext: true,
		handlerType:    reflect.TypeOf(handler1),
		async:          false,
	})
	bus.mu.Unlock()
	
	// Publish event
	Publish(bus, AccountCreated{AccountID: "test", Balance: 100})
	bus.WaitAsync()
	
	if !called1 {
		t.Error("Generic context handler was not called")
	}
	
	// Test generic context handler with error
	called2 := false
	handler2 := func(ctx context.Context, event any) error {
		called2 = true
		return nil
	}
	
	// Register handler directly using internalHandler
	bus.mu.Lock()
	eventType2 := reflect.TypeOf(MoneyDeposited{})
	if _, exists := bus.handlers[eventType2]; !exists {
		bus.handlers[eventType2] = []*internalHandler{}
	}
	bus.handlers[eventType2] = append(bus.handlers[eventType2], &internalHandler{
		handler:        handler2,
		acceptsContext: true,
		handlerType:    reflect.TypeOf(handler2),
		async:          false,
	})
	bus.mu.Unlock()
	
	// Publish event
	Publish(bus, MoneyDeposited{AccountID: "test", Amount: 50})
	bus.WaitAsync()
	
	if !called2 {
		t.Error("Generic context handler with error was not called")
	}
}
