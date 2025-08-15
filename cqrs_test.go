package eventbus

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// Test domain events
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

// Test commands
type CreateAccountCommand struct {
	AccountID      string
	InitialBalance float64
}

type DepositMoneyCommand struct {
	AccountID string
	Amount    float64
}

type WithdrawMoneyCommand struct {
	AccountID string
	Amount    float64
}

// Test queries
type GetAccountBalanceQuery struct {
	AccountID string
}

type GetAllAccountsQuery struct{}

// Test aggregate
type AccountAggregate struct {
	BaseAggregate
	Balance float64
}

func (a *AccountAggregate) LoadFromHistory(events []any) error {
	for _, event := range events {
		switch e := event.(type) {
		case AccountCreated:
			a.ID = e.AccountID
			a.Balance = e.Balance
		case MoneyDeposited:
			a.Balance += e.Amount
		case MoneyWithdrawn:
			a.Balance -= e.Amount
		}
		a.Version++
	}
	return nil
}

func (a *AccountAggregate) CreateAccount(id string, initialBalance float64) error {
	if a.ID != "" {
		return errors.New("account already exists")
	}
	event := AccountCreated{
		AccountID: id,
		Balance:   initialBalance,
	}
	a.ID = id
	a.Balance = initialBalance
	a.RaiseEvent(event)
	return nil
}

func (a *AccountAggregate) Deposit(amount float64) error {
	if amount <= 0 {
		return errors.New("amount must be positive")
	}
	event := MoneyDeposited{
		AccountID: a.ID,
		Amount:    amount,
	}
	a.Balance += amount
	a.RaiseEvent(event)
	return nil
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
	a.Balance -= amount
	a.RaiseEvent(event)
	return nil
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

func (p *AccountBalanceProjection) Handle(ctx context.Context, event any) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	switch e := event.(type) {
	case AccountCreated:
		// Account creation always sets the balance
		p.balances[e.AccountID] = e.Balance
	case MoneyDeposited:
		// Only process if account exists
		if _, exists := p.balances[e.AccountID]; exists {
			p.balances[e.AccountID] += e.Amount
		}
	case MoneyWithdrawn:
		// Only process if account exists
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
	agg := &BaseAggregate{
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
	agg.RaiseEvent(event)

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
	agg := &AccountAggregate{}

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
	agg2 := &AccountAggregate{}
	events := []any{
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

func TestProjectionBuilder(t *testing.T) {
	bus := New()
	pb := NewProjectionBuilder(bus)

	projection := NewAccountBalanceProjection()

	// Test Register
	err := pb.Register(projection, AccountCreated{}, MoneyDeposited{}, MoneyWithdrawn{})
	if err != nil {
		t.Fatalf("Failed to register projection: %v", err)
	}

	// Test Get
	p, ok := pb.Get("account-balances")
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

	// Test registering with nil event type - use a new projection to avoid duplicate handlers
	projection2 := NewAccountBalanceProjection()
	err = pb.Register(projection2, nil)
	if err == nil {
		t.Error("Expected error when registering with nil event type")
	}
}

func TestCQRS(t *testing.T) {
	bus := New()
	cqrs := NewCQRS(bus)

	// In-memory store for demo
	accounts := make(map[string]*AccountAggregate)

	// Register command handler for CreateAccount
	createHandler := func(ctx context.Context, cmd CreateAccountCommand) ([]any, error) {
		if _, exists := accounts[cmd.AccountID]; exists {
			return nil, errors.New("account already exists")
		}

		agg := &AccountAggregate{}
		err := agg.CreateAccount(cmd.AccountID, cmd.InitialBalance)
		if err != nil {
			return nil, err
		}

		accounts[cmd.AccountID] = agg
		events := agg.GetUncommittedEvents()
		agg.MarkEventsAsCommitted()
		return events, nil
	}

	err := cqrs.RegisterCommand(createHandler)
	if err != nil {
		t.Fatalf("Failed to register command handler: %v", err)
	}

	// Register command handler for DepositMoney
	depositHandler := func(ctx context.Context, cmd DepositMoneyCommand) ([]any, error) {
		agg, exists := accounts[cmd.AccountID]
		if !exists {
			return nil, errors.New("account not found")
		}

		err := agg.Deposit(cmd.Amount)
		if err != nil {
			return nil, err
		}

		events := agg.GetUncommittedEvents()
		agg.MarkEventsAsCommitted()
		return events, nil
	}

	err = cqrs.RegisterCommand(depositHandler)
	if err != nil {
		t.Fatalf("Failed to register deposit handler: %v", err)
	}

	// Register command handler for WithdrawMoney
	withdrawHandler := func(ctx context.Context, cmd WithdrawMoneyCommand) ([]any, error) {
		agg, exists := accounts[cmd.AccountID]
		if !exists {
			return nil, errors.New("account not found")
		}

		err := agg.Withdraw(cmd.Amount)
		if err != nil {
			return nil, err
		}

		events := agg.GetUncommittedEvents()
		agg.MarkEventsAsCommitted()
		return events, nil
	}

	err = cqrs.RegisterCommand(withdrawHandler)
	if err != nil {
		t.Fatalf("Failed to register withdraw handler: %v", err)
	}

	// Register query handler for GetAccountBalance
	balanceHandler := func(ctx context.Context, query GetAccountBalanceQuery) (float64, error) {
		agg, exists := accounts[query.AccountID]
		if !exists {
			return 0, errors.New("account not found")
		}
		return agg.Balance, nil
	}

	err = cqrs.RegisterQuery(balanceHandler)
	if err != nil {
		t.Fatalf("Failed to register query handler: %v", err)
	}

	// Register query handler for GetAllAccounts
	allAccountsHandler := func(ctx context.Context, query GetAllAccountsQuery) ([]string, error) {
		ids := make([]string, 0, len(accounts))
		for id := range accounts {
			ids = append(ids, id)
		}
		return ids, nil
	}

	err = cqrs.RegisterQuery(allAccountsHandler)
	if err != nil {
		t.Fatalf("Failed to register all accounts handler: %v", err)
	}

	// Register projection
	projection := NewAccountBalanceProjection()
	err = cqrs.RegisterProjection(projection, AccountCreated{}, MoneyDeposited{}, MoneyWithdrawn{})
	if err != nil {
		t.Fatalf("Failed to register projection: %v", err)
	}

	ctx := context.Background()

	// Test ExecuteCommand - Create Account
	err = cqrs.ExecuteCommand(ctx, CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 1000,
	})
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	// Test ExecuteCommand - Deposit
	err = cqrs.ExecuteCommand(ctx, DepositMoneyCommand{
		AccountID: "acc-1",
		Amount:    500,
	})
	if err != nil {
		t.Fatalf("Failed to deposit: %v", err)
	}

	// Test ExecuteCommand - Withdraw
	err = cqrs.ExecuteCommand(ctx, WithdrawMoneyCommand{
		AccountID: "acc-1",
		Amount:    200,
	})
	if err != nil {
		t.Fatalf("Failed to withdraw: %v", err)
	}

	// Wait for all async handlers to complete
	bus.WaitAsync()

	// Test ExecuteQuery - Get Balance
	result, err := cqrs.ExecuteQuery(ctx, GetAccountBalanceQuery{AccountID: "acc-1"})
	if err != nil {
		t.Fatalf("Failed to get balance: %v", err)
	}

	balance := result.(float64)
	if balance != 1300 {
		t.Errorf("Expected balance 1300, got %f", balance)
	}

	// Test ExecuteQuery - Get All Accounts
	result, err = cqrs.ExecuteQuery(ctx, GetAllAccountsQuery{})
	if err != nil {
		t.Fatalf("Failed to get all accounts: %v", err)
	}

	accountIDs := result.([]string)
	if len(accountIDs) != 1 {
		t.Errorf("Expected 1 account, got %d", len(accountIDs))
	}

	// Test GetProjection
	p, ok := cqrs.GetProjection("account-balances")
	if !ok {
		t.Fatal("Failed to get projection")
	}

	accProj := p.(*AccountBalanceProjection)
	projBalance, ok := accProj.GetBalance("acc-1")
	if !ok {
		t.Fatal("Account not found in projection")
	}

	if projBalance != 1300 {
		t.Errorf("Expected projection balance 1300, got %f", projBalance)
	}

	// Test GetBus
	if cqrs.GetBus() != bus {
		t.Error("GetBus returned different bus instance")
	}

	// Test command not found
	err = cqrs.ExecuteCommand(ctx, struct{}{})
	if err == nil {
		t.Error("Expected error for unregistered command")
	}

	// Test query not found
	_, err = cqrs.ExecuteQuery(ctx, struct{}{})
	if err == nil {
		t.Error("Expected error for unregistered query")
	}

	// Test duplicate account creation
	err = cqrs.ExecuteCommand(ctx, CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 500,
	})
	if err == nil {
		t.Error("Expected error when creating duplicate account")
	}

	// Test operations on non-existent account
	err = cqrs.ExecuteCommand(ctx, DepositMoneyCommand{
		AccountID: "acc-999",
		Amount:    100,
	})
	if err == nil {
		t.Error("Expected error for deposit to non-existent account")
	}

	_, err = cqrs.ExecuteQuery(ctx, GetAccountBalanceQuery{AccountID: "acc-999"})
	if err == nil {
		t.Error("Expected error for query on non-existent account")
	}
}

func TestCQRSInvalidHandlers(t *testing.T) {
	bus := New()
	cqrs := NewCQRS(bus)

	// Test registering non-function as command handler
	err := cqrs.RegisterCommand("not a function")
	if err == nil {
		t.Error("Expected error when registering non-function as command handler")
	}

	// Test registering command handler with wrong number of parameters
	wrongParams := func(cmd CreateAccountCommand) ([]any, error) {
		return nil, nil
	}
	err = cqrs.RegisterCommand(wrongParams)
	if err == nil {
		t.Error("Expected error for command handler with wrong number of parameters")
	}

	// Test registering command handler with wrong number of return values
	wrongReturns := func(ctx context.Context, cmd CreateAccountCommand) error {
		return nil
	}
	err = cqrs.RegisterCommand(wrongReturns)
	if err == nil {
		t.Error("Expected error for command handler with wrong number of return values")
	}

	// Test registering non-function as query handler
	err = cqrs.RegisterQuery("not a function")
	if err == nil {
		t.Error("Expected error when registering non-function as query handler")
	}

	// Test registering query handler with wrong number of parameters
	wrongQueryParams := func(query GetAccountBalanceQuery) (float64, error) {
		return 0, nil
	}
	err = cqrs.RegisterQuery(wrongQueryParams)
	if err == nil {
		t.Error("Expected error for query handler with wrong number of parameters")
	}

	// Test registering query handler with wrong number of return values
	wrongQueryReturns := func(ctx context.Context, query GetAccountBalanceQuery) float64 {
		return 0
	}
	err = cqrs.RegisterQuery(wrongQueryReturns)
	if err == nil {
		t.Error("Expected error for query handler with wrong number of return values")
	}
}

func TestCQRSWithErrors(t *testing.T) {
	bus := New()
	cqrs := NewCQRS(bus)

	// Register a command handler that returns an error
	errorHandler := func(ctx context.Context, cmd CreateAccountCommand) ([]any, error) {
		return nil, errors.New("simulated error")
	}

	err := cqrs.RegisterCommand(errorHandler)
	if err != nil {
		t.Fatalf("Failed to register command handler: %v", err)
	}

	ctx := context.Background()
	err = cqrs.ExecuteCommand(ctx, CreateAccountCommand{
		AccountID:      "acc-1",
		InitialBalance: 1000,
	})

	if err == nil {
		t.Error("Expected error from command handler")
	}

	// Register a query handler that returns an error
	queryErrorHandler := func(ctx context.Context, query GetAccountBalanceQuery) (float64, error) {
		return 0, errors.New("query error")
	}

	err = cqrs.RegisterQuery(queryErrorHandler)
	if err != nil {
		t.Fatalf("Failed to register query handler: %v", err)
	}

	_, err = cqrs.ExecuteQuery(ctx, GetAccountBalanceQuery{AccountID: "acc-1"})
	if err == nil {
		t.Error("Expected error from query handler")
	}
}

