# EBU Examples: Event Sourcing Without the Pain

These examples demonstrate how `ebu` with persistence enables powerful patterns like CQRS and event sourcing **without database migrations, schemas, or complex infrastructure**.

## 🎯 Philosophy

Traditional event sourcing often requires:
- Complex database schemas
- Migration scripts
- Specialized event stores
- Heavy infrastructure

With `ebu`, you get:
- ✅ Simple file-based persistence (or any store you want)
- ✅ Zero migrations - just append events
- ✅ Automatic replay and recovery
- ✅ Type-safe event handling
- ✅ Projections that rebuild from events

## 📚 Examples

### 1. Blockchain Sync (`blockchain/`)

Demonstrates a real-world blockchain synchronization system:

```bash
cd blockchain
go run main.go
```

**Features:**
- Watches specific wallet addresses
- Persists sync position (block height)
- Resumes from last position on restart
- Maintains balance projections
- No database required - just files!

**Key Pattern:**
```go
// Events define what happened
type TransactionFound struct {
    BlockHeight int64
    From, To    string
    Amount      int64
}

// Projections calculate current state
projection.UpdateBalance(address, amount)

// Persistence handles recovery
bus := ebu.New(ebu.WithStore(fileStore))
```

### 2. CQRS Implementation (`cqrs/`)

Shows complete Command Query Responsibility Segregation:

```bash
cd cqrs
go run main.go
```

**Architecture:**
```
Commands → Events → Projections → Queries
   ↓         ↓          ↓           ↓
CreateAccount → AccountCreated → AccountView → GetAccount()
```

**Benefits:**
- Write side (commands) separated from read side (queries)
- Multiple projections from same events
- Rebuild any projection by replaying events
- Add new projections without touching existing code

## 🔑 Key Insights

### No Migrations Ever

Traditional approach:
```sql
ALTER TABLE accounts ADD COLUMN last_login TIMESTAMP;
-- Now update all existing records...
-- Hope nothing breaks in production...
```

Event sourcing approach:
```go
// Just add a new event
type UserLoggedIn struct {
    UserID string
    Time   time.Time
}

// Old events still work
// New projection includes login time
// No migration needed!
```

### Projections Are Disposable

```go
// Delete projection? No problem!
rm -rf ./projections

// Rebuild from events
bus.SubscribeWithReplay(ctx, rebuilder)
```

### Multiple Views of Same Data

```go
// Balance by account
accountProjection.GetBalance("alice")

// Daily transaction volume
volumeProjection.GetDailyVolume(date)

// Risk score calculation
riskProjection.GetRiskScore("alice")

// All from the same events!
```

## 🚀 Your Use Case

The blockchain example shows exactly what you described:

1. **Read blockchain data** → `BlockScanned` events
2. **Watch specific addresses** → Filter in scanner
3. **Persist to avoid re-sync** → `WithStore(fileStore)`
4. **Calculate balances** → `BalanceProjection`
5. **No migrations** → Just append-only event log

## 🎓 Advanced Patterns

### Event Replay for Testing

```go
// Save production events
cp ./blockchain_data/events.jsonl ./test_events.jsonl

// Replay in test environment
testBus := ebu.New(ebu.WithStore(testStore))
testBus.SubscribeWithReplay(ctx, handler)
```

### Time Travel Debugging

```go
// What was the balance on block 1000?
events := store.GetEvents(ctx, 0)
for _, event := range events {
    if event.Position > 1000 {
        break
    }
    projection.Apply(event)
}
```

### Adding New Features

```go
// Want to track gas fees? Just add:
type GasFeesPaid struct {
    Address string
    Amount  int64
    Block   int64
}

// Subscribe and track - existing code unchanged!
bus.Subscribe(func(fees GasFeesPaid) {
    gasProjection.Update(fees)
})
```

## 💡 Tips

1. **Start simple** - File storage works great for many use cases
2. **Events are immutable** - Never change old events
3. **Projections are cheap** - Create as many as you need
4. **Test with replay** - Replay events to verify projections
5. **Version carefully** - If events must change, version them

## 🎯 Perfect For

- ✅ Blockchain sync and indexing
- ✅ Financial transactions
- ✅ Audit logs
- ✅ Order processing
- ✅ IoT data streams
- ✅ Any system where history matters

## ❌ Not Ideal For

- Simple CRUD with no history needs
- High-frequency trading (millions/sec)
- Systems with no domain events

## 🏃 Getting Started

1. Run the examples to see them in action
2. Check the persisted data in `./blockchain_data` and `./cqrs_data`
3. Run examples again to see resumption from persisted state
4. Modify and experiment!

Remember: **Events are facts about what happened. Store facts, derive everything else.**