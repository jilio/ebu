package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	ebu "github.com/jilio/ebu"
)

// Event types for blockchain sync
type BlockScanned struct {
	Height    int64     `json:"height"`
	Hash      string    `json:"hash"`
	Timestamp time.Time `json:"timestamp"`
}

type TransactionFound struct {
	BlockHeight int64  `json:"block_height"`
	TxHash      string `json:"tx_hash"`
	From        string `json:"from"`
	To          string `json:"to"`
	Amount      int64  `json:"amount"` // in smallest unit (satoshi/wei)
}

type BalanceUpdated struct {
	Address     string `json:"address"`
	Balance     int64  `json:"balance"`
	BlockHeight int64  `json:"block_height"`
}

// Projection for wallet balances
type BalanceProjection struct {
	mu       sync.RWMutex
	balances map[string]int64
	dataDir  string
}

func NewBalanceProjection(dataDir string) *BalanceProjection {
	bp := &BalanceProjection{
		balances: make(map[string]int64),
		dataDir:  dataDir,
	}
	bp.load()
	return bp
}

func (bp *BalanceProjection) load() {
	path := filepath.Join(bp.dataDir, "balances.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return // File doesn't exist yet
	}
	json.Unmarshal(data, &bp.balances)
}

func (bp *BalanceProjection) save() {
	bp.mu.RLock()
	data, _ := json.MarshalIndent(bp.balances, "", "  ")
	bp.mu.RUnlock()

	path := filepath.Join(bp.dataDir, "balances.json")
	os.WriteFile(path, data, 0644)
}

func (bp *BalanceProjection) GetBalance(address string) int64 {
	bp.mu.RLock()
	defer bp.mu.RUnlock()
	return bp.balances[address]
}

func (bp *BalanceProjection) UpdateBalance(address string, delta int64) int64 {
	bp.mu.Lock()
	bp.balances[address] += delta
	newBalance := bp.balances[address]
	bp.mu.Unlock()

	bp.save() // Persist after each update
	return newBalance
}

// Simplified file-based event store
type FileEventStore struct {
	dataDir  string
	mu       sync.RWMutex
	position int64
}

func NewFileEventStore(dataDir string) *FileEventStore {
	os.MkdirAll(dataDir, 0755)
	store := &FileEventStore{dataDir: dataDir}
	// Load current position
	if data, err := os.ReadFile(filepath.Join(dataDir, "position")); err == nil {
		fmt.Sscanf(string(data), "%d", &store.position)
	}
	return store
}

func (s *FileEventStore) Save(ctx context.Context, event *ebu.StoredEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Simple append-only log file
	f, err := os.OpenFile(
		filepath.Join(s.dataDir, "events.jsonl"),
		os.O_CREATE|os.O_APPEND|os.O_WRONLY,
		0644,
	)
	if err != nil {
		return err
	}
	defer f.Close()

	entry := map[string]interface{}{
		"type":      event.Type,
		"data":      json.RawMessage(event.Data),
		"position":  event.Position,
		"timestamp": time.Now().Unix(),
	}

	line, _ := json.Marshal(entry)
	_, err = f.Write(append(line, '\n'))

	// Update position
	if err == nil {
		s.position = event.Position
		os.WriteFile(filepath.Join(s.dataDir, "position"), []byte(fmt.Sprintf("%d", s.position)), 0644)
	}

	return err
}

func (s *FileEventStore) Load(ctx context.Context, from, to int64) ([]*ebu.StoredEvent, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var events []*ebu.StoredEvent

	file, err := os.Open(filepath.Join(s.dataDir, "events.jsonl"))
	if err != nil {
		return events, nil // No events yet
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for decoder.More() {
		var entry map[string]interface{}
		if err := decoder.Decode(&entry); err != nil {
			break
		}

		pos := int64(entry["position"].(float64))
		if pos >= from && (to == -1 || pos <= to) {
			events = append(events, &ebu.StoredEvent{
				Type:     entry["type"].(string),
				Data:     []byte(entry["data"].(json.RawMessage)),
				Position: pos,
			})
		}
	}

	return events, nil
}

func (s *FileEventStore) GetPosition(ctx context.Context) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.position, nil
}

func (s *FileEventStore) SaveSubscriptionPosition(ctx context.Context, subscriberID string, position int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	path := filepath.Join(s.dataDir, "sub_"+subscriberID)
	return os.WriteFile(path, []byte(fmt.Sprintf("%d", position)), 0644)
}

func (s *FileEventStore) GetSubscriptionPosition(ctx context.Context, subscriberID string) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	path := filepath.Join(s.dataDir, "sub_"+subscriberID)
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, nil // Start from beginning
	}

	var pos int64
	fmt.Sscanf(string(data), "%d", &pos)
	return pos, nil
}

// Blockchain scanner simulator
type BlockchainScanner struct {
	bus              *ebu.EventBus
	watchedAddresses map[string]bool
	lastBlock        int64
}

func NewBlockchainScanner(bus *ebu.EventBus, addresses []string) *BlockchainScanner {
	watched := make(map[string]bool)
	for _, addr := range addresses {
		watched[addr] = true
	}

	// Get last processed block from persistence
	ctx := context.Background()
	store := bus.GetStore()
	var lastBlock int64
	if store != nil {
		if pos, err := store.GetPosition(ctx); err == nil {
			// Position is our block height
			lastBlock = pos
		}
	}

	return &BlockchainScanner{
		bus:              bus,
		watchedAddresses: watched,
		lastBlock:        lastBlock,
	}
}

func (s *BlockchainScanner) ScanBlock(height int64) {
	// Simulate scanning a block
	s.bus.PublishAsync(BlockScanned{
		Height:    height,
		Hash:      fmt.Sprintf("0x%064d", height),
		Timestamp: time.Now(),
	})

	// Simulate finding transactions for watched addresses
	if height%3 == 0 { // Every 3rd block has a transaction
		for addr := range s.watchedAddresses {
			if height%5 == 0 { // Incoming transaction
				s.bus.PublishAsync(TransactionFound{
					BlockHeight: height,
					TxHash:      fmt.Sprintf("tx_%d_%s", height, addr),
					From:        "external",
					To:          addr,
					Amount:      1000000, // 0.01 in 8 decimal places
				})
			} else { // Outgoing transaction
				s.bus.PublishAsync(TransactionFound{
					BlockHeight: height,
					TxHash:      fmt.Sprintf("tx_%d_%s_out", height, addr),
					From:        addr,
					To:          "external",
					Amount:      500000,
				})
			}
		}
	}

	s.lastBlock = height
}

func main() {
	// Create data directory for persistence
	dataDir := "./blockchain_data"
	os.MkdirAll(dataDir, 0755)

	// Create event bus with file-based persistence
	store := NewFileEventStore(dataDir)
	bus := ebu.New(ebu.WithStore(store))

	// Create projection for balances
	projection := NewBalanceProjection(dataDir)

	// Subscribe to transaction events to update balances
	bus.SubscribeAsync(func(tx TransactionFound) {
		// Update sender balance
		if tx.From != "external" {
			newBalance := projection.UpdateBalance(tx.From, -tx.Amount)
			bus.PublishAsync(BalanceUpdated{
				Address:     tx.From,
				Balance:     newBalance,
				BlockHeight: tx.BlockHeight,
			})
			fmt.Printf("📤 %s sent %d (new balance: %d)\n", tx.From, tx.Amount, newBalance)
		}

		// Update receiver balance
		if tx.To != "external" {
			newBalance := projection.UpdateBalance(tx.To, tx.Amount)
			bus.PublishAsync(BalanceUpdated{
				Address:     tx.To,
				Balance:     newBalance,
				BlockHeight: tx.BlockHeight,
			})
			fmt.Printf("📥 %s received %d (new balance: %d)\n", tx.To, tx.Amount, newBalance)
		}
	})

	// Subscribe to block events
	bus.SubscribeAsync(func(block BlockScanned) {
		fmt.Printf("⛓️  Block %d scanned at %s\n", block.Height, block.Timestamp.Format("15:04:05"))
	})

	// Addresses we're watching
	watchedAddresses := []string{
		"wallet_alice",
		"wallet_bob",
		"wallet_charlie",
	}

	// Create scanner
	scanner := NewBlockchainScanner(bus, watchedAddresses)

	// Show current balances
	fmt.Println("\n💰 Current Balances:")
	for _, addr := range watchedAddresses {
		balance := projection.GetBalance(addr)
		fmt.Printf("  %s: %d\n", addr, balance)
	}

	// Resume from last position or start fresh
	startBlock := scanner.lastBlock + 1
	if startBlock == 1 {
		fmt.Println("\n🚀 Starting fresh sync from block 1")
	} else {
		fmt.Printf("\n♻️  Resuming from block %d\n", startBlock)

		// Replay events to rebuild in-memory state if needed
		ctx := context.Background()
		bus.SubscribeWithReplay(ctx, "tx-replay", func(tx TransactionFound) {
			// This would rebuild any in-memory state from persisted events
			log.Printf("Replayed transaction: %s", tx.TxHash)
		})
	}

	fmt.Println("\n📡 Scanning blockchain...")

	// Simulate blockchain scanning
	for i := startBlock; i < startBlock+10; i++ {
		scanner.ScanBlock(i)
		time.Sleep(500 * time.Millisecond) // Simulate block time
	}

	// Final balances
	fmt.Println("\n💰 Final Balances:")
	for _, addr := range watchedAddresses {
		balance := projection.GetBalance(addr)
		fmt.Printf("  %s: %d\n", addr, balance)
	}

	fmt.Println("\n✅ Sync complete! Run again to resume from last position.")
}
