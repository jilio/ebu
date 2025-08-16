# Performance Improvements - V2 Implementation

## Summary

Implemented sharded locks (32 shards) to reduce lock contention. Initial results show:

### ✅ Concurrent Publishers - MAJOR IMPROVEMENT
- **10 publishers**: 26% faster (172.3ns → 136.5ns)
- **100 publishers**: 53% faster (220.3ns → 143.8ns)  
- **1000 publishers**: 150% faster! (393.3ns → 156.8ns)

### 🔶 Multiple Subscribers - MINOR IMPROVEMENT
- **10 subscribers**: 7% slower (171ns → 183ns) - overhead from sharding
- **100 subscribers**: 8% faster (1201ns → 1105ns)
- **1000 subscribers**: 8% faster (11270ns → 10421ns)

## Key Insights

1. **Sharding works best for write-heavy workloads** - Concurrent publishers see massive improvements
2. **Small overhead for simple cases** - Single publisher/subscriber scenarios are slightly slower due to hashing overhead
3. **Memory usage slightly higher** - Extra 16B/op from handler copying before execution

## V2 Architecture Changes

```go
// Before: Single mutex for all event types
type EventBus struct {
    mu       sync.RWMutex
    handlers map[reflect.Type][]*internalHandler
}

// After: 32 sharded mutexes
type EventBusV2 struct {
    shards [32]*shard  // Each shard has its own mutex
}

type shard struct {
    mu       sync.RWMutex
    handlers map[reflect.Type][]*internalHandler
}
```

## Next Steps

1. **Fix worst-case scenario deadlock** - Debug why 100 subscribers + 10 publishers deadlocks
2. **Optimize memory allocations** - Reduce handler copy overhead
3. **Add event versioning** - Critical for production use
4. **Implement backpressure** - Prevent unbounded goroutine growth

## Recommendation

V2 shows promise but needs refinement:
- ✅ Use for high-concurrency scenarios (>10 concurrent publishers)
- ⚠️ Fix deadlock issue before production use
- 🔄 Consider adaptive sharding based on load