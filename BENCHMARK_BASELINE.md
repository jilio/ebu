# Performance Baseline - v0.4.0

## Key Findings

### 🔴 Critical Performance Issues

1. **Lock Contention is Severe**
   - 10 subscribers: **12x slower** (102ns → 1258ns) 
   - 100 subscribers: **122x slower** (102ns → 12,206ns)
   - This confirms the single mutex bottleneck

2. **Concurrent Publishers Scale Poorly**
   - 10 publishers: **2x slower** (115ns → 246ns)
   - 1000 publishers: **4x slower** (115ns → 444ns)

3. **Memory Allocations Scale Linearly**
   - 10 subscribers: 80 B/op
   - 100 subscribers: 896 B/op  
   - 1000 subscribers: 8192 B/op

## Baseline Metrics

| Operation | Time (ns/op) | Memory (B/op) | Allocs/op |
|-----------|-------------|---------------|-----------|
| **Basic Operations** |
| PublishSubscribe | 105.2 | 8 | 1 |
| PublishContext | 73.08 | 8 | 1 |
| Subscribe/Unsubscribe | 84.24 | 64 | 1 |
| **Concurrent Publishers** |
| 1 publisher | 115.1 | 13 | 2 |
| 10 publishers | 246.7 | 13 | 2 |
| 100 publishers | 246.8 | 15 | 2 |
| 1000 publishers | 444.2 | 21 | 2 |
| **Multiple Subscribers** |
| 1 subscriber | 102.2 | 8 | 1 |
| 10 subscribers | 188.9 | 80 | 1 |
| 100 subscribers | 1,258 | 896 | 1 |
| 1000 subscribers | 12,206 | 8,192 | 1 |
| **Lock Contention (r=readers, w=writers)** |
| r1-w1 | 425.2 | 8 | 1 |
| r10-w1 | 2,999 | 80 | 1 |
| r1-w10 | 398.8 | 8 | 1 |
| r10-w10 | 1,463 | 80 | 1 |
| r100-w10 | 5,603 | 896 | 1 |
| **CQRS Operations** |
| CommandBus | 82.27 | 40 | 2 |
| QueryBus | 14.53 | 0 | 0 |
| QueryWithCache | 2,945 | 0 | 0 |
| ProjectionManager | 120.7 | 104 | 3 |
| **Aggregate Operations** |
| RaiseEvent | 90.51 | 114 | 1 |
| LoadFromHistory | 40.15 | 96 | 1 |

## Performance Goals for v0.5.0

Based on these metrics, our optimization targets are:

1. **Reduce lock contention by 10x**
   - Target: 100 subscribers < 1,000 ns/op (currently 12,206)
   - Solution: Sharded locks

2. **Improve concurrent publisher scaling**
   - Target: Linear scaling up to 100 publishers
   - Solution: Lock-free data structures for hot paths

3. **Reduce memory allocations**
   - Target: Sub-linear memory growth with subscribers
   - Solution: Pre-allocated buffers, object pools

4. **Add async backpressure**
   - Target: Prevent unbounded goroutine growth
   - Solution: Worker pool with bounded queues