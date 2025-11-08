# Event Bus Observability Example

This example demonstrates comprehensive observability for the EBU event bus using OpenTelemetry, Prometheus, Grafana, and Jaeger.

## Architecture

```
┌─────────────────┐
│ Load Generator  │  Publishes events at random intervals
└────────┬────────┘
         │
         v
┌─────────────────┐
│   Event Bus     │  Processes events with observability
│   (with OTel)   │
└────────┬────────┘
         │
         v
┌─────────────────┐
│ OTEL Collector  │  Collects traces & metrics
└────┬────────┬───┘
     │        │
     v        v
┌─────────┐ ┌──────────┐
│ Jaeger  │ │Prometheus│
└─────────┘ └────┬─────┘
                 │
                 v
            ┌─────────┐
            │ Grafana │
            └─────────┘
```

## Components

- **Load Generator**: Go application that continuously publishes various event types
- **OpenTelemetry Collector**: Receives telemetry and exports to backends
- **Prometheus**: Stores and queries metrics
- **Grafana**: Visualizes metrics with pre-configured dashboards
- **Jaeger**: Distributed tracing UI

## Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.24+ (for local development)

### Running the Stack

1. Start all services:

```bash
docker-compose up --build
```

2. Access the UIs:

- **Grafana**: http://localhost:3000 (admin/admin)
  - Pre-configured dashboard: "Event Bus Metrics"
- **Prometheus**: http://localhost:9090
- **Jaeger**: http://localhost:16686

### Stopping

```bash
docker-compose down
```

To also remove volumes:

```bash
docker-compose down -v
```

## What You'll See

### Grafana Dashboard

The pre-configured dashboard shows:

1. **Event Publish Rate**: Events published per second
2. **Handler Execution Rate**: Handler executions per second
3. **p95 Handler Duration**: 95th percentile latency gauge
4. **Handler Error Rate**: Errors per second (from simulated failures)
5. **Handler Duration Percentiles**: p50, p95, p99 latencies over time

### Jaeger Traces

Navigate to Jaeger UI to see:

- Distributed traces showing event flow through handlers
- Spans for publish, handler execution, and nested events
- Error spans with stack traces
- Timeline visualization of async handlers

### Prometheus Metrics

Available metrics (namespace: `ebu`):

- `ebu_eventbus_publish_count` - Total events published
- `ebu_eventbus_handler_count` - Total handler executions
- `ebu_eventbus_handler_duration` - Handler execution time histogram
- `ebu_eventbus_handler_errors` - Total handler errors
- `ebu_eventbus_persist_count` - Events persisted (if enabled)
- `ebu_eventbus_persist_duration` - Persistence time histogram
- `ebu_eventbus_persist_errors` - Persistence errors

## Event Types Generated

The load generator simulates a realistic e-commerce system:

1. **UserCreatedEvent** (30%)
   - Triggers welcome email notification

2. **OrderPlacedEvent** (40%)
   - Triggers payment processing
   - Triggers inventory update

3. **NotificationSentEvent** (30%)
   - 5% error rate (simulated failures)

## Customizing

### Adjust Load Generation

Edit `generator/main.go`:

```go
// Change event frequency
ticker := time.NewTicker(100 * time.Millisecond) // Default: 100ms

// Adjust error rate
if rand.Float64() < 0.05 { // Default: 5%
    panic("notification service temporarily unavailable")
}
```

### Add Custom Metrics

The observability package automatically tracks:

- Event publish operations
- Handler executions (sync and async)
- Event persistence (if configured)
- Errors and panics

No code changes needed!

### Modify Dashboard

1. Open Grafana at http://localhost:3000
2. Navigate to "Event Bus Metrics" dashboard
3. Click "Dashboard settings" → "JSON Model"
4. Make changes and save
5. Export JSON and replace `grafana/dashboards/ebu-dashboard.json`

## Local Development

Run the load generator locally (outside Docker):

```bash
cd generator
go mod download

# Start the infrastructure (without load generator)
cd ..
docker-compose up prometheus grafana jaeger otel-collector

# Run generator locally
cd generator
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 go run main.go
```

## Troubleshooting

### No metrics in Grafana

1. Check OTEL Collector logs:
   ```bash
   docker-compose logs otel-collector
   ```

2. Verify Prometheus is scraping:
   - Go to http://localhost:9090/targets
   - Both targets should be "UP"

3. Check load generator is running:
   ```bash
   docker-compose logs load-generator
   ```

### No traces in Jaeger

1. Verify OTEL Collector is receiving traces:
   ```bash
   docker-compose logs otel-collector | grep trace
   ```

2. Check load generator OTLP endpoint:
   ```bash
   docker-compose logs load-generator | grep -i error
   ```

### Grafana dashboard is empty

1. Wait 30-60 seconds for initial metrics to collect
2. Check time range (top right) - should be "Last 15 minutes"
3. Verify data source: Dashboard → Settings → Variables → Data source should be "Prometheus"

## Production Considerations

This is a demo setup. For production:

1. **Security**:
   - Change default Grafana password
   - Enable TLS for OTLP endpoints
   - Add authentication to Prometheus

2. **Scalability**:
   - Use remote storage for Prometheus (e.g., Thanos, Cortex)
   - Scale OTEL Collector horizontally
   - Configure appropriate retention policies

3. **High Availability**:
   - Run multiple collector instances
   - Use Prometheus federation
   - Configure Grafana with HA

4. **Resource Limits**:
   - Set memory/CPU limits in docker-compose.yml
   - Configure OTEL Collector batch processors
   - Tune Prometheus retention and scrape intervals

## Learn More

- [OpenTelemetry Documentation](https://opentelemetry.io/docs/)
- [Prometheus Querying](https://prometheus.io/docs/prometheus/latest/querying/basics/)
- [Grafana Dashboards](https://grafana.com/docs/grafana/latest/dashboards/)
- [Jaeger Tracing](https://www.jaegertracing.io/docs/)
- [EBU Observability Package](../../otel/)
