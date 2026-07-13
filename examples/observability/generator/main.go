package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	eventbus "github.com/jilio/ebu"
	"github.com/jilio/ebu/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Event types
type UserCreatedEvent struct {
	UserID   string
	Username string
	Email    string
}

type OrderPlacedEvent struct {
	OrderID    string
	UserID     string
	TotalPrice float64
	ItemCount  int
}

type PaymentProcessedEvent struct {
	PaymentID     string
	OrderID       string
	Amount        float64
	PaymentMethod string
}

type InventoryUpdatedEvent struct {
	ProductID string
	Quantity  int
	Warehouse string
}

type NotificationSentEvent struct {
	NotificationID string
	UserID         string
	Channel        string
	MessageType    string
}

func main() {
	ctx := context.Background()

	// Create observability and keep the exact providers it uses so they can be
	// flushed and shut down when the generator exits.
	obs, shutdown, err := setupObservability(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := shutdown(ctx); err != nil {
			log.Printf("Error shutting down: %v", err)
		}
	}()

	bus := eventbus.New(eventbus.WithObservability(obs))

	// Register handlers
	registerHandlers(bus)

	// Start load generation
	log.Println("🚀 Starting event bus load generator...")
	log.Println("📊 Metrics available at: http://localhost:9090 (Prometheus)")
	log.Println("📈 Dashboards available at: http://localhost:3000 (Grafana)")
	log.Println("🔍 Traces available at: http://localhost:16686 (Jaeger)")
	log.Println()

	// Handle graceful shutdown
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	stopCh := make(chan struct{})
	go generateLoad(bus, stopCh)

	<-sigCh
	log.Println("\n⏹️  Shutting down gracefully...")
	close(stopCh)
	bus.Wait()
	log.Println("✅ Shutdown complete")
}

func setupObservability(ctx context.Context) (*otel.Observability, func(context.Context) error, error) {
	// Setup resource
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"", // Empty schema URL to avoid conflicts
			semconv.ServiceName("ebu-load-generator"),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	// Setup trace exporter
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithInsecure(),
		otlptracehttp.WithEndpoint(getOTelEndpoint()),
	)
	if err != nil {
		return nil, nil, err
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)

	// Setup metric exporter
	metricExporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithInsecure(),
		otlpmetrichttp.WithEndpoint(getOTelEndpoint()),
	)
	if err != nil {
		shutdownErr := shutdownOTelProviders(ctx, tracerProvider.Shutdown)
		return nil, nil, errors.Join(err, shutdownErr)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)

	obs, err := otel.New(
		otel.WithTracerProvider(tracerProvider),
		otel.WithMeterProvider(meterProvider),
	)
	if err != nil {
		shutdownErr := shutdownOTelProviders(ctx, meterProvider.Shutdown, tracerProvider.Shutdown)
		return nil, nil, errors.Join(err, shutdownErr)
	}

	shutdown := func(ctx context.Context) error {
		return shutdownOTelProviders(ctx, meterProvider.Shutdown, tracerProvider.Shutdown)
	}
	return obs, shutdown, nil
}

// shutdownOTelProviders attempts every shutdown even if an earlier provider
// fails, so one exporter cannot prevent the other from flushing and closing.
func shutdownOTelProviders(ctx context.Context, shutdowns ...func(context.Context) error) error {
	var errs []error
	for _, shutdown := range shutdowns {
		errs = append(errs, shutdown(ctx))
	}
	return errors.Join(errs...)
}

func getOTelEndpoint() string {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4318"
	}
	// Remove http:// prefix if present
	if len(endpoint) > 7 && endpoint[:7] == "http://" {
		endpoint = endpoint[7:]
	}
	return endpoint
}

func registerHandlers(bus *eventbus.EventBus) {
	// User created handlers
	eventbus.Subscribe(bus, func(e UserCreatedEvent) {
		// Simulate work
		time.Sleep(time.Duration(rand.Intn(20)) * time.Millisecond)
	})

	eventbus.SubscribeContext(bus, func(ctx context.Context, e UserCreatedEvent) {
		// Send welcome email (async)
		time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
		// Use PublishContext to propagate trace context
		eventbus.PublishContext(bus, ctx, NotificationSentEvent{
			NotificationID: fmt.Sprintf("notif-%d", rand.Int63()),
			UserID:         e.UserID,
			Channel:        "email",
			MessageType:    "welcome",
		})
	}, eventbus.Async())

	// Order placed handlers
	eventbus.SubscribeContext(bus, func(ctx context.Context, e OrderPlacedEvent) {
		// Process payment
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		// Use PublishContext to propagate trace context
		eventbus.PublishContext(bus, ctx, PaymentProcessedEvent{
			PaymentID:     fmt.Sprintf("pay-%d", rand.Int63()),
			OrderID:       e.OrderID,
			Amount:        e.TotalPrice,
			PaymentMethod: "credit_card",
		})
	})

	eventbus.SubscribeContext(bus, func(ctx context.Context, e OrderPlacedEvent) {
		// Update inventory (async)
		time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		// Use PublishContext to propagate trace context
		eventbus.PublishContext(bus, ctx, InventoryUpdatedEvent{
			ProductID: fmt.Sprintf("prod-%d", rand.Intn(100)),
			Quantity:  -e.ItemCount,
			Warehouse: "main",
		})
	}, eventbus.Async())

	// Payment processed handler
	eventbus.Subscribe(bus, func(e PaymentProcessedEvent) {
		// Send confirmation
		time.Sleep(time.Duration(rand.Intn(40)) * time.Millisecond)
	}, eventbus.Async())

	// Inventory updated handler
	eventbus.Subscribe(bus, func(e InventoryUpdatedEvent) {
		// Log inventory change
		time.Sleep(time.Duration(rand.Intn(10)) * time.Millisecond)
	})

	// Notification handler with occasional errors
	eventbus.Subscribe(bus, func(e NotificationSentEvent) {
		time.Sleep(time.Duration(rand.Intn(30)) * time.Millisecond)
		// Simulate 5% error rate
		if rand.Float64() < 0.05 {
			panic("notification service temporarily unavailable")
		}
	})
}

func generateLoad(bus *eventbus.EventBus, stopCh <-chan struct{}) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	userID := 0
	orderID := 0

	for {
		select {
		case <-stopCh:
			return
		case <-ticker.C:
			// Randomly choose event type
			eventType := rand.Intn(10)

			switch {
			case eventType < 3: // 30% user events
				userID++
				eventbus.Publish(bus, UserCreatedEvent{
					UserID:   fmt.Sprintf("user-%d", userID),
					Username: fmt.Sprintf("user%d", userID),
					Email:    fmt.Sprintf("user%d@example.com", userID),
				})
				log.Printf("👤 User created: user-%d", userID)

			case eventType < 7: // 40% order events
				orderID++
				itemCount := rand.Intn(5) + 1
				eventbus.Publish(bus, OrderPlacedEvent{
					OrderID:    fmt.Sprintf("order-%d", orderID),
					UserID:     fmt.Sprintf("user-%d", rand.Intn(userID+1)),
					TotalPrice: float64(rand.Intn(500) + 10),
					ItemCount:  itemCount,
				})
				log.Printf("🛒 Order placed: order-%d (%d items)", orderID, itemCount)

			default: // 30% notification events
				eventbus.Publish(bus, NotificationSentEvent{
					NotificationID: fmt.Sprintf("notif-%d", rand.Int63()),
					UserID:         fmt.Sprintf("user-%d", rand.Intn(userID+1)),
					Channel:        []string{"email", "sms", "push"}[rand.Intn(3)],
					MessageType:    []string{"promo", "update", "alert"}[rand.Intn(3)],
				})
				log.Printf("📧 Notification sent")
			}

			// Variable load - sometimes burst, sometimes slow
			if rand.Float64() < 0.1 {
				// 10% chance of burst
				time.Sleep(time.Duration(rand.Intn(50)) * time.Millisecond)
			} else {
				// Normal pace
				time.Sleep(time.Duration(rand.Intn(200)+100) * time.Millisecond)
			}
		}
	}
}
