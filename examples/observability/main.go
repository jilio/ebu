package main

import (
	"context"
	"fmt"
	"log"
	"time"

	eventbus "github.com/jilio/ebu"
	"github.com/jilio/ebu/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

type UserCreatedEvent struct {
	UserID   string
	Username string
	Email    string
}

type OrderPlacedEvent struct {
	OrderID    string
	UserID     string
	TotalPrice float64
}

func main() {
	// Setup resource for OpenTelemetry
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			"", // Empty schema URL to avoid conflicts
			semconv.ServiceName("ebu-example"),
			semconv.ServiceVersion("1.0.0"),
		),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Setup trace provider with stdout exporter
	traceExporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint(),
	)
	if err != nil {
		log.Fatal(err)
	}

	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(traceExporter),
		sdktrace.WithResource(res),
	)
	defer func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// Setup metric provider with stdout exporter
	metricExporter, err := stdoutmetric.New(
		stdoutmetric.WithPrettyPrint(),
	)
	if err != nil {
		log.Fatal(err)
	}

	meterProvider := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExporter)),
		sdkmetric.WithResource(res),
	)
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	// Create observability instance
	obs, err := otel.New(
		otel.WithTracerProvider(tracerProvider),
		otel.WithMeterProvider(meterProvider),
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create event bus with observability
	bus := eventbus.New(eventbus.WithObservability(obs))

	// Subscribe to events
	eventbus.Subscribe(bus, func(event UserCreatedEvent) {
		fmt.Printf("User created: %s (%s)\n", event.Username, event.UserID)
	})

	eventbus.Subscribe(bus, func(event OrderPlacedEvent) {
		fmt.Printf("Order placed: %s for user %s ($%.2f)\n",
			event.OrderID, event.UserID, event.TotalPrice)
	}, eventbus.Async())

	// Publish events
	fmt.Println("\n=== Publishing Events ===")
	fmt.Println()

	eventbus.Publish(bus, UserCreatedEvent{
		UserID:   "user123",
		Username: "john_doe",
		Email:    "john@example.com",
	})

	eventbus.Publish(bus, OrderPlacedEvent{
		OrderID:    "order456",
		UserID:     "user123",
		TotalPrice: 99.99,
	})

	// Wait for async handlers
	bus.Wait()

	// Give time for metrics to be exported
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\n=== Observability Data ===")
	fmt.Println("Check the stdout output above for traces and metrics")
}
