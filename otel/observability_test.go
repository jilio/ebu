package otel

import (
	"context"
	"errors"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestNew(t *testing.T) {
	t.Run("default_providers", func(t *testing.T) {
		obs, err := New()
		if err != nil {
			t.Fatalf("New() failed: %v", err)
		}
		if obs == nil {
			t.Fatal("New() returned nil")
		}
	})

	t.Run("custom_tracer_provider", func(t *testing.T) {
		tp := sdktrace.NewTracerProvider()
		obs, err := New(WithTracerProvider(tp))
		if err != nil {
			t.Fatalf("New() with custom tracer failed: %v", err)
		}
		if obs.tracer == nil {
			t.Fatal("tracer not set")
		}
	})

	t.Run("custom_meter_provider", func(t *testing.T) {
		mp := sdkmetric.NewMeterProvider()
		obs, err := New(WithMeterProvider(mp))
		if err != nil {
			t.Fatalf("New() with custom meter failed: %v", err)
		}
		if obs.meter == nil {
			t.Fatal("meter not set")
		}
	})
}

func TestObservabilityInterface(t *testing.T) {
	// Verify that Observability implements eventbus.Observability
	var _ eventbus.Observability = (*Observability)(nil)
}

func TestPublishTracing(t *testing.T) {
	// Create trace exporter
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	obs, err := New(WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()
	ctx = obs.OnPublishStart(ctx, "TestEvent")
	obs.OnPublishComplete(ctx, "TestEvent")

	// Force flush
	if err := tp.ForceFlush(ctx); err != nil {
		t.Fatalf("ForceFlush failed: %v", err)
	}

	spans := exporter.GetSpans()
	if len(spans) != 1 {
		t.Fatalf("expected 1 span, got %d", len(spans))
	}

	span := spans[0]
	if span.Name != "eventbus.publish" {
		t.Errorf("expected span name 'eventbus.publish', got %q", span.Name)
	}

	// Check attributes
	found := false
	for _, attr := range span.Attributes {
		if string(attr.Key) == "event.type" && attr.Value.AsString() == "TestEvent" {
			found = true
			break
		}
	}
	if !found {
		t.Error("span missing event.type attribute")
	}
}

func TestHandlerTracing(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	obs, err := New(WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("sync_handler", func(t *testing.T) {
		exporter.Reset()

		ctx := context.Background()
		ctx = obs.OnHandlerStart(ctx, "TestEvent", false)
		obs.OnHandlerComplete(ctx, 100*time.Millisecond, nil)

		tp.ForceFlush(ctx)

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "eventbus.handler" {
			t.Errorf("expected span name 'eventbus.handler', got %q", span.Name)
		}

		// Check async attribute
		foundAsync := false
		for _, attr := range span.Attributes {
			if string(attr.Key) == "async" && !attr.Value.AsBool() {
				foundAsync = true
				break
			}
		}
		if !foundAsync {
			t.Error("span missing or incorrect async attribute")
		}
	})

	t.Run("async_handler", func(t *testing.T) {
		exporter.Reset()

		ctx := context.Background()
		ctx = obs.OnHandlerStart(ctx, "TestEvent", true)
		obs.OnHandlerComplete(ctx, 100*time.Millisecond, nil)

		tp.ForceFlush(ctx)

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "eventbus.handler.async" {
			t.Errorf("expected span name 'eventbus.handler.async', got %q", span.Name)
		}
	})

	t.Run("handler_with_error", func(t *testing.T) {
		exporter.Reset()

		ctx := context.Background()
		ctx = obs.OnHandlerStart(ctx, "TestEvent", false)
		testErr := errors.New("test error")
		obs.OnHandlerComplete(ctx, 100*time.Millisecond, testErr)

		tp.ForceFlush(ctx)

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Status.Code != codes.Error {
			t.Errorf("expected error status, got %v", span.Status.Code)
		}

		// Check for error event
		if len(span.Events) == 0 {
			t.Error("expected error event in span")
		}
	})
}

func TestPersistTracing(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
	)

	obs, err := New(WithTracerProvider(tp))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	t.Run("success", func(t *testing.T) {
		exporter.Reset()

		ctx := context.Background()
		ctx = obs.OnPersistStart(ctx, "TestEvent", 123)
		obs.OnPersistComplete(ctx, 50*time.Millisecond, nil)

		tp.ForceFlush(ctx)

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Name != "eventbus.persist" {
			t.Errorf("expected span name 'eventbus.persist', got %q", span.Name)
		}

		// Check position attribute
		foundPosition := false
		for _, attr := range span.Attributes {
			if string(attr.Key) == "position" && attr.Value.AsInt64() == 123 {
				foundPosition = true
				break
			}
		}
		if !foundPosition {
			t.Error("span missing or incorrect position attribute")
		}
	})

	t.Run("error", func(t *testing.T) {
		exporter.Reset()

		ctx := context.Background()
		ctx = obs.OnPersistStart(ctx, "TestEvent", 456)
		testErr := errors.New("persistence error")
		obs.OnPersistComplete(ctx, 50*time.Millisecond, testErr)

		tp.ForceFlush(ctx)

		spans := exporter.GetSpans()
		if len(spans) != 1 {
			t.Fatalf("expected 1 span, got %d", len(spans))
		}

		span := spans[0]
		if span.Status.Code != codes.Error {
			t.Errorf("expected error status, got %v", span.Status.Code)
		}
	})
}

func TestMetrics(t *testing.T) {
	// Create a metric reader
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
	)

	obs, err := New(WithMeterProvider(mp))
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	ctx := context.Background()

	// Simulate publish
	ctx = obs.OnPublishStart(ctx, "TestEvent")
	obs.OnPublishComplete(ctx, "TestEvent")

	// Simulate handler execution
	ctx = obs.OnHandlerStart(ctx, "TestEvent", false)
	obs.OnHandlerComplete(ctx, 100*time.Millisecond, nil)

	// Simulate persistence
	ctx = obs.OnPersistStart(ctx, "TestEvent", 1)
	obs.OnPersistComplete(ctx, 50*time.Millisecond, nil)

	// Collect metrics
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Collect failed: %v", err)
	}

	// Verify we have metrics
	if len(rm.ScopeMetrics) == 0 {
		t.Fatal("no scope metrics collected")
	}

	metrics := rm.ScopeMetrics[0].Metrics
	if len(metrics) == 0 {
		t.Fatal("no metrics collected")
	}

	// We should have at least publish, handler, and persist counters
	metricNames := make(map[string]bool)
	for _, m := range metrics {
		metricNames[m.Name] = true
	}

	expectedMetrics := []string{
		"eventbus.publish.count",
		"eventbus.handler.count",
		"eventbus.handler.duration",
		"eventbus.persist.count",
		"eventbus.persist.duration",
	}

	for _, expected := range expectedMetrics {
		if !metricNames[expected] {
			t.Errorf("missing metric: %s", expected)
		}
	}
}

func TestIntegrationWithEventBus(t *testing.T) {
	// Create observability with in-memory providers
	tp := sdktrace.NewTracerProvider()
	mp := noop.NewMeterProvider()

	obs, err := New(
		WithTracerProvider(tp),
		WithMeterProvider(mp),
	)
	if err != nil {
		t.Fatalf("New() failed: %v", err)
	}

	// Create event bus with observability
	bus := eventbus.New(eventbus.WithObservability(obs))

	type TestEvent struct {
		Message string
	}

	var handlerCalled bool
	eventbus.Subscribe(bus, func(e TestEvent) {
		handlerCalled = true
	})

	eventbus.Publish(bus, TestEvent{Message: "test"})

	if !handlerCalled {
		t.Error("handler was not called")
	}
}
