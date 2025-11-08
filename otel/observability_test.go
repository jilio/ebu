package otel

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

// errorMeterProvider wraps a real MeterProvider and returns an errorMeter
type errorMeterProvider struct {
	metric.MeterProvider
	base   metric.MeterProvider
	failOn string
}

func (e *errorMeterProvider) Meter(name string, opts ...metric.MeterOption) metric.Meter {
	baseMeter := e.base.Meter(name, opts...)
	return &errorMeter{
		Meter:  baseMeter,
		base:   baseMeter,
		failOn: e.failOn,
	}
}

// errorMeter wraps a real Meter and returns errors for specific metric names
type errorMeter struct {
	metric.Meter
	base   metric.Meter
	failOn string
}

func (e *errorMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	if name == e.failOn {
		return nil, fmt.Errorf("failed to create counter: %s", name)
	}
	return e.base.Int64Counter(name, options...)
}

func (e *errorMeter) Int64UpDownCounter(name string, options ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	return e.base.Int64UpDownCounter(name, options...)
}

func (e *errorMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	return e.base.Int64Histogram(name, options...)
}

func (e *errorMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	return e.base.Int64Gauge(name, options...)
}

func (e *errorMeter) Int64ObservableCounter(name string, options ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	return e.base.Int64ObservableCounter(name, options...)
}

func (e *errorMeter) Int64ObservableUpDownCounter(name string, options ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	return e.base.Int64ObservableUpDownCounter(name, options...)
}

func (e *errorMeter) Int64ObservableGauge(name string, options ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	return e.base.Int64ObservableGauge(name, options...)
}

func (e *errorMeter) Float64Counter(name string, options ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	return e.base.Float64Counter(name, options...)
}

func (e *errorMeter) Float64UpDownCounter(name string, options ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	return e.base.Float64UpDownCounter(name, options...)
}

func (e *errorMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	if name == e.failOn {
		return nil, fmt.Errorf("failed to create histogram: %s", name)
	}
	return e.base.Float64Histogram(name, options...)
}

func (e *errorMeter) Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	return e.base.Float64Gauge(name, options...)
}

func (e *errorMeter) Float64ObservableCounter(name string, options ...metric.Float64ObservableCounterOption) (metric.Float64ObservableCounter, error) {
	return e.base.Float64ObservableCounter(name, options...)
}

func (e *errorMeter) Float64ObservableUpDownCounter(name string, options ...metric.Float64ObservableUpDownCounterOption) (metric.Float64ObservableUpDownCounter, error) {
	return e.base.Float64ObservableUpDownCounter(name, options...)
}

func (e *errorMeter) Float64ObservableGauge(name string, options ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	return e.base.Float64ObservableGauge(name, options...)
}

func (e *errorMeter) RegisterCallback(callback metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
	return e.base.RegisterCallback(callback, instruments...)
}

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

	t.Run("metric_creation_errors", func(t *testing.T) {
		// Test error on first metric (publishCounter)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.publish.count",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating publishCounter")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_handler_counter", func(t *testing.T) {
		// Test error on second metric (handlerCounter)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.handler.count",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating handlerCounter")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_handler_duration", func(t *testing.T) {
		// Test error on third metric (handlerDuration)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.handler.duration",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating handlerDuration")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_handler_errors", func(t *testing.T) {
		// Test error on fourth metric (handlerErrors)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.handler.errors",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating handlerErrors")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_persist_counter", func(t *testing.T) {
		// Test error on fifth metric (persistCounter)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.persist.count",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating persistCounter")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_persist_duration", func(t *testing.T) {
		// Test error on sixth metric (persistDuration)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.persist.duration",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating persistDuration")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
		}
	})

	t.Run("metric_creation_errors_persist_errors", func(t *testing.T) {
		// Test error on seventh metric (persistErrors)
		base := sdkmetric.NewMeterProvider()
		mp := &errorMeterProvider{
			MeterProvider: base,
			base:          base,
			failOn:        "eventbus.persist.errors",
		}
		obs, err := New(WithMeterProvider(mp))
		if err == nil {
			t.Fatal("expected error when creating persistErrors")
		}
		if obs != nil {
			t.Fatal("expected nil observability on error")
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
	mp := sdkmetric.NewMeterProvider()

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
