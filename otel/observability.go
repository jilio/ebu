package otel

import (
	"context"
	"time"

	eventbus "github.com/jilio/ebu"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName = "github.com/jilio/ebu"
)

// Observability implements eventbus.Observability using OpenTelemetry
type Observability struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Metrics
	publishCounter    metric.Int64Counter
	handlerCounter    metric.Int64Counter
	handlerDuration   metric.Float64Histogram
	handlerErrors     metric.Int64Counter
	persistCounter    metric.Int64Counter
	persistDuration   metric.Float64Histogram
	persistErrors     metric.Int64Counter
}

// Option configures the Observability
type Option func(*Observability)

// WithTracerProvider sets a custom tracer provider
func WithTracerProvider(provider trace.TracerProvider) Option {
	return func(o *Observability) {
		o.tracer = provider.Tracer(instrumentationName)
	}
}

// WithMeterProvider sets a custom meter provider
func WithMeterProvider(provider metric.MeterProvider) Option {
	return func(o *Observability) {
		o.meter = provider.Meter(instrumentationName)
	}
}

// New creates a new OpenTelemetry observability implementation
func New(opts ...Option) (*Observability, error) {
	obs := &Observability{
		tracer: otel.Tracer(instrumentationName),
		meter:  otel.Meter(instrumentationName),
	}

	// Apply options
	for _, opt := range opts {
		opt(obs)
	}

	// Initialize metrics
	var err error

	obs.publishCounter, err = obs.meter.Int64Counter(
		"eventbus.publish.count",
		metric.WithDescription("Number of events published"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	obs.handlerCounter, err = obs.meter.Int64Counter(
		"eventbus.handler.count",
		metric.WithDescription("Number of handler executions"),
		metric.WithUnit("{execution}"),
	)
	if err != nil {
		return nil, err
	}

	obs.handlerDuration, err = obs.meter.Float64Histogram(
		"eventbus.handler.duration",
		metric.WithDescription("Handler execution duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	obs.handlerErrors, err = obs.meter.Int64Counter(
		"eventbus.handler.errors",
		metric.WithDescription("Number of handler errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	obs.persistCounter, err = obs.meter.Int64Counter(
		"eventbus.persist.count",
		metric.WithDescription("Number of events persisted"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return nil, err
	}

	obs.persistDuration, err = obs.meter.Float64Histogram(
		"eventbus.persist.duration",
		metric.WithDescription("Event persistence duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}

	obs.persistErrors, err = obs.meter.Int64Counter(
		"eventbus.persist.errors",
		metric.WithDescription("Number of persistence errors"),
		metric.WithUnit("{error}"),
	)
	if err != nil {
		return nil, err
	}

	return obs, nil
}

// OnPublishStart is called when an event starts publishing
func (o *Observability) OnPublishStart(ctx context.Context, eventType string) context.Context {
	// Start a span for the publish operation
	ctx, _ = o.tracer.Start(ctx, "eventbus.publish: "+eventType,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	// Increment publish counter
	o.publishCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	return ctx
}

// OnPublishComplete is called when an event completes publishing (all sync handlers done)
func (o *Observability) OnPublishComplete(ctx context.Context, eventType string) {
	// End the publish span
	span := trace.SpanFromContext(ctx)
	span.End()
}

// OnHandlerStart is called when a handler starts executing
func (o *Observability) OnHandlerStart(ctx context.Context, eventType string, async bool) context.Context {
	// Start a span for the handler
	spanName := "eventbus.handler: " + eventType
	if async {
		spanName = "eventbus.handler.async: " + eventType
	}

	ctx, _ = o.tracer.Start(ctx, spanName,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
			attribute.Bool("async", async),
		),
	)

	// Increment handler counter
	o.handlerCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("event.type", eventType),
			attribute.Bool("async", async),
		),
	)

	return ctx
}

// OnHandlerComplete is called when a handler completes (with or without error)
func (o *Observability) OnHandlerComplete(ctx context.Context, duration time.Duration, err error) {
	span := trace.SpanFromContext(ctx)

	// Extract attributes from span
	var attrs []attribute.KeyValue
	if span.SpanContext().IsValid() {
		// Get event type and async flag from span attributes
		// We'll use metric options with empty attributes for now
		// as we can't easily extract them from the span
	}

	// Record duration
	durationMs := float64(duration.Milliseconds())
	o.handlerDuration.Record(ctx, durationMs, metric.WithAttributes(attrs...))

	// Handle errors
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		o.handlerErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	} else {
		span.SetStatus(codes.Ok, "")
	}

	span.End()
}

// OnPersistStart is called when event persistence starts
func (o *Observability) OnPersistStart(ctx context.Context, eventType string, position int64) context.Context {
	// Start a span for persistence
	ctx, _ = o.tracer.Start(ctx, "eventbus.persist: "+eventType,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
			attribute.Int64("position", position),
		),
	)

	// Increment persist counter
	o.persistCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	return ctx
}

// OnPersistComplete is called when event persistence completes
func (o *Observability) OnPersistComplete(ctx context.Context, duration time.Duration, err error) {
	span := trace.SpanFromContext(ctx)

	// Extract attributes from span
	var attrs []attribute.KeyValue
	if span.SpanContext().IsValid() {
		// Get event type from span attributes
		// We'll use metric options with empty attributes for now
	}

	// Record duration
	durationMs := float64(duration.Milliseconds())
	o.persistDuration.Record(ctx, durationMs, metric.WithAttributes(attrs...))

	// Handle errors
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		o.persistErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	} else {
		span.SetStatus(codes.Ok, "")
	}

	span.End()
}

// Ensure Observability implements eventbus.Observability
var _ eventbus.Observability = (*Observability)(nil)
