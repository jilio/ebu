// Package otel provides an OpenTelemetry implementation of
// eventbus.Observability, emitting spans and metrics for publish, handler,
// and persist operations.
//
// # Cardinality warning
//
// Event type names (as produced by the bus's TypeNamer) are embedded in span
// names and recorded as the event.type attribute on every metric. Attributes
// returned by SpanAttributer are attached to publish spans verbatim. Both are
// user-controlled escape hatches: a TypeNamer that derives names from dynamic
// data, or a SpanAttributer that returns high-cardinality values (user IDs,
// request IDs, ...), will explode metric cardinality and span-name
// cardinality in your telemetry backend. Keep type names static and keep
// SpanAttributes to low-cardinality, domain-level values.
//
// # Publish spans and persist errors
//
// OnPublishComplete carries no error parameter: a publish whose persistence
// failed still produces an unblemished publish span. Only the child persist
// span records the error (status Error plus a recorded error event). When
// hunting persistence failures, look at eventbus.persist spans and the
// eventbus.persist.errors metric, not the publish span's status.
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

// Context keys for storing event metadata
type contextKey int

const (
	asyncKey contextKey = iota
	publishSpanKey
	handlerSpanKey
	persistSpanKey
)

// durationBucketBoundaries are the explicit bucket boundaries (in
// milliseconds) for the duration histograms. The SDK defaults start at 5ms,
// but in-process handlers routinely finish in well under 1ms, which would
// collapse nearly all samples into the first bucket. These boundaries add
// sub-millisecond resolution while still covering slow I/O-bound handlers.
var durationBucketBoundaries = []float64{
	0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 5000,
}

// spanFromContext returns the span stored under key, falling back to the
// span in ctx. Retrieving the exact span started by the matching Start hook
// guards against a chained Observability implementation (or a future bus
// change) replacing the context's active span between Start and Complete.
func spanFromContext(ctx context.Context, key contextKey) trace.Span {
	if span, ok := ctx.Value(key).(trace.Span); ok {
		return span
	}
	return trace.SpanFromContext(ctx)
}

// millis converts a duration to fractional milliseconds. Using
// duration.Milliseconds() would truncate: in-process handlers routinely
// finish in well under 1ms and would all record 0.
func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// SpanAttributer allows events to provide custom span attributes.
// Events implementing this interface can enrich observability spans
// with domain-specific attributes.
//
// Example:
//
//	import (
//		ebuotel "github.com/jilio/ebu/otel"
//		"go.opentelemetry.io/otel/attribute"
//	)
//
//	type QueryExecuted struct {
//		OperationName string
//		Fields        []string
//	}
//
//	func (e QueryExecuted) SpanAttributes() []attribute.KeyValue {
//		return []attribute.KeyValue{
//			attribute.String("graphql.operation", e.OperationName),
//			attribute.StringSlice("graphql.fields", e.Fields),
//		}
//	}
type SpanAttributer interface {
	SpanAttributes() []attribute.KeyValue
}

// Observability implements eventbus.Observability using OpenTelemetry
type Observability struct {
	tracer trace.Tracer
	meter  metric.Meter

	// Metrics
	publishCounter  metric.Int64Counter
	handlerCounter  metric.Int64Counter
	handlerDuration metric.Float64Histogram
	handlerErrors   metric.Int64Counter
	persistCounter  metric.Int64Counter
	persistDuration metric.Float64Histogram
	persistErrors   metric.Int64Counter
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
		metric.WithExplicitBucketBoundaries(durationBucketBoundaries...),
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
		metric.WithExplicitBucketBoundaries(durationBucketBoundaries...),
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
func (o *Observability) OnPublishStart(ctx context.Context, eventType string, event any) context.Context {
	// Start a span for the publish operation
	ctx, span := o.tracer.Start(ctx, "eventbus.publish: "+eventType,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	// Add custom span attributes if event implements SpanAttributer
	if sa, ok := event.(SpanAttributer); ok {
		span.SetAttributes(sa.SpanAttributes()...)
	}

	// Remember the exact span so OnPublishComplete ends it even if a chained
	// implementation replaces the context's active span.
	ctx = context.WithValue(ctx, publishSpanKey, span)

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
	span := spanFromContext(ctx, publishSpanKey)
	span.End()
}

// OnHandlerStart is called when a handler starts executing.
//
// Note on async spans: async handler spans are children of the publish
// span's context, but the publish span may already have ended by the time
// they start (OnPublishComplete fires when synchronous handlers finish).
// This is legal in OpenTelemetry — the publish span's duration simply does
// not include async handler work.
func (o *Observability) OnHandlerStart(ctx context.Context, eventType string, async bool) context.Context {
	// The async flag is needed again in OnHandlerComplete, which does not
	// receive it; carry it via the context.
	ctx = context.WithValue(ctx, asyncKey, async)

	// Start a span for the handler
	spanName := "eventbus.handler: " + eventType
	if async {
		spanName = "eventbus.handler.async: " + eventType
	}

	ctx, span := o.tracer.Start(ctx, spanName,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
			attribute.Bool("async", async),
		),
	)

	// Remember the exact span so OnHandlerComplete ends it even if a chained
	// implementation replaces the context's active span.
	ctx = context.WithValue(ctx, handlerSpanKey, span)

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
func (o *Observability) OnHandlerComplete(ctx context.Context, eventType string, duration time.Duration, err error) {
	span := spanFromContext(ctx, handlerSpanKey)

	attrs := []attribute.KeyValue{
		attribute.String("event.type", eventType),
	}
	if async, ok := ctx.Value(asyncKey).(bool); ok {
		attrs = append(attrs, attribute.Bool("async", async))
	}

	// Record duration, tagged with error status so error latencies
	// (typically timeout-shaped) are separable from successes.
	durationAttrs := append(attrs, attribute.Bool("error", err != nil))
	o.handlerDuration.Record(ctx, millis(duration), metric.WithAttributes(durationAttrs...))

	// Handle errors. On success the span status is left Unset: the OTel spec
	// reserves Ok for explicit operator affirmation.
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		o.handlerErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	span.End()
}

// OnPersistStart is called when event persistence starts. The event's
// offset is not known yet; it is attached to the span in OnPersistComplete.
func (o *Observability) OnPersistStart(ctx context.Context, eventType string) context.Context {
	// Start a span for persistence
	ctx, span := o.tracer.Start(ctx, "eventbus.persist: "+eventType,
		trace.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	// Remember the exact span so OnPersistComplete ends it even if a chained
	// implementation replaces the context's active span.
	ctx = context.WithValue(ctx, persistSpanKey, span)

	// Increment persist counter
	o.persistCounter.Add(ctx, 1,
		metric.WithAttributes(
			attribute.String("event.type", eventType),
		),
	)

	return ctx
}

// OnPersistComplete is called when event persistence completes. On success
// the store-assigned offset is recorded on the persist span (span-only:
// offsets are high-cardinality and do not belong on metrics).
func (o *Observability) OnPersistComplete(ctx context.Context, eventType string, duration time.Duration, offset eventbus.Offset, err error) {
	span := spanFromContext(ctx, persistSpanKey)

	if offset != "" {
		span.SetAttributes(attribute.String("event.offset", string(offset)))
	}

	attrs := []attribute.KeyValue{
		attribute.String("event.type", eventType),
	}

	// Record duration, tagged with error status so error latencies
	// (typically timeout-shaped) are separable from successes.
	durationAttrs := append(attrs, attribute.Bool("error", err != nil))
	o.persistDuration.Record(ctx, millis(duration), metric.WithAttributes(durationAttrs...))

	// Handle errors. On success the span status is left Unset: the OTel spec
	// reserves Ok for explicit operator affirmation.
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		o.persistErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
	}

	span.End()
}

// Ensure Observability implements eventbus.Observability
var _ eventbus.Observability = (*Observability)(nil)
