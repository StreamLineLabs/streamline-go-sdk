// Package streamline provides an idiomatic Go client for Streamline.
//
// This file adds optional OpenTelemetry tracing support. Tracing wraps
// produce and consume operations with OTel spans following the semantic
// conventions for messaging systems.
//
// Span naming: "{topic} {operation}" (e.g., "orders produce", "events consume").
// Attributes:  messaging.system=streamline, messaging.destination.name={topic},
//
//	messaging.operation={produce|consume}.
//
// Kind: PRODUCER for produce, CONSUMER for consume.
package streamline

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

const (
	instrumentationName    = "streamline-go-sdk"
	instrumentationVersion = "0.2.0"
)

// messagingAttrs returns common OTel attributes for messaging spans.
func messagingAttrs(topic, operation string) []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String("messaging.system", "streamline"),
		attribute.String("messaging.destination.name", topic),
		attribute.String("messaging.operation", operation),
	}
}

// HeaderCarrier adapts a map[string][]byte for OTel context propagation.
type HeaderCarrier map[string][]byte

// Get returns the value for a key.
func (c HeaderCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	return string(v)
}

// Set sets a key-value pair.
func (c HeaderCarrier) Set(key string, value string) {
	c[key] = []byte(value)
}

// Keys returns all keys.
func (c HeaderCarrier) Keys() []string {
	keys := make([]string, 0, len(c))
	for k := range c {
		keys = append(keys, k)
	}
	return keys
}

// TracingProducer wraps a Producer with automatic OTel span creation.
type TracingProducer struct {
	inner  *Producer
	tracer trace.Tracer
}

// NewTracingProducer wraps an existing Producer with OpenTelemetry tracing.
//
// Every call to Send, SendMessage, and SendBatch will create a PRODUCER span
// and inject trace context into message headers.
func NewTracingProducer(producer *Producer) *TracingProducer {
	return &TracingProducer{
		inner:  producer,
		tracer: otel.GetTracerProvider().Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion)),
	}
}

// Send sends a single message with tracing.
func (tp *TracingProducer) Send(ctx context.Context, topic string, key, value []byte) (*ProducerResult, error) {
	return tp.SendMessage(ctx, &Message{
		Topic: topic,
		Key:   key,
		Value: value,
	})
}

// SendMessage sends a message with full options and tracing.
func (tp *TracingProducer) SendMessage(ctx context.Context, msg *Message) (*ProducerResult, error) {
	spanName := fmt.Sprintf("%s produce", msg.Topic)
	ctx, span := tp.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messagingAttrs(msg.Topic, "produce")...),
	)
	defer span.End()

	// Inject trace context into message headers
	if msg.Headers == nil {
		msg.Headers = make(map[string][]byte)
	}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(msg.Headers))

	result, err := tp.inner.SendMessage(ctx, msg)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, err
	}

	span.SetAttributes(
		attribute.Int64("messaging.destination.partition.id", int64(result.Partition)),
		attribute.Int64("messaging.message.offset", result.Offset),
	)
	span.SetStatus(codes.Ok, "")
	return result, nil
}

// SendBatch sends multiple messages with tracing (one parent span).
func (tp *TracingProducer) SendBatch(ctx context.Context, messages []*Message) ([]*ProducerResult, error) {
	if len(messages) == 0 {
		return nil, nil
	}

	topic := messages[0].Topic
	spanName := fmt.Sprintf("%s produce", topic)
	ctx, span := tp.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messagingAttrs(topic, "produce")...),
		trace.WithAttributes(attribute.Int("messaging.batch.message_count", len(messages))),
	)
	defer span.End()

	// Inject context into each message
	for _, msg := range messages {
		if msg.Headers == nil {
			msg.Headers = make(map[string][]byte)
		}
		otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(msg.Headers))
	}

	results, err := tp.inner.SendBatch(ctx, messages)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return results, err
	}

	span.SetStatus(codes.Ok, "")
	return results, nil
}

// SendAsync sends a message asynchronously with tracing.
func (tp *TracingProducer) SendAsync(msg *Message) <-chan *AsyncProducerResult {
	if msg.Headers == nil {
		msg.Headers = make(map[string][]byte)
	}

	ctx := context.Background()
	spanName := fmt.Sprintf("%s produce", msg.Topic)
	ctx, span := tp.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(messagingAttrs(msg.Topic, "produce")...),
	)
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(msg.Headers))

	resultCh := tp.inner.SendAsync(msg)

	wrappedCh := make(chan *AsyncProducerResult, 1)
	go func() {
		defer span.End()
		result := <-resultCh
		if result.Err != nil {
			span.SetStatus(codes.Error, result.Err.Error())
			span.RecordError(result.Err)
		} else {
			span.SetStatus(codes.Ok, "")
			span.SetAttributes(
				attribute.Int64("messaging.destination.partition.id", int64(result.Partition)),
				attribute.Int64("messaging.message.offset", result.Offset),
			)
		}
		wrappedCh <- result
	}()

	return wrappedCh
}

// Flush delegates to the underlying producer.
func (tp *TracingProducer) Flush(timeout time.Duration) error {
	return tp.inner.Flush(timeout)
}

// Close closes the underlying producer.
func (tp *TracingProducer) Close() error {
	return tp.inner.Close()
}

// TracingConsumer wraps a Consumer with automatic OTel span creation.
type TracingConsumer struct {
	inner  *Consumer
	tracer trace.Tracer
}

// NewTracingConsumer wraps an existing Consumer with OpenTelemetry tracing.
//
// Start and Poll operations will create CONSUMER spans, and individual
// message processing can extract parent context from headers.
func NewTracingConsumer(consumer *Consumer) *TracingConsumer {
	return &TracingConsumer{
		inner:  consumer,
		tracer: otel.GetTracerProvider().Tracer(instrumentationName, trace.WithInstrumentationVersion(instrumentationVersion)),
	}
}

// Start begins consuming with tracing on the consumer loop.
func (tc *TracingConsumer) Start(ctx context.Context) (<-chan *ConsumerMessage, <-chan error) {
	topics := tc.inner.Topics()
	topicStr := "unknown"
	if len(topics) > 0 {
		topicStr = topics[0]
	}

	spanName := fmt.Sprintf("%s consume", topicStr)
	ctx, span := tc.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(messagingAttrs(topicStr, "consume")...),
	)

	msgCh, errCh := tc.inner.Start(ctx)

	// Wrap the message channel to end the span when context is done
	go func() {
		<-ctx.Done()
		span.End()
	}()

	return msgCh, errCh
}

// Poll returns messages with a tracing span.
func (tc *TracingConsumer) Poll(ctx context.Context, maxRecords int, timeout time.Duration) ([]*ConsumerMessage, error) {
	topics := tc.inner.Topics()
	topicStr := "unknown"
	if len(topics) > 0 {
		topicStr = topics[0]
	}

	spanName := fmt.Sprintf("%s consume", topicStr)
	ctx, span := tc.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(messagingAttrs(topicStr, "consume")...),
	)
	defer span.End()

	messages, err := tc.inner.Poll(ctx, maxRecords, timeout)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
		return nil, err
	}

	span.SetAttributes(attribute.Int("messaging.batch.message_count", len(messages)))
	span.SetStatus(codes.Ok, "")
	return messages, nil
}

// ExtractContext extracts a trace context from message headers, returning a
// new context that can be used as a parent for processing spans.
func (tc *TracingConsumer) ExtractContext(ctx context.Context, msg *ConsumerMessage) context.Context {
	if msg.Headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(msg.Headers))
}

// TraceProcess creates a processing span for a single consumed message.
// It extracts parent context from the message headers if available.
func (tc *TracingConsumer) TraceProcess(ctx context.Context, msg *ConsumerMessage) (context.Context, trace.Span) {
	// Extract parent context from headers
	ctx = tc.ExtractContext(ctx, msg)

	spanName := fmt.Sprintf("%s process", msg.Topic)
	return tc.tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "streamline"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.operation", "process"),
			attribute.Int64("messaging.destination.partition.id", int64(msg.Partition)),
			attribute.Int64("messaging.message.offset", msg.Offset),
		),
	)
}

// Commit delegates to the underlying consumer.
func (tc *TracingConsumer) Commit() error {
	return tc.inner.Commit()
}

// GroupID returns the consumer group ID.
func (tc *TracingConsumer) GroupID() string {
	return tc.inner.GroupID()
}

// Topics returns the subscribed topics.
func (tc *TracingConsumer) Topics() []string {
	return tc.inner.Topics()
}

// Close closes the underlying consumer.
func (tc *TracingConsumer) Close() error {
	return tc.inner.Close()
}

// InjectContext is a convenience function to inject the current trace context
// into message headers for manual propagation.
func InjectContext(ctx context.Context, headers map[string][]byte) {
	if headers == nil {
		return
	}
	otel.GetTextMapPropagator().Inject(ctx, HeaderCarrier(headers))
}

// ExtractContextFromHeaders extracts trace context from message headers.
func ExtractContextFromHeaders(ctx context.Context, headers map[string][]byte) context.Context {
	if headers == nil {
		return ctx
	}
	return otel.GetTextMapPropagator().Extract(ctx, HeaderCarrier(headers))
}
