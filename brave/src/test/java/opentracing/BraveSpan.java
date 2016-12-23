package opentracing;

import io.opentracing.SpanContext;
import java.util.Map;

// compilation test mostly. there's a lot of options that don't make sense in zipkin
final class BraveSpan implements io.opentracing.Span {
  final brave.Span delegate;
  final SpanContext context;
  final long timestamp;

  BraveSpan(brave.Span delegate, long timestamp) {
    this.delegate = delegate;
    this.timestamp = timestamp;
    this.context = new BraveTracer.BraveSpanContext(delegate.context());
  }

  @Override public SpanContext context() {
    return context;
  }

  @Override public void finish() {
    delegate.finish();
  }

  @Override public void finish(long finishMicros) {
    delegate.finish(finishMicros - timestamp);
  }

  @Override public void close() {
    finish();
  }

  @Override public io.opentracing.Span setTag(String key, String value) {
    delegate.tag(key, value);
    return this;
  }

  @Override public io.opentracing.Span setTag(String key, boolean value) {
    return setTag(key, Boolean.toString(value));
  }

  @Override public io.opentracing.Span setTag(String key, Number value) {
    return setTag(key, value.toString());
  }

  @Override public io.opentracing.Span log(Map<String, ?> yolo) {
    if (yolo.isEmpty()) return this;
    return log(yolo.values().iterator().next().toString());
  }

  @Override public io.opentracing.Span log(long timestampMicroseconds, Map<String, ?> yolo) {
    if (yolo.isEmpty()) return this;
    return log(timestampMicroseconds, yolo.values().iterator().next().toString());
  }

  @Override public io.opentracing.Span log(String event) {
    delegate.annotate(event);
    return this;
  }

  @Override public io.opentracing.Span log(long timestampMicroseconds, String event) {
    delegate.annotate(timestampMicroseconds, event);
    return this;
  }

  @Override public io.opentracing.Span setBaggageItem(String key, String value) {
    return this;
  }

  @Override public String getBaggageItem(String key) {
    return null;
  }

  @Override public io.opentracing.Span setOperationName(String operationName) {
    delegate.name(operationName);
    return this;
  }

  @Override public io.opentracing.Span log(String eventName, Object yolo) {
    return log(eventName);
  }

  @Override
  public io.opentracing.Span log(long timestampMicroseconds, String eventName, Object yolo) {
    return log(timestampMicroseconds, eventName);
  }
}
