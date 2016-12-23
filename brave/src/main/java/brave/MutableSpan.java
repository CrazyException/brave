package brave;

import brave.internal.Nullable;
import zipkin.Annotation;
import zipkin.BinaryAnnotation;
import zipkin.Constants;
import zipkin.Endpoint;

import static zipkin.Constants.LOCAL_COMPONENT;

final class MutableSpan {
  final Endpoint localEndpoint;
  final zipkin.Span.Builder span;
  boolean shared;
  // fields which are added late
  long startTimestamp;
  Endpoint remoteEndpoint;

  // flags which help us know how to reassemble the span
  Span.Kind kind;
  // Until model v2, we have to ensure at least one annotation or binary annotation exists
  boolean spanHasLocalEndpoint;
  boolean finished;

  // Since this is not exposed, this class could be refactored later as needed to act in a pool
  // to reduce GC churn. This would involve calling span.clear and resetting the fields below.
  MutableSpan(TraceContext context, Endpoint localEndpoint) {
    this.localEndpoint = localEndpoint;
    this.span = zipkin.Span.builder()
        .traceIdHigh(context.traceId().hi())
        .traceId(context.traceId().lo())
        .parentId(context.parentId())
        .id(context.spanId())
        .debug(context.debug())
        .name(""); // avoid a NPE
    shared = context.shared();
    startTimestamp = 0;
    remoteEndpoint = null;
    kind = null;
    spanHasLocalEndpoint = false;
    finished = false;
  }

  synchronized MutableSpan start(long timestamp) {
    startTimestamp = timestamp;
    return this;
  }

  synchronized MutableSpan name(String name) {
    span.name(name);
    return this;
  }

  synchronized MutableSpan kind(Span.Kind kind) {
    this.kind = kind;
    return this;
  }

  synchronized MutableSpan annotate(long timestamp, String value) {
    span.addAnnotation(Annotation.create(timestamp, value, localEndpoint));
    spanHasLocalEndpoint = true;
    return this;
  }

  synchronized MutableSpan tag(String key, String value) {
    span.addBinaryAnnotation(BinaryAnnotation.create(key, value, localEndpoint));
    spanHasLocalEndpoint = true;
    return this;
  }

  synchronized MutableSpan remoteEndpoint(Endpoint remoteEndpoint) {
    this.remoteEndpoint = remoteEndpoint;
    return this;
  }

  /** Completes and reports the span */
  synchronized MutableSpan finish(@Nullable Long finishTimestamp, @Nullable Long duration) {
    if (finished) return this;
    finished = true;
    if (startTimestamp == 0) throw new IllegalStateException("span was never started");

    finishTimestamp = addTimestampAndDuration(finishTimestamp, duration);
    if (kind != null) {
      String remoteEndpointType;
      String startAnnotation;
      String finishAnnotation;
      switch (kind) {
        case CLIENT:
          remoteEndpointType = Constants.SERVER_ADDR;
          startAnnotation = Constants.CLIENT_SEND;
          finishAnnotation = Constants.CLIENT_RECV;
          break;
        case SERVER:
          remoteEndpointType = Constants.CLIENT_ADDR;
          startAnnotation = Constants.SERVER_RECV;
          finishAnnotation = Constants.SERVER_SEND;
          // don't report server-side timestamp on shared-spans
          if (shared) span.timestamp(null).duration(null);
          break;
        default:
          throw new AssertionError("update kind mapping");
      }
      if (remoteEndpoint != null) {
        span.addBinaryAnnotation(BinaryAnnotation.address(remoteEndpointType, remoteEndpoint));
      }
      span.addAnnotation(Annotation.create(startTimestamp, startAnnotation, localEndpoint));
      if (finishAnnotation != null) {
        span.addAnnotation(Annotation.create(finishTimestamp, finishAnnotation, localEndpoint));
      }
      spanHasLocalEndpoint = true;
    }

    if (!spanHasLocalEndpoint) { // create a small dummy annotation
      span.addBinaryAnnotation(BinaryAnnotation.create(LOCAL_COMPONENT, "", localEndpoint));
    }
    return this;
  }

  synchronized boolean isFinished() {
    return finished;
  }

  synchronized zipkin.Span toSpan() {
    return span.build();
  }

  // one or the other will be null
  @Nullable Long addTimestampAndDuration(@Nullable Long finishTimestamp, @Nullable Long duration) {
    assert finishTimestamp != null || duration != null; // guard programming errors
    if (duration != null) {
      finishTimestamp = startTimestamp + duration;
    } else {
      duration = finishTimestamp - startTimestamp;
    }
    span.timestamp(startTimestamp).duration(duration);
    return finishTimestamp;
  }
}