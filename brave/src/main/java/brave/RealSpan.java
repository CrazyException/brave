package brave;

import zipkin.Endpoint;

/** This wraps the public api and guards access to a mutable span. */
final class RealSpan extends Span {

  final TraceContext context;
  final Clock clock;
  final SpanRecorder recorder; // guarded by itself

  RealSpan(TraceContext context, Clock clock, SpanRecorder recorder) {
    this.context = context;
    this.clock = clock;
    this.recorder = recorder;
  }

  @Override public boolean isNoop() {
    return false;
  }

  @Override public TraceContext context() {
    return context;
  }

  @Override public Span start() {
    return start(clock.epochMicros());
  }

  @Override public Span start(long timestamp) {
    recorder.start(context, timestamp);
    return this;
  }

  @Override public Span name(String name) {
    if (name == null) throw new NullPointerException("name == null");
    recorder.name(context, name);
    return this;
  }

  @Override public Span kind(Kind kind) {
    if (kind == null) throw new NullPointerException("kind == null");
    recorder.kind(context, kind);
    return this;
  }

  @Override public Span annotate(String value) {
    return annotate(clock.epochMicros(), value);
  }

  @Override public Span annotate(long timestamp, String value) {
    if (value == null) throw new NullPointerException("value == null");
    recorder.annotate(context, timestamp, value);
    return this;
  }

  @Override public Span tag(String key, String value) {
    if (key == null) throw new NullPointerException("key == null");
    if (key.isEmpty()) throw new IllegalArgumentException("key is empty");
    if (value == null) throw new NullPointerException("value == null");
    recorder.tag(context, key, value);
    return this;
  }

  @Override public Span remoteEndpoint(Endpoint remoteEndpoint) {
    if (remoteEndpoint == null) throw new NullPointerException("remoteEndpoint == null");
    recorder.remoteEndpoint(context, remoteEndpoint);
    return this;
  }

  @Override public void finish() {
    finish(clock.epochMicros());
  }

  @Override public void finish(long duration) {
    recorder.finish(context, null, duration);
  }

  @Override
  public String toString() {
    return "RealSpan(" + context + ")";
  }
}
