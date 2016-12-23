package brave;

import brave.internal.Internal;
import brave.internal.Nullable;
import brave.internal.Platform;
import brave.sampler.Sampler;
import zipkin.Endpoint;
import zipkin.reporter.Reporter;

public final class Tracer implements Span.Factory {
  public static Builder builder() {
    return new Builder();
  }

  /** Configuration including defaults needed to send spans to a Kafka topic. */
  public static final class Builder {
    Endpoint localEndpoint;
    Reporter<zipkin.Span> reporter;
    Clock clock;
    Sampler sampler = Sampler.ALWAYS_SAMPLE;
    boolean traceId128Bit = false;

    public Builder localEndpoint(Endpoint localEndpoint) {
      if (localEndpoint == null) throw new NullPointerException("localEndpoint == null");
      this.localEndpoint = localEndpoint;
      return this;
    }

    public Builder reporter(Reporter<zipkin.Span> reporter) {
      if (reporter == null) throw new NullPointerException("reporter == null");
      this.reporter = reporter;
      return this;
    }

    public Builder clock(Clock clock) {
      if (clock == null) throw new NullPointerException("clock == null");
      this.clock = clock;
      return this;
    }

    public Builder sampler(Sampler sampler) {
      this.sampler = sampler;
      return this;
    }

    public Builder traceId128Bit(boolean traceId128Bit) {
      this.traceId128Bit = traceId128Bit;
      return this;
    }

    public Tracer build() {
      return new Tracer(
          clock != null ? clock : Platform.get(),
          new SpanRecorder(
              localEndpoint != null ? localEndpoint : Platform.get().localEndpoint(),
              reporter != null ? reporter : Platform.get()
          ),
          sampler,
          traceId128Bit
      );
    }
  }

  static {
    Internal.instance = new Internal() {
      @Override public TraceContext.Builder newTraceContextBuilder() {
        return new AutoValue_TraceContext.Builder().debug(false).shared(false);
      }
    };
  }

  final Clock clock;
  final SpanRecorder spanRecorder;
  final Sampler sampler;
  final boolean traceId128Bit;

  Tracer(Clock clock, SpanRecorder spanRecorder, Sampler sampler, boolean traceId128Bit) {
    this.clock = clock;
    this.spanRecorder = spanRecorder;
    this.sampler = sampler;
    this.traceId128Bit = traceId128Bit;
  }

  /** Continues a span started on another host. This is only used for */
  public Span joinSpan(TraceContext context) {
    return toSpan(context);
  }

  public Span newTrace() {
    return toSpan(nextContext(null));
  }

  @Override
  public Span newSpan(@Nullable TraceContext parent) {
    if (parent != null && Boolean.FALSE.equals(parent.sampled())) {
      return new NoopSpan(parent);
    }
    return toSpan(nextContext(parent));
  }

  Span toSpan(TraceContext context) {
    if (context.sampled() == null) {
      context = context.toBuilder()
          .sampled(sampler.isSampled(context.traceId()))
          .shared(false)
          .build();
    }

    if (context.sampled()) {
      return new RealSpan(context, clock, spanRecorder);
    }
    return new NoopSpan(context);
  }

  TraceContext nextContext(@Nullable TraceContext parent) {
    long nextId = Platform.get().randomLong();
    if (parent != null) {
      return parent.toBuilder().spanId(nextId).parentId(parent.spanId()).build();
    }
    return Internal.instance.newTraceContextBuilder()
        .traceId(TraceId.create(traceId128Bit ? Platform.get().randomLong() : 0, nextId))
        .spanId(nextId).build();
  }
}
