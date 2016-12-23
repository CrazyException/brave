package opentracing;

import brave.Clock;
import brave.Span;
import brave.TraceContext;
import brave.propagation.Propagation;
import io.opentracing.SpanContext;
import io.opentracing.Tracer;
import io.opentracing.propagation.Format;
import io.opentracing.propagation.TextMap;
import java.util.Collections;
import java.util.Map;

// compilation test mostly. there's a lot of options that don't make sense in zipkin
final class BraveTracer implements Tracer {
  final Span.Factory braveSpanFactory;
  final Clock clock;

  BraveTracer(brave.Span.Factory braveSpanFactory, Clock clock) {
    this.braveSpanFactory = braveSpanFactory;
    this.clock = clock;
  }

  @Override public SpanBuilder buildSpan(String operationName) {
    return new BraveSpanBuilder(braveSpanFactory, clock, operationName);
  }

  @Override public <C> void inject(SpanContext spanContext, Format<C> format, C carrier) {
    if (format != Format.Builtin.HTTP_HEADERS) {
      throw new UnsupportedOperationException(format + " != Format.Builtin.HTTP_HEADERS");
    }
    TraceContext traceContext = ((BraveSpanContext) spanContext).context;
    TextMap textMap = (TextMap) carrier;
    Propagation.B3_STRING.injector(TextMap::put).inject(traceContext, textMap);
  }

  @Override public <C> SpanContext extract(Format<C> format, C carrier) {
    if (format != Format.Builtin.HTTP_HEADERS) {
      throw new UnsupportedOperationException(format.toString());
    }
    TextMap textMap = (TextMap) carrier;
    return new BraveSpanContext(Propagation.B3_STRING.extractor(textMapGetter).extract(textMap));
  }

  static final Propagation.Getter<TextMap, String> textMapGetter = (carrier, key) -> {
    for (Map.Entry<String, String> entry : carrier) {
      if (entry.getKey().equalsIgnoreCase(key)) {
        return entry.getValue();
      }
    }
    return null;
  };

  static final class BraveSpanContext implements SpanContext {
    final TraceContext context;

    BraveSpanContext(TraceContext context) {
      this.context = context;
    }

    @Override public Iterable<Map.Entry<String, String>> baggageItems() {
      return Collections.EMPTY_SET;
    }
  }
}
