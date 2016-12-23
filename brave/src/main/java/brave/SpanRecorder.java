package brave;

import brave.internal.Nullable;
import java.util.concurrent.ConcurrentHashMap;
import zipkin.Endpoint;
import zipkin.reporter.Reporter;

/**
 * Offset-based recorder: Uses a single point of reference and offsets to create annotation
 * timestamps.
 *
 * <p>Method signatures are based on the zipkin 2 model eventhough it isn't out, yet.
 */
final class SpanRecorder {
  final Endpoint localEndpoint;
  final Reporter<zipkin.Span> reporter;

  // TODO: add bookkeeping so that spans which are perpetually unfinished don't OOM
  // For example, finagle has a flusher thread to report spans that lived longer than 2 minutes
  final ConcurrentHashMap<TraceContext, MutableSpan> spanMap = new ConcurrentHashMap(64);

  SpanRecorder(Endpoint localEndpoint, Reporter<zipkin.Span> reporter) {
    this.localEndpoint = localEndpoint;
    this.reporter = reporter;
  }

  void start(TraceContext context, long timestamp) {
    getSpan(context).start(timestamp);
  }

  void name(TraceContext context, String name) {
    getSpan(context).name(name);
  }

  void kind(TraceContext context, Span.Kind kind) {
    getSpan(context).kind(kind);
  }

  void annotate(TraceContext context, long timestamp, String value) {
    MutableSpan span = getSpan(context).annotate(timestamp, value);
    maybeFinish(context, span); // maybe finish as this could be an end annotation
  }

  void tag(TraceContext context, String key, String value) {
    getSpan(context).tag(key, value);
  }

  void remoteEndpoint(TraceContext context, Endpoint remoteEndpoint) {
    getSpan(context).remoteEndpoint(remoteEndpoint);
  }

  void finish(TraceContext context, @Nullable Long finishTimestamp, @Nullable Long duration) {
    if (!spanMap.containsKey(context)) return;
    MutableSpan span = getSpan(context).finish(finishTimestamp, duration);
    maybeFinish(context, span);
  }

  MutableSpan getSpan(TraceContext context) {
    MutableSpan span = spanMap.get(context);
    if (span != null) return span;

    MutableSpan newSpan = new MutableSpan(context, localEndpoint);
    MutableSpan prev = spanMap.putIfAbsent(context, newSpan);
    return prev != null ? prev : newSpan;
  }

  void maybeFinish(TraceContext context, MutableSpan span) {
    if (!span.isFinished()) return;
    synchronized (span) {
      spanMap.remove(context, span);
      reporter.report(span.toSpan());
    }
  }
}
