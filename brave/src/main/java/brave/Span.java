package brave;

import brave.internal.Nullable;
import java.io.Closeable;
import zipkin.Constants;
import zipkin.Endpoint;
import zipkin.TraceKeys;

/** All mutators are thread-safe */
// Implements closeable not auto-closable for JRE 6
public abstract class Span implements Closeable {
  public enum Kind {
    CLIENT,
    SERVER
  }

  /**
   * When true, no recording is done and nothing is reported to zipkin. However, this span should
   * still be injected into outgoing requests. Use this flag to avoid performing expensive
   * computation.
   */
  public abstract boolean isNoop();

  public abstract TraceContext context();

  /**
   * Starts the span with an implicit timestamp.
   *
   * <p>Spans can be modified before calling start. For example, you can add tags to the span and
   * set its name without lock contention.
   */
  public abstract Span start();

  /** Like {@link #start()}, except with a given timestamp in microseconds. */
  public abstract Span start(long timestamp);

  /**
   * Sets the string name for the logical operation this span represents.
   */
  public abstract Span name(String name);

  /**
   * The kind of span is optional. When set, it affects how a span is reported. For example, if the
   * kind is {@link Kind#SERVER}, the span's start timestamp is implicitly annotated as "sr"
   * and that plus its duration as "ss".
   */
  public abstract Span kind(Kind kind);

  /**
   * Associates an event that explains latency with the current system time.
   *
   * <p>Implicitly {@link #start() starts} the span if needed.
   *
   * <p>Implicitly {@link #finish() finishes} the span when the value is special (ex. "ss" or "cr").
   *
   * @param value A short tag indicating the event, like "finagle.retry"
   * @see Constants
   */
  public abstract Span annotate(String value);

  /** Like {@link #annotate(long, String)}, except with a given timestamp in microseconds. */
  public abstract Span annotate(long timestamp, String value);

  /**
   * Tags give your span context for search, viewing and analysis. For example, a key
   * "your_app.version" would let you lookup spans by version. A tag {@link TraceKeys#SQL_QUERY}
   * isn't searchable, but it can help in debugging when viewing a trace.
   *
   * @param key Name used to lookup spans, such as "your_app.version". See {@link TraceKeys} for
   * standard ones.
   * @param value String value, cannot be <code>null</code>.
   */
  public abstract Span tag(String key, String value);

  /**
   * For a client span, this would be the server's address.
   *
   * <p>It is often expensive to derive a remote address: always check {@link #isNoop()} first!
   */
  public abstract Span remoteEndpoint(Endpoint endpoint);

  /** Completes the span, assigning the most precise duration possible. */
  public abstract void finish();

  /** Completes the span with a given duration in microseconds. */
  public abstract void finish(long duration);

  /** Convenience for use in try-with-resources, which calls {@link #finish()} */
  @Override
  public final void close() {
    finish();
  }

  // Design note: this uses a factory instead of a builder as the span is mutable anyway. Having a
  // single mutation interface is less code to maintain. Those looking to prepare a span before
  // starting it can simply call start when they are ready.
  public interface Factory {
    Span newSpan(@Nullable TraceContext parent);
  }
}
