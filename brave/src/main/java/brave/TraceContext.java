package brave;

import brave.internal.Nullable;
import brave.propagation.Propagation;
import com.google.auto.value.AutoValue;

import static brave.internal.HexCodec.writeHexLong;

/**
 * Contains trace data that's propagated in-band across requests, sometimes known as Baggage.
 *
 * <p>Particularly, this includes trace identifiers and sampled state.
 *
 * <p>The implementation was originally {@code com.github.kristofa.brave.SpanId}, which was a
 * port of {@code com.twitter.finagle.tracing.TraceId}. Unlike these mentioned, this type does not
 * expose a single binary representation. That's because {@link Propagation} forms can now vary.
 */
@AutoValue
public abstract class TraceContext {

  /** Unique 8 or 16-byte identifier for a trace, set on all spans within it. */
  public abstract TraceId traceId();

  /** The parent's {@link #spanId} or null if this the root span in a trace. */
  @Nullable public abstract Long parentId();

  /**
   * Unique 8-byte identifier of this span within a trace.
   *
   * <p>A span is uniquely identified in storage by ({@linkplain #traceId}, {@linkplain #spanId}).
   */
  public abstract long spanId();

  /**
   * Should we sample this request or not? True means sample, false means don't, null means we defer
   * decision to someone further down in the stack.
   */
  @Nullable public abstract Boolean sampled();

  /**
   * True is a request to store this span even if it overrides sampling policy. Defaults to false.
   */
  public abstract boolean debug();

  /**
   * True if we are contributing to a span started by another tracer (ex on a different host).
   * Defaults to false.
   *
   * <p>When an RPC trace is client-originated, it will be sampled and the same span ID is used for
   * the server side. However, the server shouldn't set span.timestamp or duration since it didn't
   * start the span.
   */
  public abstract boolean shared();

  public abstract Builder toBuilder();

  /** Returns true for instances of {@linkplain TraceContext} with equal trace and span ids */
  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (o instanceof TraceContext) {
      TraceContext that = (TraceContext) o;
      return (this.traceId().equals(that.traceId()))
          && (this.spanId() == that.spanId());
    }
    return false;
  }

  @Override
  public int hashCode() {
    int h = 1;
    h *= 1000003;
    h ^= this.traceId().hashCode();
    h *= 1000003;
    h ^= (this.spanId() >>> 32) ^ this.spanId();
    return h;
  }

  /** Returns {@code $traceId/$spanId} */
  @Override
  public String toString() {
    boolean traceHi = traceId().hi() != 0;
    char[] result = new char[((traceHi ? 3 : 2) * 16) + 1]; // 2 ids and the delimiter
    int pos = 0;
    if (traceHi) {
      writeHexLong(result, pos, traceId().hi());
      pos += 16;
    }
    writeHexLong(result, pos, traceId().lo());
    pos += 16;
    result[pos++] = '/';
    writeHexLong(result, pos, spanId());
    return new String(result);
  }

  @AutoValue.Builder
  public static abstract class Builder {
    /** @see TraceContext#traceId */
    public abstract Builder traceId(TraceId traceId);

    /** Like {@link #traceId(TraceId)}, except for a 64-bit trace ID */
    public final Builder traceId(long traceIdLo) {
      return traceId(TraceId.create(0L, traceIdLo));
    }

    /** @see TraceContext#parentId */
    public abstract Builder parentId(@Nullable Long parentId);

    /** @see TraceContext#spanId */
    public abstract Builder spanId(long spanId);

    /** @see TraceContext#sampled */
    public abstract Builder sampled(Boolean nullableSampled);

    /** @see TraceContext#debug() */
    public abstract Builder debug(boolean debug);

    /** @see TraceContext#shared() */
    public abstract Builder shared(boolean shared);

    public abstract TraceContext build();

    Builder() {
    }
  }

  TraceContext() {
    // Manual construction is blocked for now. Use Internal.instance cautiously if you must.
  }
}
