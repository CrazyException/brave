package brave;

import com.google.auto.value.AutoValue;

import static brave.internal.HexCodec.writeHexLong;

/** Unique 8 or 16-byte identifier for a trace, set on all spans within it. */
@AutoValue
public abstract class TraceId {
  public static TraceId create(long hi, long lo) {
    return new AutoValue_TraceId(hi, lo);
  }

  /** 0 may imply 8-byte identifiers are in use */
  public abstract long hi();

  public abstract long lo();

  /** Returns 16 or 32 character hex string depending on if {@code high} is zero. */
  @Override
  public String toString() {
    char[] result = new char[hi() != 0 ? 32 : 16];
    int pos = 0;
    if (hi() != 0) {
      writeHexLong(result, pos, hi());
      pos += 16;
    }
    writeHexLong(result, pos, lo());
    return new String(result);
  }

  TraceId() {
  }
}
