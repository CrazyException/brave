package brave.sampler;

import brave.TraceId;

// TODO: replace with the existing sampler code once base code is merged.
public abstract class Sampler {

  public static final Sampler ALWAYS_SAMPLE = new Sampler() {
    @Override public boolean isSampled(TraceId traceId) {
      return true;
    }

    @Override public String toString() {
      return "AlwaysSample";
    }
  };

  /** Returns true if the trace ID should be measured. */
  public abstract boolean isSampled(TraceId traceId);

}
