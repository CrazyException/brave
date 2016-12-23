package brave;

import brave.internal.Internal;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.assertj.core.api.Assertions.assertThat;

public class TraceContextTest {
  static {
    Internal.initializeInstanceForTests();
  }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test public void compareUnequalIds() {
    TraceContext id = contextBuilder().traceId(333L).spanId(0L).build();

    assertThat(id)
        .isNotEqualTo(contextBuilder().traceId(333L).spanId(1L).build());
  }

  @Test public void compareEqualIds() {
    TraceContext id = contextBuilder().traceId(333L).spanId(444L).build();

    assertThat(id)
        .isEqualTo(contextBuilder().traceId(333L).spanId(444L).build());
  }

  @Test public void equalsOnlyAccountsForIdFields() {
    TraceContext id = contextBuilder().traceId(333L).spanId(444L).debug(true).build();

    assertThat(id)
        .isEqualTo(contextBuilder().traceId(333L).spanId(444L).sampled(true).build());
  }

  @Test public void hashCodeOnlyAccountsForIdFields() {
    TraceContext id = contextBuilder().traceId(333L).spanId(444L).debug(true).build();

    assertThat(id.hashCode())
        .isEqualTo(contextBuilder().traceId(333L).spanId(444L).sampled(true).build().hashCode());
  }

  @Test
  public void testToString_lo() {
    TraceContext id = contextBuilder().traceId(333L).spanId(3).parentId(2L).build();

    assertThat(id.toString())
        .isEqualTo("000000000000014d/0000000000000003");
  }

  @Test
  public void testToString() {
    TraceContext id =
        contextBuilder().traceId(TraceId.create(333L, 444L)).spanId(3).parentId(2L).build();

    assertThat(id.toString())
        .isEqualTo("000000000000014d00000000000001bc/0000000000000003");
  }

  static TraceContext.Builder contextBuilder() {
    return Internal.instance.newTraceContextBuilder();
  }
}
