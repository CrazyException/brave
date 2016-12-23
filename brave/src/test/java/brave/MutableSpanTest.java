package brave;

import brave.internal.Platform;
import org.junit.Test;
import zipkin.BinaryAnnotation;
import zipkin.Endpoint;

import static brave.Span.Kind.CLIENT;
import static brave.Span.Kind.SERVER;
import static org.assertj.core.api.Assertions.assertThat;
import static zipkin.Constants.CLIENT_ADDR;
import static zipkin.Constants.LOCAL_COMPONENT;
import static zipkin.Constants.SERVER_ADDR;

public class MutableSpanTest {
  Endpoint localEndpoint = Platform.get().localEndpoint();
  TraceContext context = Tracer.builder().build().nextContext(null);

  // zipkin needs one annotation or binary annotation so that the local endpoint can be read
  @Test public void addsDefaultBinaryAnnotation() {
    MutableSpan span = new MutableSpan(context, localEndpoint);

    span.start(1L);
    span.finish(2L, null);

    assertThat(span.toSpan().binaryAnnotations.get(0)).isEqualTo(
        BinaryAnnotation.create(LOCAL_COMPONENT, "", localEndpoint)
    );
  }

  @Test public void whenKindIsClient_addsCsCr() {
    MutableSpan span = new MutableSpan(context, localEndpoint);

    span.kind(CLIENT);
    span.start(1L);
    span.finish(2L, null);

    assertThat(span.toSpan().annotations).extracting(a -> a.value)
        .containsExactly("cs", "cr");
  }

  @Test public void whenKindIsClient_addsSa() {
    MutableSpan span = new MutableSpan(context, localEndpoint);

    Endpoint endpoint = Endpoint.create("server", 127 | 1);
    span.kind(CLIENT);
    span.remoteEndpoint(endpoint);
    span.start(1L);
    span.finish(2L, null);

    assertThat(span.toSpan().binaryAnnotations.get(0)).isEqualTo(
        BinaryAnnotation.address(SERVER_ADDR, endpoint)
    );
  }

  @Test public void whenKindIsServer_addsSrSs() {
    MutableSpan span = new MutableSpan(context, localEndpoint);

    span.kind(SERVER);
    span.start(1L);
    span.finish(1L, null);

    assertThat(span.toSpan().annotations).extracting(a -> a.value)
        .containsExactly("sr", "ss");
  }

  @Test public void whenKindIsServer_addsCa() {
    MutableSpan span = new MutableSpan(context, localEndpoint);

    Endpoint endpoint = Endpoint.create("caller", 127 | 1);
    span.kind(SERVER);
    span.remoteEndpoint(endpoint);
    span.start(1L);
    span.finish(2L, null);

    assertThat(span.toSpan().binaryAnnotations.get(0)).isEqualTo(
        BinaryAnnotation.address(CLIENT_ADDR, endpoint)
    );
  }
}
