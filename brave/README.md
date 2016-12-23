Brave Api (v4)
==============

This module includes a work-in-progress shiny new api

### Zipkin idiomatic

When tracing local code, just run it inside a span.
```java
try (Span span = tracer.newTrace().name("encode").start()) {
  doSomethingExpensive();
}
```

When tracing remote code, note events of interest that explain latency
```java
// before you send a request, add metadata that describes the operation
span = tracer.newTrace().name("get).type(CLIENT);
span.tag("clnt/finagle.version", "6.36.0");
span.tag(TraceKeys.HTTP_PATH, "/api");
span.remoteEndpoint(Endpoint.builder()
    .serviceName("backend")
    .ipv4(127 << 24 | 1)
    .port(8080).build());

// when the request is scheduled, start the span
span.start();

// if you have callbacks for when data is on the wire, note those events
span.annotate(Constants.WIRE_SEND);
span.annotate(Constants.WIRE_RECV);

// when the response is complete, finish the span
span.finish();
```

### Performant
Brave's new api has been built with performance in mind. Using the core
Span api, you can record spans in sub-microseconds. When a span is
sampled, there's effectively no overhead (as it is a noop).

Unlike previous implementations, Brave 4 only needs one timestamp per
span. All annotations are recorded on an offset basis, using the less
expensive and more precise `System.nanoTime()` function.

### OpenTracing ready

Brave 4 has been designed with OpenTracing readiness in mind. A bridge
from Brave 4 to OpenTracing v0.20.2 is relatively little code. It should
be able to implement future versions of OpenTracing as well.
