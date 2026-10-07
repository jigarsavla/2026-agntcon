# When Agents Leave the Model

Reliability lessons from tool-calling inference systems. Companion repository for the
poster at **AGNTCon + MCPCon North America 2026**, San Jose, Thursday October 22, 2026,
poster session 5:45 to 7:00 PM in the Solutions Showcase.

Jigar Kuverji Savla, Product Lead for AI Inference Systems at Tensordyne.
jigar@tensordyne.ai | mail@jigarsavla.com

## The story in one paragraph

An agent turn does not end when the model emits its last token. The result crosses the
transport, a router, often separate prefill and decode workers, the client that runs the
tool call, and the state that must be back in place before the next turn starts. The
painful failures happen at those boundaries, after the model has done its job. The one
number every dashboard already has, call duration, cannot tell a fast turn from a slow
one. Two ordinary streaming numbers can: when the first message arrived and how far apart
the messages were. The transport can emit both without knowing what a model is.

## One agent turn crosses seven boundaries

In order: the user request, the model server, the tool-call decision, the client or
orchestration layer that runs the tool, the structured result returned to the model, the
resumed generation path, and the infrastructure that keeps state alive across that flow.
Each one is a place for stale state, replayed tool results, lost context, hidden batching
delays, bad routing, or partial failure to show up as a bad user experience.

## Four measured failures at the boundaries

All measurements: loopback, single host, CPython, simulated model cost. The transport and
the timing are real; the GPU is not.

| number | what it means | failure mode |
|---|---|---|
| **826 ms vs 45 ms** | Two calls with the same 1 s duration. Time to first message differs by more than an order of magnitude. Call duration cannot tell them apart. | hidden batching delays |
| **4 of 6** | Trace-header mixes (`traceparent` and `grpc-trace-bin`) across five hops that broke the trace. Every call returned OK. | lost context |
| **0 of 6** | Streams over 3 s kept by a 1 s latency policy with a 2 s decision wait. At 2 s it kept 6 of 6. The sampler saw 51 ms of a 12 s trace. The sampler is a re-implementation from the Collector README, see [`tail-sampling/`](tail-sampling/). | partial failure |
| **150 ms** | A slower KV copy from the prefill worker to the decode worker. Neither worker's clock covers the copy, so neither server metric moved. | partial failure |

Not yet measured: stale state, replayed tool results, bad routing.

## Treat the turn as one traceable transaction

The reference path for MCP-style agent infrastructure has four layers. Tool execution
stays client-side (design). The inference layer detects tool-call intent (the transport
timing here is measured). The API layer routes by model and session state (design). The
serving layer tracks KV cache, device health, shard placement, and queueing delay as
first-class signals (the KV copy here is measured).

Four fixes, each one something a platform team can do this week:

1. **Two histograms at the transport read boundary**: time to first message and
   inter-message gap. Same buckets and labels as gRPC's existing A66 metrics,
   experimental and off by default, the shape gRFC A96 already took.
2. **Both trace headers at every hop**: extract and inject `traceparent` and
   `grpc-trace-bin`. One wrong hop breaks the trace.
3. **Tail-sampler decision wait above the p99 stream duration**, then
   `OTEL_METRICS_EXEMPLAR_FILTER=always_on`. The first fix is what makes the second
   worth having.
4. **Run the contract test on your dashboard**: is each metric you depend on emitted,
   labeled, stable, bucketed, joinable. My own dashboard passed 4 of its 11 assumptions.

## What's here

| folder | what it is |
|---|---|
| [`poster/`](poster/) | The poster as printed, 36 x 24 in PDF. |
| [`contract-test/`](contract-test/) | The telemetry contract test. A CI check for your dashboard's assumptions. Point it at your own stack. |
| [`tail-sampling/`](tail-sampling/) | The repro behind "0 of 6". A re-implementation of the Collector's tail-sampling processor written from its README, line by line. Run it against a real Collector and tell me where it diverges. |

The measurements, the demo recording, and the fuller 17-check contract live in the
gRPConf companion repo: [jigarsavla/2026-grpconf](https://github.com/jigarsavla/2026-grpconf).

## Primary sources

[gRFC A66](https://github.com/grpc/proposal/blob/master/A66-otel-stats.md) ·
[A79](https://github.com/grpc/proposal/blob/master/A79-non-per-call-metrics-architecture.md) ·
[A80](https://github.com/grpc/proposal/blob/master/A80-tcp-telemetry.md) ·
[A96](https://github.com/grpc/proposal/blob/master/A96-retry-otel-stats.md) ·
[A108](https://github.com/grpc/proposal/blob/master/A108-otel-custom-per-call-label.md) ·
[OpenTelemetry semantic conventions v1.40.0](https://github.com/open-telemetry/semantic-conventions/releases/tag/v1.40.0) ·
[GenAI semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai/blob/main/docs/gen-ai/gen-ai-metrics.md) ·
[Collector tail sampling processor README](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/tailsamplingprocessor/README.md)

## License

GPL-3.0, the same license as the gRPConf repo.
