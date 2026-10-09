"""
Shared rig: the locked cost model, a real gRPC server-streaming
inference service, and real OpenTelemetry spans stamped around real work.

Locked demo config (unchanged throughout):
  prefill-heavy : prompt 4000 tok, max_tokens 128  -> ~800 ms prefill, 17 msgs
  decode-heavy  : prompt  250 tok, max_tokens 624  -> ~ 50 ms prefill, 78 msgs
"""
import time, threading, os
from concurrent import futures

import grpc
import inference_pb2 as pb
import inference_pb2_grpc as pbg

# ---------------------------------------------------------------- cost model
PREFILL_MS_PER_TOKEN = 0.200     # 4000 tok -> 800.0 ms
DECODE_MS_PER_TOKEN  = 1.650     # 8 tok/chunk -> ~13.2 ms per message
TOKENS_PER_CHUNK     = 8
ADMIT_MS             = 0.8
DETOKENIZE_MS_PER_CHUNK = 0.15

WORKLOADS = {
    "prefill-heavy": dict(prompt_tokens=4000, max_tokens=128),
    "decode-heavy":  dict(prompt_tokens=250,  max_tokens=624),
}

def busy_sleep(ms):
    """Deterministic-ish delay. time.sleep is fine at these magnitudes and is
    what previous days used; keep it identical so numbers stay comparable."""
    if ms > 0:
        time.sleep(ms / 1000.0)

# ------------------------------------------------------- OTel span plumbing
from opentelemetry import trace as ot_trace
from opentelemetry.sdk.trace import TracerProvider, ReadableSpan
from opentelemetry.sdk.trace.export import SpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON
from opentelemetry.sdk.resources import Resource
from opentelemetry.propagate import inject, extract
from opentelemetry.trace import set_span_in_context

class RecordingProcessor(SpanProcessor):
    """Records the wall-clock moment each span STARTS and the moment it is
    handed to a processor (OnEnd). The gap between the two is the interval
    during which the span exists but is invisible to every downstream
    component -- exporter, collector, sampler."""
    def __init__(self, sink=None):
        self.started = []   # (name, t_start_wall)
        self.ended   = []   # ReadableSpan, appended in OnEnd order
        self.end_wall = {}  # span_id -> wall time OnEnd fired
        self.sink = sink    # optional: forward to a downstream component
        self.lock = threading.Lock()

    def on_start(self, span, parent_context=None):
        with self.lock:
            self.started.append((span.name, time.time()))

    def on_end(self, span: ReadableSpan):
        with self.lock:
            self.end_wall[span.context.span_id] = time.time()
            self.ended.append(span)
        if self.sink is not None:
            self.sink(span)

    def shutdown(self): pass
    def force_flush(self, timeout_millis=30000): return True

def make_tracer(name="tailsampling", sink=None, sampler=None):
    provider = TracerProvider(resource=Resource.create({"service.name": name}),
                              sampler=sampler or ALWAYS_ON)
    rec = RecordingProcessor(sink=sink)
    provider.add_span_processor(rec)
    return provider, provider.get_tracer(name), rec

# ------------------------------------------------------------ the service
class InferenceServicer(pbg.InferenceServicer):
    """Server-streaming inference with a real phase structure.

    Extracts W3C trace context from metadata (so client and server spans share
    one trace id) and emits three REAL child spans around real work:
      schedule  -- admit + queue            (ends early)
      prefill   -- the prefill forward pass (ends at TTFT)
      decode    -- the whole decode loop    (ends with the RPC)
    This is the span shape any gateway -> scheduler -> worker chain produces,
    and it is what makes span ARRIVAL spread over the life of the call.
    """
    def __init__(self, tracer):
        self.tracer = tracer

    def Generate(self, request, context):
        tr = self.tracer
        carrier = {k: v for k, v in context.invocation_metadata()
                   if k in ("traceparent", "tracestate")}
        parent_ctx = extract(carrier)
        srv = tr.start_span("Recv.tailsampling.Inference/Generate", context=parent_ctx)
        srv_ctx = set_span_in_context(srv)
        try:
            sp_sched = tr.start_span("schedule", context=srv_ctx)
            busy_sleep(ADMIT_MS)
            sp_sched.set_attribute("tailsampling.phase", "schedule")
            sp_sched.end()

            sp_pre = tr.start_span("prefill", context=srv_ctx)
            busy_sleep(request.prompt_tokens * PREFILL_MS_PER_TOKEN)
            sp_pre.set_attribute("tailsampling.phase", "prefill")
            sp_pre.set_attribute("tailsampling.prompt_tokens", request.prompt_tokens)
            sp_pre.end()

            sp_dec = tr.start_span("decode", context=srv_ctx)
            sp_dec.set_attribute("tailsampling.phase", "decode")
            produced = 0
            idx = 0
            while produced < request.max_tokens:
                n = min(TOKENS_PER_CHUNK, request.max_tokens - produced)
                # one sleep per chunk: per-token sleeps add ~0.2 ms of CPython
                # overhead each, which inflates the gap by ~12%.
                busy_sleep(n * DECODE_MS_PER_TOKEN)
                lo, hi = produced + 1, produced + n
                produced += n
                if request.stall_at >= 0 and lo <= request.stall_at <= hi:
                    busy_sleep(request.stall_ms)
                busy_sleep(DETOKENIZE_MS_PER_CHUNK)
                yield pb.Chunk(text="x" * n, index=idx, tokens=n,
                               final=(produced >= request.max_tokens))
                idx += 1
            sp_dec.set_attribute("tailsampling.output_tokens", produced)
            sp_dec.set_attribute("tailsampling.messages", idx)
            sp_dec.end()
        finally:
            srv.end()


def start_server(tracer, max_workers=32):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=max_workers))
    pbg.add_InferenceServicer_to_server(InferenceServicer(tracer), server)
    port = server.add_insecure_port("127.0.0.1:0")
    server.start()
    return server, f"127.0.0.1:{port}"

# ------------------------------------------------------------- the client
def run_call(stub, tracer, workload, max_tokens=None, stall_at=-1, stall_ms=0,
             on_gap=None, on_ttfm=None):
    """One real RPC inside one real client span. Returns (ttfm_ms, gaps, dur_ms,
    trace_id, msgs)."""
    cfg = dict(WORKLOADS[workload])
    if max_tokens is not None:
        cfg["max_tokens"] = max_tokens
    req = pb.GenerateRequest(prompt_tokens=cfg["prompt_tokens"],
                             max_tokens=cfg["max_tokens"],
                             stall_at=stall_at, stall_ms=stall_ms,
                             workload=workload)
    with tracer.start_as_current_span(f"Sent.tailsampling.Inference/Generate") as root:
        root.set_attribute("tailsampling.workload", workload)
        carrier = {}
        inject(carrier)
        md = tuple(carrier.items())
        t0 = time.perf_counter()
        prev = None
        ttfm = None
        gaps = []
        n = 0
        for _chunk in stub.Generate(req, metadata=md):
            now = time.perf_counter()
            n += 1
            if prev is None:
                ttfm = (now - t0) * 1000.0
                if on_ttfm: on_ttfm(ttfm)
            else:
                g = (now - prev) * 1000.0
                gaps.append(g)
                if on_gap: on_gap(g)
            prev = now
        dur = (time.perf_counter() - t0) * 1000.0
        root.set_attribute("tailsampling.messages", n)
        tid = root.get_span_context().trace_id
    return ttfm, gaps, dur, tid, n

def pct(xs, q):
    if not xs: return float("nan")
    s = sorted(xs)
    i = min(len(s) - 1, max(0, int(round((len(s) - 1) * q))))
    return s[i]

def hdr(t):
    print("\n" + "=" * 78); print(t); print("=" * 78)
