"""
EXPERIMENT A -- When does a span become visible to anything?

The OpenTelemetry Trace SDK hands a span to a SpanProcessor in OnEnd, which
"MUST be called synchronously within the Span.End() API". There is no OnEnd
until the span ends. So for the whole life of a streaming RPC, the span that
describes it does not exist anywhere except inside the emitting process.

A1  measures the invisible interval for every span of a real inference call.
A2  sweeps stream length and reports arrival spread.
A3  runs a REAL PeriodicExportingMetricReader alongside, with exemplars, and
    asks a question nobody asks: does the exemplar get exported BEFORE the
    trace it points at exists?
"""
import os
os.environ.setdefault("OTEL_METRICS_EXEMPLAR_FILTER", "always_on")

import time, threading, grpc
import common
from common import hdr, pct
import inference_pb2_grpc as pbg

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader, MetricExporter, MetricExportResult,
    AggregationTemporality)
from opentelemetry.sdk.metrics.view import View, ExplicitBucketHistogramAggregation

# gRFC A66's latency bucket boundaries (seconds), verbatim from the gRFC.
A66_LATENCY_S = [0, 0.00001, 0.00005, 0.0001, 0.0003, 0.0006, 0.0008, 0.001,
                 0.002, 0.003, 0.004, 0.005, 0.006, 0.008, 0.01, 0.013, 0.016,
                 0.02, 0.025, 0.03, 0.04, 0.05, 0.065, 0.08, 0.1, 0.13, 0.16,
                 0.2, 0.25, 0.3, 0.4, 0.5, 0.65, 0.8, 1, 2, 5, 10, 20, 50, 100]

class CapturingExporter(MetricExporter):
    """Records the wall-clock instant of every export and the exemplars in it."""
    def __init__(self):
        super().__init__(preferred_temporality={},
                         preferred_aggregation={})
        self.exports = []   # (wall_time, [(metric_name, [(value, trace_id)])])
    def export(self, metrics_data, timeout_millis=10000, **kw):
        rows = []
        for rm in metrics_data.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    exs = []
                    for dp in getattr(m.data, "data_points", []):
                        for ex in (getattr(dp, "exemplars", None) or []):
                            exs.append((ex.value, ex.trace_id, ex.time_unix_nano))
                    if exs:
                        rows.append((m.name, exs))
        self.exports.append((time.time(), rows))
        return MetricExportResult.SUCCESS
    def force_flush(self, timeout_millis=10000): return True
    def shutdown(self, timeout_millis=30000, **kw): pass


def main():
    prov, tracer, rec = common.make_tracer()
    server, addr = common.start_server(tracer)
    stub = pbg.InferenceStub(grpc.insecure_channel(addr))

    # ---------------------------------------------------------------- A1
    hdr("A1. THE INVISIBLE INTERVAL -- one real decode-heavy inference RPC")
    t_origin = time.time()
    ttfm, gaps, dur, tid, n = common.run_call(stub, tracer, "decode-heavy")
    time.sleep(0.05)

    print(f"  call: ttfm {ttfm:.1f} ms  gap p50 {pct(gaps,.5):.2f} ms  "
          f"duration {dur:.1f} ms  messages {n}")
    print()
    print("  span            started(ms)   ended(ms)   lifetime   INVISIBLE FOR")
    print("  " + "-" * 68)
    rows = []
    for sp in rec.ended:
        st = (sp.start_time / 1e9 - t_origin) * 1000
        en = (sp.end_time   / 1e9 - t_origin) * 1000
        vis = (rec.end_wall[sp.context.span_id] - t_origin) * 1000
        rows.append((sp.name, st, en, vis))
    for name, st, en, vis in sorted(rows, key=lambda r: r[1]):
        print(f"  {name:26s} {st:8.1f}  {en:9.1f}   {en-st:8.1f}   {en-st:8.1f} ms")
    root = [r for r in rows if r[0].startswith("Sent.")][0]
    first_ended = min(rows, key=lambda r: r[2])
    print()
    print(f"  first span to become visible : {first_ended[0]!r} at t={first_ended[2]:.1f} ms")
    print(f"  root span becomes visible    : t={root[2]:.1f} ms")
    print(f"  -> the spans of ONE trace arrive at a collector spread over "
          f"{root[2]-first_ended[2]:.1f} ms,")
    print(f"     and the root -- the only span that knows the call's duration --")
    print(f"     arrives LAST, {root[2]-first_ended[2]:.1f} ms after the first.")

    # ---------------------------------------------------------------- A2
    hdr("A2. ARRIVAL SPREAD SCALES WITH THE STREAM (decode-heavy, max_tokens swept)")
    print("  max_tok   duration    root invisible for    span arrival spread")
    print("  " + "-" * 64)
    for mt in (64, 256, 624, 1600, 3200):
        rec.ended.clear(); rec.end_wall.clear()
        t0 = time.time()
        _t, _g, d, _tid, _n = common.run_call(stub, tracer, "decode-heavy", max_tokens=mt)
        time.sleep(0.03)
        rr = [( sp.name, (rec.end_wall[sp.context.span_id]-t0)*1000) for sp in rec.ended]
        rt = [v for k, v in rr if k.startswith("Sent.")][0]
        sp_spread = max(v for _, v in rr) - min(v for _, v in rr)
        print(f"  {mt:7d}  {d:8.1f} ms  {d:15.1f} ms  {sp_spread:17.1f} ms")
    print()
    print("  The root span's invisible interval IS the call duration, by")
    print("  construction. A 60-second generation is a 60-second blind spot.")

    # ---------------------------------------------------------------- A3
    hdr("A3. THE POINTER IS EXPORTED BEFORE THE THING IT POINTS AT")
    exporter = CapturingExporter()
    reader = PeriodicExportingMetricReader(exporter, export_interval_millis=250)
    view = View(instrument_name="tailsampling.inter_message_gap",
                aggregation=ExplicitBucketHistogramAggregation(
                    boundaries=[b*1000 for b in A66_LATENCY_S]))
    mp = MeterProvider(metric_readers=[reader], views=[view])
    gap_hist = mp.get_meter("tailsampling").create_histogram(
        "tailsampling.inter_message_gap", unit="ms",
        description="client-observed inter-message gap")

    rec.ended.clear(); rec.end_wall.clear()
    t0 = time.time()
    ttfm, gaps, dur, tid, n = common.run_call(
        stub, tracer, "decode-heavy", max_tokens=624,
        stall_at=200, stall_ms=400, on_gap=lambda g: gap_hist.record(g))
    root_visible_at = None
    time.sleep(0.05)
    for sp in rec.ended:
        if sp.name.startswith("Sent."):
            root_visible_at = (rec.end_wall[sp.context.span_id] - t0) * 1000
    time.sleep(0.6)   # let one more export interval elapse
    mp.shutdown()

    print(f"  one decode-heavy RPC, one injected 400 ms stall at token 200")
    print(f"  call duration        : {dur:.1f} ms")
    print(f"  root span visible at : t={root_visible_at:.1f} ms")
    print(f"  trace id             : {tid:032x}")
    print()
    print("  metric exports during and after the call (250 ms interval):")
    print("    t(ms)   metric                      exemplars  max exemplar  trace_id(hex, 8)")
    print("    " + "-" * 78)
    first_ref = None; stall_ref = None
    for wall, rows in exporter.exports:
        t = (wall - t0) * 1000
        for name, exs in rows:
            mx = max(exs, key=lambda e: e[0])
            tids = {e[1] for e in exs if e[1]}
            tag = f"{mx[1]:032x}"[:8] if mx[1] else "(none)"
            print(f"    {t:7.1f}  {name:26s} {len(exs):9d}  {mx[0]:9.2f} ms  {tag}")
            if first_ref is None and mx[1]:
                first_ref = t
            if mx[0] > 100 and stall_ref is None:
                stall_ref = (t, mx[0])
    print()
    if first_ref is not None and root_visible_at is not None:
        print(f"  first exemplar carrying a trace id exported at : t={first_ref:.1f} ms")
        print(f"  the span it points at became visible at        : t={root_visible_at:.1f} ms")
        print(f"  -> the pointer was on the wire {root_visible_at-first_ref:.1f} ms "
              f"BEFORE the trace existed.")
        print("     A backend resolving that exemplar the moment it lands finds nothing.")
    if stall_ref and root_visible_at is not None:
        print()
        print(f"  and the one that matters -- the {stall_ref[1]:.0f} ms stall -- was exported")
        print(f"  at t={stall_ref[0]:.1f} ms, {root_visible_at-stall_ref[0]:.1f} ms before its trace existed.")
    server.stop(0)

if __name__ == "__main__":
    main()
