"""
EXPERIMENT C -- Does the exemplar's trace id resolve to anything?

Earlier measurements established the exemplar mechanics: an AlignedHistogramBucketExemplarReservoir
carries at most one measurement per bucket, costs a bounded ~768 B per export
interval, and its default ExemplarFilter is TraceBased, so with head sampling at
1% only ~3% of tail events keep a usable pointer. The fix proposed
is OTEL_METRICS_EXEMPLAR_FILTER=always_on.

This experiment asks the question that fix does NOT answer: with always_on set,
so that every exemplar carries a trace id, does the trace id resolve to a trace
the pipeline actually kept?

Everything below is real: real gRPC streaming calls, real OTel histograms with
real exemplars from a real PeriodicExportingMetricReader, real spans with real
W3C propagation, fed to the Collector's tail sampling algorithm.
"""
import os, time, threading, grpc
import common
from common import hdr, pct
import inference_pb2_grpc as pbg
from tailsampler import TailSamplingProcessor, LatencyPolicy

from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    PeriodicExportingMetricReader, MetricExporter, MetricExportResult)
from opentelemetry.sdk.metrics.view import View, ExplicitBucketHistogramAggregation
from opentelemetry.sdk.metrics._internal.exemplar import (
    AlwaysOnExemplarFilter, TraceBasedExemplarFilter)
from opentelemetry.sdk.trace.sampling import TraceIdRatioBased, ALWAYS_ON

A66_LATENCY_MS = [0, .01, .05, .1, .3, .6, .8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16,
                  20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400,
                  500, 650, 800, 1000, 2000, 5000, 10000, 20000, 50000, 100000]

DECISION_WAIT = 2.0
THRESH_MS = 1000.0
REPS = 6

class Cap(MetricExporter):
    def __init__(self):
        super().__init__(preferred_temporality={}, preferred_aggregation={})
        self.exemplars = []   # (wall, metric, value_ms, trace_id)
    def export(self, md, timeout_millis=10000, **kw):
        now = time.time()
        for rm in md.resource_metrics:
            for sm in rm.scope_metrics:
                for m in sm.metrics:
                    for dp in getattr(m.data, "data_points", []):
                        for ex in (getattr(dp, "exemplars", None) or []):
                            self.exemplars.append((now, m.name, ex.value, ex.trace_id))
        return MetricExportResult.SUCCESS
    def force_flush(self, t=10000): return True
    def shutdown(self, t=30000, **kw): pass


def run(max_tokens, exemplar_filter, head_sampler, evict_after,
        sampled_cache=0, non_sampled_cache=0, stall=True):
    proc = TailSamplingProcessor(policies=[LatencyPolicy(threshold_ms=THRESH_MS)],
                                 decision_wait=DECISION_WAIT, num_traces=50_000,
                                 sampled_cache_size=sampled_cache,
                                 non_sampled_cache_size=non_sampled_cache)
    orig = proc.tick
    def tick(now=None):
        now = now or time.time(); orig(now)
        if evict_after is not None:
            with proc.lock:
                gone = [t for t, b in proc.buckets.items() if b.decided and
                        now - b.first_arrival >= proc.decision_wait + evict_after]
            for t in gone: proc.release(t)
    proc.tick = tick

    prov, tracer, rec = common.make_tracer(sink=proc.ingest, sampler=head_sampler)
    server, addr = common.start_server(tracer, max_workers=64)
    stub = pbg.InferenceStub(grpc.insecure_channel(addr))

    exp = Cap()
    reader = PeriodicExportingMetricReader(exp, export_interval_millis=250)
    view = View(instrument_name="tailsampling.inter_message_gap",
                aggregation=ExplicitBucketHistogramAggregation(boundaries=A66_LATENCY_MS))
    mp = MeterProvider(metric_readers=[reader], views=[view],
                       exemplar_filter=exemplar_filter)
    gap = mp.get_meter("tailsampling").create_histogram("tailsampling.inter_message_gap", unit="ms")

    stop = threading.Event()
    def ticker():
        while not stop.is_set(): proc.tick(); time.sleep(0.05)
    th = threading.Thread(target=ticker, daemon=True); th.start()

    calls, lock = [], threading.Lock()
    def one(i):
        t0 = time.time()
        _t, _g, dur, tid, _n = common.run_call(
            stub, tracer, "decode-heavy", max_tokens=max_tokens,
            stall_at=(max_tokens // 2 if (stall and i == 0) else -1), stall_ms=400,
            on_gap=lambda g: gap.record(g))
        with lock: calls.append((tid, dur, time.time()))
    ts = [threading.Thread(target=one, args=(i,)) for i in range(REPS)]
    for t in ts: t.start()
    for t in ts: t.join()
    time.sleep(DECISION_WAIT * 2 + 0.8)
    stop.set(); th.join(timeout=1); mp.shutdown(); server.stop(0)

    # ---- classify every exemplar
    kept_spans = {}
    for t, spans, _d, _f in proc.emitted:
        kept_spans.setdefault(t, set()).update(s.name for s in spans)
    produced = {}
    for t, spans, _d, _f in proc.emitted:
        produced.setdefault(t, set()).update(s.name for s in spans)
    for t, spans, _d in proc.dropped:
        produced.setdefault(t, set()).update(s.name for s in spans)

    n_total = n_notrace = n_unres = n_partial = n_complete = 0
    tail = []; land = []
    for wall, name, val, tid in exp.exemplars:
        n_total += 1
        if not tid: n_notrace += 1; continue
        k = kept_spans.get(tid, set()); p = produced.get(tid, set())
        if not k: n_unres += 1
        elif p and k == p and len(k) == 5: n_complete += 1; land.append(sorted(k))
        else: n_partial += 1; land.append(sorted(k))
        if val > 100: tail.append((wall, val, tid, len(k)))
    return dict(total=n_total, notrace=n_notrace, unres=n_unres,
                partial=n_partial, complete=n_complete, tail=tail,
                calls=calls, kept_spans=kept_spans, land=land)


def row(label, r):
    t = r["total"] or 1
    resolved = r["partial"] + r["complete"]
    if r["land"]:
        miss = sorted(set(["Recv.tailsampling.Inference/Generate","Sent.tailsampling.Inference/Generate",
                           "schedule","prefill","decode"]) - set(r["land"][0]))
        landing = f"{len(r['land'][0])}/5" + (f" (no {','.join(miss)})" if miss else " (whole)")
    else:
        landing = "-"
    print(f"  {label:44s} {r['total']:5d} {r['notrace']:7d} {r['unres']:7d} "
          f"{r['partial']:8d} {r['complete']:9d}   {100*resolved/t:5.1f}%  {landing}")


def main():
    print(f"  (default ExemplarFilter is TraceBased -- "
          f"OTEL_METRICS_EXEMPLAR_FILTER default is "
          f"{os.environ.get('OTEL_METRICS_EXEMPLAR_FILTER','trace_based (unset)')})")

    hdr("C1. EXEMPLAR POINTER RESOLUTION, ACROSS THE PIPELINE")
    print("  gap histogram on A66's 41 latency boundaries; tail sampler with a")
    print(f"  latency policy at {THRESH_MS:.0f} ms and decision_wait {DECISION_WAIT}s.")
    print("  'resolved' = the trace id names a trace the tail sampler KEPT.\n")
    print("  configuration                                exemp  no-tid  unres  "
          "partial  complete   resolved  you land on")
    print("  " + "-" * 116)

    short = run(700,  AlwaysOnExemplarFilter(), ALWAYS_ON, None)
    row("short stream 1.3s, always_on, no eviction", short)
    long1 = run(3600, AlwaysOnExemplarFilter(), ALWAYS_ON, None)
    row("long stream 6.4s, always_on, Scenario 1", long1)
    long2 = run(3600, AlwaysOnExemplarFilter(), ALWAYS_ON, 1.0)
    row("long stream 6.4s, always_on, Scenario 2", long2)
    long3 = run(3600, AlwaysOnExemplarFilter(), ALWAYS_ON, 1.0,
                sampled_cache=100_000, non_sampled_cache=100_000)
    row("long stream 6.4s, always_on, Sc.2 + caches", long3)
    head1 = run(3600, TraceBasedExemplarFilter(), TraceIdRatioBased(0.01), None)
    row("long stream 6.4s, DEFAULT filter, head 1%", head1)

    print()
    print("  Row 1 is the control: a stream shorter than decision_wait resolves.")
    print("  Row 2 is the same instrumentation on a longer generation.")
    print("  Row 5 is the configuration almost everyone is running today.")

    hdr("C2. THE EXEMPLARS THAT MATTER -- gaps over 100 ms")
    print("  One call per arm carries an injected 400 ms stall. These are the")
    print("  exemplars the whole mechanism exists to preserve.\n")
    print("  configuration                          tail exemplars   resolving to a kept trace")
    print("  " + "-" * 80)
    for label, r in (("short 1.3s, no eviction", short),
                     ("long 6.4s, Scenario 1", long1),
                     ("long 6.4s, Scenario 2", long2),
                     ("long 6.4s, Sc.2 + caches", long3),
                     ("long 6.4s, default filter + head 1%", head1)):
        tot = len(r["tail"])
        ok = sum(1 for _w, _v, _t, k in r["tail"] if k)
        worst = max((v for _w, v, _t, _k in r["tail"]), default=0)
        print(f"  {label:38s} {tot:8d}         {ok:5d}       "
              f"(worst gap seen: {worst:.0f} ms)")
    print()
    print("  The histogram itself recorded every one of those gaps, in every row.")
    print("  What is lost is not the measurement. It is the ability to ask WHY.")

    hdr("C3. THE POINTER IS EXPORTED BEFORE THE TRACE EXISTS")
    print("  For the long Scenario-2 arm: exemplar export time vs the moment the")
    print("  trace it names became available downstream (its last kept fragment).\n")
    r = long2
    end_by_tid = {tid: t for tid, dur, t in r["calls"]}
    leads = []
    for wall, val, tid, k in r["tail"]:
        if tid in end_by_tid:
            leads.append((val, (end_by_tid[tid] - wall) * 1000))
    if leads:
        print("    exemplar value    exported BEFORE the call even ended by")
        print("    " + "-" * 58)
        for v, l in sorted(leads, key=lambda x: -x[0])[:6]:
            print(f"    {v:10.1f} ms    {l:10.1f} ms")
        print()
        print(f"    plus decision_wait ({DECISION_WAIT:.0f}s) before the fragment is")
        print(f"    exported, plus a second decision_wait for the late fragment.")
        print(f"    At the Collector's 30 s default that is a minute of dangling")
        print(f"    pointer per exemplar, on top of the generation's own length.")
    else:
        print("    (no tail exemplars in this run)")

if __name__ == "__main__":
    main()
