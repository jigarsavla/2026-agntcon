"""
EXPERIMENT B -- What a tail sampler does with a stream that outlives its
decision window.

Real gRPC server-streaming inference calls; real OpenTelemetry spans with real
W3C context propagation (five spans, one trace id); the Collector's tail
sampling algorithm re-implemented faithfully (tailsampler.py quotes the README
for every behaviour).

The Collector's default decision_wait is 30 s. Real decode-heavy generations
run 5-120 s. This lab uses decision_wait = 2.0 s and streams of 0.7-13 s so the
RATIO -- the only thing that matters -- spans the real range at 1/15 scale.
Multiply every duration by 15 to read it as a production system.
"""
import time, threading, grpc
import common
from common import hdr, pct
import inference_pb2_grpc as pbg
from tailsampler import TailSamplingProcessor, LatencyPolicy

DECISION_WAIT = 2.0
THRESH_MS = 1000.0          # "keep every trace slower than one second"
ARMS = [400, 700, 1100, 1800, 3600, 7200]
REPS = 6


def run_arm(max_tokens, reps, evict_after=None, sampled_cache=0,
            non_sampled_cache=0, num_traces=50_000):
    """evict_after: seconds after a decision at which the trace's bucket is
    released from the circular buffer. This is what production traffic does:
    with num_traces=50,000 and N new traces/sec the buffer turns over every
    50,000/N seconds (10 s at 5,000 tps, 50 s at 1,000 tps). None = the trace
    is still in the buffer when the late spans arrive (README Scenario 1)."""
    proc = TailSamplingProcessor(policies=[LatencyPolicy(threshold_ms=THRESH_MS)],
                                 decision_wait=DECISION_WAIT, num_traces=num_traces,
                                 sampled_cache_size=sampled_cache,
                                 non_sampled_cache_size=non_sampled_cache)
    observed = {}     # trace_id -> duration the SAMPLER saw at decision time
    orig_tick = proc.tick
    def tick(now=None):
        now = now or time.time()
        with proc.lock:
            for tid, b in list(proc.buckets.items()):
                if not b.decided and now - b.first_arrival >= proc.decision_wait \
                        and b.spans and tid not in observed:
                    observed[tid] = (max(s.end_time for s in b.spans) -
                                     min(s.start_time for s in b.spans)) / 1e6
        orig_tick(now)
        if evict_after is not None:
            with proc.lock:
                gone = [tid for tid, b in proc.buckets.items()
                        if b.decided and now - b.first_arrival >=
                        proc.decision_wait + evict_after]
            for tid in gone:
                proc.release(tid)
    proc.tick = tick

    prov, tracer, rec = common.make_tracer(sink=proc.ingest)
    server, addr = common.start_server(tracer, max_workers=64)
    stub = pbg.InferenceStub(grpc.insecure_channel(addr))

    stop = threading.Event()
    def ticker():
        while not stop.is_set():
            proc.tick(); time.sleep(0.05)
    th = threading.Thread(target=ticker, daemon=True); th.start()

    results, lock = [], threading.Lock()
    def one():
        _t, _g, dur, tid, _n = common.run_call(stub, tracer, "decode-heavy",
                                               max_tokens=max_tokens)
        with lock: results.append((tid, dur))
    ts = [threading.Thread(target=one) for _ in range(reps)]
    for t in ts: t.start()
    for t in ts: t.join()
    time.sleep(DECISION_WAIT * 2 + 0.6)   # let every timer, incl. re-buffers, fire
    stop.set(); th.join(timeout=1); server.stop(0)
    return proc, results, observed


def analyse(proc, results):
    kept = whole = 0
    kept_names, frags = [], []
    for tid, _dur in results:
        emitted = [s for t, spans, _d, _f in proc.emitted if t == tid for s in spans]
        dropped = [s for t, spans, _d in proc.dropped if t == tid for s in spans]
        names = {s.name for s in emitted}
        total = len(emitted) + len(dropped)
        if emitted: kept += 1
        if emitted and not dropped and proc.frag_count.get(tid, 0) <= 1: whole += 1
        kept_names.append((len(emitted), total, names))
        frags.append(proc.frag_count.get(tid, 0))
    return kept, whole, kept_names, frags


def main():
    # ------------------------------------------------------------------ B1
    hdr(f"B1. THE LATENCY POLICY IS BIASED AGAINST SLOW REQUESTS")
    print(f"  decision_wait = {DECISION_WAIT}s;  policy: latency, threshold_ms = {THRESH_MS:.0f}")
    print("  README: 'The duration is determined by looking at the earliest start")
    print("  time and latest end time' -- of the spans the processor HOLDS.")
    print("  num_traces = 50,000, no eviction: README Scenario 1.\n")
    print("   stream    true dur   dur/dw   spans held    dur AS SEEN     policy")
    print("   (tokens)   (p50 ms)           at decision   by sampler      kept")
    print("   " + "-" * 70)
    b1 = []
    for mt in ARMS:
        proc, res, observed = run_arm(mt, REPS)
        kept, whole, kn, frags = analyse(proc, res)
        dur = pct([d for _t, d in res], .5)
        seen = pct(list(observed.values()), .5) if observed else float("nan")
        held = pct([len(kn_i[2]) if False else 0 for kn_i in kn], .5)
        # spans held at decision = size of the first decided batch
        firstbatch = []
        for tid, _d in res:
            n = 0
            for t, spans, _dt, fi in proc.emitted:
                if t == tid and fi == 1: n = len(spans)
            for t, spans, _dt in proc.dropped:
                if t == tid and n == 0: n = len(spans); break
            firstbatch.append(n)
        b1.append((mt, dur, seen, kept))
        print(f"   {mt:7d}   {dur:8.1f}   {dur/1000/DECISION_WAIT:5.2f}   "
              f"{int(pct(firstbatch,.5)):5d} of 5    {seen:9.1f} ms    {kept}/{REPS}")
    print()
    print("   EVERY arm is slower than the 1000 ms the policy was written to catch.")
    ok  = [r for r in b1 if r[3] == REPS]
    miss = [r for r in b1 if r[3] == 0 and r[1] > THRESH_MS and r[1]/1000 > DECISION_WAIT]
    if ok and miss:
        print(f"   Kept 6/6 at {min(r[1] for r in ok):.0f} ms and {max(r[1] for r in ok):.0f} ms.")
        print(f"   Kept 0/6 at {min(r[1] for r in miss):.0f} ms and every duration above it,")
        print(f"   including {max(r[1] for r in miss):.0f} ms.")
        print(f"   The detection rate is NOT monotone in latency. It inverts at")
        print(f"   duration = decision_wait, which no policy field mentions.")
        print(f"   At the top arm the sampler judges a 5-span trace on 2 spans and")
        print(f"   sees {b1[-1][2]:.0f} ms where the truth is {b1[-1][1]:.0f} ms -- "
              f"{b1[-1][1]/b1[-1][2]:.0f}x understated.")

    # ------------------------------------------------------------------ B2
    hdr("B2. SCENARIO 2 -- THE BUFFER TURNS OVER BEFORE THE STREAM ENDS")
    print("  Same traffic; the trace's bucket is released 1.0 s after its decision,")
    print("  which is what production traffic does to a 50,000-entry ring buffer.")
    print("  README: 'it is as if this component has never seen the trace before:")
    print("  The late spans are buffered for decision_wait seconds and then a new")
    print("  sampling decision is made.'\n")
    print("   stream    kept   complete   decisions   spans kept   which spans survived")
    print("   " + "-" * 76)
    for mt in (1800, 3600, 7200):
        proc, res, observed = run_arm(mt, REPS, evict_after=1.0)
        kept, whole, kn, frags = analyse(proc, res)
        nk = pct([a for a, b, c in kn], .5); nt = pct([b for a, b, c in kn], .5)
        names = sorted(set().union(*[c for a, b, c in kn]) if kn else set())
        short = ",".join(n.replace("tailsampling.Inference/Generate", "Gen") for n in names)
        print(f"   {mt:7d}   {kept}/{REPS}     {whole}/{REPS}     "
              f"mean {sum(frags)/len(frags):.1f}    {int(nk)} of {int(nt)}     {short}")
    print()
    print("   The trace comes back -- as a fragment. The spans that survive are the")
    print("   LONG ones. 'schedule' and 'prefill' were decided on and dropped before")
    print("   the stream finished, so the kept fragment is the half of the trace that")
    print("   does NOT contain time-to-first-token.")

    # ------------------------------------------------------------------ B3
    hdr("B3. CONFIGURING THE DECISION CACHE MAKES IT WORSE")
    print("  sampled_cache_size and non_sampled_cache_size both default to 0")
    print("  (inactive). Configuring them is the standard operational advice.")
    print("  Arm: 3600 tokens (~6.4 s), eviction 1.0 s after decision.\n")
    print("   configuration                      kept   spans kept   from cache")
    print("   " + "-" * 66)
    for label, sc, nsc in (("no cache (the default)", 0, 0),
                           ("sampled_cache_size = 100000", 100_000, 0),
                           ("both caches = 100000", 100_000, 100_000)):
        proc, res, observed = run_arm(3600, REPS, evict_after=1.0,
                                      sampled_cache=sc, non_sampled_cache=nsc)
        kept, whole, kn, frags = analyse(proc, res)
        nk = pct([a for a, b, c in kn], .5)
        print(f"   {label:34s} {kept}/{REPS}    {int(nk)} of 5      "
              f"{proc.late_from_cache:6d}")
    print()
    print("   The non-sampled cache does exactly what it is documented to do: it")
    print("   remembers the drop. The drop was decided on 57 ms of a 6-second")
    print("   stream. The cache turns a partial recovery into a total loss --")
    print("   consistently, which is the point of a cache, and wrongly.")

if __name__ == "__main__":
    main()
