"""
A faithful re-implementation of the OpenTelemetry Collector's
`tailsamplingprocessor`, in Python, so it can be driven by REAL spans from
REAL gRPC calls inside one process.

Every behaviour below is taken from the processor's README:

  decision_wait               (default = 30s): "Time before timer handling for
                              a trace."
  num_traces                  (default = 50000): "Number of traces kept in
                              memory."
  sampled_cache_size          (default = 0, cache inactive)
  non_sampled_cache_size      (default = 0, cache inactive)

  "A span's arrival is considered 'late' if it arrives after its trace's
   sampling decision is made. Late spans can cause different sampling
   decisions for different parts of the trace."

  Scenario 1: "While the sampling decision of the trace remains in the circular
   buffer of num_traces length, the late spans inherit that decision. That
   means late spans do not influence the trace's sampling decision."
  Scenario 2 (default, no decision cache): "After the sampling decision is
   removed from the buffer, it's as if this component has never seen the trace
   before: The late spans are buffered for decision_wait seconds and then a new
   sampling decision is made."
  Scenario 3 (decision cache configured): "When a 'keep' decision is made on a
   trace, the trace ID is cached."

  latency policy: "Sample based on the duration of the trace. The duration is
   determined by looking at the earliest start time and latest end time,
   without taking into consideration what happened in between."
"""
import time, threading
from collections import OrderedDict


class Policy:
    def evaluate(self, trace_id, spans): raise NotImplementedError

class LatencyPolicy(Policy):
    """threshold_ms / upper_threshold_ms, per the README."""
    name = "latency"
    def __init__(self, threshold_ms, upper_threshold_ms=None):
        self.threshold_ms = threshold_ms
        self.upper_threshold_ms = upper_threshold_ms
    def evaluate(self, trace_id, spans):
        if not spans: return False
        earliest = min(s.start_time for s in spans)
        latest   = max(s.end_time   for s in spans)
        dur_ms   = (latest - earliest) / 1e6
        if dur_ms < self.threshold_ms: return False
        if self.upper_threshold_ms is not None and dur_ms > self.upper_threshold_ms:
            return False
        return True

class ProbabilisticPolicy(Policy):
    name = "probabilistic"
    def __init__(self, sampling_percentage):
        self.pct = sampling_percentage
    def evaluate(self, trace_id, spans):
        return (trace_id % 10_000) < (self.pct * 100)

class AlwaysSamplePolicy(Policy):
    name = "always_sample"
    def evaluate(self, trace_id, spans): return True


class _Bucket:
    __slots__ = ("spans", "first_arrival", "decided", "decision")
    def __init__(self, first_arrival):
        self.spans = []
        self.first_arrival = first_arrival
        self.decided = False
        self.decision = None


class TailSamplingProcessor:
    def __init__(self, policies, decision_wait=30.0, num_traces=50_000,
                 sampled_cache_size=0, non_sampled_cache_size=0):
        self.policies = policies
        self.decision_wait = decision_wait
        self.num_traces = num_traces
        self.sampled_cache_size = sampled_cache_size
        self.non_sampled_cache_size = non_sampled_cache_size
        self.buckets = OrderedDict()          # the circular buffer
        self.sampled_cache = OrderedDict()
        self.non_sampled_cache = OrderedDict()
        self.lock = threading.Lock()
        # outputs
        self.emitted = []      # (trace_id, [spans], decision_time, fragment_ix)
        self.dropped = []      # (trace_id, [spans], decision_time)
        self.frag_count = {}   # trace_id -> number of separate decisions made
        self.evictions = 0
        self.late_inherited = 0     # Scenario 1
        self.late_rebuffered = 0    # Scenario 2
        self.late_from_cache = 0    # Scenario 3

    # ------------------------------------------------------------------
    def ingest(self, span, now=None):
        now = now or time.time()
        tid = span.context.trace_id
        with self.lock:
            b = self.buckets.get(tid)
            if b is not None:
                if b.decided:
                    self.late_inherited += 1
                    if b.decision:
                        self.emitted.append((tid, [span], now,
                                             self.frag_count.get(tid, 1)))
                else:
                    b.spans.append(span)
                return
            # not in the buffer
            if self.sampled_cache_size and tid in self.sampled_cache:
                self.late_from_cache += 1
                self.emitted.append((tid, [span], now, self.frag_count.get(tid, 1)))
                return
            if self.non_sampled_cache_size and tid in self.non_sampled_cache:
                self.late_from_cache += 1
                return
            # Scenario 2 (or a genuinely new trace): start a fresh bucket and a
            # fresh decision_wait timer.
            if tid in self.frag_count:
                self.late_rebuffered += 1
            b = _Bucket(now)
            b.spans.append(span)
            self.buckets[tid] = b
            while len(self.buckets) > self.num_traces:
                self.buckets.popitem(last=False)
                self.evictions += 1

    # ------------------------------------------------------------------
    def tick(self, now=None):
        """Timer handling, as the processor's own ticker does."""
        now = now or time.time()
        with self.lock:
            due = [tid for tid, b in self.buckets.items()
                   if not b.decided and now - b.first_arrival >= self.decision_wait]
            for tid in due:
                b = self.buckets[tid]
                keep = any(p.evaluate(tid, b.spans) for p in self.policies)
                b.decided = True
                b.decision = keep
                self.frag_count[tid] = self.frag_count.get(tid, 0) + 1
                if keep:
                    self.emitted.append((tid, list(b.spans), now, self.frag_count[tid]))
                    if self.sampled_cache_size:
                        self.sampled_cache[tid] = now
                        while len(self.sampled_cache) > self.sampled_cache_size:
                            self.sampled_cache.popitem(last=False)
                else:
                    self.dropped.append((tid, list(b.spans), now))
                    if self.non_sampled_cache_size:
                        self.non_sampled_cache[tid] = now
                        while len(self.non_sampled_cache) > self.non_sampled_cache_size:
                            self.non_sampled_cache.popitem(last=False)

    # ------------------------------------------------------------------
    def release(self, tid):
        """Simulate the circular buffer moving on (traffic pushing this trace
        out). This is what makes Scenario 2 rather than Scenario 1 apply."""
        with self.lock:
            self.buckets.pop(tid, None)
            self.evictions += 1

    def kept_spans(self, tid):
        out = []
        for t, spans, _dt, _fi in self.emitted:
            if t == tid: out.extend(spans)
        return out
