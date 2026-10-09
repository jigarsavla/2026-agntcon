"""The locked demo rig, restated. DEADLINE-PACED:
every message is released at an absolute target time, never by accumulating
per-message sleeps -- CPython's overshoot compounds 78x on the decode-heavy arm."""
import time, concurrent.futures, contextlib, socket, sys, os
import grpc
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import inference_pb2 as pb, inference_pb2_grpc as pbg

ADMIT_FIXED_MS, ADMIT_PER_PROMPT_MS = 0.40, 0.0002
PREFILL_PER_TOK_MS, DECODE_PER_TOK_MS = 0.40, 1.62
WORKLOADS = {
    "prefill-heavy": dict(prompt_tokens=2000, max_output_tokens=136, tokens_per_chunk=8),
    "decode-heavy":  dict(prompt_tokens=50,   max_output_tokens=624, tokens_per_chunk=8),
}

def _until(deadline):
    while time.perf_counter() < deadline:
        pass

class Servicer(pbg.InferenceServicer):
    def Infer(self, request, context):
        t0 = time.perf_counter(); p = request.prompt_tokens
        elapsed_ms = ADMIT_FIXED_MS + ADMIT_PER_PROMPT_MS * p + PREFILL_PER_TOK_MS * p
        _until(t0 + elapsed_ms / 1000.0)
        emitted, idx = 0, 0
        per_chunk = request.tokens_per_chunk or 8
        while emitted < request.max_output_tokens:
            n = min(per_chunk, request.max_output_tokens - emitted)
            elapsed_ms += DECODE_PER_TOK_MS * n
            _until(t0 + elapsed_ms / 1000.0)      # ABSOLUTE deadline, no accumulation
            emitted += n
            yield pb.InferChunk(text="tok " * n, tokens_in_chunk=n, chunk_index=idx,
                                t_server_ms=(time.perf_counter() - t0) * 1000.0,
                                finished=emitted >= request.max_output_tokens)
            idx += 1

def _free_port():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p

@contextlib.contextmanager
def serving():
    port = _free_port()
    srv = grpc.server(concurrent.futures.ThreadPoolExecutor(max_workers=4))
    pbg.add_InferenceServicer_to_server(Servicer(), srv)
    srv.add_insecure_port(f"127.0.0.1:{port}"); srv.start()
    try: yield f"127.0.0.1:{port}"
    finally: srv.stop(None)

def measure(channel, name):
    w = WORKLOADS[name]; stub = pbg.InferenceStub(channel)
    recv, tokens = [], 0
    t = time.perf_counter()
    for c in stub.Infer(pb.InferRequest(workload=name, **w)):
        recv.append((time.perf_counter() - t) * 1000.0); tokens += c.tokens_in_chunk
    dur = (time.perf_counter() - t) * 1000.0
    return dict(workload=name, prompt_tokens=w["prompt_tokens"], output_tokens=tokens,
                messages=len(recv), duration_ms=dur, ttfm_ms=recv[0])

def warm(channel):
    list(pbg.InferenceStub(channel).Infer(pb.InferRequest(prompt_tokens=1, max_output_tokens=8,
                                                          tokens_per_chunk=8, workload="w")))
