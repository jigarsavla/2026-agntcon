"""Shared helpers for the framework census: OTel setup for the agent process, server
command, and the summary that turns the raw logs into one census row."""
import importlib.metadata as md
import json
import os

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

HERE = os.path.dirname(os.path.abspath(__file__))


def setup(run_dir):
    os.makedirs(run_dir, exist_ok=True)

    class FileExp(SpanExporter):
        def export(self, spans):
            with open(f"{run_dir}/spans.jsonl", "a") as f:
                for s in spans:
                    f.write(json.dumps({"side": "agent", "name": s.name,
                                        "trace": format(s.context.trace_id, "032x")}) + "\n")
            return SpanExportResult.SUCCESS

    tp = TracerProvider()
    tp.add_span_processor(SimpleSpanProcessor(FileExp()))
    trace.set_tracer_provider(tp)
    return tp


def server_cmd(run_dir, server_py, slow_s="1.0"):
    """Command, args and env for the stub server wrapped by the request logger."""
    env = dict(os.environ, LEDGER=f"{run_dir}/ledger.jsonl", SPANS=f"{run_dir}/spans.jsonl",
               SLOW_S=slow_s)
    return "python3", [f"{HERE}/tee_stdio.py", f"{run_dir}/requests.jsonl", server_py,
                       f"{HERE}/census_server.py"], env


def summarize(run_dir, framework, agent_trace, model_calls, default_timeout, override, extra=None):
    reqs = [json.loads(l) for l in open(f"{run_dir}/requests.jsonl")]
    calls = [r for r in reqs if r["method"] == "tools/call"]
    ids = [c["id"] for c in calls]
    ledger = [json.loads(l) for l in open(f"{run_dir}/ledger.jsonl")] if os.path.exists(f"{run_dir}/ledger.jsonl") else []
    spans = [json.loads(l) for l in open(f"{run_dir}/spans.jsonl")] if os.path.exists(f"{run_dir}/spans.jsonl") else []
    tool_spans = [s for s in spans if s.get("side") == "server" and s["name"].startswith("tools/call")]
    body_spans = [s for s in spans if s.get("side") == "server" and s["name"].startswith("tool body")]
    joined = any(s["trace"] == agent_trace for s in tool_spans + body_spans)
    metas = [c["meta"] for c in calls]
    tp_present = [bool(m and "traceparent" in m) for m in metas]
    key_fields = sorted({k for c in calls for k in ((c["meta"] or {}).keys() | (c["args"] or {}).keys())
                         if "idempot" in k.lower() or "dedup" in k.lower() or "request_id" in k.lower()})
    row = {
        "framework": framework,
        "mcp": md.version("mcp"),
        "server_mcp": tool_spans[0]["mcp"] if tool_spans else (body_spans[0]["mcp"] if body_spans else None),
        "default_timeout_s": default_timeout,
        "override_timeout_s": override,
        "tools_call_requests": len(calls),
        "tools_call_ids": ids,
        "distinct_ids": len(set(map(str, ids))),
        "effects": len(ledger),
        "model_calls": model_calls,
        "idempotency_or_dedup_field": key_fields or None,
        "traceparent_in_meta": tp_present,
        "metas": metas,
        "server_span_joined_agent_trace": joined,
        "agent_trace": agent_trace,
        "server_tool_spans": [(s["name"], s["trace"]) for s in tool_spans + body_spans],
    }
    if extra:
        row.update(extra)
    with open(f"{run_dir}/row.json", "w") as f:
        json.dump(row, f, indent=1)
    print(json.dumps({k: row[k] for k in ["framework", "mcp", "server_mcp", "tools_call_requests",
                                           "distinct_ids", "effects", "model_calls",
                                           "idempotency_or_dedup_field", "traceparent_in_meta",
                                           "server_span_joined_agent_trace"]}))
    return row
