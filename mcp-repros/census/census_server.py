"""Census stub MCP server (stdio). Works on mcp 1.30.0 and mcp 2.3.0.

One mutating tool, create_order. Each call that reaches the handler commits one effect
(a line in LEDGER) BEFORE replying, then sleeps SLOW_S seconds so the reply arrives after
the client's timeout. Every server span is exported to SPANS as JSON lines with its trace id,
so the caller can tell whether the tool span joined the agent's trace.

The JSON-RPC id and _meta of every incoming request are logged by tee_stdio.py, which wraps
this process, so the log does not depend on the SDK version.
"""
import asyncio
import importlib.metadata as md
import json
import os
import time

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

LEDGER = os.environ.get("LEDGER", "ledger.jsonl")
SPANS = os.environ.get("SPANS", "spans.jsonl")
SLOW_S = float(os.environ.get("SLOW_S", "1.0"))
MCP_VERSION = md.version("mcp")


class FileExp(SpanExporter):
    def export(self, spans):
        with open(SPANS, "a") as f:
            for s in spans:
                f.write(json.dumps({
                    "side": "server", "mcp": MCP_VERSION, "name": s.name,
                    "trace": format(s.context.trace_id, "032x"),
                    "span": format(s.context.span_id, "016x"),
                    "parent": format(s.parent.span_id, "016x") if s.parent else None,
                    "attrs": {k: str(v) for k, v in (s.attributes or {}).items()},
                }) + "\n")
        return SpanExportResult.SUCCESS


tp = TracerProvider()
tp.add_span_processor(SimpleSpanProcessor(FileExp()))
trace.set_tracer_provider(tp)

if MCP_VERSION.startswith("1."):
    from mcp.server.fastmcp import FastMCP as Server
else:
    from mcp.server.mcpserver import MCPServer as Server

app = Server("census-stub")


@app.tool()
async def create_order(order_id: str, amount: int = 1) -> str:
    """Create an order. Mutating: one call must create exactly one order."""
    with trace.get_tracer("tool").start_as_current_span("tool body: create_order") as sp:
        with open(LEDGER, "a") as f:  # the effect commits here, before the reply
            f.write(json.dumps({"order_id": order_id, "t": time.time(),
                                "trace": format(sp.get_span_context().trace_id, "032x")}) + "\n")
        await asyncio.sleep(SLOW_S)  # reply arrives after the client gave up
        return json.dumps({"order_id": order_id, "status": "created"})


if __name__ == "__main__":
    app.run()
