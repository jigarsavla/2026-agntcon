"""MCP stdio server that exports every span it creates to a JSON-lines file.
INSTR selects an optional third-party instrumentor to load on the server side."""
import json, os
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExporter, SpanExportResult

OUT = os.environ["SPANS"]
class FileExp(SpanExporter):
    def export(self, spans):
        with open(OUT, "a") as f:
            for s in spans:
                f.write(json.dumps({"side": "server", "name": s.name,
                    "trace": format(s.context.trace_id, "032x"),
                    "parent": format(s.parent.span_id, "016x") if s.parent else None,
                    "attrs": {k: str(v) for k, v in (s.attributes or {}).items()}}) + "\n")
        return SpanExportResult.SUCCESS
tp = TracerProvider(); tp.add_span_processor(SimpleSpanProcessor(FileExp())); trace.set_tracer_provider(tp)

from instrumentors import load
load(os.environ.get("INSTR", "none"))

from mcp.server.mcpserver import MCPServer
app = MCPServer("trace-demo")

@app.tool()
async def lookup(q: str) -> str:
    """A read-only tool."""
    with trace.get_tracer("tool").start_as_current_span("tool body: database query"):
        return f"result for {q}"

if __name__ == "__main__":
    app.run()
