"""Does an agent's trace survive the MCP tool boundary?
Client: an 'agent turn' span wraps one tools/call over stdio to a separate server process.
Server: its spans go to a file. We check whether the server's spans land in the client's
trace, whether an execute_tool span with gen_ai.tool.name exists, and how many traces one
tool call produced. Repeated with each open-source instrumentor loaded on both sides."""
import asyncio, json, os, sys, tempfile, subprocess
HERE = os.path.dirname(os.path.abspath(__file__))
INSTR = sys.argv[1] if len(sys.argv) > 1 else "none"
CLIENT_OTEL = (sys.argv[2] if len(sys.argv) > 2 else "sdk") == "sdk"

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
mem = InMemorySpanExporter()
if CLIENT_OTEL:
    tp = TracerProvider(); tp.add_span_processor(SimpleSpanProcessor(mem)); trace.set_tracer_provider(tp)
sys.path.insert(0, HERE)
from instrumentors import load
status = load(INSTR)

from mcp import Client, StdioServerParameters

async def main():
    spans = tempfile.mktemp(prefix="spans-")
    params = StdioServerParameters(command=sys.executable, args=[os.path.join(HERE, "trace_server.py")],
                                   env={**os.environ, "SPANS": spans, "INSTR": INSTR})
    async with Client(params) as c:
        with trace.get_tracer("agent").start_as_current_span("agent turn") as turn:
            await c.call_tool("lookup", {"q": "orders"})
            turn_trace = format(turn.get_span_context().trace_id, "032x")
    await asyncio.sleep(0.5)
    srv = [json.loads(l) for l in open(spans)] if os.path.exists(spans) else []
    cli = [{"side": "client", "name": s.name, "trace": format(s.context.trace_id, "032x"),
            "attrs": dict(s.attributes or {})} for s in mem.get_finished_spans()]
    tool_srv = [s for s in srv if "lookup" in s["name"] or s["attrs"].get("gen_ai.tool.name") == "lookup"
                or s["name"].startswith("tool body")]
    joined = bool(tool_srv) and all(s["trace"] == turn_trace for s in tool_srv)
    exec_tool = [s for s in srv + cli if s["attrs"].get("gen_ai.operation.name") == "execute_tool"]
    traces = {s["trace"] for s in srv + cli if s["name"] not in ("server/discover", "initialize")
              and "discover" not in s["name"] and "initialize" not in s["name"]}
    print(json.dumps({"instrumentor": INSTR, "client_otel_sdk": CLIENT_OTEL, "load": status,
        "server_tool_spans_in_agent_trace": joined,
        "execute_tool_spans": [(s["side"], s["name"], s["attrs"].get("gen_ai.tool.name")) for s in exec_tool],
        "distinct_traces_for_one_tool_call": len(traces),
        "span_names": sorted({(s["side"], s["name"], s["trace"][:6], s["trace"]==turn_trace) for s in srv + cli})}))

asyncio.run(main())
