"""Census row: CrewAI MCPClient.call_tool at the tool boundary (stdio).
Usage: python3 fw_crewai.py <run_dir> <server_python>
No model in the loop: the retry loop under test is MCPClient._retry_operation
(max_retries=3 attempts by default, 2**attempt backoff, execution_timeout 30 s).
"""
import asyncio, os, sys, importlib.metadata as md
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
cc.setup(RUN)
from opentelemetry import trace
from crewai.mcp.client import MCPClient, MCP_TOOL_EXECUTION_TIMEOUT, MCP_MAX_RETRIES
from crewai.mcp.transports.stdio import StdioTransport

OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=os.environ.get("SLOW_S", "1.0"))
    client = MCPClient(StdioTransport(command=cmd, args=args, env=env), execution_timeout=OVERRIDE)
    with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
        agent_trace = format(sp.get_span_context().trace_id, "032x")
        try:
            await client.connect()
            r = await client.call_tool("create_order", {"order_id": "o1"})
            outcome = f"ok: {str(r)[:80]}"
        except Exception as e:
            outcome = f"raised {type(e).__name__}: {str(e)[:120]}"
    await asyncio.sleep(2.0)
    try:
        await client.disconnect()
    except Exception:
        pass
    cc.summarize(RUN, f"CrewAI {md.version('crewai')}", agent_trace, model_calls=0,
                 default_timeout=MCP_TOOL_EXECUTION_TIMEOUT, override=OVERRIDE,
                 extra={"outcome": outcome, "default_max_retries_attempts": MCP_MAX_RETRIES,
                        "backoff": "2**attempt seconds"})

asyncio.run(main())
