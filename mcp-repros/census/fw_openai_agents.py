"""Census row: OpenAI Agents SDK, MCPServerStdio.call_tool at the tool boundary.
Usage: python3 fw_openai_agents.py <run_dir> <server_python> <max_retry_attempts>
No model in the loop: the retry loop under test lives in MCPServerStdio._run_with_retries.
"""
import asyncio, os, sys, importlib.metadata as md
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc

RUN, SERVER_PY, RETRIES = sys.argv[1], sys.argv[2], int(sys.argv[3])
cc.setup(RUN)
from opentelemetry import trace
from agents.mcp import MCPServerStdio
import inspect

sig = inspect.signature(MCPServerStdio.__init__)
default_timeout = sig.parameters["client_session_timeout_seconds"].default
OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=os.environ.get("SLOW_S", "1.0"))
    server = MCPServerStdio(params={"command": cmd, "args": args, "env": env},
                            client_session_timeout_seconds=OVERRIDE,
                            max_retry_attempts=RETRIES, retry_backoff_seconds_base=0.1)
    async with server:
        with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
            agent_trace = format(sp.get_span_context().trace_id, "032x")
            try:
                await server.call_tool("create_order", {"order_id": "o1"})
                outcome = "ok"
            except Exception as e:
                outcome = f"raised {type(e).__name__}"
        await asyncio.sleep(1.5 * (RETRIES + 1) + 0.5)
    cc.summarize(RUN, f"OpenAI Agents SDK {md.version('openai-agents')} (max_retry_attempts={RETRIES})",
                 agent_trace, model_calls=0, default_timeout=default_timeout, override=OVERRIDE,
                 extra={"outcome": outcome, "default_max_retry_attempts": sig.parameters["max_retry_attempts"].default})

asyncio.run(main())
