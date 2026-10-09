"""Census row: Langflow (lfx) MCPStdioClient driven directly, no LLM in the loop.

Usage: fw/langflow/bin/python fw_langflow.py <run_dir> <server_python>
Env:   OVERRIDE_S (client tool timeout, default 0.3), SLOW_S (server sleep, default 1.0)
"""
import asyncio
import importlib.metadata as md
import os
import shlex
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc  # noqa: E402

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))
SLOW_S = os.environ.get("SLOW_S", "1.0")
cc.setup(RUN)

from opentelemetry import trace  # noqa: E402
from lfx.base.mcp.util import MCPStdioClient, _resolve_mcp_tool_execution_timeout  # noqa: E402
import lfx.base.mcp.util as lfx_util  # noqa: E402


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=SLOW_S)
    # Langflow builds the child env itself ({"DEBUG","PATH"} + user env), so pass only what the
    # stub needs. Its security policy scans these names against a reserved-name blocklist.
    child_env = {k: env[k] for k in ("LEDGER", "SPANS", "SLOW_S")}
    default_timeout = _resolve_mcp_tool_execution_timeout(None)
    client = MCPStdioClient(tool_execution_timeout=OVERRIDE)
    command_str = " ".join(shlex.quote(a) for a in [cmd, *args])
    tools = await client.connect_to_server(command_str, env=child_env)
    print("tools:", [t.name for t in tools])
    outcome = None
    with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
        agent_trace = format(sp.get_span_context().trace_id, "032x")
        try:
            res = await client.run_tool("create_order", {"order_id": "o1"}, timeout=OVERRIDE)
            outcome = f"returned {type(res).__name__}"
        except BaseException as e:  # noqa: BLE001
            outcome = f"raised {type(e).__name__}: {e}"
            print(outcome)
    # keep the pooled session (and the server process) alive long enough for every retry and the
    # slow reply to land in the logs
    await asyncio.sleep(1.5 * 2 + float(SLOW_S) + 0.5)
    try:
        await client.disconnect()
    except Exception as e:  # noqa: BLE001
        print("disconnect:", type(e).__name__, e)
    await asyncio.sleep(0.5)
    cc.summarize(
        RUN,
        f"Langflow {md.version('langflow')} / lfx {md.version('lfx')} MCPStdioClient.run_tool "
        f"(no LLM; tool_execution_timeout={OVERRIDE}s, SLOW_S={SLOW_S})",
        agent_trace,
        model_calls=0,
        default_timeout=default_timeout,
        override=OVERRIDE,
        extra={
            "server_python": SERVER_PY,
            "slow_s": float(SLOW_S),
            "outcome": outcome,
            "lfx": md.version("lfx"),
            "fastmcp": md.version("fastmcp"),
            "client_module": lfx_util.__file__,
            "pr_15460_present": "It was not retried" in open(lfx_util.__file__).read(),
        },
    )


asyncio.run(main())
