"""Census row: Pydantic AI with a FunctionModel (fake model) and MCPToolset over stdio.
Usage: python3 fw_pydantic_ai.py <run_dir> <server_python>
The fake model asks for create_order exactly once, then answers with text whatever comes
back, so any second tools/call request must come from the framework or the transport,
not from the model. Model calls are counted separately.
"""
import asyncio, os, sys, importlib.metadata as md
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
cc.setup(RUN)
from opentelemetry import trace
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPToolset
from pydantic_ai.models.function import FunctionModel, AgentInfo
from pydantic_ai.messages import ModelMessage, ModelResponse, TextPart, ToolCallPart
from fastmcp.client.transports import StdioTransport

OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))
DEFAULT_TIMEOUT = "5 min (read_timeout default 5 * 60 s)"
model_calls = []


def fake_model(messages: list[ModelMessage], info: AgentInfo) -> ModelResponse:
    model_calls.append(len(messages))
    if len(model_calls) == 1:
        return ModelResponse(parts=[ToolCallPart(tool_name="create_order", args={"order_id": "o1"})])
    return ModelResponse(parts=[TextPart("done")])


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=os.environ.get("SLOW_S", "1.0"))
    toolset = MCPToolset(StdioTransport(command=cmd, args=args, env=env), read_timeout=OVERRIDE)
    agent = Agent(FunctionModel(fake_model), toolsets=[toolset])
    with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
        agent_trace = format(sp.get_span_context().trace_id, "032x")
        try:
            async with agent:
                r = await agent.run("create order o1")
                kinds = [type(pt).__name__ for m in r.all_messages() for pt in m.parts]
                outcome = f"ok: {r.output!r}; message parts: {kinds}"
                await asyncio.sleep(2.0)
        except Exception as e:
            outcome = f"raised {type(e).__name__}: {str(e)[:120]}"
    await asyncio.sleep(0.5)
    cc.summarize(RUN, f"Pydantic AI {md.version('pydantic-ai-slim')} (fastmcp {md.version('fastmcp-slim')})",
                 agent_trace, model_calls=len(model_calls), default_timeout=DEFAULT_TIMEOUT,
                 override=OVERRIDE, extra={"outcome": outcome})

asyncio.run(main())
