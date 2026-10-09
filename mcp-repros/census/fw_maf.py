"""Census row: Microsoft Agent Framework (agent-framework-core) + MCPStdioTool.

Usage: python fw_maf.py <run_dir> <server_python>
Env:   OVERRIDE_S (request_timeout passed to MCPStdioTool, default 0.3)
       SLOW_S     (server sleep after committing the effect, default 1.0)
       SETTLE_S   (how long to keep the session open after the turn, default 4.0)
"""
import asyncio, os, sys, json, importlib.metadata as md
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))
SLOW_S = os.environ.get("SLOW_S", "1.0")
SETTLE_S = float(os.environ.get("SETTLE_S", "4.0"))
INCLUDE_ERRORS = os.environ.get("INCLUDE_DETAILED_ERRORS", "1") == "1"
cc.setup(RUN)  # census owns the global TracerProvider; the framework never sets one unless configure_otel_providers() is called

from opentelemetry import trace
from agent_framework.observability import ChatTelemetryLayer
from agent_framework import (Agent, BaseChatClient, ChatResponse, ChatResponseUpdate, Message, Content, MCPStdioTool,
                             ChatMiddlewareLayer, FunctionInvocationLayer)

MODEL_CALLS = {"n": 0, "seen": []}


class FakeChatClient(ChatMiddlewareLayer, FunctionInvocationLayer, ChatTelemetryLayer, BaseChatClient):
    # same layer stack as the provider clients (middleware -> function invocation -> telemetry -> base)
    """No-network chat client: first call -> function call create_order(o1); afterwards -> plain text."""
    OTEL_PROVIDER_NAME = "fake"

    def _inner_get_response(self, *, messages, stream, options, **kwargs):
        MODEL_CALLS["n"] += 1
        n = MODEL_CALLS["n"]
        # record what the model was shown (tool results etc.)
        last = messages[-1] if messages else None
        MODEL_CALLS["seen"].append([(c.type, getattr(c, "text", None) or getattr(c, "result", None) or getattr(c, "name", None))
                                    for c in (last.contents if last is not None else [])])
        if n == 1:
            tool_names = [getattr(t, "name", str(t)) for t in (options.get("tools") or [])]
            MODEL_CALLS["tools_offered"] = tool_names
            name = next((t for t in tool_names if "create_order" in t), "create_order")
            contents = [Content.from_function_call(call_id="call_1", name=name, arguments={"order_id": "o1"})]
        else:
            contents = [Content.from_text("done")]
        msg = Message(role="assistant", contents=contents)
        if stream:
            async def _stream():
                yield ChatResponseUpdate(role="assistant", contents=contents, response_id=f"r{n}")
            return _stream()

        async def _resp():
            return ChatResponse(messages=[msg], response_id=f"r{n}")
        return _resp()

    def _inner_get_streaming_response(self, *, messages, options, **kwargs):
        return self._inner_get_response(messages=messages, stream=True, options=options, **kwargs)


async def run_turn(mcp_tool, outcome):
    async with mcp_tool:
        agent = Agent(client=FakeChatClient(), instructions="Create orders.", tools=[mcp_tool], name="census-agent")
        if INCLUDE_ERRORS:
            agent.client.function_invocation_configuration["include_detailed_errors"] = True
        with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
            outcome["agent_trace"] = format(sp.get_span_context().trace_id, "032x")
            try:
                resp = await agent.run("Create order o1")
                outcome["result"] = "returned"
                outcome["final_text"] = resp.text
                outcome["messages"] = [[(c.type, (getattr(c, "result", None) or getattr(c, "text", None)))
                                        for c in m.contents] for m in resp.messages]
            except Exception as e:
                outcome["result"] = "raised"
                outcome["exception"] = f"{type(e).__name__}: {e}"
        # keep the session (and thus the server process) open so late replies / retries are logged
        await asyncio.sleep(SETTLE_S)


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=SLOW_S)
    mcp_tool = MCPStdioTool(name="census", command=cmd, args=args, env=env, request_timeout=OVERRIDE)
    outcome = {}
    agent_trace = None
    try:
        await run_turn(mcp_tool, outcome)
    except Exception as e:
        outcome["result"] = "connect_failed"
        outcome["exception"] = f"{type(e).__name__}: {e}"
    agent_trace = outcome.pop("agent_trace", "") or ""
    open(f"{RUN}/requests.jsonl", "a").close()
    outcome["model_calls_seen"] = MODEL_CALLS["seen"]
    outcome["tools_offered"] = MODEL_CALLS.get("tools_offered")
    outcome["server_py"] = SERVER_PY
    outcome["slow_s"] = SLOW_S
    outcome["enable_instrumentation_env"] = os.environ.get("ENABLE_INSTRUMENTATION")
    cc.summarize(RUN, f"Microsoft Agent Framework {md.version('agent-framework-core')} (MCPStdioTool request_timeout={OVERRIDE}s)",
                 agent_trace, model_calls=MODEL_CALLS["n"], default_timeout=None, override=OVERRIDE, extra=outcome)
    print(json.dumps(outcome, default=str)[:1500])


asyncio.run(main())
