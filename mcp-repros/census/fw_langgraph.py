"""Census row: LangGraph create_react_agent + langchain-mcp-adapters MultiServerMCPClient.

Usage: python fw_langgraph.py <run_dir> <server_python> [percall|session]
  percall  (adapter default): MultiServerMCPClient.get_tools() -> every tool call opens a
           fresh stdio session (spawns a new server process), initializes, calls, closes.
  session  : MultiServerMCPClient.session(name) + load_mcp_tools -> one long-lived session.
Env: OVERRIDE_S (client read timeout, session wide on the mcp 1.x client; default 1.0),
     SLOW_S (server sleep before replying; default 3.0), AGENT (react|create_agent).
"""
import asyncio
import importlib.metadata as md
import json
import os
import sys
import time
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc  # noqa: E402

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
MODE = sys.argv[3] if len(sys.argv) > 3 else "percall"
OVERRIDE = float(os.environ.get("OVERRIDE_S", "1.0"))
SLOW_S = os.environ.get("SLOW_S", "3.0")
AGENT = os.environ.get("AGENT", "react")
cc.setup(RUN)

from opentelemetry import trace  # noqa: E402
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel  # noqa: E402
from langchain_core.messages import AIMessage, HumanMessage  # noqa: E402
from langchain_mcp_adapters.client import MultiServerMCPClient  # noqa: E402
from langchain_mcp_adapters.tools import load_mcp_tools  # noqa: E402


class FakeToolModel(FakeMessagesListChatModel):
    """Scripted model: first reply calls create_order, later replies are plain text."""
    calls: int = 0

    def bind_tools(self, tools, **kw):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kw):
        self.calls += 1
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kw)


def make_model():
    return FakeToolModel(responses=[
        AIMessage(content="", tool_calls=[{"id": "call_1", "name": "create_order",
                                           "args": {"order_id": "o1"}, "type": "tool_call"}]),
        AIMessage(content="done"),
        AIMessage(content="done again"),
        AIMessage(content="done yet again"),
    ])


def build_agent(model, tools):
    if AGENT == "create_agent":
        from langchain.agents import create_agent
        return create_agent(model, tools)
    from langgraph.prebuilt import create_react_agent
    return create_react_agent(model, tools)


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=SLOW_S)
    conn = {"transport": "stdio", "command": cmd, "args": args, "env": env,
            "session_kwargs": {"read_timeout_seconds": timedelta(seconds=OVERRIDE)}}
    client = MultiServerMCPClient({"census": conn})
    model = make_model()
    outcome = {"mode": MODE, "agent_ctor": AGENT, "exception": None, "messages": []}

    async def run_agent(tools):
        agent = build_agent(model, tools)
        with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
            at = format(sp.get_span_context().trace_id, "032x")
            t0 = time.time()
            try:
                res = await agent.ainvoke({"messages": [HumanMessage(content="create order o1")]})
                for m in res["messages"]:
                    outcome["messages"].append({"type": m.type, "content": str(m.content)[:300],
                                                "status": getattr(m, "status", None)})
            except Exception as e:  # noqa: BLE001
                outcome["exception"] = f"{type(e).__name__}: {str(e)[:300]}"
                leaves = []

                def walk(x):
                    if isinstance(x, BaseExceptionGroup):
                        for s in x.exceptions:
                            walk(s)
                    else:
                        leaves.append(f"{type(x).__module__}.{type(x).__name__}: {str(x)[:200]}")
                        if x.__cause__ is not None:
                            walk(x.__cause__)
                walk(e)
                outcome["exception_leaves"] = leaves
            outcome["agent_wall_s"] = round(time.time() - t0, 2)
        return at

    if MODE == "session":
        async with client.session("census") as session:
            tools = await load_mcp_tools(session)
            at = await run_agent(tools)
            await asyncio.sleep(float(SLOW_S) + 2.0)  # let late reply / retries land while server lives
    else:
        tools = await client.get_tools()  # a listing session is opened and closed here
        at = await run_agent(tools)
        await asyncio.sleep(float(SLOW_S) + 2.0)

    ledger_path = f"{RUN}/ledger.jsonl"
    ledger_traces = [json.loads(l)["trace"] for l in open(ledger_path)] if os.path.exists(ledger_path) else []
    outcome["ledger_traces"] = ledger_traces
    outcome["ledger_trace_joined_agent_trace"] = any(t == at for t in ledger_traces)
    outcome["server_processes_spawned"] = sum(
        1 for l in open(f"{RUN}/requests.jsonl") if json.loads(l)["method"] == "initialize")
    outcome["versions"] = {p: md.version(p) for p in
                           ["langchain", "langgraph", "langchain-core", "langchain-mcp-adapters",
                            "langgraph-prebuilt", "mcp", "anyio", "pydantic"]}
    cc.summarize(RUN, f"LangGraph {md.version('langgraph')} + langchain-mcp-adapters "
                      f"{md.version('langchain-mcp-adapters')} ({AGENT}, {MODE}, mcp client "
                      f"{md.version('mcp')}, SLOW_S={SLOW_S})",
                 at, model_calls=model.calls, default_timeout=None, override=OVERRIDE, extra=outcome)
    print(json.dumps({k: outcome.get(k) for k in ["exception", "exception_leaves", "messages", "agent_wall_s",
                                              "server_processes_spawned",
                                              "ledger_trace_joined_agent_trace"]}, indent=1))


asyncio.run(main())
