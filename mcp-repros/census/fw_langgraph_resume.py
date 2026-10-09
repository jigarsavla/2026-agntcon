"""Census row: langgraph issue #9185 shape (resume after a tool error re-runs the tool node).

create_react_agent + InMemorySaver + fixed thread_id. The MCP tool (create_order) commits its
effect, then the client times out, so invoke #1 raises. invoke(None, config) on the same thread
resumes from the checkpoint: the tool node is re-executed with the same checkpointed tool call.
Per-call adapter mode (fresh stdio session per tool call) so the read timeout can be raised for
the resume, letting the second attempt succeed and the model produce its final answer.

Usage: python fw_langgraph_resume.py <run_dir> <server_python>
Env: OVERRIDE_S (first attempt read timeout; default 1.5), SLOW_S (server sleep; default 3.0).
"""
import asyncio
import importlib.metadata as md
import json
import os
import sys
from datetime import timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc  # noqa: E402

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
OVERRIDE = float(os.environ.get("OVERRIDE_S", "1.5"))
SLOW_S = os.environ.get("SLOW_S", "3.0")
cc.setup(RUN)

from opentelemetry import trace  # noqa: E402
from langchain_core.language_models.fake_chat_models import FakeMessagesListChatModel  # noqa: E402
from langchain_core.messages import AIMessage, HumanMessage  # noqa: E402
from langchain_mcp_adapters.client import MultiServerMCPClient  # noqa: E402
from langgraph.checkpoint.memory import InMemorySaver  # noqa: E402
from langgraph.prebuilt import create_react_agent  # noqa: E402


class FakeToolModel(FakeMessagesListChatModel):
    calls: int = 0

    def bind_tools(self, tools, **kw):
        return self

    def _generate(self, messages, stop=None, run_manager=None, **kw):
        self.calls += 1
        return super()._generate(messages, stop=stop, run_manager=run_manager, **kw)


async def main():
    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=SLOW_S)
    conn = {"transport": "stdio", "command": cmd, "args": args, "env": env,
            "session_kwargs": {"read_timeout_seconds": timedelta(seconds=OVERRIDE)}}
    client = MultiServerMCPClient({"census": conn})
    tools = await client.get_tools()
    model = FakeToolModel(responses=[
        AIMessage(content="", tool_calls=[{"id": "call_1", "name": "create_order",
                                           "args": {"order_id": "o1"}, "type": "tool_call"}]),
        AIMessage(content="done"), AIMessage(content="done again")])
    agent = create_react_agent(model, tools, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": "t1"}}
    out = {"first_invoke": None, "resume": None}

    def ledger_n():
        p = f"{RUN}/ledger.jsonl"
        return sum(1 for _ in open(p)) if os.path.exists(p) else 0

    with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
        at = format(sp.get_span_context().trace_id, "032x")
        try:
            await agent.ainvoke({"messages": [HumanMessage(content="create order o1")]}, config)
            out["first_invoke"] = "completed (unexpected)"
        except Exception as e:  # noqa: BLE001
            out["first_invoke"] = f"raised {type(e).__name__}"
        await asyncio.sleep(0.5)
        out["effects_after_first_invoke"] = ledger_n()
        out["model_calls_after_first_invoke"] = model.calls
        snap = await agent.aget_state(config)
        out["checkpoint_next"] = list(snap.next)
        out["checkpoint_msgs"] = [m.type for m in snap.values["messages"]]
        out["checkpointed_tool_call_ids"] = [tc["id"] for m in snap.values["messages"]
                                            if m.type == "ai" for tc in m.tool_calls]
        # resume: same thread, input None; give the second attempt a long enough timeout
        conn["session_kwargs"]["read_timeout_seconds"] = timedelta(seconds=float(SLOW_S) + 5)
        try:
            res = await agent.ainvoke(None, config)
            out["resume"] = "completed"
            out["final_messages"] = [{"type": m.type, "content": str(m.content)[:120],
                                      "tool_call_id": getattr(m, "tool_call_id", None)}
                                     for m in res["messages"]]
        except Exception as e:  # noqa: BLE001
            out["resume"] = f"raised {type(e).__name__}: {str(e)[:200]}"
        await asyncio.sleep(0.5)
    out["effects_after_resume"] = ledger_n()
    out["model_calls_total"] = model.calls
    out["note"] = ("per-call adapter mode: each tools/call runs in its own server process, so the "
                   "JSON-RPC ids restart per process (both calls carry id 1); count requests, not "
                   "distinct ids. Resume re-executed the tool node with the same tool_call_id "
                   "(call_1); no idempotency key or attempt signal reaches the server.")
    out["issue"] = "langchain-ai/langgraph#9185 (open, 2026-10-04, labels bug/external)"
    cc.summarize(RUN, f"LangGraph {md.version('langgraph')} + langchain-mcp-adapters "
                      f"{md.version('langchain-mcp-adapters')} (react, InMemorySaver resume after "
                      f"tool timeout, issue #9185 shape, mcp client {md.version('mcp')})",
                 at, model_calls=model.calls, default_timeout=None, override=OVERRIDE, extra=out)
    print(json.dumps(out, indent=1))


asyncio.run(main())
