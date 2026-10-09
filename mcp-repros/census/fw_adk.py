"""Census row: Google ADK (google-adk) McpToolset against the stub stdio MCP server.

Usage: python fw_adk.py <run_dir> <server_python> [label]
Env: OVERRIDE_S (default 0.3) client timeout; SLOW_S (default 1.0) server sleep;
     SETTLE_S (default 6.0) time to keep the session open after the turn.
"""
import asyncio, os, sys, json, time, importlib.metadata as md
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc

RUN, SERVER_PY = sys.argv[1], sys.argv[2]
LABEL = sys.argv[3] if len(sys.argv) > 3 else "defaults"
OVERRIDE = float(os.environ.get("OVERRIDE_S", "0.3"))
SLOW_S = os.environ.get("SLOW_S", "1.0")
SETTLE_S = float(os.environ.get("SETTLE_S", "6.0"))

TP = cc.setup(RUN)  # must own the global TracerProvider before ADK is imported
from opentelemetry import trace
from opentelemetry.sdk.trace import SpanProcessor

SPAN_IDS = []  # (name, span_id) of agent-side spans, to attribute the _meta traceparent

class IdRecorder(SpanProcessor):
  def on_end(self, span):
    SPAN_IDS.append((span.name, format(span.context.span_id, "016x")))
TP.add_span_processor(IdRecorder())

from typing import AsyncGenerator
from google.genai import types
from google.adk.models import BaseLlm, LlmRequest, LlmResponse
from google.adk.agents import LlmAgent
from google.adk.runners import Runner
from google.adk.sessions import InMemorySessionService
from google.adk.tools.mcp_tool import McpToolset
from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
from mcp import StdioServerParameters

MODEL_CALLS = {"n": 0, "tool_results": []}


class FakeLlm(BaseLlm):
  model: str = "fake"

  @classmethod
  def supported_models(cls):
    return ["fake"]

  async def generate_content_async(self, llm_request: LlmRequest, stream: bool = False
                                   ) -> AsyncGenerator[LlmResponse, None]:
    MODEL_CALLS["n"] += 1
    # record any function responses the framework fed back to the model
    for c in llm_request.contents or []:
      for p in c.parts or []:
        if p.function_response is not None:
          MODEL_CALLS["tool_results"].append(
              json.loads(json.dumps(p.function_response.response, default=str)))
    if MODEL_CALLS["n"] == 1:
      part = types.Part(function_call=types.FunctionCall(name="create_order", args={"order_id": "o1"}))
    else:
      part = types.Part(text="done")
    yield LlmResponse(content=types.Content(role="model", parts=[part]), turn_complete=True)


async def main():
  cmd, args, env = cc.server_cmd(RUN, SERVER_PY, slow_s=SLOW_S)
  toolset = McpToolset(connection_params=StdioConnectionParams(
      server_params=StdioServerParameters(command=cmd, args=args, env=env),
      timeout=OVERRIDE))
  agent = LlmAgent(name="census_agent", model=FakeLlm(), instruction="Create the order.",
                   tools=[toolset])
  svc = InMemorySessionService()
  runner = Runner(agent=agent, app_name="census", session_service=svc)
  session = await svc.create_session(app_name="census", user_id="u")
  msg = types.Content(role="user", parts=[types.Part(text="create order o1")])

  events, raised = [], None
  t0 = time.time()
  with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
    agent_trace = format(sp.get_span_context().trace_id, "032x")
    try:
      async for ev in runner.run_async(user_id="u", session_id=session.id, new_message=msg):
        for p in (ev.content.parts if ev.content and ev.content.parts else []):
          if p.function_call:
            events.append({"t": round(time.time() - t0, 3), "function_call": p.function_call.name,
                           "args": dict(p.function_call.args or {})})
          elif p.function_response:
            events.append({"t": round(time.time() - t0, 3), "function_response":
                           json.loads(json.dumps(p.function_response.response, default=str))})
          elif p.text:
            events.append({"t": round(time.time() - t0, 3), "text": p.text})
    except Exception as e:
      raised = f"{type(e).__name__}: {e}"
  turn_s = round(time.time() - t0, 3)
  # keep the MCP session (and the server process) alive so late replies / retries are logged
  await asyncio.sleep(SETTLE_S)
  await toolset.close()

  # who injected the traceparent the server saw on tools/call?
  reqs = [json.loads(l) for l in open(f"{RUN}/requests.jsonl")]
  attribution = []
  for r in reqs:
    if r["method"] == "tools/call" and r.get("meta") and "traceparent" in r["meta"]:
      sid = r["meta"]["traceparent"].split("-")[2]
      owner = [n for n, i in SPAN_IDS if i == sid]
      attribution.append({"id": r["id"], "parent_span": sid, "agent_span": owner or None})
  cc.summarize(RUN, f"Google ADK {md.version('google-adk')} ({LABEL}; McpToolset stdio, "
               f"StdioConnectionParams.timeout={OVERRIDE}s, SLOW_S={SLOW_S})",
               agent_trace, model_calls=MODEL_CALLS["n"], default_timeout=5.0, override=OVERRIDE,
               extra={"label": LABEL, "server_python": SERVER_PY, "slow_s": SLOW_S,
                      "turn_wall_s": turn_s, "turn_raised": raised, "events": events,
                      "tool_results_seen_by_model": MODEL_CALLS["tool_results"],
                      "meta_traceparent_attribution": attribution,
                      "injector": "google-adk McpTool._run_async_impl (propagate.get_global_textmap().inject -> call_tool(meta=...))",
                      "timeout_path": "StdioConnectionParams.timeout -> SessionContext -> ClientSession(read_timeout_seconds=timeout) (session wide; also bounds initialize)"})

asyncio.run(main())
