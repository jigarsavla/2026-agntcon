"""Census row: Claude Agent SDK (claude-agent-sdk -> bundled Claude Code CLI as MCP client).

No real model, no API key: a local HTTP stub speaks just enough of the Anthropic Messages API
(SSE streaming) to return one tool_use for the MCP tool on the first "real" turn and an
end_turn text once a tool_result is in the conversation. The CLI is pointed at it via
ANTHROPIC_BASE_URL + a dummy ANTHROPIC_API_KEY. The stub census MCP server is registered as an
external stdio server in ClaudeAgentOptions.mcp_servers.

Usage: fw/claude/bin/python fw_claude_sdk.py runs/claude-<variant> <server_python> [MCP_TOOL_TIMEOUT_ms]
"""
import asyncio
import json
import os
import shutil
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import census_common as cc  # noqa: E402

RUN = os.path.abspath(sys.argv[1])
SERVER_PY = os.path.abspath(sys.argv[2])
TOOL_TIMEOUT_MS = sys.argv[3] if len(sys.argv) > 3 else None
SLOW_S = os.environ.get("CENSUS_SLOW_S", "1.0")
TOOL = "mcp__census__create_order"

if os.path.isdir(RUN):
    shutil.rmtree(RUN)
cc.setup(RUN)
from opentelemetry import trace  # noqa: E402

MODEL_CALLS = []  # one record per POST /v1/messages


def sse(events):
    out = []
    for ev in events:
        out.append(f"event: {ev['type']}\ndata: {json.dumps(ev)}\n\n")
    return "".join(out).encode()


def tool_use_stream(tool_name, args):
    tid = "toolu_census_001"
    return sse([
        {"type": "message_start", "message": {"id": "msg_stub_1", "type": "message", "role": "assistant",
                                               "model": "stub", "content": [], "stop_reason": None,
                                               "stop_sequence": None,
                                               "usage": {"input_tokens": 10, "output_tokens": 1}}},
        {"type": "content_block_start", "index": 0,
         "content_block": {"type": "tool_use", "id": tid, "name": tool_name, "input": {}}},
        {"type": "content_block_delta", "index": 0,
         "delta": {"type": "input_json_delta", "partial_json": json.dumps(args)}},
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": "tool_use", "stop_sequence": None},
         "usage": {"output_tokens": 20}},
        {"type": "message_stop"},
    ])


def text_stream(text):
    return sse([
        {"type": "message_start", "message": {"id": "msg_stub_2", "type": "message", "role": "assistant",
                                               "model": "stub", "content": [], "stop_reason": None,
                                               "stop_sequence": None,
                                               "usage": {"input_tokens": 10, "output_tokens": 1}}},
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": text}},
        {"type": "content_block_stop", "index": 0},
        {"type": "message_delta", "delta": {"stop_reason": "end_turn", "stop_sequence": None},
         "usage": {"output_tokens": 5}},
        {"type": "message_stop"},
    ])


class Stub(BaseHTTPRequestHandler):
    def log_message(self, *a):  # quiet
        pass

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        body = self.rfile.read(n)
        try:
            req = json.loads(body)
        except Exception:
            req = {}
        tools = [t.get("name") for t in req.get("tools", []) if isinstance(t, dict)]
        msgs = req.get("messages", [])
        has_result = any(isinstance(m.get("content"), list) and
                         any(isinstance(b, dict) and b.get("type") == "tool_result" for b in m["content"])
                         for m in msgs)
        last_user_text = ""
        for m in msgs:
            if m.get("role") == "user":
                c = m.get("content")
                if isinstance(c, str):
                    last_user_text = c
                elif isinstance(c, list):
                    for b in c:
                        if isinstance(b, dict) and b.get("type") == "text":
                            last_user_text = b.get("text", "")
        rec = {"path": self.path, "model": req.get("model"), "stream": req.get("stream"),
               "n_tools": len(tools), "has_census_tool": TOOL in tools, "has_tool_result": has_result,
               "t": time.time(), "last_user_text": last_user_text[:120]}
        if TOOL in tools and not has_result and "create_order" in last_user_text:
            rec["reply"] = "tool_use"
            payload = tool_use_stream(TOOL, {"order_id": "o1"})
        elif has_result:
            # record what the CLI told the model about the tool result
            for m in msgs:
                c = m.get("content")
                if isinstance(c, list):
                    for b in c:
                        if isinstance(b, dict) and b.get("type") == "tool_result":
                            rec["tool_result"] = json.dumps(b.get("content"))[:400]
                            rec["tool_result_is_error"] = b.get("is_error")
            rec["reply"] = "end_turn"
            payload = text_stream("done")
        else:
            rec["reply"] = "end_turn(other)"
            payload = text_stream("ok")
        MODEL_CALLS.append(rec)
        with open(f"{RUN}/model_calls.jsonl", "a") as f:
            f.write(json.dumps(rec) + "\n")
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self.send_response(404)
        self.end_headers()


async def main():
    httpd = ThreadingHTTPServer(("127.0.0.1", 0), Stub)
    port = httpd.server_address[1]
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    base = f"http://127.0.0.1:{port}"

    cmd, args, env = cc.server_cmd(RUN, SERVER_PY, SLOW_S)
    # only pass the census-specific env to the stdio server; the CLI merges its own env
    server_env = {k: env[k] for k in ("LEDGER", "SPANS", "SLOW_S")}

    cli_env = {
        "ANTHROPIC_BASE_URL": base,
        "ANTHROPIC_API_KEY": "sk-ant-census-dummy",
        "CLAUDE_CODE_DISABLE_NONESSENTIAL_TRAFFIC": "1",
        "DISABLE_TELEMETRY": "1",
        "DISABLE_ERROR_REPORTING": "1",
        "DISABLE_AUTOUPDATER": "1",
        "CLAUDE_CODE_MAX_RETRIES": "0",
        "HOME": os.environ.get("CENSUS_HOME", os.path.abspath(f"{RUN}/home")),
    }
    for k in ("HTTPS_PROXY", "HTTP_PROXY", "https_proxy", "http_proxy", "ALL_PROXY"):
        cli_env[k] = ""  # keep the stub call local; NO_PROXY as belt and braces
    cli_env["NO_PROXY"] = "127.0.0.1,localhost"
    if TOOL_TIMEOUT_MS:
        cli_env["MCP_TOOL_TIMEOUT"] = str(TOOL_TIMEOUT_MS)
    os.makedirs(cli_env["HOME"], exist_ok=True)

    from claude_agent_sdk import ClaudeAgentOptions, query

    opts = ClaudeAgentOptions(
        mcp_servers={"census": {"type": "stdio", "command": cmd, "args": args, "env": server_env}},
        allowed_tools=[TOOL],
        permission_mode="bypassPermissions",
        max_turns=3,
        env=cli_env,
        cwd=os.path.abspath(RUN),
        stderr=lambda line: open(f"{RUN}/cli_stderr.log", "a").write(line + "\n"),
    )
    events = []
    err = None
    with trace.get_tracer("agent").start_as_current_span("agent turn") as sp:
        at = format(sp.get_span_context().trace_id, "032x")
        t0 = time.time()
        try:
            async for msg in query(prompt="Call create_order with order_id o1.", options=opts):
                events.append(repr(msg)[:600])
                with open(f"{RUN}/sdk_messages.log", "a") as f:
                    f.write(repr(msg) + "\n")
        except Exception as e:  # noqa: BLE001
            err = f"{type(e).__name__}: {e}"
            events.append("EXC " + err)
        dt = time.time() - t0
    await asyncio.sleep(float(SLOW_S) + 1.0)
    httpd.shutdown()

    if not os.path.exists(f"{RUN}/requests.jsonl"):
        open(f"{RUN}/requests.jsonl", "w").close()
    model_calls = len([m for m in MODEL_CALLS if m["has_census_tool"] or m["has_tool_result"]])
    cc.summarize(RUN, "claude-agent-sdk 0.2.165 / Claude Code CLI 2.1.294 (stub Messages API)",
                 at, model_calls, "1e8 ms (per-server timeout -> MCP_TOOL_TIMEOUT -> 1e8), idle 30 min stdio",
                 TOOL_TIMEOUT_MS,
                 extra={"all_model_calls": MODEL_CALLS, "sdk_error": err, "wall_s": round(dt, 2),
                        "n_sdk_messages": len(events), "sdk_messages_tail": events[-6:],
                        "agent_trace_note": "CLI-side trace not observable (no OTel export from the CLI "
                                            "was configured); joined column = what is on the wire in _meta"})


asyncio.run(main())
