# Framework census at the tool boundary

Every row is a run in a sandbox: one host, CPython 3.13, Node 22, stdio transport, a fake
model where the framework has one, no network. The stub MCP server (`census_server.py`)
has one mutating tool, `create_order`, that commits its effect to a ledger file BEFORE
replying, then sleeps past the client's timeout. `tee_stdio.py` wraps the server and logs
the JSON-RPC id and `_meta` of every request, so the counts do not depend on either side's
SDK version. The server exports its spans with trace ids, so "joined the agent's trace"
is read from the data, not from docs.

The stub server runs on both `mcp` 2.3.0 and `mcp` 1.30.0, because every `mcp` 2.x client
injects `traceparent` itself and every 2.x server reads it. Running a framework on the
1.x line, or against a 1.x server, shows which layer did the propagation.

```bash
python3 -m venv v23  && v23/bin/pip  install -r requirements-server-mcp2.3.0.txt
python3 -m venv v130 && v130/bin/pip install -r requirements-server-mcp1.30.0.txt

# TypeScript SDK lines, run against the Python stub server
(cd ts2 && npm install && node client.mjs $PWD/../runs/ts2 $PWD/../v23/bin/python without)
(cd ts1 && npm install && node client.mjs $PWD/../runs/ts1 $PWD/../v23/bin/python without)

# frameworks, one venv each (requirements-<name>.txt pins the packages that decide the result)
fw/openai/bin/python   fw_openai_agents.py runs/openai  v23/bin/python 0   # max_retry_attempts
fw/crewai/bin/python   fw_crewai.py        runs/crewai  v23/bin/python
fw/pydantic/bin/python fw_pydantic_ai.py   runs/pydantic v23/bin/python
```

Each script writes `row.json` in its run directory and prints one summary line. Columns:
tools/call requests by JSON-RPC id (automatic replay after a timeout), any idempotency or
dedup field, `traceparent` in `_meta`, and whether the server's tool span joined the agent's
trace; plus the resolved `mcp` version on each side, the framework's default timeout and
the override used. Model re-calls are counted separately from tools/call requests.

`OVERRIDE_S` (client timeout, default 0.3) and `SLOW_S` (tool delay, default 1.0) can be set
in the environment. On the `mcp` 1.x line the timeout is session wide, so a 0.3 s timeout
also kills `initialize`; those rows use 1.0 s and a 3 s tool delay.

Results of the October 8, 2026 run are in `runs/census_rows.json`.

| framework | client mcp | server mcp | tools/call per one call | effects | model re-calls | key | traceparent | joined |
|---|---|---|---|---|---|---|---|---|
| CrewAI 1.15.25 (defaults: 30 s, 3 attempts, 2^n backoff) | 1.28.1 | 2.3.0 / 1.30.0 | 3 | 3 | 0 | none | no | no |
| OpenAI Agents SDK 0.23.1, max_retry_attempts=0 (default) | 2.3.0 | 2.3.0 | 1 | 1 | 0 | none | yes (from the mcp 2.x client) | yes |
| OpenAI Agents SDK 0.23.1, max_retry_attempts=1 | 2.3.0 | 2.3.0 | 2 | 2 | 0 | none | yes | yes |
| OpenAI Agents SDK 0.23.1, max_retry_attempts=1, pinned mcp 1.30.0 | 1.30.0 | 2.3.0 | 2 | 2 | 0 | none | no | no |
| Pydantic AI 2.54.0 (FunctionModel) | 2.3.0 | 2.3.0 | 1 | 1 | 2 (ModelRetry) | none | yes | yes |
| Pydantic AI 2.54.0, pinned mcp 1.30.0 (fastmcp 3.4.8) | 1.30.0 | 2.3.0 | 1 | 1 | 2 | none | yes (from the fastmcp client) | yes |
| any client | 1.30.0 | | | | | | | no: a 1.x server ignores _meta.traceparent |

TypeScript lines (`ts1/`, `ts2/`): neither @modelcontextprotocol/sdk 1.32.1 nor
@modelcontextprotocol/client 2.3.1 sends `traceparent` by default; both pass an
app-supplied `_meta.traceparent` through and a 2.x Python server joins it.

## Part 2 (run October 9, 2026): Langflow, LangGraph, Google ADK, Microsoft Agent Framework, Claude Agent SDK

Same rig, same columns. Rows are in `runs/census_rows_part2.json` (one entry per run directory,
with the wire ids, the span trace ids and the outcome). The timed-out call is the same `create_order`
that committed before replying. Client timeouts that are session wide (the whole `mcp` 1.x line,
Google ADK's `StdioConnectionParams.timeout`) used 1.0 s or 1.5 s with a 3 s tool delay, because
0.3 s also kills `initialize`.

```bash
fw/langflow/bin/python  fw_langflow.py          runs/langflow-v23  v23/bin/python
fw/langgraph/bin/python fw_langgraph.py         runs/langgraph-v23 v23/bin/python session   # or percall
fw/langgraph/bin/python fw_langgraph_resume.py  runs/langgraph-v23-resume-9185 v23/bin/python
OVERRIDE_S=1.0 SLOW_S=3.0 fw/adk/bin/python fw_adk.py runs/adk-v23 v23/bin/python
OVERRIDE_S=1.0 SLOW_S=3.0 fw/maf/bin/python fw_maf.py runs/maf-v23 v23/bin/python
fw/claude/bin/python    fw_claude_sdk.py        runs/claude-timeout1s v23/bin/python 1000   # MCP_TOOL_TIMEOUT ms
```

| framework | client mcp | server mcp | tools/call per one call | effects | model re-calls | key | traceparent in _meta | joined | on timeout |
|---|---|---|---|---|---|---|---|---|---|
| Langflow 1.12.5 (lfx MCPStdioClient.run_tool, no model; default 180 s, override 0.3 s) | 1.30.0 | 2.3.0 / 1.30.0 | 1 | 1 | 0 | none | no | no | raises "not retried because the MCP server may have already run it" (PR #15460 is in 1.12.5) |
| LangGraph 1.2.14 + langchain-mcp-adapters 0.3.2 (FakeMessagesListChatModel; default no timeout, override 1.5 s) | 1.30.0 | 2.3.0 / 1.30.0 | 1 | 1 | 0 | none | no | no | McpError propagates out of the graph; no error ToolMessage, no retry |
| LangGraph 1.2.14, resume after the failed turn (issue #9185, InMemorySaver, same thread_id) | 1.30.0 | 2.3.0 | 2 (two processes) | 2 | 1 after resume | none | no | no | the tool node re-runs the checkpointed tool call |
| Google ADK 2.11.0 (BaseLlm fake, McpToolset; default 5 s session wide, override 1.0 s) | 2.3.0 | 2.3.0 / 1.30.0 | 1 | 1 | 1 (error result to the model) | none | yes | yes / no | error result to the model; retry only around session setup and tools/list, by design |
| Google ADK 2.11.0, pinned mcp 1.30.0 | 1.30.0 | 2.3.0 / 1.30.0 | 1 | 1 | 1 | none | yes (ADK injects: `propagate.inject` into `meta=`) | yes / no | same |
| Microsoft Agent Framework 1.21.0 (agent-framework-core, BaseChatClient fake, MCPStdioTool; default no timeout, override 1.0 s; its constraint is mcp<2) | 1.30.0 | 2.3.0 / 1.30.0 | 1 | 1 | 1 ("Error: Function failed.") | none | yes (framework injects: `_inject_otel_into_mcp_meta`) | yes / no | error result to the model; retry only after a closed or terminated session |
| Claude Agent SDK 0.2.165, bundled Claude Code CLI 2.1.294 (stub Messages API via ANTHROPIC_BASE_URL; default MCP_TOOL_TIMEOUT unbounded, idle 30 min; override 1000 ms) | 2.3.0 (CLI's own TS client) | 2.3.0 / 1.30.0 | 1 | 1 | 1 (is_error tool_result) | none (`claudecode/toolUseId` and `progressToken` only) | no | n/a (CLI trace not observable) | notifications/cancelled, error to the model, no re-send |

Not run: Langflow and LangGraph on an mcp 2.x client (both pin mcp<2; forcing 2.3.0 breaks their imports);
Microsoft Agent Framework on mcp 2.3.0 (initialize fails: the framework passes a timedelta where 2.x takes a float,
and reads `protocolVersion`); the 0.3 s override on ADK and the Agent Framework (session setup times out first).

Tally across both parts, default settings: CrewAI is the only framework that re-sends `tools/call` after a timeout.
The OpenAI Agents SDK does when retries are switched on; LangGraph does when a checkpointed run is resumed.
No framework sends an idempotency key. Trace context reaches the server from the mcp 2.x client (OpenAI Agents SDK,
ADK on its default pin, the stub client), from the fastmcp client (Pydantic AI), or from the framework itself
(Google ADK, Microsoft Agent Framework); CrewAI, Langflow, LangGraph and the Claude Code CLI send none.
