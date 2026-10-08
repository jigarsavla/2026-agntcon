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
