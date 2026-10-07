# MCP tool-call repros

The two MCP measurements on the poster. Python MCP SDK 2.2.0 over stdio, one host, CPython.

```bash
pip install mcp opentelemetry-sdk
python3 exp_replay.py          # replayed tool results, about 1 minute
python3 exp_trace.py none sdk  # does the agent's trace reach the tool server?
# optional add-on instrumentors:
pip install opentelemetry-instrumentation-mcp openinference-instrumentation-mcp openlit
python3 exp_trace.py openllmetry sdk   # also: openinference, openlit
python3 exp_trace.py none nosdk        # agent process with no OpenTelemetry SDK set up
```

## Replayed tool results (`exp_replay.py`)

A mutating tool commits its effect, then replies after 1.0 s. The client times out at
0.3 s and retries once with the same arguments, the shape reported in
[modelcontextprotocol #3394](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394)
and [langflow #15321](https://github.com/langflow-ai/langflow/issues/15321).

| arm | logical calls | timeouts | effects | calls with 2 effects |
|---|---|---|---|---|
| no idempotency key | 20 | 20 | 40 | **20** |
| idempotency key, deduplicated at the effect | 20 | 20 | 20 | **0** |

## Trace across the tool boundary (`exp_trace.py`)

An "agent turn" span wraps one `tools/call` to a separate server process.

- Python SDK 2.2.0 injects `traceparent` into `_meta` and the server joins the agent's trace
  natively (SEP-414); the server emits a `tools/call` span with
  `gen_ai.operation.name = execute_tool`.
- The trace crosses only if the agent process has an OpenTelemetry SDK configured. Without
  one, the tool's spans land in a separate trace.
- TypeScript SDK 1.32.1 (the npm `latest` tag) has no trace propagation in its source.
  TypeScript 2.3.1 (`@modelcontextprotocol/client`) defines the `traceparent` `_meta` key
  but has no OpenTelemetry dependency, so filling it is left to the application. Both read
  from source, not run.
- Add-on instrumentors on Python SDK 2.2.0: OpenLLMetry 0.62.4 failed to load
  (`PathResolutionError` on `streamablehttp_client`); OpenInference 2.0.12 loaded and logged
  an OpenTelemetry context-detach error; OpenLIT 1.45.0 loaded and added client spans in the
  agent's trace plus session spans in separate traces.

Each script prints its result as one line per arm or configuration.
