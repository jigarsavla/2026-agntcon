# When Agents Leave the Model

Reliability lessons from tool-calling inference systems. Companion repository for the
poster at **AGNTCon + MCPCon North America 2026**, San Jose, Thursday October 22, 2026,
poster session 5:45 to 7:00 PM in the Solutions Showcase.

Jigar Kuverji Savla, Product Lead for AI Inference Systems @ Tensordyne.
jigar@tensordyne.ai | mail@jigarsavla.com

Every number on the poster comes from a run in this repo. Each folder below reruns one
tile in minutes on a laptop, with its requirements pinned.

## The story in one paragraph

An agent turn does not end when the model emits its last token. The result crosses the
transport, the model server, the client that runs the tool call, and the state that must
be back in place before the next turn starts. The painful failures happen at those
boundaries, after the model has done its job. The fix is to treat the turn as one
traceable transaction: track time to first message beside call duration, put an
idempotency key on every write, carry the trace in `_meta` at every hop, and make the
sampler wait past your slowest turn.

## Which folder reruns which tile

| tile | what the poster shows | folder | how long |
|---|---|---|---|
| **1** | **826 ms vs 45 ms.** Same total time; time to first message more than an order of magnitude apart. | [`time-to-first-message/`](time-to-first-message/) | about 15 s |
| **2** | **1 of 8.** Only CrewAI re-sends a timed-out tool call by default; OpenAI Agents does with retries on, LangGraph on resume. All eight skip the idempotency key; with one, 0 duplicates in 20 runs. | [`mcp-repros/census/`](mcp-repros/census/) and [`mcp-repros/`](mcp-repros/) (the 20-run replay) | a few minutes per framework |
| **3** | **5 of 10.** Your tool call leaves your trace unless something puts it in `_meta`. Google ADK and Microsoft's Agent Framework add it themselves; the rest inherit whichever mcp version they pin. | [`mcp-repros/census/`](mcp-repros/census/) | a few minutes per framework |
| **4** | **0 of 6.** A "keep anything slower than 1 s" sampler keeps the fast turns and drops the slow ones once a turn outlives its decision wait. It sees 51 ms of a 12 s trace. | [`tail-sampling/`](tail-sampling/) | about 3 minutes |

## How the framework census works (tiles 2 and 3)

One stub MCP server with one tool, `create_order`. The tool writes the order to a ledger
file **before** it replies, then stalls past the client's timeout, which is exactly the
case where a retry creates a second order. A small logger sits on the wire between the
framework and the server and records the JSON-RPC id and `_meta` of every request, so the
counts come from what was actually sent, not from what the framework reports. Each
framework runs with a fake model (no API keys, no network), against the server on both
mcp lines (2.3.0 and 1.30.0), and writes one row: requests on the wire, orders in the
ledger, and whether the server's tool span joined the agent's trace.

Frameworks in the census: CrewAI 1.15.25, OpenAI Agents SDK 0.23.1, Pydantic AI 2.54.0,
Langflow 1.12.5, LangGraph 1.2.14 with langchain-mcp-adapters 0.3.2, Google ADK 2.11.0,
Microsoft Agent Framework 1.21.0, and the Claude Agent SDK 0.2.165 (its bundled Claude
Code CLI is the MCP client). SDK lines: Python mcp 2.x, TypeScript 1.32.1 and 2.3.1.
Rows from both runs are in `mcp-repros/census/runs/`, and the census README has the table.

## What's here

| folder | what it is |
|---|---|
| [`mcp-repros/`](mcp-repros/) | The lost-response replay (20 logical calls, 40 effects without a key, 20 with one) and the first trace check across the MCP boundary. |
| [`mcp-repros/census/`](mcp-repros/census/) | The framework census rig: stub server, wire logger, one script per framework, pinned requirements, every row. |
| [`time-to-first-message/`](time-to-first-message/) | Tile 1. A real streaming call over loopback with a simulated model cost; prints total time and time to first message for a prefill-heavy and a decode-heavy request. |
| [`tail-sampling/`](tail-sampling/) | Tile 4. The OpenTelemetry Collector's tail-sampling processor rebuilt from its README, line by line, plus the experiment that shows the inversion. Run it against a real Collector and tell me where it diverges. |

Tiles 1 and 4 were first built for the September 2026 gRPConf talk, so their test traffic
is a gRPC stream; the findings are about streaming calls and tail sampling in general, not
about gRPC. The fuller telemetry contract test and the talk's demo recording live in
[jigarsavla/2026-grpconf](https://github.com/jigarsavla/2026-grpconf).

## Caveats

One machine, CPython 3.13, stdio transport for MCP, loopback for the streams, a fake or
simulated model everywhere. The wire and the timing are real; the GPU is not. The tail
sampler is a rebuild from the Collector README, not the Collector binary. Version numbers
are the ones that resolved on October 8 and 9, 2026.

## Primary sources

[MCP specification 2026-07-28](https://modelcontextprotocol.io/specification/2026-07-28)
(SEP-414 trace context in `_meta`, SEP-2575 re-issue after a lost response) ·
[modelcontextprotocol issue #3394](https://github.com/modelcontextprotocol/modelcontextprotocol/issues/3394) ·
[OpenTelemetry GenAI semantic conventions](https://github.com/open-telemetry/semantic-conventions-genai) ·
[Collector tail sampling processor README](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/main/processor/tailsamplingprocessor/README.md) ·
[langgraph issue #9185](https://github.com/langchain-ai/langgraph/issues/9185) ·
[langflow PR #15460](https://github.com/langflow-ai/langflow/pull/15460)

## License

GPL-3.0 (see [`LICENSE`](LICENSE)), the same license as the gRPConf repo the
`time-to-first-message/` and `tail-sampling/` code comes from.
