"""Replayed tool results across a real stdio MCP boundary (Python mcp SDK).
The client calls a mutating tool with a 0.3 s timeout; the tool commits its effect and
replies after 1.0 s. The application retries once with identical arguments, the shape
reported in modelcontextprotocol issue #3394 and langflow issue #15321.
Arm A: no idempotency key.  Arm B: a stable idempotency key, deduplicated at the effect."""
import asyncio, json, os, sys, tempfile, uuid
from mcp import Client, StdioServerParameters

N = 20
HERE = os.path.dirname(os.path.abspath(__file__))

async def run_arm(use_key: bool):
    ledger = tempfile.mktemp(prefix="ledger-")
    params = StdioServerParameters(command=sys.executable, args=[os.path.join(HERE, "replay_server.py")],
                                   env={**os.environ, "LEDGER": ledger})
    timeouts = retried_ok = 0
    async with Client(params) as c:
        for i in range(N):
            oid = f"order-{i}"
            args = {"order_id": oid, "amount": 100}
            if use_key:
                args["idempotency_key"] = str(uuid.uuid5(uuid.NAMESPACE_URL, oid))
            try:
                await c.call_tool("create_order", args, read_timeout_seconds=0.3)
            except Exception:
                timeouts += 1
                # the app sees an unknown outcome and retries once, same arguments
                r = await c.call_tool("create_order", args, read_timeout_seconds=3.0)
                retried_ok += 1
        await asyncio.sleep(1.5)  # let late replies drain
    effects = {}
    for line in open(ledger):
        oid = json.loads(line)["order_id"]; effects[oid] = effects.get(oid, 0) + 1
    dup = sum(1 for v in effects.values() if v > 1)
    return dict(logical_calls=N, timeouts=timeouts, retries=retried_ok,
                effects=sum(effects.values()), calls_with_2_effects=dup)

async def main():
    import mcp, platform
    print(f"python {platform.python_version()}  mcp {getattr(mcp,'__version__', 'see pip')}  transport stdio")
    a = await run_arm(False); print("A no idempotency key :", a)
    b = await run_arm(True);  print("B idempotency key    :", b)

asyncio.run(main())
