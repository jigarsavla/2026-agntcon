"""MCP server with one mutating tool. Each call that runs the handler commits one effect
(appended to a ledger file) BEFORE replying. With slow=True the reply is delayed past the
client's timeout, so the client cannot tell whether the effect happened."""
import json, os, sys, time, asyncio
from mcp.server.mcpserver import MCPServer

LEDGER = os.environ["LEDGER"]
SEEN = os.environ["LEDGER"] + ".keys"
app = MCPServer("replay-demo")

def _seen():
    try:
        return set(open(SEEN).read().split())
    except FileNotFoundError:
        return set()

@app.tool()
async def create_order(order_id: str, amount: int, idempotency_key: str = "", slow: bool = True) -> str:
    """Create an order. Mutating: one call should create one order."""
    if idempotency_key and idempotency_key in _seen():
        return json.dumps({"order_id": order_id, "status": "deduped"})
    with open(LEDGER, "a") as f:                      # the effect commits here
        f.write(json.dumps({"order_id": order_id, "t": time.time()}) + "\n")
    if idempotency_key:
        with open(SEEN, "a") as f:
            f.write(idempotency_key + "\n")
    if slow:
        await asyncio.sleep(1.0)                      # reply arrives after the client gave up
    return json.dumps({"order_id": order_id, "status": "created"})

if __name__ == "__main__":
    app.run()   # stdio
