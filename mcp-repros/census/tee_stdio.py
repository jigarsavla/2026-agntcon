"""Wrap a stdio MCP server and log every JSON-RPC request the client sends.

Usage: python3 tee_stdio.py <logfile> <command...>
Each incoming line that parses as JSON is logged as {"id", "method", "meta", "name", "args"}.
Nothing in the stream is changed, so the measurement is independent of the SDK version
on either side.
"""
import json
import subprocess
import sys
import threading

LOG = sys.argv[1]
CMD = sys.argv[2:]

child = subprocess.Popen(CMD, stdin=subprocess.PIPE, stdout=sys.stdout.buffer, stderr=sys.stderr)


def pump():
    for raw in sys.stdin.buffer:
        try:
            msg = json.loads(raw)
            params = msg.get("params") or {}
            rec = {"id": msg.get("id"), "method": msg.get("method"),
                   "meta": params.get("_meta"), "name": params.get("name"),
                   "args": params.get("arguments")}
            with open(LOG, "a") as f:
                f.write(json.dumps(rec) + "\n")
        except Exception:
            pass
        child.stdin.write(raw)
        child.stdin.flush()
    try:
        child.stdin.close()
    except Exception:
        pass


t = threading.Thread(target=pump, daemon=True)
t.start()
rc = child.wait()
sys.exit(rc)
