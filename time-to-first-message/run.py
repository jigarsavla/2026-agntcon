"""Tile 1 of the AGNTCon + MCPCon 2026 poster: same total time, very different first message.

Runs the two locked workloads through a real streaming RPC over loopback (rig.py) and prints
total call duration and time to first message for each. Prefill-heavy: a long prompt and a
short answer. Decode-heavy: a short prompt and a long answer.

Usage: python3 run.py [takes]      (default 5 takes per workload; prints the median)
"""
import statistics
import sys

import grpc

import rig

TAKES = int(sys.argv[1]) if len(sys.argv) > 1 else 5

with rig.serving() as addr, grpc.insecure_channel(addr) as ch:
    rig.warm(ch)
    rows = {}
    for name in ("prefill-heavy", "decode-heavy"):
        takes = [rig.measure(ch, name) for _ in range(TAKES)]
        rows[name] = dict(duration=statistics.median(t["duration_ms"] for t in takes),
                          ttfm=statistics.median(t["ttfm_ms"] for t in takes),
                          messages=takes[0]["messages"])

print(f"{'workload':15s} {'messages':>8s} {'total ms':>9s} {'first msg ms':>13s}")
for name, r in rows.items():
    print(f"{name:15s} {r['messages']:8d} {r['duration']:9.1f} {r['ttfm']:13.1f}")
a, b = rows["prefill-heavy"], rows["decode-heavy"]
print(f"\ntotal time within {abs(a['duration'] - b['duration']) / max(a['duration'], b['duration']) * 100:.1f}% "
      f"of each other; time to first message {a['ttfm'] / b['ttfm']:.0f}x apart "
      f"(the ratio moves from run to run; the poster says 'more than an order of magnitude')")
