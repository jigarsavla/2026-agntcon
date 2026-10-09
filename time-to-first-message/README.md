# Tile 1: same total time, very different first message

Two requests take the same total time. One shows the user its first words in about
45 ms; the other makes the user wait about 820 ms. A dashboard that only tracks call
duration sees two identical calls.

```bash
pip install -r requirements.txt
python3 run.py          # about 15 seconds
```

Output from the October 9, 2026 run (grpcio 1.84.0, CPython 3.13, one host):

```
workload        messages  total ms  first msg ms
prefill-heavy         17    1022.7         820.3
decode-heavy          78    1032.8          44.9
```

How it works. `rig.py` is a real streaming server over loopback with a simulated model
cost: reading the prompt costs 0.4 ms per prompt token, writing the answer costs 1.62 ms
per output token, and tokens leave in messages of 8. The prefill-heavy request has a
2,000-token prompt and a short answer, so it spends most of its time before the first
message. The decode-heavy request has a 50-token prompt and a long answer, so its first
message is almost immediate and the time goes into streaming. Each message is released
at a fixed target time, so timing noise does not pile up across the 78 messages.

The exact ratio moves from run to run (13x to 24x across 330 takes on four machines),
because the decode-heavy first message is only tens of milliseconds long and any
overhead lands on it. That is why the poster says "more than an order of magnitude"
and never quotes one ratio. The model cost is simulated; the streaming call is real.

The fix on the poster: track time to first message beside call duration. It is an
ordinary streaming number that the transport can emit without knowing what a model is.

Copied from the companion repo of the September 2026 gRPConf talk
(github.com/jigarsavla/2026-grpconf, `demo/`); `run.py` is new and prints the two rows.
The rig uses gRPC streaming because that is where it was first built; the finding is
about streaming calls in general, not about gRPC.
