# Tile 4: the sampler drops your slowest turns

A tail sampler with a rule like "keep every trace slower than 1 second" keeps the
fast turns and drops the slow ones once a turn runs longer than the sampler's decision
wait. It decides on the spans that have already ended, so it judges a 12-second trace
by the 51 ms it has seen.

```bash
pip install -r requirements.txt
python3 exp_b_tailsampling.py    # just the inversion, about 3 minutes
./run.sh                          # all four experiments, about 6 minutes
```

Output from the October 9, 2026 run (decision wait scaled to 2 s, rule: keep slower than 1 s):

```
 stream    true dur   dur/dw   spans held    dur AS SEEN     policy
 (tokens)   (p50 ms)           at decision   by sampler      kept
 ----------------------------------------------------------------------
     400      753.0    0.38       5 of 5        753.0 ms    0/6
     700     1268.3    0.63       5 of 5       1268.3 ms    6/6
    1100     1960.0    0.98       5 of 5       1960.1 ms    6/6
    1800     3181.0    1.59       2 of 5         51.1 ms    0/6
    3600     6302.7    3.15       2 of 5         51.2 ms    0/6
    7200    12532.9    6.27       2 of 5         51.1 ms    0/6
```

How it works. `tailsampler.py` rebuilds the OpenTelemetry Collector's tail-sampling
processor from its README. Each behavior it implements is quoted from the README on the
line above the code, so you can check it line by line. It is not the Collector binary;
if a real Collector gives you a different answer, please open an issue. The decision
wait is scaled from the Collector's 30 s default to 2 s so the run fits in minutes;
multiply every duration by 15 to read it as production.

The fix on the poster: set the sampler's decision wait above your slowest (p99) turn.

`exp_a` shows when each span becomes visible downstream, `exp_b` is the result above,
`exp_c` shows what happens to exemplar links that point at dropped traces, and `exp_d`
prices the histograms and the sampler's memory. `run.sh` writes the full output of all
four to `out/OUTPUT.txt`.

Code copied unchanged from the companion repo of the September 2026 gRPConf talk
(github.com/jigarsavla/2026-grpconf, `tail-sampling-repro/`). The test traffic is
streamed over gRPC because that is where it was first built; the sampler behavior does
not depend on the transport.
