# Latency Sweep Benchmark — v1.2.2 (rmp/flat) vs v1.4.3 (mm32/ivf_vct)

> **Supersedes** `latency_bench_plan_envector_v1.2.2.md`,
> `latency_bench_plan_envector_v1.2.2_extended.md`, and
> `latency_bench_plan_envector_v1.4.3.md`. Those describe the older
> per-version runners (`latency_bench_v1.4.3.py`, since removed). The current
> work uses one adapter-driven runner — `benchmark/runners/latency_bench.py`
> — with an index-size sweep mode.

## Goal

Does v1.4.3's `mm32 + ivf_vct` have a real latency advantage over v1.2.2's
`rmp + flat`, in the regime where ivf_vct's centroid pruning is active?

envector `ivf_vct` adds a virtual centroid every ~4096 rows, and a query
probes `default_nprobe` of them — so query cost ≈ `nprobe × 4096` vectors,
roughly constant once the index holds more than `nprobe` centroids. Below
that the query still scans every centroid, i.e. behaves like a flat scan.
With `default_nprobe = 6` (see Index config), the ivf advantage appears only
above the **crossover N ≈ nprobe × 4096 ≈ 24,576**.

> ivf_vct is **not yet in production** — the live `runecontext` index runs
> v1.2.2 `rmp/flat`. This benchmark is a pre-adoption evaluation; the v1.4.3
> index parameters below are evaluation choices, not a mirror of a live index.

## Index config — v1.4.3 bench index

The `--direct-envector` bench index is created (`SdkAdapter.create_index` →
`V143Adapter.index_params`, in `benchmark/runners/sdk/v143.py`) with:

| param | value | note |
|---|---|---|
| index_type | `IVF_VCT` | |
| dim | 1024 | `BENCH_DIM` |
| nlist | **32768** | centroid-capacity headroom. **UNVERIFIED** — only `nlist=256` is known-good (the v1.4.3-native runner). The Step 2 probe must confirm it before any long run; fall back to 256 if it fails. |
| default_nprobe | **6** | centroids probed per query → crossover N ≈ 6 × 4096 ≈ 24,576 |
| eval_mode | `mm32` | |
| query encryption | `plain` | metadata encryption off |

`nlist` / `default_nprobe` are **fixed for the whole sweep** — only N varies.

## Unified runner

`benchmark/runners/latency_bench.py` is SDK-agnostic. `get_sdk_adapter()`
detects the installed `pyenvector.__version__` → V122Adapter (1.2.x) or
V143Adapter (1.4.x). Sweep mode (`--primer-rows N1,N2,...`) measures the
selected scenarios across a grid of index sizes N; see the runner's
`run_sweep` docstring. Raw samples stream to a long-format CSV
(`N,scenario,run_idx,phase,latency_ms`).

## Methodology — asymmetric grid

| | v1.2.2 / flat | v1.4.3 / ivf_vct |
|---|---|---|
| Scaling | O(N), no regime change | ≈ flat scan below the crossover, then ≈ constant |
| High-N measurement | infeasible — score ≈ 11 ms/record, so N=16384 ≈ 3 min/call | feasible — ivf cost caps at ≈ nprobe × 4096 |
| Grid strategy | measure [0..8192], **extrapolate the line above** | measure realistic sizes, dense around the crossover |

flat is a brute scan — provably linear, and the data confirms a near-perfect
line. Extrapolating it above the measured range is rigorous, not a guess.
ivf changes regime at the crossover, so it cannot be extrapolated — it must
be measured, with points bracketing the crossover.

The two grids no longer share points: v1.2.2 is the measured-then-extrapolated
flat line, v1.4.3 is measured directly. The comparison overlays them on a
latency-vs-N plot. ivf cost ≈ `nprobe × 4096` vectors (≈ constant once the
index has more than `nprobe` centroids), so the crossover N — where ivf meets
the flat line — sits near `nprobe × 4096` ≈ 24,576, bracketed by the
N=20,000 / 25,000 / 32,000 measurement points.

> The two SDKs run on different machines (the SDK versions cannot coexist).
> The dominant N-dependent phase (`score`) is cluster-side, so client-machine
> differences mostly affect the smaller phases — note this in the report.

## Phase 3 — v1.2.2 sweep   `[machine: pyenvector 1.2.2]`

**Status: running** (launched 2026-05-16, ~16 h).

```
python benchmark/runners/latency_bench.py \
    --direct-envector \
    --primer-rows 0,256,1024,2048,4096,8192 \
    --runs 11 --warmup 3 \
    --report benchmark/reports/latency_sweep_v122_rmpflat_2026-05-16.md \
    --raw-csv benchmark/reports/raw/latency_sweep_v122_rmpflat_2026-05-16.csv
```

8 effective runs/N. flat is a line, so few runs/N suffice — the 6-point line
fit averages the noise.

## Phase 4 — v1.4.3 sweep + comparison   `[machine: pyenvector 1.4.3]`

Prerequisites: pyenvector 1.4.3 installed; the v1.4.3 cluster endpoint set in
`~/.rune/config.json`; the `rune/.venv` (Python 3.12).

### Step 1 — V143Adapter   (implemented)

`benchmark/runners/sdk/v143.py` implements `connect` / `insert` /
`measure_insert_to_searchable`. `SdkAdapter.create_index` passes the full
`index_params` dict per adapter — an earlier refactor had dropped `nlist` /
`default_nprobe`, which created a malformed IVF index that crashed the
cluster on the first insert (`get index info ... INTERNAL`); that is fixed.

Verify the adapter loads:

```
.venv/bin/python -c "import sys; sys.path.insert(0,'benchmark/runners'); from runners.sdk import get_sdk_adapter; print(get_sdk_adapter().sdk_version)"
```

### Step 2 — create_index probe + smoke test

`nlist=32768` is unverified (only `nlist=256` is known-good). Before any long
run, confirm the configured `index_params` create cleanly and survive inserts:

```
.venv/bin/python benchmark/runners/create_index_probe.py \
    > benchmark/reports/raw/create_probe_verify.log 2>&1
```

Exit 0 = create + load + inserts survived the configured `index_params`. If
it fails, fall back to `nlist=256` in `v143.py` before continuing. Then run
the sweep-loop smoke test:

```
.venv/bin/python benchmark/runners/latency_bench.py --direct-envector \
    --primer-rows 100,1000,10000 --runs 5 --warmup 1 --raw-csv /tmp/smoke_v143.csv
```

Confirms the sweep loop completes with no errors before the long run.

### Step 3 — full sweep

```
.venv/bin/python benchmark/runners/latency_bench.py \
    --direct-envector \
    --primer-rows 100,1000,10000,20000,25000,32000,50000,100000 \
    --runs 15 --warmup 3 \
    --report benchmark/reports/latency_sweep_v143_mm32ivf_<date>.md \
    --raw-csv benchmark/reports/raw/latency_sweep_v143_mm32ivf_<date>.csv
```

Grid = realistic org-memory corpus sizes, dense around the crossover.
Centroids ≈ ⌈N/4096⌉; with nprobe=6 the crossover (~24,576) is bracketed by
N=20,000 and N=25,000:

| N | centroids | nprobe=6 probes | regime |
|---|---|---|---|
| 100 | 1 | 1 | flat |
| 1,000 | 1 | 1 | flat |
| 10,000 | 3 | 3 (all) | ≈ flat (pre-crossover) |
| 20,000 | 5 | 5 (all) | ≈ flat (just below crossover) |
| 25,000 | 7 | 6 / 7 | pruning begins (just past crossover) |
| 32,000 | 8 | 6 / 8 | ivf pruning active |
| 50,000 | 13 | 6 / 13 | ivf pruning active |
| 100,000 | 25 | 6 / 25 | ivf pruning active |

12 effective runs/N. Priming inserts N records one-by-one, so N=100,000 has a
long priming phase (~10⁵ sequential insert RPCs) before measurement — expect
the full run to be multi-hour.

> **Caveat** — verify the ivf centroid merge has completed after priming each
> N, before measuring. `_prime_bench_index` waits only for `score` readiness,
> which may not imply merge-complete; measuring an un-merged ivf index would
> understate its optimization. Record the pyenvector build and SDK bug state
> in the report env (see the v143.py `measure_insert_to_searchable`
> docstring).

### Step 4 — comparison report

Merge the two raw CSVs. Per scenario, overlay v1.2.2-flat (measured 0–8192 +
linear extrapolation above) and v1.4.3-ivf (measured) latency-vs-N curves.
Report the crossover N (bracketed by the 20k/25k/32k points), the winner in
each region, and the speed-up factor. State which config each SDK measured
and the two-machine caveat in the environment section. (`searchable` is
measured by different mechanisms per SDK — see v143.py — so compare only
total insert→searchable time there.)

## Parallel execution

Phase 3 (this machine, v1.2.2) and Phase 4 (other machine, v1.4.3) are
independent — they run concurrently, and the comparison (Step 4) merges their
CSVs afterward.
