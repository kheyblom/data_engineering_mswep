# data_engineering_mswep

Convert the raw MSWEP daily precipitation netCDF files on GLADE into
icechunk-backed zarr stores: one chunked for reading global maps, one chunked
for reading point time series, both built from the raw files and verified
against them independently.

Input comes from
[access_mswep](/glade/u/home/kheyblom/work/data_access/access_mswep), which
lands the record as

```
/glade/derecho/scratch/$USER/data/mswep/v_3_16/raw/past/daily/<year>/YYYYDOY.nc
```

Structured like
[data_engineering_gleam](/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam):
one build script driven by one YAML config per store, a thin `utils/` layer, a
read-only verifier, and a separate script for the mutating finalization steps.

## The stores

MSWEP publishes each release as two products that are **not** the same estimate:
`Past` is the gauge-corrected reanalysis, `NRT` the near-real-time stream, and
`Past` stops well short of the present in both releases. They are kept as
separate stores rather than merged. Each product gets a spatial and a temporal
layout, so eight stores in all:

| store | record | days | best for |
|---|---|---|---|
| `spatial/mswep.v_3_16.past.day.native_0p1x0p1.precipitation.zarr` | 1979-01-01 .. 2025-06-29 | 16,982 | maps, fields |
| `temporal/mswep.v_3_16.past.day.native_0p1x0p1.precipitation.zarr` | " | " | point time series |
| `spatial/mswep.v_3_16.nrt.day.native_0p1x0p1.precipitation.zarr` | 2024-10-30 .. 2026-09-13 | 684 | maps, fields |
| `temporal/mswep.v_3_16.nrt.day.native_0p1x0p1.precipitation.zarr` | " | " | point time series |
| `spatial/mswep.v_2_8.past.day.native_0p1x0p1.precipitation.zarr` | 1979-01-02 .. 2020-12-30 | 15,339 | maps, fields |
| `temporal/mswep.v_2_8.past.day.native_0p1x0p1.precipitation.zarr` | " | " | point time series |
| `spatial/mswep.v_2_8.nrt.day.native_0p1x0p1.precipitation.zarr` | 2020-11-27 .. 2026-09-13 | 2,117 | maps, fields |
| `temporal/mswep.v_2_8.nrt.day.native_0p1x0p1.precipitation.zarr` | " | " | point time series |

They live under `<download>/<version>/zarr/`. **Prefer a Past store wherever it
covers the day you want** — every store's `product_caveat` attribute says so,
and the NRT stores say plainly that they are not gauge-corrected.

The names follow the data engineering style guide: the variable in the name, the
canonical frequency token `day`, and the layout as the directory above.
`nomenclature-key_mswep.md` records that mapping, the two approved deviations
from the guide's filename, and the fact that respelling the units `mm/d` ->
`mm d-1` changed no value.

## Status

**All eight stores are built, verified and finalized** (2026-09-14, 312 GB
total). Every one has the expected axis length, a strictly regular daily
spacing, the intended chunking, and values bit-exact against the raw netCDF.

`verify_mswep_zarr.py` ran against all eight: **396 checks, 0 failures**, ~630
million cells compared against the raw files. Each store then took its
provenance and discovery attributes, and an immutable tag
`<release>-<product>-<layout>-verified-20260914`. A store carries a
`verification` attribute only because the verifier actually passed on it.

**All eight were then migrated onto the data engineering style guide**
(2026-09-15, `migrate_nomenclature.py`): renamed, units respelled `mm d-1`, and
the nomenclature and frequency attributes added. Metadata only -- the chunk
manifest, the storage statistics and every array's shape, chunks, dtype and fill
value are identical either side, so the verification above still stands and was
not re-run. Each store carries a second tag,
`<release>-<product>-<layout>-nomenclature-20260915`, at the migrated tip; the
first tag is the rollback point. The stores were **not** rebuilt: the pipeline
was refactored separately (Task 1.b) so that a build from scratch lands in the
same state, checked attribute by attribute against all eight. The refactored
code was then tested end to end from raw on the one-year fixture, on both write
paths: **60 and 64 checks, 0 failures**, and identical shape, chunking, dtype and
chunk occupancy against fixtures built by the pre-refactor code. See
[TESTING.md](TESTING.md) and [STATE.md](STATE.md).

Garbage collection ran on `v_2_8.past.temporal` alone -- the only store holding
real orphans, 600 chunks left by a walltime-killed bench block, 3.57 GiB. The
other seven hold only icechunk's per-commit snapshot forks, which `--gc` reports
but which reclaim **0.00 GiB**, so it was deliberately not run there. See
[TESTING.md](TESTING.md).

[STATE.md](STATE.md) is the current working state and the place to pick up from;
[TASKS.md](TASKS.md) is the task list; [TESTING.md](TESTING.md) records what was
validated and what it cost; [CLAUDE.md](CLAUDE.md) records the design rules and
which `data_engineering_gleam` findings transfer here.

## Commands

```bash
uv sync    # create/refresh .venv from uv.lock; Python is pinned >=3.13,<3.14

# build a store; the config decides everything, including which write strategy.
# One config per store -- config/config_zarr_<release>_<product>_<layout>.yaml
uv run python mswep_zarr.py --config config/config_zarr_v3_16_past_spatial.yaml
uv run python mswep_zarr.py --config config/config_zarr_v3_16_nrt_temporal.yaml
uv run python mswep_zarr.py --config config/config_zarr_v2_8_past_spatial.yaml   # ...and so on

# the one-year correctness fixture (see TESTING.md for how to stage it)
uv run python mswep_zarr.py --config config/config_zarr_tiny_spatial.yaml
uv run python mswep_zarr.py --config config/config_zarr_tiny_temporal.yaml
```

Production builds go through the batch queue. `CONFIG` is **required** --
there is no default, because a wrong one would silently start building the
wrong store:

```bash
QUEUE=develop NCPUS=4 MEM=16GB WALLTIME=02:00:00 \
    CONFIG=config/config_zarr_v3_16_nrt_spatial.yaml ./submit_mswep_zarr.sh

# chain a resume behind a running job so the two never write the store at once
AFTER=<jobid> CONFIG=<same config> ./submit_mswep_zarr.sh
```

A run is restartable: relaunching with the same config resumes from the last
commit rather than starting over. Only one writer at a time can hold the
icechunk branch, so chained batch jobs must never overlap --
`submit_mswep_zarr.sh` chains them with `-W depend=afterany:<jobid>`.
