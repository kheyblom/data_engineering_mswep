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

## Status

The pipeline is not built yet. [STATE.md](STATE.md) is the current working
state and the place to pick up from; [TASKS.md](TASKS.md) is the task list;
[CLAUDE.md](CLAUDE.md) records the design rules and which
`data_engineering_gleam` findings transfer here.

## Setup

```bash
uv sync    # create/refresh .venv from uv.lock; Python is pinned >=3.13,<3.14
```
