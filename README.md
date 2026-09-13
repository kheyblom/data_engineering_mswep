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

The build pipeline is written and validated on a one-year fixture, both write
strategies, including interrupt and resume. Nothing has been built at production
scale, and the chunking still needs sign-off.

[STATE.md](STATE.md) is the current working state and the place to pick up from;
[TASKS.md](TASKS.md) is the task list; [TESTING.md](TESTING.md) records what was
validated and what it cost; [CLAUDE.md](CLAUDE.md) records the design rules and
which `data_engineering_gleam` findings transfer here.

## Commands

```bash
uv sync    # create/refresh .venv from uv.lock; Python is pinned >=3.13,<3.14

# build a store; the config decides everything, including which write strategy
uv run python mswep_zarr.py --config config/config_zarr_spatial.yaml
uv run python mswep_zarr.py --config config/config_zarr_temporal.yaml

# the one-year correctness fixture (see TESTING.md for how to stage it)
uv run python mswep_zarr.py --config config/config_zarr_tiny_spatial.yaml
uv run python mswep_zarr.py --config config/config_zarr_tiny_temporal.yaml
```

A run is restartable: relaunching with the same config resumes from the last
commit rather than starting over. Only one writer at a time can hold the
icechunk branch, so chained batch jobs must never overlap --
`submit_mswep_zarr.sh` chains them with `-W depend=afterany:<jobid>`.
