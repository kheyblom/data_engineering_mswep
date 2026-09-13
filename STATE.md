# Working state

The handoff document. Anyone — or any session — picking this project up should
be able to read this file and know exactly where things stand and what to do
next. Update it whenever a task changes state, not at the end.

Task list: [TASKS.md](TASKS.md). Design rules and inherited findings:
[CLAUDE.md](CLAUDE.md).

## Where things stand

Last updated: 2026-09-13

| # | Task | State |
|---|------|-------|
| 1 | Set up git repo | **done** |
| 2 | Build the zarr-building codebase | not started |
| 3 | Decide the chunking strategy (needs user approval) | not started |
| 4 | Test the codebase for the spatial build | not started |
| 5 | Run the spatial build | not started |
| 6 | Verify the spatial store | not started |
| 7 | Test the codebase for the temporal build | not started |
| 8 | Verify the temporal store | not started |

## Next action

Start Task 2 — build the zarr-building codebase.

## Decisions made

- **2026-09-13, time axis:** the stores are **reindexed onto the regular
  16,982-step daily axis** (1979-01-01..2025-06-29), with the two days V3.16 is
  missing written as all-NaN, rather than carrying the irregular 16,980-step
  axis the files give. Chosen so `time_coverage_resolution: P1D` is honest and
  downstream date arithmetic cannot silently read the wrong day. See CLAUDE.md
  for the consequences the build and the verifier have to honour — in
  particular, the build must **fail** rather than reindex silently if the gap is
  anything but those two known days.

## Open decisions needing the user

1. **Chunking for both stores** — Task 3, explicitly flagged in TASKS.md as
   needing approval. The GLEAM grid is identical (1800 x 3600 at 0.1 degree), so
   GLEAM's `(1, 1800, 3600)` spatial and `(record, 20, 20)` temporal shapes are
   the obvious starting proposal, but the block shape and the write cost differ
   here (one file per day, 200x200 source chunks) and should be measured.

## Facts established so far

Measured 2026-09-13; see CLAUDE.md for the full input-data section.

- Raw tree: `/glade/derecho/scratch/kheyblom/data/mswep/v_3_16/raw/past/daily/<year>/YYYYDOY.nc`
- V3.16: 16,980 files, 81.67 GB on disk, download verified complete against the
  remote listing on 2026-09-13 14:49.
- V2.8.0 also present: 15,339 files, 51.84 GB.
- One variable, `precipitation`, float32, `_FillValue = -9999`, `mm/d`.
- Grid 1800 lat x 3600 lon at 0.1 degree, identical to GLEAM's.
- Source chunking `[1, 200, 200]` (v3.16), deflate 1 + shuffle.
- Full record uncompressed: 410 GiB.
- Days present: 1979-01-01 through 2025-06-29, less the two 1993 days above.

## Work log

### 2026-09-13 — Task 1

- Read `data_engineering_gleam` end to end (`CLAUDE.md`, configs, `gleam_zarr.py`,
  `utils/`, `submit_gleam_zarr.sh`) and `access_mswep` (`README.md`, configs,
  path/version logic) to establish what transfers.
- Inspected the raw V3.16 and V2.8.0 netCDF headers, coordinates and per-year
  file counts; found the two missing 1993 days.
- Initialised the repo, wrote `.gitignore`, `pyproject.toml`, `CLAUDE.md` and
  this file; connected the `origin` remote over SSH.
