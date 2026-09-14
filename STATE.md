# Working state

The handoff document. Anyone — or any session — picking this project up should
be able to read this file and know exactly where things stand and what to do
next. Update it whenever a task changes state, not at the end.

Task list: [TASKS.md](TASKS.md). Design rules and inherited findings:
[CLAUDE.md](CLAUDE.md).

## Where things stand

Last updated: 2026-09-13 (Tasks 1-3 done; Tier 2 bench awaiting submission approval)

| # | Task | State |
|---|------|-------|
| 1 | Set up git repo | **done** |
| 2 | Build the zarr-building codebase | **done** — both write strategies validated end to end |
| 3 | Decide the chunking strategy (needs user approval) | **done** — approved by the user 2026-09-13 |
| 4 | Test the codebase for the spatial build | Tier 0 + Tier 1 **passed**; Tier 2 (batch bench) not run |
| 5 | Run the spatial build | not started |
| 6 | Verify the spatial store | not started |
| 7 | Test the codebase for the temporal build | Tier 0 + Tier 1 **passed**; Tier 2 (batch bench) not run |
| 8 | Verify the temporal store | not started |

## Next action

**Run the Tier 2 bench — two batch jobs, awaiting the user's go-ahead to
submit.** Configs are written (`config_zarr_bench_spatial.yaml`,
`config_zarr_bench_temporal.yaml`); each is byte-identical to its production
counterpart except for the store suffix and the log file. The submit commands
are in [TESTING.md](TESTING.md). Delete both bench stores once measured.

Then size the production chain from what they measure, and run Task 5.

## Decisions made

- **2026-09-13, undeclared fill sentinel (V3.16):** MSWEP V3.16 declares
  `_FillValue = -9999`, which **never occurs in the data**. The real sentinel is
  `-239976.0` = `-9999 x 24`, the fill summed over the 24 hourly steps a daily
  total is built from, so upstream aggregated without propagating the mask. It
  marks a solid 150 x 150 block at array indices `[0:150, 0:150]`
  (75.05-89.95 N, 180-165 W, Arctic Ocean near the dateline) on **every day** of
  the record. V2.8.0 has no negative values at all. Because the value in the
  data does not match the attribute, xarray's CF masking leaves it in place, and
  it would have been written to the store as though it were a measurement.
  Handled by the `source_fill_values` config key: declared rather than detected
  (a 'mask anything negative' rule would also hide a defect nobody has noticed),
  checked to actually occur before masking, and recorded in the store's
  `source_fill_values_masked` attribute. Consequence for the temporal store: the
  7 x 7 = 49 tiles lying entirely inside that block are NaN across the whole
  record, so it should hold 16151 chunks rather than 16200.
  **Open to revisit:** V2.8.0 does have data over that block, so backfilling it
  is possible at the cost of mixing two releases in one store.

- **2026-09-13, time axis:** the stores are **reindexed onto the regular
  16,982-step daily axis** (1979-01-01..2025-06-29), with the two days V3.16 is
  missing written as all-NaN, rather than carrying the irregular 16,980-step
  axis the files give. Chosen so `time_coverage_resolution: P1D` is honest and
  downstream date arithmetic cannot silently read the wrong day. See CLAUDE.md
  for the consequences the build and the verifier have to honour — in
  particular, the build must **fail** rather than reindex silently if the gap is
  anything but those two known days.

## Open decisions needing the user

1. **Whether to backfill the 150 x 150 corner block from V2.8.0** (see the fill
   sentinel decision above). Currently masked to NaN. V2.8.0 has real data there,
   but using it mixes two releases in one store.

## Task 3: the approved chunking

Approved by the user 2026-09-13, together with the decision to bench both paths
before building anything at production scale.

| | spatial store | temporal store |
|---|---|---|
| chunk (time, lat, lon) | `(1, 1800, 3600)` | `(16982, 20, 20)` |
| chunk size | 24.7 MiB | 25.9 MiB |
| chunks in the array | 16,980 of 16,982 | 16,151 of 16,200 |
| cheap read | a global map = 1 chunk | a point series = 1 chunk |
| worst-case read | a point series = 16,982 chunks | a global map = 16,200 chunks |
| write strategy | `append`, 100 steps per commit | `region`, 9 blocks of `lat: 200, lon: -1` |
| estimated size | ~86 GiB | ~85 GiB |

Both shapes are carried straight from `data_engineering_gleam`, which is
defensible because the grid is **identical** (1800 x 3600 at 0.1 degree) and
both land in the 24-26 MiB range that is a good zarr chunk. The MSWEP-specific
part is the block shape: `lat: 200` because the v3.16 source chunks are
`[1, 200, 200]` and 1800/200 = 9 exactly, so a block covers whole source
chunks and covers each once.

The temporal store's **9 blocks** is the resume granularity, and with a single
variable there is no second dimension to subdivide along (GLEAM had 126 blocks
across 14 variables). If a 45.5 GiB block does not fit one walltime, the
fallback is `lat: 100` at the cost of reading each source chunk twice. The
Tier 2 bench is what decides whether that is needed.

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
