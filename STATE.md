# Working state

The handoff document. Anyone — or any session — picking this project up should
be able to read this file and know exactly where things stand and what to do
next. Update it whenever a task changes state, not at the end.

Task list: [TASKS.md](TASKS.md). Design rules and inherited findings:
[CLAUDE.md](CLAUDE.md).

## Where things stand

Last updated: 2026-09-14 (all 8 stores built and verified; finalization is what
remains)

**All eight tasks in TASKS.md are complete.** 396 checks across the eight
stores, **0 failures**, ~630 million cells compared bit-exactly against the raw
netCDF files.

| store | days | size | checks | absent chunks |
|---|---|---|---|---|
| `v_3_16.past.spatial` | 16,982 | 87 G | 49 / 0 fail | 2 (1993 gap days) |
| `v_3_16.past.temporal` | 16,982 | 86 G | 52 / 0 fail | 49 (corner tiles) |
| `v_3_16.nrt.spatial` | 684 | 3.5 G | 47 / 0 fail | 0 |
| `v_3_16.nrt.temporal` | 684 | 3.5 G | 51 / 0 fail | 0 |
| `v_2_8.past.spatial` | 15,339 | 53 G | 48 / 0 fail | 0 |
| `v_2_8.past.temporal` | 15,339 | 57 G | 51 / 0 fail | 0 |
| `v_2_8.nrt.spatial` | 2,117 | 13 G | 47 / 0 fail | 0 |
| `v_2_8.nrt.temporal` | 2,117 | 14 G | 51 / 0 fail | 0 |

Every absent chunk was traced to raw rather than assumed.

## Next action

**One decision outstanding: whether to run `--gc --apply`.** Everything else is
done -- all eight stores are built, verified, attributed and tagged.

Garbage collection would reclaim **3.57 GiB, all of it in
`v_2_8.past.temporal`** (600 chunks orphaned by a Tier 2 bench block killed
after writing but before committing). Every other store would reclaim 0.00 GiB;
their unreachable snapshots and manifests are the fork-per-commit behaviour and
free no space.

It is irreversible, and **no second copy of these stores exists** -- scratch is
not backed up. 3.57 GiB is about 1% of the 315 GB collection, against a rebuild
cost of ~14 core-hours if anything went wrong. The dry run has been recorded in
TESTING.md finding 16; the decision is the user's.

If it is run: `--gc --apply` per store, then re-run
`verify_mswep_zarr.py --phases structure,sweep` against each, which is the gate
that proves collection did no harm.

## Decisions made

- **2026-09-13, separate Past and NRT stores (scope: 8 stores).** MSWEP
  publishes each release as two products that are NOT the same estimate: `Past`
  is gauge-corrected, `NRT` is near-real-time. `Past` ends 2025-06-29 (V3.16)
  and 2020-12-30 (V2.8), so a Past-only store silently ends years ago. They are
  kept as **separate stores, not merged**: across V2.8's 34-day overlap they
  correlate 0.94-0.96 with RMSE ~1.9 mm/day and only ~15% of cells identical, so
  merging would bury that discontinuity inside one array. Every store carries a
  `product_caveat` attribute naming which to prefer where they overlap (always
  Past). Scope is therefore {V3.16, V2.8} x {past, nrt} x {spatial, temporal}.

- **2026-09-13, temporal tile stays 20 x 20 for every store.** Uniform tiling
  so a point time series is always the same 2 x 2 degree tile and stores are
  directly comparable. Accepted consequence: the chunk is 25.9 MiB for V3.16
  Past but only 1.0 MiB for V3.16 NRT and 3.2 MiB for V2.8 NRT, and every
  temporal store holds ~16,200 chunks regardless of record length.

- **2026-09-13, undeclared fill sentinel (V3.16 Past only):** MSWEP V3.16 declares
  `_FillValue = -9999`, which **never occurs in the data**. The real sentinel is
  `-239976.0` = `-9999 x 24`, the fill summed over the 24 hourly steps a daily
  total is built from, so upstream aggregated without propagating the mask. It
  marks a solid 150 x 150 block at array indices `[0:150, 0:150]`
  (75.05-89.95 N, 180-165 W, Arctic Ocean near the dateline) on **every day** of
  the record. V2.8 has no negative values at all. Because the value in the
  data does not match the attribute, xarray's CF masking leaves it in place, and
  it would have been written to the store as though it were a measurement.
  Handled by the `source_fill_values` config key: declared rather than detected
  (a 'mask anything negative' rule would also hide a defect nobody has noticed),
  checked to actually occur before masking, and recorded in the store's
  `source_fill_values_masked` attribute. Consequence for the temporal store: the
  7 x 7 = 49 tiles lying entirely inside that block are NaN across the whole
  record, so it should hold 16151 chunks rather than 16200.
  **Verified upstream 2026-09-13** by reading the raw netCDF with `ncks` (no
  Python/xarray/decoding) and by re-downloading a file fresh from the GloH2O
  Drive, which came back byte-identical (md5 `e7e089885ee84e53f3c32404f85a4a32`)
  and holds the same values. The defect is in GloH2O's published product.
  **Confirmed by the user 2026-09-13 after that verification: keep masking to
  NaN.** Storing the raw `-239976` exactly was considered and rejected -- it is
  `-9999 x 24`, not a measurement, so it would silently destroy any spatial
  aggregate touching the Arctic corner. Backfilling from V2.8 was also
  rejected: it correlates only 0.63-0.89 with V3.16 next to the hole, runs
  1.5-3x wetter, and ends in 2020 so it could not cover ~1,642 days anyway.

- **2026-09-13, time axis:** the stores are **reindexed onto the regular
  16,982-step daily axis** (1979-01-01..2025-06-29), with the two days V3.16 is
  missing written as all-NaN, rather than carrying the irregular 16,980-step
  axis the files give. Chosen so `time_coverage_resolution: P1D` is honest and
  downstream date arithmetic cannot silently read the wrong day. See CLAUDE.md
  for the consequences the build and the verifier have to honour — in
  particular, the build must **fail** rather than reindex silently if the gap is
  anything but those two known days.

## Open decisions needing the user

1. ~~Promote the bench stores?~~ **Decided 2026-09-13: promoted.** Original
   question kept for the reasoning:
   `bench_spatial` for V3.16 Past and V2.8 Past are complete and verified
   bit-exact. Renaming a store directory is safe (tested: an icechunk store
   opens fine at a new path with commits and attrs intact), and their attributes
   already name the *production* siblings, since the templates hard-code
   `spatial`/`temporal` rather than the suffix. Promoting saves **~9 core-hours
   and ~2.3 h of wall time**; rebuilding is the plan as written and keeps the
   "bench stores are throwaway" rule clean.

2. ~~Region build chaining?~~ **Decided 2026-09-13: chain two 6 h develop jobs
   per region store** with `AFTER=<jobid>`. The second is cheap insurance -- if
   the first finishes, the second pays only the ~20 min cold open before finding
   the store complete and exiting.

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
- V2.8 also present: 15,339 files, 51.84 GB. Both file counts are the
  `past/daily` product; an `nrt/daily` product is also on disk under each
  version and is not read by any config here.
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
- Inspected the raw V3.16 and V2.8 netCDF headers, coordinates and per-year
  file counts; found the two missing 1993 days.
- Initialised the repo, wrote `.gitignore`, `pyproject.toml`, `CLAUDE.md` and
  this file; connected the `origin` remote over SSH.
