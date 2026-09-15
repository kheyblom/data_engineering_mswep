# Working state

The handoff document. Anyone — or any session — picking this project up should
be able to read this file and know exactly where things stand and what to do
next. Update it whenever a task changes state, not at the end.

Task list: [TASKS.md](TASKS.md). Design rules and inherited findings:
[CLAUDE.md](CLAUDE.md).

## Where things stand

Last updated: 2026-09-15 (all 8 stores migrated onto the data engineering style
guide; **task 1.a done, 1.b next**)

The original eight tasks are complete: 396 checks across the eight stores, **0
failures**, ~630 million cells compared bit-exactly against the raw netCDF
files. A new task list, [TASKS.md](TASKS.md) Task 1, aligns the project with the
data engineering style guide
(`/glade/u/home/kheyblom/work/style_guides/style-guide_data_engineering.md`).

**Task 1.a is done.** All eight stores were migrated in place on 2026-09-15 --
metadata only, no data read, written or moved, no core-hours spent. They now
live under a layout directory and carry the canonical spellings:

```
<download>/<version>/zarr/{spatial,temporal}/mswep.<version>.<period>.day.native_0p1x0p1.precipitation.zarr
```

| store (new path) | days | checks | chunks |
|---|---|---|---|
| `spatial/mswep.v_3_16.past.day...precipitation.zarr` | 16,982 | 49 / 0 fail | 16,980 (2 absent: 1993 gap days) |
| `temporal/mswep.v_3_16.past.day...precipitation.zarr` | 16,982 | 52 / 0 fail | 16,151 (49 absent: corner tiles) |
| `spatial/mswep.v_3_16.nrt.day...precipitation.zarr` | 684 | 47 / 0 fail | 684 |
| `temporal/mswep.v_3_16.nrt.day...precipitation.zarr` | 684 | 51 / 0 fail | 16,200 |
| `spatial/mswep.v_2_8.past.day...precipitation.zarr` | 15,339 | 48 / 0 fail | 15,339 |
| `temporal/mswep.v_2_8.past.day...precipitation.zarr` | 15,339 | 51 / 0 fail | 16,200 |
| `spatial/mswep.v_2_8.nrt.day...precipitation.zarr` | 2,117 | 47 / 0 fail | 2,117 |
| `temporal/mswep.v_2_8.nrt.day...precipitation.zarr` | 2,117 | 51 / 0 fail | 16,200 |

The chunk counts are from the migration's own fingerprint, taken before and
after and again after the directory moved; every one is identical to what the
build wrote, and every absent chunk was traced to raw rather than assumed. Each
store now carries two tags: the original
`<release>-<product>-<layout>-verified-20260914`, which is the rollback point,
and `<release>-<product>-<layout>-nomenclature-20260915` at the migrated tip.

## Next action

**Task 1.b: make the pipeline build into this state directly.** The stores are
migrated; the code that produced them is not. Start with:

1. `config/*.yaml` -- the filename template becomes
   `{suffix}/mswep.{version}.{period}.{frequency}.{grid_name}.{variable}.zarr`,
   with `frequency` and `variable` resolved through `utils/nomenclature.py`
   rather than written by hand. `migrate_nomenclature.MIGRATED_FILENAME` is the
   string to move.
2. `variable_attrs` -- must now set `units`, `long_name` and the `original_*`
   provenance. It deliberately did not set `units` before.
3. The three `attrs` entries whose prose the migration rewrote:
   `cf_compliance`, `chunking`, `related_store`. See the hazard below.
4. `verify_mswep_zarr.py` -- its required-global-attrs tuple, its `LAYOUT_ATTRS`
   map and its commit-count expectation all predate the migration and do not
   know about the new attributes or the extra commit. It was **not** run after
   the migration, by decision; it is the first thing 1.b has to pick up.
5. Delete `migrate_nomenclature.py` once 1.b and 1.c land. It says so itself.

### Hazard: do not run `finalize --attrs --apply` until 1.b updates the configs

The finalizer merges the config's `attrs` section over what the store holds, so
against today's configs it would **revert** exactly three attributes the
migration rewrote -- `cf_compliance`, `chunking` and `related_store` -- back to
prose that names `daily` store paths and claims the units do not parse under
udunits. `--status` reports this as '3 pending'; that is the stale config, not
work left undone. Verified 2026-09-15 by dry run.

Likewise **do not run `--gc --apply`** on any store yet. It is irreversible and
would discard the snapshot the `...-verified-20260914` tag makes the rollback
point.

If the record is refreshed later, the cycle is:

```bash
# 1. rebuild or extend (resumes from the last commit if interrupted)
CONFIG=config/config_zarr_<release>_<product>_<layout>.yaml ./submit_mswep_zarr.sh
# 2. verify -- finalization refuses to write without a passing, complete run
VERIFY=1 CONFIG=<same> ./submit_mswep_zarr.sh
# 3. finalize, in this order, --status first
uv run python finalize_mswep_zarr.py --config <same> --status
uv run python finalize_mswep_zarr.py --config <same> --attrs --apply
uv run python finalize_mswep_zarr.py --config <same> --tag <name> --apply
uv run python finalize_mswep_zarr.py --config <same> --gc --apply   # only if it frees bytes
```

Note `--status` on the four spatial stores still offers `--gc`. That is
correct -- they hold unreachable snapshots from the fork-per-commit behaviour --
but collecting them frees **0.00 GiB**, and it was deliberately not run. Do not
read it as work left undone.

## Decisions made

- **2026-09-15, style guide alignment migrated in place, not rebuilt (task
  1.a).** Every gap between the built stores and the data engineering style
  guide was metadata: `precipitation` is already the canonical variable name,
  float32 is already the published dtype, and `mm/d` -> `mm d-1` is a respelling
  of the same unit, so `nomenclature-key_mswep.md` records the unit conversion
  as `none`. Rebuilding would have spent the whole original build cost to
  produce byte-identical chunks. `migrate_nomenclature.py` instead wrote one
  metadata commit per store and renamed eight directories, on a login node, in
  minutes, for zero core-hours.
  **Verification was by chunk-manifest fingerprint rather than a verifier
  re-run**, chosen by the user: the complete manifest, the chunk storage
  statistics and the array's shape, chunks, dtype and fill value are compared
  before and after the commit and again after the directory moves. That proves
  not one chunk moved, which a sampled re-read of values could not, and it costs
  a metadata walk. All eight passed, and the `verification` attribute each store
  already carried is therefore still true and was left untouched.

- **2026-09-15, two approved deviations from the guide's filename.** The guide
  specifies five components,
  `<data_source>.<version>.<temporal_frequency>.<grid_name>.<variable>.zarr`,
  and MSWEP needs two facts it has no slot for. The **layout** became a parent
  directory, `spatial/` or `temporal/` -- the guide mandates separate spatial
  and temporal stores and then gives the name nowhere to say which is which, and
  a directory matches how `data_engineering_gleam` lays its stores out. The
  **product** kept its own component, `past` or `nrt`, making six. Folding it
  into the version (`v_3_16_past`) or the source (`mswep_past`) would have kept
  five and was rejected: the product is neither a release nor a source, and
  spelling it as one would misdescribe it in a slot downstream tooling reads as
  one. Both approved by the user, both recorded in `nomenclature-key_mswep.md`.

- **2026-09-15, `standard_name` stays `precipitation_flux`.** This diverges from
  `data_engineering_gleam`, whose migration overwrites `standard_name` with the
  canonical variable name. It can do that because GLEAM's canonical names are
  not CF standard names and nothing is lost. `precipitation_flux` *is* a genuine
  CF standard name, so overwriting it with `precipitation` would trade real
  information for a duplicate of the variable name, and the style guide has no
  rule about `standard_name` either way.

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

### 2026-09-15 — Task 1.a, style guide alignment

- Read the data engineering style guide and `nomenclature_data.md`, and the
  uncommitted alignment work in `data_engineering_gleam`
  (`migrate_nomenclature.py`, `utils/nomenclature.py`,
  `nomenclature-key_gleam.md`). That design was lifted rather than re-derived;
  MSWEP's job is smaller because there is no array to rename.
- Wrote `nomenclature-key_mswep.md` in the machine-read shape and
  `utils/nomenclature.py` to parse it, so the key and the stores cannot drift.
- Established that `finalize_mswep_zarr.py --attrs` could not do this job: it
  opens only the root group, so it cannot touch the variable's own attributes,
  and it merges rather than replaces, so it cannot drop the `cf_compliance` text
  that claims the units do not parse.
- Taught `build_history` to ignore the migration's commit message, or
  `date_created` would jump to the migration date on every later `--attrs`.
- Migrated all eight stores, dry run first. Each fingerprint matched either side
  of the commit and again after the rename; a re-run reports 'already migrated'.
- Tagged all eight at the migrated tip through `finalize --tag`, driven by
  rendered configs so the tags were made by the same code path every future tag
  will use.
- Checked all eight end to end, read only: paths, layout directories, variable
  and root attributes, sibling cross-references, dims, chunk shapes, dtype and
  both tags. All pass.

### 2026-09-13 — Task 1

- Read `data_engineering_gleam` end to end (`CLAUDE.md`, configs, `gleam_zarr.py`,
  `utils/`, `submit_gleam_zarr.sh`) and `access_mswep` (`README.md`, configs,
  path/version logic) to establish what transfers.
- Inspected the raw V3.16 and V2.8 netCDF headers, coordinates and per-year
  file counts; found the two missing 1993 days.
- Initialised the repo, wrote `.gitignore`, `pyproject.toml`, `CLAUDE.md` and
  this file; connected the `origin` remote over SSH.
