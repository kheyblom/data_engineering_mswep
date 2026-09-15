# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project

Convert the raw MSWEP precipitation netCDF files downloaded by
[access_mswep](/glade/u/home/kheyblom/work/data_access/access_mswep) into
icechunk-backed zarr stores on NCAR Derecho/GLADE.

**Eight stores**, one pair per raw product: {V3.16, V2.8} x {past, nrt} x
{spatial, temporal}. Each pair is built from the same raw files and differs only
in chunking, and so in which read is cheap: a **spatial** store (one global map
per chunk, for reading fields) and a **temporal** store (one 2 x 2 degree tile
through the whole record per chunk, for reading point time series).

**Nothing here is derived from anything else.** Every store is built from the
raw netCDF files and verified against them independently, so nothing about one
store has to be trusted to trust another. That is a deliberate choice carried
over from `data_engineering_gleam`: do not "optimise" a temporal build into a
rechunk of its spatial sibling, and do not merge Past with NRT.

One config drives one store. The store sits in a directory named for its
layout, and its name carries the release, the product and the variable:

```
{suffix}/mswep.{version}.{period}.{frequency}.{grid_name}.{variable}.zarr
spatial/mswep.v_3_16.past.day.native_0p1x0p1.precipitation.zarr
```

That spelling comes from the data engineering style guide -- see the section
below, and do not change it without reading that section first.

**This project is modelled directly on
[data_engineering_gleam](/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam).**
Read that repo's `CLAUDE.md`, `README.md` and `TESTING.md` before designing
anything here. Most of the hard-won findings transfer; the section below records
which ones do not, and why.

Working state, and where to pick up after a lost connection or a new session,
is in [STATE.md](STATE.md). Keep it current — it is the handoff document.
The task list the project is working through is [TASKS.md](TASKS.md).

## Nomenclature, units and store naming

Governed by
[style-guide_data_engineering.md](/glade/u/home/kheyblom/work/style_guides/style-guide_data_engineering.md)
and the authoritative
[nomenclature_data.md](/glade/u/home/kheyblom/work/style_guides/nomenclature_data.md).
The project's own mapping onto them is `nomenclature-key_mswep.md` at the repo
root, and **that file is machine read**: `utils/nomenclature.py` parses its two
tables and is the only place the pipeline learns what a variable is called.
Edit the tables, not the code. Never restate the mapping in Python -- a dict
here and a table there are two sources of truth for the one thing the guide
exists to make single.

What the guide costs this project, all of it settled 2026-09-15:

- **`precipitation` needs no rename.** It is already the canonical name, which
  is why there is no array-rename step here and there is one in
  `data_engineering_gleam`.
- **Units are `mm d-1`, not `mm/d`.** The same unit, respelled so udunits can
  parse it; the `unit_conversion` in the key is `none` and **no value is ever
  changed**. Note the four raw products do not agree upstream -- V2.8 Past
  already publishes `mm d-1` and the other three publish `mm/d` -- so the
  `original_units` attribute is read off the data, never from the key's single
  column.
- **The frequency token is `day`, not `daily`.** `daily` is GloH2O's spelling
  and stays in the raw directory path (`raw/past/daily/`) and in prose
  describing the source. `day` is what goes in the store name and in the
  `temporal_frequency` attribute, with `original_temporal_frequency: daily`
  beside it. Same split `format_version` already makes between `V3.16` in the
  config and `v_3_16` in the name.
- **`standard_name` stays `precipitation_flux`**, diverging from GLEAM
  deliberately: it is a genuine CF standard name, so overwriting it with the
  canonical variable name would lose information. The guide says nothing about
  `standard_name`.
- **Two approved deviations from the guide's five component filename**: the
  layout is a parent directory, and the product keeps its own component. Both
  are written up in `nomenclature-key_mswep.md` with the alternatives that were
  rejected. The guide requires any deviation to be approved; these were, on
  2026-09-15. Do not add a third without asking.

### Migrating a built store rather than rebuilding it

`migrate_nomenclature.py` did this once, for the eight stores built before the
guide, and is **temporary** -- delete it once the pipeline refactor and its
tests land. Three findings from it are worth keeping:

- **`finalize_mswep_zarr.py --attrs` cannot migrate a store.** It opens only the
  root group, so it never touches a variable's own attributes, and it merges
  rather than replaces, so it can add and overwrite but never drop. Anything
  that has to change a variable attribute or remove a root one needs its own
  code.
- **A migration commit must not look like a build commit.** `build_history`
  derives `date_created` and the commit count from the commits that are not
  finalization, so an unrecognised message would push `date_created` forward on
  every later `--attrs` run. `NON_BUILD_MESSAGES` in `finalize_mswep_zarr.py` is
  where that is handled, and it has to outlive the migration script because the
  commits outlive it.
- **The right proof for a metadata-only change is the chunk manifest, not a
  re-read.** Comparing the complete manifest, the chunk storage statistics and
  the array's shape, chunks, dtype and fill value either side proves not one
  chunk moved, which no amount of sampling values could, and it costs a metadata
  walk instead of core-hours. Renaming the directory is checked the same way,
  against the repository reopened at its new path.

## Working on this system

The user's global instructions (`~/.claude/CLAUDE.md`) govern and are not
repeated in full here. The two that bite hardest in this project:

- **Never recurse or search from a shared root.** The raw tree is ~17,000 files
  under `/glade/derecho/scratch/$USER/data/mswep/`. Scope every traversal to an
  exact subpath and bound the depth. Prefer a known path or a config-derived
  glob over a search.
- **Schedule heavy work; do not run it on a login node.** Show the user any
  `qcmd`/`qsub` command or job script and wait for approval before submitting —
  jobs cost core-hours against a finite allocation. Minimising compute cost is
  an explicit project requirement, not a nicety: measure on a tiny staged
  subset first, then a single-variable-scale bench, then production.

## The input data

Measured 2026-09-13 from the downloaded tree; re-measure rather than trust these
numbers if the download is ever refreshed.

```
/glade/derecho/scratch/kheyblom/data/mswep/<version>/raw/<product>/<year>/YYYYDOY.nc
                                           v_3_16     past/daily     1979   1979001.nc
```

MSWEP publishes each release as two products, and **they are not the same
data**. `Past` is the gauge-corrected reanalysis; `NRT` is the near-real-time
stream. `Past` stops well short of the present in both releases and `NRT`
carries the record forward, so a Past-only store silently ends years ago.

**Four raw products are on disk, and each gets its own pair of stores.**

| product | files | span | gaps | source chunks | lsd | units | time dtype | corner defect |
|---|---|---|---|---|---|---|---|---|
| `v_3_16/past` | 16,980 | 1979-01-01 .. 2025-06-29 | **2** | `[1,200,200]` | 2 | `mm/d` | float32 | **yes** |
| `v_3_16/nrt` | 684 | 2024-10-30 .. 2026-09-13 | 0 | `[1,200,200]` | 2 | `mm/d` | float32 | no |
| `v_2_8/past` | 15,339 | 1979-01-02 .. 2020-12-30 | 0 | `[1,32,32]` | 1 | `mm d-1` | int32 | no |
| `v_2_8/nrt` | 2,117 | 2020-11-27 .. 2026-09-13 | 0 | `[1,32,32]`→`[1,200,200]` | 2 | `mm/d` | float32 | no |

- The version is spelled `V3.16` in the config and `v_3_16` on disk, and the
  product `Past/Daily` becomes `past/daily`. `access_mswep`'s `format_version`
  and `format_product` are the reference implementations of both translations;
  keep the two projects' spellings identical or a config cannot drive both.
  Note an MSWEP version is dot-separated numbers with no letter suffix and no
  fixed part count (`V3.16`, `V2.8`), so this is not GLEAM's `v4.3a` regex.
- **One file per day**, not per year, and **one variable**, `precipitation`.
  Both differ from GLEAM and both change the cost model — see below.
- Grid: `lat` 1800 (89.95 N to -89.95 S, north-to-south), `lon` 3600
  (-179.95 to 179.95), 0.1 degree, float32, identical in all four products.
  **Identical to the GLEAM grid**, so GLEAM's spatial chunk reasoning transfers.
- `precipitation` carries no `standard_name` and no `long_name`; both are
  supplied through each config's `variable_attrs`.
- `_FillValue = -9999.f` is declared by all four and **used by none** — see the
  V3.16 Past section below for what is actually written instead.

### Past and NRT are separate stores, by decision (2026-09-13)

They are **not** merged, and the temporal stores are not derived from the
spatial ones. Measured across V2.8's 34-day Past/NRT overlap, the two products
correlate 0.94-0.96 with RMSE ~1.9 mm/day, bias under 0.1 mm/day, and only ~15%
of cells identical. Merging would bury that discontinuity inside one array;
separate stores let a consumer choose the corrected record or the current one,
and every store's `product_caveat` attribute says which to prefer where they
overlap (always `Past`).

Overlap, where both products of a release cover the same day: V3.16 = 243 days
(2024-10-30 .. 2025-06-29), V2.8 = 34 days (2020-11-27 .. 2020-12-30). Nothing
has to be resolved — both stores simply cover those days.

### V2.8 is chunked `[1, 32, 32]`, and that changes what a read costs

This is the single most important difference between the two releases, and the
reason a V3.16 measurement does not predict a V2.8 one:

- One timestep is **57 x 113 = 6,441** source chunks of 4 KiB, against
  **9 x 18 = 162** chunks of 160 KiB in V3.16.
- A `(1, 200, 3600)` region block read touches ~791 source chunks against 18 —
  roughly **44x more chunk operations for the same bytes**.
- `DEFAULT_CHUNK_CACHE_SLOTS` is 2003, far below 6,441, so HDF5 would evict
  chunks it is about to need. **The V2.8 configs set `chunk_cache_slots: 8009`**
  (the next convenient prime above 6,441) and `chunk_cache_size_mib: 64`
  (a decompressed plane is 6,441 x 4 KiB = 25.8 MiB).
- `block_shape.lat: 200` cannot align with a 32-row source grid — 1800/32 =
  56.25, so the source grid does not tile the axis evenly and **no** block size
  aligns exactly. 200 straddles at most one row of source chunks per edge, which
  is the cheapest misalignment available.
- `v_2_8/nrt` changes chunking partway through its record (`[1,32,32]` early,
  `[1,200,200]` late). Correctness is unaffected — dask chunks come from the
  explicit `chunks=` argument, not from the file — but throughput varies.

### The `int32` time in V2.8 Past is real, and harmless

`ncdump` reports `int time(time)` for V2.8 Past and `float time(time)` for the
other three; a scan of 84 files across all 42 years found int32 in every one.
GloH2O changed its writer after V2.8 Past. It does not affect the build: the
values are whole-day offsets so int32 holds them exactly, float32 is also exact
at these magnitudes (integers exact to 2^24 = 16,777,216 against a largest
offset of 46,265), and `build_encoding` pins every store to int64
`days since 1900-01-01`, proleptic_gregorian regardless. Recorded so the next
person who notices it does not have to re-check.

### The declared fill value is not the one in the data (V3.16 **Past** only)

`precipitation:_FillValue = -9999.f`, and that value **occurs nowhere in the
V3.16 Past record**. V3.16 NRT and both V2.8 products are clean — they carry no
negative values at all — so `source_fill_values` is set on **exactly one of the
eight configs**. The sentinel actually written is **`-239976.0`**, which is
`-9999 x 24` — the fill summed over the 24 hourly steps a daily total is
aggregated from, so upstream did not propagate the mask through the
aggregation. Because the value in the data does not match the attribute,
xarray's CF masking leaves it untouched and it decodes as a precipitation
measurement four orders of magnitude below zero.

It covers a solid **150 x 150 block at array indices `[0:150, 0:150]`** — the
grid's top-left corner, 75.05-89.95 N and 180-165 W, Arctic Ocean near the
dateline — and is byte-identical on every day sampled across 1979-2025. It is
the *only* negative value in the files. V2.8 has no negative values at all and
does have data over that block, so this is a V3.16 production defect rather than
a property of MSWEP.

Handled by the `source_fill_values` config key, which lists values to mask to
NaN on top of the declared `_FillValue`. Three rules hold it together:

- **Declared, not detected.** A rule like "mask anything negative" would also
  hide a defect nobody has noticed yet; a named value fails loudly when the next
  release changes it. Same philosophy as `expected_missing_times`.
- **`check_source_fill` runs first** and fails if a declared sentinel does not
  occur, so a typo or a release that fixed the bug cannot leave the config
  claiming a masking it never applied.
- The build records what it actually masked in the store's
  `source_fill_values_masked` attribute, rather than leaving the claim to the
  hand-written `known_data_gaps` prose.

Consequence for the **temporal** store: the 7 x 7 = 49 tiles lying entirely
inside `[0:150, 0:150]` are NaN across the whole record, so zarr writes no chunk
for them and the array should hold **16151 chunks rather than 16200**. The
spatial store is unaffected — every plane still has data outside the corner.

Whether any *other* cell carries the sentinel is a full-coverage question and
belongs to the verifier, not the build, which only checks the first timestep.

**Verified upstream, 2026-09-13, three independent ways** — do not re-litigate
this without new evidence:

1. `ncks` (NCO's C binary) reading the downloaded netCDF directly, with no
   Python, no xarray and no CF decoding, returns `-239976` at `[0:150, 0:150]`
   and `0` at index 150 in both lat and lon. The boundary is exact.
2. `2005100.nc` was re-downloaded fresh from the GloH2O Drive and is
   **byte-identical** to the stored copy (md5
   `e7e089885ee84e53f3c32404f85a4a32`); reading that fresh file shows the same
   values. So the defect is in GloH2O's published product, not in the transfer.
3. V2.8 holds ordinary values at the identical cells (1.3125, 0, 0.5625 on
   the days checked), which is what makes this V3.16-specific.

**Decided 2026-09-13 by the user, after that verification: keep masking to
NaN.** The alternative considered and rejected was storing `-239976.0` exactly
as raw and documenting it. The deciding argument is that `-239976` is not a
measurement but `-9999 x 24`, so a stored sentinel silently destroys any
spatial mean, sum or area-weighted aggregate whose domain touches the Arctic
corner — a global mean for one day comes out near `-831 mm/day` — whereas NaN
propagates as an explicit absence. The masking is recorded in the store's
`source_fill_values_masked` attribute so it is never invisible.

**Backfilling the block from V2.8 was also considered and rejected.** V2.8
does have data there, but it is a different product: compared against V3.16 in
the band immediately adjacent to the hole it correlates only 0.63-0.89, is
systematically 1.5-3x wetter (bias -0.47 to -1.02 mm/day), and its RMSE against
V3.16 is the same magnitude as the signal. Splicing it in would insert a
15 x 15 degree rectangle of wet-biased values with a hard seam that looks like
real data. V2.8 also ends 2020-12-30 against V3.16's 2025-06-29, so ~1,642 of
16,982 days could not be backfilled at all.

### Known gap in V3.16 Past

`1993241.nc` (1993-08-29) and `1993243.nc` (1993-08-31) **do not exist** in the
V3.16 remote. The download verified 16,980/16,980 against the remote listing, so
this is upstream, not a download failure; V2.8 has a complete 365 days for
1993. Every other year is complete (365/366, and 180 days for the partial 2025).

A complete daily axis 1979-01-01..2025-06-29 would be 16,982 steps; there are
16,980 files.

**Decided 2026-09-13 by the user: reindex onto the regular 16,982-step daily
axis, writing the two absent days as all-NaN.** The store's time axis is
therefore strictly 1-day spaced and `time_coverage_resolution: P1D` is honest,
so downstream date arithmetic cannot silently read the wrong day. Consequences
to hold on to:

- The reindex is a step in the build, applied to the lazy dataset after the
  files are concatenated and before anything is written. The expected axis is
  derived from the first and last dates on disk, not hardcoded.
- **The build must fail rather than reindex silently** if the gap is anything
  other than the two known 1993 days — a quiet reindex is exactly how an
  incomplete download turns into a NaN-filled store. This is the MSWEP
  equivalent of GLEAM's cross-variable `join='exact'` check, which does not
  exist here because there is only one variable.
- In the **spatial** store those two days are entirely fill, so zarr writes no
  chunk for them: expect **16,980 chunks against 16,982 timesteps**, and that is
  correct. This is precisely the GLEAM `E`-variable situation and the verifier
  must trace such holes back to raw rather than treating them as failures.
- In the **temporal** store they fall inside chunks that also hold valid days,
  so they leave no hole at all — again as in GLEAM.
- The store carries a `known_data_gaps` attribute recording both dates and
  naming them as upstream, the way the GLEAM stores record the 25 all-fill `E`
  days.

## What carries over from data_engineering_gleam

These were established there by measurement. Unless marked otherwise they apply
here unchanged, and re-deriving them costs core-hours that have already been
spent once.

### Applies unchanged

- **Python is capped below 3.14.** On 3.14 numpy's temporary-elision
  optimisation segfaults on `condition |= data == fill_value` in
  `xarray/coding/variables.py` — the CF masking every read of a full lat/lon
  slab goes through. Same numpy and xarray are fine on 3.13. MSWEP files are CF
  masked the same way (`_FillValue = -9999`), so the cap stands. Do not raise it
  without re-running the tiny config.
- **`parallel=True` is deliberately omitted from `open_mfdataset`.** Opening
  netCDF from several threads crashes the HDF5 library in this build.
- **The netCDF encoding xarray carries over is rejected by the zarr backend**,
  so encoding is cleared off every variable and rebuilt rather than overridden.
- **1-D index coordinates get a single chunk in the encoding.** Without that
  they inherit the chunking of the data they index — at `lat: 20` the 1800-long
  `lat` coordinate would be split into 90 chunks.
- **`chunks: -1` means the whole dimension**, resolvable only once the files are
  open.
- **Zarr does not write a chunk whose cells are all fill**, so a variable can
  hold fewer chunks than the grid has and still be correct. A short chunk count
  is not by itself a reason to rebuild — trace it back to raw.
- **Every write through `to_icechunk` forks the session**, and the fork's
  snapshot is not in the branch's ancestry, so a store accumulates one
  unreachable snapshot per commit as a matter of course. That is normal, not
  damage. `garbage_collect` alone cleans it up; `expire_snapshots` is not
  needed and reclaims essentially nothing.
- **The two write strategies, and the rule that the strategy must match the
  chunking.** `append` for a store chunked shallowly along time; `region` for
  one chunked along the whole time axis. Both mismatches fail *quietly* — an
  `append` build of a whole-time-axis store collapses into one uninterruptible
  commit that a walltime kill loses entirely — so the pairing is validated
  rather than trusted.
- **Region-path invariants:** a block must be a whole number of output chunks;
  `block_shape.lon` must be -1 (a narrower block reads a strided subset of the
  source chunks rather than contiguous runs, which measured pathologically slow
  for the same volume); a block is read into a preallocated buffer a time chunk
  at a time rather than with `.load()`, which transiently doubles the block as
  dask concatenates it; the block is released before the next is read; the
  index coordinates naming the region are dropped from what is written; resume
  state is parsed back out of the commit messages.
- **`create_skeleton` rechunks time to -1 before `to_zarr(compute=False)`.** No
  data is written, but xarray validates chunk alignment anyway and refuses a
  store chunk straddling several dask chunks — which the per-file time chunks
  `open_mfdataset` leaves behind always do.
- **The verifier stays read-only.** It is what proves a finalization step did no
  harm, and a tool that both acts and audits answers two questions with one exit
  status. Mutation lives in a separate `finalize_*` script.
- **The finalization write order `--attrs`, then `--tag`, then `--gc` is
  load-bearing.** Tags are immutable, so one created before the attributes exist
  permanently names a store that does not describe itself. `--gc --apply` is
  irreversible: dry-run first, re-verify after.
- **A store must not carry a `verification` attribute it has not earned** — it
  is added only after the verifier has actually run against that store.
- **Batch job conventions** (see `submit_gleam_zarr.sh`): resolve `PBS_ACCOUNT`
  in the wrapper rather than in a `#PBS` line, which cannot expand a variable;
  count cpus from `os.sched_getaffinity(0)`, not `$NCPUS` (a shared develop node
  reports 1 whatever it granted) or `nproc` (reports `OMP_NUM_THREADS`);
  `OMP_NUM_THREADS=1` so a threaded BLAS does not oversubscribe dask;
  `MALLOC_TRIM_THRESHOLD_=0`; `TMPDIR` on scratch; chain resumes with
  `-W depend=afterany:<jobid>`, **afterany not afterok**, because a job stopped
  by walltime exits non-zero and that is exactly the case the next job exists to
  resume. Only one writer at a time can hold the icechunk branch, so chained
  jobs must never overlap.

### Differs here — do not carry the GLEAM number across

- **`chunk_cache_size_mib` is not the dominant cost it was.** GLEAM's raw files
  are chunked `[12, 57, 113]`, twelve timesteps deep, while an append build
  writes one timestep at a time — so without a cache holding a whole 12-deep row
  each source chunk was decompressed twelve times, and fixing it measured 7.6x
  end to end. **MSWEP source chunks are one timestep deep**: `[1, 200, 200]` in
  v3.16 and `[1, 32, 32]` in v2.8. There is nothing to re-decompress, so the
  large chunk cache buys little. Size it and confirm by measurement rather than
  copying 512. What *does* matter for v2.8 is `chunk_cache_slots`, not the cache
  size — see the `[1, 32, 32]` section above.
- **There is one variable, not fourteen.** So: no cross-variable merge and no
  `join='exact'` time-axis check between variables (the equivalent completeness
  check has to be against the expected *date* range instead — see the 1993 gap);
  no variable-major block ordering; the region build has roughly 1/14 the blocks
  GLEAM's did, which makes resume granularity coarser and worth thinking about.
  It also makes GLEAM's "split into one store per variable" outstanding work
  (`OUTSTANDING.md` there) moot here — the axis this collection splits along is
  release and product, not variable.
- **~17,000 input files, not 644.** `open_mfdataset` over 16,980 files is itself
  a cost before any data is read, and `file_cache_maxsize` now bounds a cache
  that cannot possibly hold the whole set. On the region path a block spans
  every file, so each of the 9-ish blocks reopens all ~17,000 — measure that
  open cost before sizing the block, because it is a term GLEAM never had.
- **Source spatial chunks differ by release.** In v3.16 they are 200x200, and
  1800/200 = 9 and 3600/200 = 18 exactly, so `block_shape.lat: 200` aligns
  perfectly and reads 18 contiguous chunks per file. In v2.8 they are 32x32 and
  1800/32 = 56.25, so **no** block size aligns; 200 is kept for uniformity and
  straddles at most one row of source chunks per edge. Never assume one
  release's read profile predicts the other's.
- **The raw tree is `raw/<product>/<year>/` with one file per day**, not
  `raw/<resolution>/<variable>/` with one file per year. Path construction,
  variable discovery and the `variables: all` shorthand all have to be rethought
  rather than ported. A year directory sits between the product and the files
  because 15,000+ entries in one directory is above what GLADE is happy with.

## Layout

Mirrors `data_engineering_gleam`, and for the same reason: entry-point scripts
sit at the repo root and `utils/` is importable only because a script's own
directory is what lands on `sys.path`. That is why there is no install step, and
it is the constraint any reorganisation runs into first — moving an entry point
into a subdirectory breaks `from utils...` immediately. Do not paper over it
with `sys.path.insert`.

Configs all live in `config/`, test configs beside production ones.

## Conventions

Single quotes, f-strings for log messages, Google-style docstrings with
Args / Returns / Raises on every non-trivial function. Comments explain *why* a
choice was made, not what the line does. Module docstrings carry the context
needed to read the file.

Everything is derived from the YAML config — no paths, variable names, or chunk
sizes hardcoded in the pipeline.

Version control: commit as work lands, with messages in the imperative mood
describing the change and its reason. The remote is
`git@github.com:kheyblom/data_engineering_mswep.git` (SSH, matching
`data_engineering_gleam`; the HTTPS URL in TASKS.md names the same repository).
