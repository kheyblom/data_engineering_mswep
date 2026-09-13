# CLAUDE.md

Guidance for Claude Code (claude.ai/code) when working in this repository.

## Project

Convert the raw MSWEP precipitation netCDF files downloaded by
[access_mswep](/glade/u/home/kheyblom/work/data_access/access_mswep) into
icechunk-backed zarr stores on NCAR Derecho/GLADE.

Two stores are built from the same raw files, differing only in chunking and so
in which read is cheap: a **spatial** store (one global map per chunk, for
reading fields) and a **temporal** store (one small lat/lon tile through the
whole record per chunk, for reading point time series). Neither is derived from
the other — both are built from raw and verified against raw independently — so
nothing about one has to be trusted to trust the other. That independence is a
deliberate choice carried over from `data_engineering_gleam`, not an accident of
how it was built; do not "optimise" the temporal build into a rechunk of the
spatial store.

**This project is modelled directly on
[data_engineering_gleam](/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam).**
Read that repo's `CLAUDE.md`, `README.md` and `TESTING.md` before designing
anything here. Most of the hard-won findings transfer; the section below records
which ones do not, and why.

Working state, and where to pick up after a lost connection or a new session,
is in [STATE.md](STATE.md). Keep it current — it is the handoff document.
The task list the project is working through is [TASKS.md](TASKS.md).

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

- Versions on disk: `v_3_16` (16,980 files, 81.67 GB) and `v_2_8_0` (15,339
  files, 51.84 GB). V3.16 is the target.
- The version is spelled `V3.16` in the config and `v_3_16` on disk, and the
  product `Past/Daily` becomes `past/daily`. `access_mswep`'s `format_version`
  and `format_product` are the reference implementations of both translations;
  keep the two projects' spellings identical or a config cannot drive both.
  Note MSWEP versions carry two *or* three dot-separated parts (`V3.16`,
  `V2.8.0`), so this is not GLEAM's `v4.3a` regex.
- **One file per day**, not per year, and **one variable**, `precipitation`.
  Both differ from GLEAM and both change the cost model — see below.
- Grid: `lat` 1800 (89.95 N to -89.95 S, north-to-south), `lon` 3600
  (-179.95 to 179.95), 0.1 degree, float32. **Identical to the GLEAM grid**, so
  GLEAM's spatial chunk reasoning transfers directly.
- `time` is one step per file, float32 (v3.16) / int32 (v2.8.0)
  `days since 1900-1-1 00:00:00`. First 28854 (1979-01-01), last 45835
  (2025-06-29).
- `precipitation`: float32, `_FillValue = -9999.f`, `units = 'mm/d'` (v3.16) or
  `'mm d-1'` (v2.8.0), `least_significant_digit = 2` (v3.16) / `1` (v2.8.0).
  There is no `standard_name` and no `long_name` on the data variable.
- Full record uncompressed: 16,980 x 1800 x 3600 x 4 B = **410 GiB**. Raw on
  disk is 81.67 GB, so the source compresses about 5.4x.

### The declared fill value is not the one in the data (V3.16)

`precipitation:_FillValue = -9999.f`, and that value **occurs nowhere in the
record**. The sentinel actually written is **`-239976.0`**, which is
`-9999 x 24` — the fill summed over the 24 hourly steps a daily total is
aggregated from, so upstream did not propagate the mask through the
aggregation. Because the value in the data does not match the attribute,
xarray's CF masking leaves it untouched and it decodes as a precipitation
measurement four orders of magnitude below zero.

It covers a solid **150 x 150 block at array indices `[0:150, 0:150]`** — the
grid's top-left corner, 75.05-89.95 N and 180-165 W, Arctic Ocean near the
dateline — and is byte-identical on every day sampled across 1979-2025. It is
the *only* negative value in the files. V2.8.0 has no negative values at all and
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

### Known gap in V3.16

`1993241.nc` (1993-08-29) and `1993243.nc` (1993-08-31) **do not exist** in the
V3.16 remote. The download verified 16,980/16,980 against the remote listing, so
this is upstream, not a download failure; V2.8.0 has a complete 365 days for
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
  v3.16 and `[1, 32, 32]` in v2.8.0. There is nothing to re-decompress, so the
  large chunk cache buys little. Size it and confirm by measurement rather than
  copying 512.
- **There is one variable, not fourteen.** So: no cross-variable merge and no
  `join='exact'` time-axis check between variables (the equivalent completeness
  check has to be against the expected *date* range instead — see the 1993 gap);
  no variable-major block ordering; the region build has roughly 1/14 the blocks
  GLEAM's did, which makes resume granularity coarser and worth thinking about.
  It also makes GLEAM's "split into one store per variable" outstanding work
  (`OUTSTANDING.md` there) moot here.
- **~17,000 input files, not 644.** `open_mfdataset` over 16,980 files is itself
  a cost before any data is read, and `file_cache_maxsize` now bounds a cache
  that cannot possibly hold the whole set. On the region path a block spans
  every file, so each of the 9-ish blocks reopens all ~17,000 — measure that
  open cost before sizing the block, because it is a term GLEAM never had.
- **Source spatial chunks are 200x200 (v3.16).** 1800/200 = 9 and 3600/200 = 18
  exactly, so a `block_shape.lat` of 200 aligns perfectly with the source chunk
  grid and reads 18 contiguous chunks per file. A block that is not a multiple
  of 200 re-reads source chunks across block boundaries.
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
