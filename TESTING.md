# Validating the MSWEP zarr build

How the pipeline was tested before anything was run at production scale, what
was found, and how to reproduce it. Structured like
`data_engineering_gleam`'s `TESTING.md`, and tiered for the same reason: a
correctness bug found on a 365 day fixture costs minutes, and the same bug found
on the full 410 GiB record costs core-hours against a finite allocation.

| Tier | What it is | Cost | Status |
|------|-----------|------|--------|
| 0 | Unit-level guards, no data read | seconds | **passed** (2026-09-13) |
| 1 | One staged year, both write strategies, build + interrupt + resume | ~15 min, login node | **passed** (2026-09-13) |
| 2 | Full record, throwaway store, to measure throughput and peak memory | batch job | not run |
| 3 | Production build | batch chain | not run |

Tier 2 is what the production job has to be sized from, and it has not been run.
Nothing below should be read as a statement about production cost.

## The fixture

One year of symlinks into the real raw tree, with one day deliberately removed:

```bash
SRC=/glade/derecho/scratch/$USER/data/mswep/v_3_16/raw/past/daily
DST=/glade/derecho/scratch/$USER/data/mswep_tiny/v_3_16/raw/past/daily
mkdir -p "$DST/1979"
for f in "$SRC"/1979/*.nc; do b=$(basename "$f")
  [ "$b" = "1979182.nc" ] && continue      # 1979-07-01, removed on purpose
  ln -sf "$f" "$DST/1979/$b"
done
```

364 files on a 365 step axis. The removed day is *not* a real MSWEP gap: the
production configs only exercise the gap path on 2 days out of 16,982, and a
fixture that never exercises it at all would leave the splice, the coverage
guard and the all-fill-chunk behaviour untested until production.

```bash
uv run python mswep_zarr.py --config config/config_zarr_tiny_spatial.yaml
uv run python mswep_zarr.py --config config/config_zarr_tiny_temporal.yaml
```

## Findings

### 1. An unnamed dimension in `chunks` is read with the *file's* chunking, not whole

The first probe opened the files with `chunks={'time': 1}` and took **118 ms per
file**; naming all three dimensions took **5 ms**. The cause is that a dimension
absent from the dict does not default to the whole axis -- it inherits the
netCDF file's own chunking, which in MSWEP v3.16 is `[1, 200, 200]`. So lat and
lon silently became 9 x 18 = 162 dask chunks per file instead of 1, and the task
graph went from 3 tasks per file to ~650.

Over the full record that is the difference between ~51,000 tasks and ~11
million. GLEAM never hit this because every config there happened to name all
three dimensions, so the failure mode was never visible.

`resolve_chunks` now **requires** every dimension of the dataset to be named and
raises otherwise. This is the single most expensive mistake available in this
config format, and it is silent -- the build still produces a correct store,
just far more slowly.

### 2. The declared `_FillValue` is not the value in the data

Covered in full in `CLAUDE.md`. In short: `_FillValue = -9999` occurs **nowhere**
in V3.16; the sentinel actually written is **-239976.0** = `-9999 x 24`, on a
solid 150 x 150 block at array indices `[0:150, 0:150]`, identical on every day
sampled across 1979-2025. CF masking therefore does nothing and the value would
have been stored as though it were a measurement four orders of magnitude below
zero. V2.8.0 has no negative values at all.

Handled by the `source_fill_values` config key, with `check_source_fill` failing
the build if a declared sentinel does not actually occur.

This was found by looking at the decoded values of a single plane -- `min`,
`max`, `mean` -- before writing anything. It is worth doing that on any new
product before trusting its metadata.

### 3. The gap splice is `concat`, not `reindex`

Both produce identical values and identical chunk structure. `reindex` rebuilds
the array through a fancy index over the whole time axis and had a visibly
larger graph in the probe; splicing leaves every segment exactly as
`open_mfdataset` chunked it and adds one chunk per gap. With a handful of gaps
the concat is over a handful of pieces and costs nothing.

### 4. Cold-opening the full record is slow, and it is a per-job cost

A probe that opened all 16,980 files cold ran for **over 24 minutes** and was
still going when it was killed (state `D`, I/O wait, 0.6 GB resident). Warm, the
same open is ~5 ms per file, so the cost is cold Lustre metadata plus each
file's HDF5 header -- it does not go away between jobs, because 82 GB will not
stay in page cache.

Two consequences for sizing, neither yet measured properly:

- Every job, including every resume, pays this before it writes anything.
  Prefer fewer, longer jobs over many short ones.
- On the region path the open happens **once per job, not once per block**, so a
  job that gets through several blocks amortises it. That argues against
  splitting the region build into more, smaller jobs than walltime requires.

`raw_files` itself is not part of this cost: it reads timestamps out of the
*filenames*, so the whole 16,982 step axis is known in **0.1 s** without opening
a single file.

## What passed

Tier 0, all guards raising as intended:

| Guard | Checked |
|-------|---------|
| `check_coverage` | undeclared gap rejected; over-declared gap rejected; exact match accepted |
| `resolve_chunks` | unnamed dimension rejected; unknown dimension rejected |
| `resolve_write_strategy` | `append` + whole-time chunk rejected; `region` + shallow chunk rejected; unknown strategy rejected |
| `resolve_block_shape` | block that is not a whole number of output chunks rejected |
| `BLOCK_MESSAGE_RE` | round-trips a block through its commit message |

Tier 1, append path (`config_zarr_tiny_spatial.yaml`):

- Builds 365 timesteps in 4 commits, including a short final batch of 65.
- Values **bit-exact** against the raw files on 4 sampled days.
- Chunking `(1, 1800, 3600)`; `lat`/`lon` coordinates land in a single chunk.
- `least_significant_digit=2` carried from the netCDF encoding onto the
  variable's attributes; upstream `units` preserved; configured `long_name` and
  `standard_name` applied on top.
- The absent day is present on the axis, all-NaN, and **writes no chunk** --
  364 data chunks for 365 timesteps, which is correct.
- The corner block is NaN and the cell immediately outside it is finite.
- Rerunning a complete store is a no-op.
- Interrupted after 2 of 4 batches, then resumed: picked up at timestep 200,
  wrote the remaining 2 batches, bit-exact across the resume boundary.

Tier 1, region path (`config_zarr_tiny_temporal.yaml`):

- Creates a skeleton, then fills 9 blocks of `lat: 200, lon: -1`.
- Chunking `(365, 20, 20)`; coordinates single-chunked.
- Point time series **bit-exact** against raw at three locations.
- A point inside the corner block is NaN for all 365 steps; a point outside has
  exactly 1 NaN, the gap day.
- **16,151 chunks on disk against a 16,200 tile grid** -- exactly the predicted
  count, since the 7 x 7 = 49 tiles lying entirely inside the 150 x 150 corner
  block are NaN across the whole record. This is an independent confirmation
  that the sentinel masking did what it was meant to.
- Interrupted after 6 of 9 blocks, then resumed: read the completed blocks back
  out of the **commit messages**, wrote the remaining 3, and finished with the
  same 16,151 chunks and bit-exact values across the boundary.
- Resuming into a store that holds only a skeleton and no blocks also works.

## Sizes, and what they suggest

| | 365 days | implied 16,982 days |
|---|---|---|
| uncompressed | 8.81 GiB | 409.9 GiB |
| spatial store | 1.85 GiB | ~86 GiB |
| temporal store | 1.82 GiB | ~85 GiB |

So roughly **4.8x** compression, and the two layouts compress almost identically
-- the chunk shape barely matters to zstd here. Budget ~170 GiB for both stores
together, plus the 82 GB raw tree.

These are extrapolations from one year, not measurements. One year is not a
representative sample of a 46 year record, and the region path's blocks are 43x
larger in production than in the fixture. Tier 2 has to confirm both.

## Not done

- Tier 2 and Tier 3 (see the table above).
- A verifier. `verify_mswep_zarr.py` does not exist yet; Tasks 6 and 8 need it.
  It must be read-only, and it is what the `verification` store attribute is
  gated on.
- Whether any cell **other** than the 150 x 150 corner block carries the
  -239976.0 sentinel. The build only checks the first timestep, deliberately --
  proving it record-wide is a full-coverage question and belongs to the verifier.
- Peak memory on the region path at the production block size (45.5 GiB
  resident, against 1.05 GiB in the fixture).
