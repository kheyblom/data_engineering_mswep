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
zero. V2.8 has no negative values at all.

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

## Tier 2: the bench jobs

Approved 2026-09-13. **Four** jobs, not two: one per release per write strategy,
each against the real raw tree with the real production settings, writing a
**throwaway** store. Each bench config differs from its production counterpart
only in `output_conventions.suffix` and `log_file` — verified by diff.

### Why V2.8 needs its own bench

The V3.16 numbers do not predict V2.8, because the two releases are chunked
differently at source:

| | V3.16 | V2.8 |
|---|---|---|
| source chunk | `[1, 200, 200]` | `[1, 32, 32]` |
| chunk size | 160 KiB | 4 KiB |
| chunks per timestep | 9 x 18 = **162** | 57 x 113 = **6,441** |
| chunks touched by a `(1,200,3600)` block read | 18 | ~791 |

That is roughly **44x more chunk operations for identical bytes**. It also means
the default `chunk_cache_slots` of 2003 is far below the 6,441 chunks one V2.8
plane touches, so the V2.8 configs set `chunk_cache_slots: 8009` and
`chunk_cache_size_mib: 64`. Whether that is enough is one of the things the
bench measures.

```bash
# V3.16 Past -- append, then region
QUEUE=develop NCPUS=4 MEM=16GB WALLTIME=02:00:00 \
    CONFIG=config/config_zarr_bench_v3_16_past_spatial.yaml ./submit_mswep_zarr.sh
QUEUE=develop NCPUS=1 MEM=96GB WALLTIME=03:00:00 \
    CONFIG=config/config_zarr_bench_v3_16_past_temporal.yaml ./submit_mswep_zarr.sh

# V2.8 Past -- same shapes; this is the pair whose cost is unknown
QUEUE=develop NCPUS=4 MEM=16GB WALLTIME=02:00:00 \
    CONFIG=config/config_zarr_bench_v2_8_past_spatial.yaml ./submit_mswep_zarr.sh
QUEUE=develop NCPUS=1 MEM=96GB WALLTIME=03:00:00 \
    CONFIG=config/config_zarr_bench_v2_8_past_temporal.yaml ./submit_mswep_zarr.sh
```

~22 core-hours if all four run to walltime. The `MEM=96GB` on the region jobs is
sized from GLEAM, whose equivalent 45.1 GiB block measured 70-80 GB resident at
`NCPUS=1`; our blocks are 45.5 GiB (V3.16) and 41.1 GiB (V2.8).

The two NRT records are **not** benched: at 684 and 2,117 timesteps their
production builds are small enough to be their own measurement, and their region
blocks are 1.8 and 5.7 GiB rather than tens of GiB.

Neither path needs to finish. The append path writes from timestep 0 in order,
so a walltime kill just stops it and the per-commit log lines give throughput;
the region path only needs one or two committed blocks to show peak memory and
per-block wall time.

What to read off them:

| | from the append bench | from the region bench |
|---|---|---|
| cold open | first log gap, before the first commit | same |
| throughput | seconds per 100 step commit | seconds per block |
| peak memory | `qhist` Mem column | `qhist` Mem column, against the block size |
| compression | store bytes / timesteps written | store bytes / blocks written |
| cpu efficiency | `qhist` CPU vs Elapsed x NCPUs | same |

Then: delete the four bench stores, size the production chain, and build.

```bash
rm -rf /glade/derecho/scratch/$USER/data/mswep/v_3_16/zarr/*.bench_*.zarr
rm -rf /glade/derecho/scratch/$USER/data/mswep/v_2_8/zarr/*.bench_*.zarr
```

## Tier 0 for the 8-store config set

Run before any job is submitted; it costs nothing and reads no data. For each of
the 12 configs it asserts the store name carries the right release and product,
the axis length and gap count match the measured raw tree, `source_fill_values`
is present on V3.16 Past and **absent everywhere else**, `chunk_cache_slots` is
8009 on V2.8 and unset on V3.16, the temporal tile is 20 x 20, and every
attribute template renders with no unsubstituted braces.

Passed 2026-09-13 for all 12 configs. The exact script is in the approved plan
and is cheap to re-run after any config edit.

## Tier 2 outcome (2026-09-13/14, jobs 7439058/61/62/63)

Four jobs on `cpudev`, all against the real raw trees writing throwaway
`bench_*` stores. ~11 core-hours spent.

| | V3.16 Past | V2.8 Past |
|---|---|---|
| **append** (4 cpu, 16 GB, 2 h cap) | **completed in 1.13 h** | **completed in 1.16 h** |
| cold open | 13.2 min | 11.5 min |
| per 100-step commit | 19.4 s | 22.7 s |
| commits | 170 | 154 |
| store size | 87 GB | 53 GB |
| **region** (1 cpu, 96 GB, 3 h cap) | 5 of 9 blocks, walltime | 4 of 9 blocks, walltime |
| cold open | 19.6 min | 16.4 min |
| mean per block | 31.8 min | 36.6 min |
| block size | 45.5 GiB | 41.1 GiB |
| projected full build | ~5.1 h | ~5.8 h |

### 5. The append path is far cheaper than expected, and both stores completed

Neither append job needed its 2 h cap: the whole 16,982 step V3.16 record was
written in **1.13 h** and the 15,339 step V2.8 record in **1.16 h**, at 4 cpus.
That is ~4.6 core-hours per spatial store.

Both stores were then verified: correct shape, dtype and chunking, a regular
axis of the right length, coordinates single-chunked, the two 1993 gap days
all-NaN in V3.16, and values **bit-exact against the raw netCDF** on days
sampled across the whole record (1979, 1993, 2010, 2025 for V3.16; 1979, 2000,
2020 for V2.8). V2.8 has zero NaN cells, confirming the corner defect is V3.16
Past only.

### 6. `[1, 32, 32]` costs far less than the chunk count suggests

V2.8 touches ~40x more source chunks per timestep than V3.16 (6,441 vs 162), and
~44x more per region block read (~791 vs 18). The predicted penalty was large.
Measured, it is **17% on the append path** (22.7 vs 19.4 s per commit) and
**15% on the region path** (36.6 vs 31.8 min per block).

The reason is that both paths read *contiguously*: an append reads a whole
plane and a region block reads a full-lon hyperslab, so HDF5 walks many small
adjacent chunks rather than seeking between scattered ones. Chunk **count** is
a poor proxy for cost when the access pattern is sequential; chunk **locality**
is what matters, which is the same lesson `block_shape.lon: -1` encodes.

The V2.8 bench was still worth running — a 15-17% penalty is a real number to
size a chain from, and it could not have been predicted from the chunk counts.

### 7. A region build does not fit one develop-queue walltime

At 31.8 and 36.6 min per block plus a ~20 min cold open, a full 9-block region
build projects to **5.1 h (V3.16)** and **5.8 h (V2.8)**. The develop queue caps
at 6 h, so a single job would be racing its own walltime with no margin.

Chain two jobs with `AFTER=<jobid>` instead. Resume is proven and cheap: the
second job re-reads the commit messages, skips the finished blocks, and pays
only the ~20 min cold open again.

Block times are **not** uniform — V3.16 ran 11.9, 28.5, 36.6, 44.9, 37.0 min for
blocks 1-5. Block 1 is the Arctic band containing the all-NaN corner and
compresses away quickly; the mid-latitude bands are the expensive ones. Do not
size a chain from the first block.

### 8. xarray warns once per file about the V2.8 misalignment

The V2.8 region job produced a **4.5 MB** PBS output file, 15,339 copies of:

```
UserWarning: The specified chunks separate the stored chunks along dimension
"lat" starting at index 200. This could degrade performance.
```

One per opened file. It is correct — a 200 row read chunk does straddle the
32 row source chunks — and it is a documented, accepted trade (no block size can
align with a 32 row grid, since 1800/32 = 56.25). But at one warning per file it
buries the job log. Worth suppressing for this known case rather than leaving
every V2.8 job output at 4.5 MB.

## Tier 3: the production builds (2026-09-14)

All eight stores built. Four were promoted from the Tier 2 bench rather than
rebuilt (the two completed append stores, and the two partial region stores
resumed from 5 and 4 committed blocks), which saved ~14 core-hours.

| store | build | days | size | chunk objects |
|---|---|---|---|---|
| `v_3_16.past.spatial` | append, 1.13 h | 16,982 | 87 G | 17,320 |
| `v_3_16.past.temporal` | region, 3.01 h + 1.73 h resume | 16,982 | 86 G | **16,154** |
| `v_3_16.nrt.spatial` | append, 2.4 min | 684 | 3.5 G | 698 |
| `v_3_16.nrt.temporal` | region, 5.4 min | 684 | 3.5 G | 16,203 |
| `v_2_8.past.spatial` | append, 1.16 h | 15,339 | 53 G | 15,647 |
| `v_2_8.past.temporal` | region, 2.72 h + 2.18 h resume | 15,339 | 57 G | 16,807 |
| `v_2_8.nrt.spatial` | append, 10.2 min | 2,117 | 13 G | 2,161 |
| `v_2_8.nrt.temporal` | region, 23.4 min | 2,117 | 14 G | 16,203 |

Total 315 GB. Every store: correct shape and chunking, axis of the expected
length, strictly regular spacing, and values **bit-exact against the raw netCDF**
on sampled days.

### 9. The corner-tile prediction came out exactly right

`v_3_16.past.temporal` holds **16,154** chunk objects: 16,151 data chunks plus
3 coordinates. 16,200 tiles minus the 7 x 7 = 49 tiles lying entirely inside the
150 x 150 all-NaN corner is 16,151, which is what CLAUDE.md predicted before the
build. Nothing else in this project has confirmed the sentinel masking so
directly -- the arithmetic and the store agree with no slack.

The other three temporal stores hold 16,203 (16,200 + 3), as they must: none of
them has the corner defect.

### 10. Chunk objects on disk are NOT the reachable chunk count

Three stores hold more objects than the arithmetic allows:

| store | objects | reachable arithmetic | excess |
|---|---|---|---|
| `v_2_8.past.temporal` | 16,807 | 16,203 | **604** |
| `v_3_16.past.spatial` | 17,320 | 17,152 | 168 |
| `v_2_8.past.spatial` | 15,647 | 15,495 | 152 |

Two causes, both expected and both harmless:

- **Partial region writes.** Both Tier 2 region benches were killed by walltime
  *inside* a block. A block that never committed still left the chunks it had
  already written on disk, unreferenced by any snapshot. `v_2_8.past.temporal`
  carries 604 of them from the block 5 it was killed in.
- **Fork snapshots.** Every `to_icechunk` write forks the session, so an append
  build accumulates roughly one unreachable snapshot per commit -- 168 against
  170 commits, 152 against 154. This is the behaviour GLEAM documented.

**Neither is damage, and a file count cannot tell the difference, so it was
checked in the direction that matters**: the interrupted blocks were read back
and compared against raw. `v_3_16.past.temporal` at lat 1050-1199 (the block it
was killed in) is bit-exact with exactly 2 NaN per series -- the two 1993 gap
days, present *within* chunks rather than as missing chunks, as the temporal
layout requires. `v_2_8.past.temporal` at lat 850-999 is bit-exact with 0 NaN.

`finalize_mswep_zarr.py --gc` is what reclaims these, and per the GLEAM
procedure it runs after `--attrs` and `--tag`, dry-run first, re-verify after.
Until then the excess is ~2 GB and costs nothing but disk.

### 11. Region blocks run faster than the bench projected

The bench measured 31.8 and 36.6 min per block and projected 5.1 h and 5.8 h for
a full 9-block build. The resumes ran their remaining blocks at 12-25 min each
and finished in 1.73 h and 2.18 h. Block cost varies with how much of the band
is ocean, so the mid-latitude bands the bench happened to measure are the
expensive ones. Size a chain from the *slowest* blocks, not the mean.
