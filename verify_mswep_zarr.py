"""Verify a finished MSWEP icechunk zarr store against the raw netCDF files.

Reads the same config the build did, so it checks the store that config names
and needs no paths of its own. Nothing here writes to the store: every session
is read-only, and the one destructive operation it touches (``garbage_collect``)
is only ever called as a dry run.

The raw side is read with ``netCDF4`` and masking switched **off**, so the fill
sentinels arrive as the literal values the files hold. That is deliberate:
comparing against ``xr.open_dataset`` would send both sides through the same CF
decoding the build used, and could only prove the pipeline agrees with itself.
Every value comparison is exact rather than ``allclose`` -- the pipeline does no
arithmetic, so anything short of bit-identical is a defect.

Two things about MSWEP shape the checks, and both differ from GLEAM:

**A raw timestep may have no file at all.** MSWEP publishes one file per day, and
V3.16 Past is missing 1993-08-29 and 1993-08-31 outright. The store carries them
as all-NaN planes on a regular axis. The raw index therefore maps the *expected*
axis to files, with a hole where upstream published none, and a hole reads back
as all-NaN so the comparison expects NaN there.

**The declared fill value is not the one in the data.** All four products declare
``_FillValue = -9999`` and none of them writes it. V3.16 Past writes -239976.0
(= -9999 x 24) over a 150 x 150 corner block instead. The build masks whatever
``source_fill_values`` declares, so the verifier reads the same key rather than
hardcoding a sentinel -- and the ``sentinel`` phase exists to check that nothing
the config failed to declare survived into the store.

The store's chunking decides what a cheap read is, and the phases that read data
follow it rather than assuming one layout. The unit of comparison is a *box* --
a (time, lat, lon) range aligned to the store's chunk grid:

``spatial``  (chunks ``1 x 1800 x 3600``) a box is one whole global plane on one
    day, and costs one raw file to check.
``temporal`` (chunks ``N x 20 x 20``) a box is one lat/lon tile. **Unlike GLEAM,
    a full-record tile here spans every one of ~17,000 daily files**, so a box
    is cut to ``--box-timesteps`` days rather than the whole record. Checking
    one whole-record tile would cost ~17,000 file opens; several windowed tiles
    at different positions cover the same ground for a fraction of it.

Six phases, selectable with ``--phases``. The first five run by default; the
sixth needs a second store to compare against.

``structure``
    Dimensions, chunk shapes, dtypes, fill value, codecs and attributes, read
    from the zarr metadata alone, plus the repository's commit history and its
    unreachable objects.
``index``
    That the time axis is the complete regular axis the config implies, that
    lat/lon are bit-identical to raw, and that every declared gap day is present
    and entirely NaN.
``samples``
    Boxes drawn at random and at chosen positions, compared cell by cell against
    raw.
``sweep``
    Every chunk in the store, via the manifest rather than by reading data:
    presence, placement, and compressed size. An absent chunk is checked against
    raw before it counts as a finding, since zarr does not write a chunk whose
    cells are all fill.
``sentinel``
    That no undeclared fill sentinel survived into the store, and that what was
    masked is exactly what the config declared. This is the phase that would
    catch a release changing its sentinel.
``metadata``
    The store's attributes and coordinates against a sibling store's, so stores
    built from the same files describe the same data the same way. Needs
    ``--compare-with``, so it is not part of a default run.

A phase reports FAIL only for something the *store* got wrong. Properties of the
upstream data that look like defects -- the two absent 1993 days, the V3.16
corner block -- are confirmed against raw and reported as notes, because
asserting them as invariants produces false alarms rather than findings.
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import os
import sys

import icechunk
import netCDF4
import numpy as np
import xarray as xr

from utils.log_utils import setup_logging
from utils.path_utils import (
    check_coverage,
    expected_times,
    file_naming,
    load_config,
    raw_files,
    store_path,
)
from utils.zarr_utils import (
    BRANCH,
    commit_batch_size,
    iter_blocks,
    resolve_block_shape,
    resolve_chunks,
)

LOG = logging.getLogger(__name__)

PHASES = ('structure', 'index', 'samples', 'sweep', 'sentinel', 'metadata')

# 'metadata' compares two stores, so it needs a second config and cannot run in
# a default pass over a single one
DEFAULT_PHASES = tuple(p for p in PHASES if p != 'metadata')

# Physical bounds for daily precipitation, in mm/day. The lower bound is hard:
# a negative accumulation is impossible, and it is exactly what an unmasked fill
# sentinel looks like. The upper bound is advisory -- the world record daily
# rainfall is around 1825 mm (Foc-Foc, Reunion, 1966) -- so a value above it is
# reported as a note to look at rather than a failure, since a gridded product
# is not bound by a station record.
PRECIPITATION_FLOOR = 0.0
PRECIPITATION_CEILING = 2000.0

# A written chunk smaller than this is suspicious on the spatial layout, where
# every chunk is a whole global plane and cannot legitimately compress to
# nothing. On the temporal layout a tile really can be almost empty, so no floor
# is applied and the smallest chunks are read back and compared instead.
DEGENERATE_BYTES = {'spatial': 50 * 1024, 'temporal': 0}

# Global attributes that describe how a store is laid out, when it was built, or
# what has been proven about it. Two stores built from the same files
# legitimately differ on these and must agree on everything else.
LAYOUT_ATTRS = frozenset(
    {
        'title',
        'chunking',
        'history',
        'date_created',
        'related_store',
        'verification',
    }
)


class Report:
    """Collects check outcomes so the exit status can reflect all of them.

    Attributes:
        failures (list): Labels of the checks that failed.
        n_checks (int): How many checks were recorded.
    """

    def __init__(self):
        self.failures = []
        self.n_checks = 0

    def check(self, label, ok, detail=''):
        """Record a pass or fail.

        Args:
            label (str): What was checked.
            ok (bool): Whether it held.
            detail (str): Measured values to log alongside the verdict.

        Returns:
            bool: The ``ok`` that was passed in, so callers can branch on it.
        """
        self.n_checks += 1
        LOG.info(f'{"PASS" if ok else "FAIL"}  {label}{"  " + detail if detail else ""}')
        if not ok:
            self.failures.append(label)
        return ok

    def note(self, message):
        """Log an observation that is not a pass/fail judgement.

        Args:
            message (str): The observation.
        """
        LOG.info(f'note  {message}')


def parse_args():
    """Parse the command line.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='verify a built mswep zarr store against the raw netcdf files.'
    )
    parser.add_argument(
        '--config', type=str, required=True, help='Path to YAML configuration file.'
    )
    parser.add_argument(
        '--phases',
        type=str,
        default=','.join(DEFAULT_PHASES),
        help=f'Comma separated subset of {",".join(PHASES)}. '
        'metadata needs --compare-with and is not run by default.',
    )
    parser.add_argument(
        '--samples',
        type=int,
        default=24,
        help='Boxes to compare against raw. A box is a chunk of the store: a '
        'whole global plane on the spatial layout, a lat/lon tile over '
        '--box-timesteps days on the temporal one.',
    )
    parser.add_argument(
        '--box-timesteps',
        type=int,
        default=400,
        help='Days a temporal-layout box spans. One MSWEP file is one day, so a '
        'whole-record tile would open ~17,000 files; windowing keeps a box '
        'affordable without weakening what it proves about those days.',
    )
    parser.add_argument(
        '--sentinel-planes',
        type=int,
        default=40,
        help='Raw planes, spread over the record, checked for fill sentinels '
        'the config did not declare.',
    )
    parser.add_argument(
        '--mask-days',
        type=int,
        default=12,
        help='Raw planes used to justify absent chunks on the temporal layout, '
        'where a hole claims a tile was empty for the whole record.',
    )
    parser.add_argument(
        '--smallest',
        type=int,
        default=3,
        help='Smallest written chunks to read back and compare against raw on '
        'the temporal layout, where a size floor cannot tell a sparse chunk '
        'from a truncated one.',
    )
    parser.add_argument(
        '--seed', type=int, default=20260914, help='Seed for the sample draw.'
    )
    parser.add_argument(
        '--compare-with',
        type=str,
        default=None,
        help='Config of a sibling store to compare metadata against. Required '
        'by the metadata phase and ignored by every other one.',
    )
    return parser.parse_args()


def open_store(settings):
    """Open the store the config names, read only.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        tuple: The repository, a read-only session, and the decoded dataset.
    """
    path = store_path(settings)
    LOG.info(f'verifying {path}')
    repository = icechunk.Repository.open(icechunk.local_filesystem_storage(path))
    session = repository.readonly_session(branch=BRANCH)
    dataset = xr.open_zarr(session.store, consolidated=False)
    return repository, session, dataset


def sentinel_values(settings):
    """Fill sentinels the raw files use, declared and undeclared.

    The declared ``_FillValue`` is included even though no MSWEP product
    actually writes it, so that a release which starts using it is handled
    without a code change.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        list: float32 sentinel values.
    """
    declared = [np.float32(-9999.0)]
    extra = [np.float32(v) for v in (settings.get('source_fill_values') or [])]
    return declared + extra


def build_raw_index(settings):
    """Map the store's expected time axis onto the raw files.

    A timestep with no file is a hole rather than an error: MSWEP publishes one
    file per day and a release can simply not publish one.
    ``check_coverage`` has already established that the holes are the ones the
    config declares, so this records them and moves on.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        dict: ``expected`` (the complete axis), ``paths`` (one file path or None
            per timestep), ``missing`` (positions with no file), and ``shape``
            (the raw lat/lon grid).
    """
    files = raw_files(settings)
    expected = expected_times(files.index, settings)
    check_coverage(files.index, expected, settings)
    aligned = files.reindex(expected)
    paths = [None if not isinstance(p, str) else p for p in aligned.values]
    missing = [i for i, p in enumerate(paths) if p is None]
    # the grid is read once here rather than per box: a box whose every timestep
    # is absent has no file to take a shape from, which is exactly the case the
    # spatial sweep hits when it traces the gap days
    first = next(p for p in paths if p is not None)
    sample = netCDF4.Dataset(first)
    shape = (sample.dimensions['lat'].size, sample.dimensions['lon'].size)
    sample.close()

    LOG.info(
        f'raw index: {len(expected)} timesteps, {len(expected) - len(missing)} '
        f'files, {len(missing)} published nowhere, grid {shape[0]}x{shape[1]}'
    )
    return {
        'expected': expected, 'paths': paths, 'missing': missing, 'shape': shape
    }


def read_raw_box(index, variable, t0, t1, lat=slice(None), lon=slice(None)):
    """Read a (time, lat, lon) box from the raw files, fill sentinels intact.

    One MSWEP file is one timestep, so this opens ``t1 - t0`` files and takes one
    hyperslab from each. A timestep the release never published is returned as
    NaN, which is what the store holds there.

    Args:
        index (dict): The raw index from ``build_raw_index``.
        variable (str): Variable to read.
        t0 (int): First timestep, inclusive.
        t1 (int): Last timestep, exclusive.
        lat (slice): Latitude range to take.
        lon (slice): Longitude range to take.

    Returns:
        numpy.ndarray: The float32 box, still holding the raw fill sentinels,
            and NaN at any timestep with no file.
    """
    pieces = []
    for timestep in range(t0, t1):
        path = index['paths'][timestep]
        if path is None:
            # upstream published nothing for this day; the store holds NaN and
            # so does the expectation
            pieces.append(None)
            continue
        dataset = netCDF4.Dataset(path)
        # masking and scaling off, so the comparison does not run through the
        # same CF decoding the build used and cannot agree by construction
        dataset.set_auto_mask(False)
        dataset.set_auto_scale(False)
        pieces.append(
            np.asarray(dataset.variables[variable][0, lat, lon]).astype(np.float32)
        )
        dataset.close()

    # derived from the grid and the slices, not from a piece: every piece may be
    # absent, which is what a box covering only unpublished days looks like
    n_lat, n_lon = index['shape']
    shape = (
        len(range(*lat.indices(n_lat))),
        len(range(*lon.indices(n_lon))),
    )
    return np.stack(
        [np.full(shape, np.nan, dtype=np.float32) if p is None else p for p in pieces]
    )


def compare_box(stored, raw, sentinels):
    """Compare a box read from the store against its raw counterpart.

    Args:
        stored (numpy.ndarray): The decoded box from the store.
        raw (numpy.ndarray): The raw box, fill sentinels intact.
        sentinels (list): Values the raw files use as fill.

    Returns:
        tuple: Whether the NaN pattern matches the fill pattern, whether every
            non-fill cell is bit-identical, and the count of valid cells.
    """
    # NaN in the raw box means the timestep has no file at all, which the store
    # also holds as NaN
    is_fill = np.isnan(raw)
    for sentinel in sentinels:
        is_fill |= raw == sentinel
    mask_ok = np.array_equal(is_fill, np.isnan(stored))
    values_ok = bool(np.all(stored[~is_fill] == raw[~is_fill]))
    return mask_ok, values_ok, int((~is_fill).sum())


def store_layout(chunks, sizes):
    """Which layout the store's chunking describes.

    Args:
        chunks (dict): Resolved chunk sizes.
        sizes (Mapping): Length of each dimension.

    Returns:
        str: ``'spatial'`` if a chunk is a whole plane, ``'temporal'`` if it
            spans the whole time axis.
    """
    return 'temporal' if chunks['time'] >= sizes['time'] else 'spatial'


def tile_grid(chunks, sizes):
    """The store's chunk grid over lat and lon.

    Args:
        chunks (dict): Resolved chunk sizes.
        sizes (Mapping): Length of each dimension.

    Returns:
        tuple: Number of chunks along lat and along lon.
    """
    return (
        -(-sizes['lat'] // chunks['lat']),
        -(-sizes['lon'] // chunks['lon']),
    )


def grid_divides(chunks, sizes):
    """Whether the chunk grid tiles lat and lon exactly.

    Args:
        chunks (dict): Resolved chunk sizes.
        sizes (Mapping): Length of each dimension.

    Returns:
        bool: True if neither axis has a short final chunk.
    """
    return (
        sizes['lat'] % chunks['lat'] == 0 and sizes['lon'] % chunks['lon'] == 0
    )


def coarsen_to_tiles(plane, chunks, grid, sentinels):
    """Reduce a raw plane to a per-tile 'holds any valid cell' mask.

    Args:
        plane (numpy.ndarray): One raw lat/lon plane, sentinels intact.
        chunks (dict): Resolved chunk sizes.
        grid (tuple): Tile counts along lat and lon.
        sentinels (list): Values the raw files use as fill.

    Returns:
        numpy.ndarray: Boolean array of shape ``grid``.
    """
    valid = ~np.isnan(plane)
    for sentinel in sentinels:
        valid &= plane != sentinel
    return valid.reshape(
        grid[0], chunks['lat'], grid[1], chunks['lon']
    ).any(axis=(1, 3))


def check_structure(report, dataset, settings, chunks, layout):
    """Check the store's shape, encoding and attributes, without reading data.

    Args:
        report (Report): Where to record outcomes.
        dataset (xarray.Dataset): The decoded store.
        settings (dict): The loaded configuration.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
    """
    variables = sorted(dataset.data_vars)
    report.check(
        'store holds exactly one data variable',
        len(variables) == 1,
        f'holds {variables}',
    )
    report.check(
        'dimensions are (time, lat, lon)',
        tuple(dataset[variables[0]].dims) == ('time', 'lat', 'lon'),
        f'{dataset[variables[0]].dims}',
    )
    report.check(
        'grid is 1800 x 3600',
        (dataset.sizes['lat'], dataset.sizes['lon']) == (1800, 3600),
        f'{dataset.sizes["lat"]} x {dataset.sizes["lon"]}',
    )

    for name in variables:
        array = dataset[name]
        report.check(
            f'{name} is float32',
            array.dtype == np.float32,
            str(array.dtype),
        )
        shape = tuple(chunks[d] for d in array.dims)
        report.check(
            f'{name} chunked {shape}',
            tuple(array.encoding['chunks']) == shape,
            f'{tuple(array.encoding["chunks"])}',
        )
        report.check(
            f'{name} fill value is NaN',
            np.isnan(array.encoding.get('_FillValue', 0.0)),
            repr(array.encoding.get('_FillValue')),
        )
        report.note(f'{name} codecs: {array.encoding.get("compressors", "unset")}')

    # lat and lon are read whole by anything that opens the store, so they must
    # not inherit the data's chunking.
    for name in ('lat', 'lon'):
        report.check(
            f'{name} coordinate is a single chunk',
            tuple(dataset[name].encoding['chunks']) == (dataset.sizes[name],),
            f'{tuple(dataset[name].encoding["chunks"])} of {dataset.sizes[name]}',
        )

    # time is the exception, and deliberately so. On the append path the build
    # chunks it at the commit batch size, so that every append lands on a chunk
    # boundary rather than having to rewrite a partly filled one; on the region
    # path there are no batches and it spans the axis. Asserting 'one chunk'
    # here would report the append path's correct behaviour as a defect.
    if layout == 'temporal':
        expected_time_chunk = dataset.sizes['time']
        why = 'the whole axis, as the region path writes it'
    else:
        expected_time_chunk = commit_batch_size(chunks, settings)
        why = 'the commit batch size, so each append lands on a chunk boundary'
    report.check(
        f'time coordinate is chunked at {expected_time_chunk} ({why})',
        tuple(dataset['time'].encoding['chunks']) == (expected_time_chunk,),
        f'{tuple(dataset["time"].encoding["chunks"])} of {dataset.sizes["time"]}',
    )
    report.check(
        'time encoded as int64 days since 1900-01-01',
        dataset['time'].encoding.get('units') == 'days since 1900-01-01'
        and str(dataset['time'].encoding.get('dtype')) == 'int64',
        f'{dataset["time"].encoding.get("units")!r}, '
        f'{dataset["time"].encoding.get("dtype")}',
    )

    required = (
        'title', 'summary', 'Conventions', 'source', 'references',
        'chunking', 'related_store', 'known_data_gaps', 'product_caveat',
        'time_coverage_start', 'time_coverage_end', 'time_coverage_resolution',
    )
    absent = [a for a in required if a not in dataset.attrs]
    report.check(
        f'the {len(required)} required global attributes are present',
        not absent,
        f'missing {absent}' if absent else f'{len(dataset.attrs)} attributes',
    )

    # the store must not claim an audit it has not had; finalize adds this only
    # after this verifier has run
    if 'verification' in dataset.attrs:
        report.note(f'store already carries a verification attribute')

    declared = settings.get('source_fill_values') or []
    recorded = dataset.attrs.get('source_fill_values_masked')
    report.check(
        'source_fill_values_masked matches the config',
        bool(declared) == bool(recorded),
        f'config declares {declared or "none"}, store records {recorded or "none"}',
    )
    report.note(f'layout {layout!r}, chunks {chunks}')


def check_repository(report, repository, dataset, settings, chunks, layout):
    """Report the commit history and the unreachable objects.

    The garbage collection call is a dry run, which reads and reports without
    deleting anything.

    Args:
        report (Report): Where to record outcomes.
        repository (icechunk.Repository): The repository holding the store.
        dataset (xarray.Dataset): The decoded store.
        settings (dict): The loaded configuration.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
    """
    snapshots = list(repository.ancestry(branch=BRANCH))
    if layout == 'temporal':
        block_shape = resolve_block_shape(settings, chunks, dataset.sizes)
        units = len(iter_blocks(sorted(dataset.data_vars), dataset.sizes, block_shape))
        expected = units + 2      # one per block, the skeleton, the initial snapshot
        noun = f'{units} block commits, the skeleton'
    else:
        batch = commit_batch_size(chunks, settings)
        units = -(-dataset.sizes['time'] // batch)
        expected = units + 1      # one per batch, plus the initial snapshot
        noun = f'{units} batch commits'
    # finalization legitimately adds commits, so the build count is a floor
    report.check(
        f'at least one reachable snapshot per commit ({noun}, and the initial one)',
        len(snapshots) >= expected,
        f'{len(snapshots)} of {expected} expected from the build',
    )

    summary = repository.garbage_collect(
        datetime.datetime.now(datetime.timezone.utc), dry_run=True
    )
    report.note(
        f'unreachable objects (DRY RUN, nothing deleted): '
        f'{summary.bytes_deleted / 1024**3:.2f} GiB, '
        f'{summary.chunks_deleted} chunks, {summary.snapshots_deleted} snapshots'
    )


def check_index(report, dataset, index, settings):
    """Check the time axis and the spatial coordinates against raw.

    Args:
        report (Report): Where to record outcomes.
        dataset (xarray.Dataset): The decoded store.
        index (dict): The raw index.
        settings (dict): The loaded configuration.
    """
    stored = dataset.indexes['time']
    expected = index['expected']
    report.check(
        'time axis is exactly the complete expected axis',
        stored.equals(expected),
        f'{len(stored)} steps {stored[0].date()}..{stored[-1].date()} '
        f'against {len(expected)} expected',
    )
    steps = np.unique(np.diff(stored.values))
    report.check(
        'time axis is regularly spaced',
        len(steps) == 1,
        f'{len(steps)} distinct step(s)',
    )
    duration = file_naming(settings).duration
    report.check(
        f'time_coverage_resolution is {duration}',
        dataset.attrs.get('time_coverage_resolution') == duration,
        repr(dataset.attrs.get('time_coverage_resolution')),
    )

    # lat/lon come from the first raw file and must survive untouched
    first = next(p for p in index['paths'] if p is not None)
    raw = netCDF4.Dataset(first)
    raw.set_auto_mask(False)
    for name in ('lat', 'lon'):
        report.check(
            f'{name} is bit-identical to raw',
            np.array_equal(
                dataset[name].values.astype(np.float32),
                np.asarray(raw.variables[name][:]).astype(np.float32),
            ),
            f'{dataset.sizes[name]} values',
        )
    raw.close()

    # every timestep the release never published must be present and all-NaN
    variable = sorted(dataset.data_vars)[0]
    if index['missing']:
        bad = []
        for position in index['missing']:
            plane = dataset[variable].isel(time=position).values
            if not bool(np.isnan(plane).all()):
                bad.append(str(stored[position].date()))
        report.check(
            f'the {len(index["missing"])} timesteps with no raw file are all-NaN',
            not bad,
            f'not all-NaN at {bad}' if bad else
            ', '.join(str(stored[p].date()) for p in index['missing']),
        )
    else:
        report.note('no timestep is missing from raw; the record is complete')


def sample_boxes(dataset, chunks, layout, args, rng):
    """Choose the boxes to compare against raw.

    Always includes the first and last position, since an off-by-one at either
    end of the record is the failure this is most likely to catch.

    Args:
        dataset (xarray.Dataset): The decoded store.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
        args (argparse.Namespace): Parsed arguments.
        rng (numpy.random.Generator): Source of randomness.

    Returns:
        list: ``(t0, t1, lat slice, lon slice)`` tuples.
    """
    sizes = dataset.sizes
    n_time = sizes['time']
    if layout == 'spatial':
        positions = [0, n_time - 1]
        positions += list(rng.choice(n_time, size=max(0, args.samples - 2), replace=False))
        return [
            (int(t), int(t) + 1, slice(None), slice(None))
            for t in sorted(set(positions))
        ]

    grid = tile_grid(chunks, sizes)
    span = min(args.box_timesteps, n_time)
    boxes = []
    # corners first: the tile at the grid origin is the one holding the V3.16
    # corner block, and the last tile is the far end of both spatial axes
    picks = [(0, 0), (grid[0] - 1, grid[1] - 1)]
    picks += [
        (int(rng.integers(grid[0])), int(rng.integers(grid[1])))
        for _ in range(max(0, args.samples - 2))
    ]
    for i, j in picks:
        t0 = int(rng.integers(0, max(1, n_time - span + 1)))
        boxes.append(
            (
                t0,
                min(t0 + span, n_time),
                slice(i * chunks['lat'], (i + 1) * chunks['lat']),
                slice(j * chunks['lon'], (j + 1) * chunks['lon']),
            )
        )
    return boxes


def check_samples(report, dataset, index, args, chunks, layout, sentinels):
    """Compare sampled boxes against raw, cell by cell.

    Args:
        report (Report): Where to record outcomes.
        dataset (xarray.Dataset): The decoded store.
        index (dict): The raw index.
        args (argparse.Namespace): Parsed arguments.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
        sentinels (list): Values the raw files use as fill.
    """
    variable = sorted(dataset.data_vars)[0]
    rng = np.random.default_rng(args.seed)
    boxes = sample_boxes(dataset, chunks, layout, args, rng)
    dates = dataset['time'].values
    if layout == 'temporal':
        span = min(args.box_timesteps, dataset.sizes['time'])
        report.note(
            f'a temporal box spans {span} of the record\'s '
            f'{dataset.sizes["time"]} days for one tile, so each box opens '
            f'{span} raw files'
            + ('' if span == dataset.sizes['time']
               else f' rather than all {dataset.sizes["time"]}')
        )

    total_valid = 0
    for t0, t1, lat, lon in boxes:
        stored = dataset[variable].isel(
            time=slice(t0, t1), lat=lat, lon=lon
        ).values
        raw = read_raw_box(index, variable, t0, t1, lat, lon)
        mask_ok, values_ok, n_valid = compare_box(stored, raw, sentinels)
        total_valid += n_valid
        label = (
            f'{variable} box t[{t0}:{t1}) '
            f'lat[{lat.start or 0}:{lat.stop if lat.stop is not None else "end"}) '
            f'lon[{lon.start or 0}:{lon.stop if lon.stop is not None else "end"})'
        )
        report.check(
            f'{label} fill pattern and values match raw',
            mask_ok and values_ok,
            f'{str(dates[t0])[:10]}, {n_valid} valid cells'
            + ('' if mask_ok else ', MASK MISMATCH')
            + ('' if values_ok else ', VALUE MISMATCH'),
        )
    report.note(
        f'{len(boxes)} boxes compared, {total_valid:,} valid cells bit-checked'
    )


async def check_sweep(report, session, dataset, index, chunks, layout, args, sentinels):
    """Check every chunk in the store for presence, placement and size.

    Runs off the manifest rather than by reading data, so it covers the whole
    store in seconds. An absent chunk is not assumed to be a defect: zarr does
    not write a chunk whose cells are all equal to the fill value, so a chunk
    holding nothing but fill correctly leaves a hole that reads back as NaN.

    What justifies a hole depends on the layout. On the spatial layout a chunk is
    one day, so a hole is a day and every one is traced to its own raw plane. On
    the temporal layout a chunk is a tile through the whole record, so a hole
    claims the tile never held data at all -- which no affordable read can
    confirm exhaustively. It is checked in the direction that matters instead:
    every tile holding data on any sampled day must have been written.

    Args:
        report (Report): Where to record outcomes.
        session (icechunk.Session): Read-only session on the store.
        dataset (xarray.Dataset): The decoded store.
        index (dict): The raw index.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
        args (argparse.Namespace): Parsed arguments.
        sentinels (list): Values the raw files use as fill.
    """
    sizes = dataset.sizes
    n_time = sizes['time']
    dates = dataset['time'].values
    grid = tile_grid(chunks, sizes)
    n_time_chunks = -(-n_time // chunks['time'])
    n_grid = n_time_chunks * grid[0] * grid[1]
    floor = DEGENERATE_BYTES[layout]
    variable = sorted(dataset.data_vars)[0]

    coordinates = [c async for c in session.chunk_coordinates(f'/{variable}')]
    report.check(
        f'every chunk sits inside the {n_time_chunks}x{grid[0]}x{grid[1]} grid, '
        f'none duplicated',
        len(coordinates) == len(set(coordinates))
        and all(
            0 <= c[0] < n_time_chunks and 0 <= c[1] < grid[0] and 0 <= c[2] < grid[1]
            for c in coordinates
        ),
        f'{len(coordinates)} of {n_grid} grid positions written',
    )
    if not coordinates:
        report.check('the variable has at least one written chunk', False, 'manifest empty')
        return

    chunk_sizes = np.asarray(
        await asyncio.gather(
            *(session.store.getsize(f'{variable}/c/{t}/{i}/{j}') for t, i, j in coordinates)
        )
    )
    degenerate = [coordinates[k] for k in np.flatnonzero(chunk_sizes <= floor)]
    report.check(
        f'no written chunk is degenerate or truncated (> {floor / 1024:g} KiB)',
        not degenerate,
        f'compressed min={chunk_sizes.min() / 1024**2:.3f} '
        f'median={np.median(chunk_sizes) / 1024**2:.3f} '
        f'max={chunk_sizes.max() / 1024**2:.3f} MiB'
        + (f', degenerate at {degenerate[:5]}' if degenerate else ''),
    )
    report.note(
        f'{len(coordinates)} chunks, {chunk_sizes.sum() / 1024**3:.1f} GiB compressed'
    )

    if layout == 'spatial':
        # a chunk is a day, so a hole is a day and every one is traceable
        present = {c[0] for c in coordinates}
        missing = sorted(set(range(n_time)) - present)
        if not missing:
            report.check('every timestep has a written chunk', True, f'{n_time} of {n_time}')
            return
        real_loss = []
        for timestep in missing:
            raw = read_raw_box(index, variable, timestep, timestep + 1)
            all_fill = np.isnan(raw)
            for sentinel in sentinels:
                all_fill |= raw == sentinel
            if not bool(all_fill.all()):
                real_loss.append(str(dates[timestep])[:10])
        report.check(
            'every absent chunk is an all-fill raw plane',
            not real_loss,
            f'{len(missing)} absent, raw all-fill at every one'
            if not real_loss
            else f'DATA LOSS at {real_loss[:5]}',
        )
        report.note(
            f'no chunk on {len(missing)} day(s) because raw is entirely fill or '
            f'absent there: {", ".join(str(dates[t])[:10] for t in missing)}'
        )
        return

    # temporal: a hole claims a tile was empty for the whole record
    if not grid_divides(chunks, sizes):
        report.note(
            'absent chunks not checked: the grid is not a whole number of '
            'chunks, so a raw plane cannot be coarsened onto it'
        )
        return
    sample_days = sorted(
        set(int(x) for x in np.linspace(0, n_time - 1, args.mask_days).round())
    )
    sample_days = [t for t in sample_days if index['paths'][t] is not None]
    ever = np.zeros(grid, dtype=bool)
    for timestep in sample_days:
        ever |= coarsen_to_tiles(
            read_raw_box(index, variable, timestep, timestep + 1)[0],
            chunks, grid, sentinels,
        )
    written = np.zeros(grid, dtype=bool)
    for _, i, j in coordinates:
        written[i, j] = True
    unwritten_with_data = np.argwhere(ever & ~written)
    report.check(
        'every tile holding data on a sampled raw day was written',
        len(unwritten_with_data) == 0,
        f'{int(ever.sum())} tiles hold data on {len(sample_days)} sampled days, '
        f'{int(written.sum())} written'
        + (f', MISSING at {unwritten_with_data[:5].tolist()}'
           if len(unwritten_with_data) else ''),
    )
    absent = int((~written).sum())
    report.note(
        f'{absent} of {grid[0] * grid[1]} tiles have no chunk; none of them held '
        f'data on any of the {len(sample_days)} sampled days'
    )

    # a size floor cannot tell a sparse tile from a truncated one here, so the
    # smallest written chunks are read back and compared against raw instead
    order = np.argsort(chunk_sizes)[: args.smallest]
    for k in order:
        _, i, j = coordinates[int(k)]
        lat = slice(i * chunks['lat'], (i + 1) * chunks['lat'])
        lon = slice(j * chunks['lon'], (j + 1) * chunks['lon'])
        span = min(args.box_timesteps, n_time)
        stored = dataset[variable].isel(time=slice(0, span), lat=lat, lon=lon).values
        raw = read_raw_box(index, variable, 0, span, lat, lon)
        mask_ok, values_ok, n_valid = compare_box(stored, raw, sentinels)
        report.check(
            f'smallest chunk tile[{i},{j}] ({chunk_sizes[int(k)] / 1024:.1f} KiB) '
            f'matches raw over its first {span} days',
            mask_ok and values_ok,
            f'{n_valid} valid cells',
        )


def check_sentinel(report, dataset, index, args, sentinels, settings, chunks, layout):
    """Check that no undeclared fill sentinel survived into the store.

    The build only looks at the first timestep, deliberately -- proving a value
    occurs nowhere else is a full-coverage question and belongs here. It is not
    free, so this samples rather than reads the record, and what it samples it
    settles exactly.

    Two directions are checked, and they cost very differently:

    **From raw**, any negative value on a sampled plane must be a sentinel the
    config declares, or the config is incomplete. One raw plane is one file, so
    this is cheap whatever the store's layout is. This is the direction that
    would catch a release changing its sentinel.

    **From the store**, no value below the physical floor may survive, which is
    what an undeclared sentinel would look like after masking. This one *must*
    follow the store's chunking. Reading a whole plane out of a temporal store
    touches every chunk of the variable -- 86 GiB for one map of V3.16 Past --
    so the unit here is a plane on the spatial layout and a tile through the
    record on the temporal one. Both are exactly one chunk.

    Args:
        report (Report): Where to record outcomes.
        dataset (xarray.Dataset): The decoded store.
        index (dict): The raw index.
        args (argparse.Namespace): Parsed arguments.
        sentinels (list): Values the raw files use as fill.
        settings (dict): The loaded configuration.
        chunks (dict): Resolved chunk sizes.
        layout (str): From ``store_layout``.
    """
    variable = sorted(dataset.data_vars)[0]
    sizes = dataset.sizes
    n_time = sizes['time']
    dates = dataset['time'].values
    missing = set(index['missing'])

    # --- from raw: one file per plane, cheap on either layout
    planes = sorted(
        set(int(x) for x in np.linspace(0, n_time - 1, args.sentinel_planes).round())
    )
    planes = [t for t in planes if index['paths'][t] is not None]
    undeclared = {}
    for timestep in planes:
        raw = read_raw_box(index, variable, timestep, timestep + 1)[0]
        for value in np.unique(raw[raw < PRECIPITATION_FLOOR]):
            if not any(value == sentinel for sentinel in sentinels):
                undeclared.setdefault(float(value), []).append(
                    str(dates[timestep])[:10]
                )
    report.check(
        f'every negative raw value on {len(planes)} sampled planes is a '
        f'declared sentinel',
        not undeclared,
        f'declared {[float(x) for x in sentinels]}'
        if not undeclared
        else f'UNDECLARED {dict(list(undeclared.items())[:3])}',
    )

    # --- from the store: read one chunk at a time, in the layout's own unit
    rng = np.random.default_rng(args.seed)
    if layout == 'spatial':
        units = [
            (timestep, timestep + 1, slice(None), slice(None)) for timestep in planes
        ]
        unit_name = 'plane'
    else:
        grid = tile_grid(chunks, sizes)
        # the origin tile first: on V3.16 Past it sits inside the corner block,
        # so it is the one that must be entirely masked
        picks = [(0, 0)]
        picks += [
            (int(rng.integers(grid[0])), int(rng.integers(grid[1])))
            for _ in range(max(0, args.sentinel_planes // 4))
        ]
        units = [
            (
                0,
                n_time,
                slice(i * chunks['lat'], (i + 1) * chunks['lat']),
                slice(j * chunks['lon'], (j + 1) * chunks['lon']),
            )
            for i, j in picks
        ]
        unit_name = 'tile'
        report.note(
            f'reading {len(units)} whole-record tiles from the store rather than '
            f'planes; one plane out of this layout would touch all '
            f'{grid[0] * grid[1]} chunks'
        )

    low, high, footprints, moving = [], [], set(), []
    for t0, t1, lat, lon in units:
        stored = dataset[variable].isel(time=slice(t0, t1), lat=lat, lon=lon).values
        finite = stored[np.isfinite(stored)]
        where = f'{str(dates[t0])[:10]}' if layout == 'spatial' else \
                f'lat[{lat.start}:{lat.stop}) lon[{lon.start}:{lon.stop})'
        if finite.size and float(finite.min()) < PRECIPITATION_FLOOR:
            low.append((where, float(finite.min())))
        if finite.size and float(finite.max()) > PRECIPITATION_CEILING:
            high.append((where, float(finite.max())))

        if layout == 'spatial':
            footprints.add(int(np.isnan(stored).sum()))
        else:
            # a tile's masked footprint must not move through the record. The
            # timesteps the release never published are all-NaN everywhere and
            # would otherwise look like a moving footprint, so they are excluded
            masks = {
                np.isnan(stored[k]).tobytes()
                for k in range(stored.shape[0])
                if (t0 + k) not in missing
            }
            if len(masks) > 1:
                moving.append(where)
            footprints.add(int(np.isnan(stored[0]).sum()) if 0 not in missing else -1)

    report.check(
        f'no stored value is below {PRECIPITATION_FLOOR} mm/day across '
        f'{len(units)} {unit_name}s',
        not low,
        'every finite value is non-negative' if not low else f'NEGATIVE at {low[:5]}',
    )
    if high:
        report.note(
            f'{len(high)} {unit_name}(s) exceed {PRECIPITATION_CEILING} mm/day: '
            f'{high[:5]} -- above the world daily rainfall record, worth a look '
            f'rather than automatically wrong for a gridded product'
        )
    else:
        report.note(f'no sampled {unit_name} exceeds {PRECIPITATION_CEILING} mm/day')

    if layout == 'temporal':
        report.check(
            f'each sampled tile has the same masked footprint at every '
            f'published timestep',
            not moving,
            'the mask does not move through the record'
            if not moving
            else f'MOVING at {moving[:5]}',
        )
    else:
        declared = settings.get('source_fill_values') or []
        report.check(
            'the masked footprint is the same on every sampled plane',
            len(footprints) == 1,
            f'{sorted(footprints)[:5]} NaN cells per plane',
        )
        if declared:
            report.note(
                f'masking {sorted(footprints)[0]} cells per plane '
                f'({sorted(footprints)[0] / (sizes["lat"] * sizes["lon"]) * 100:.3f}%'
                f' of the grid)'
            )


def check_metadata(report, dataset, other_settings):
    """Compare the store's metadata against a sibling store's.

    Two stores built from the same raw files must describe the same data the
    same way. Attributes in ``LAYOUT_ATTRS`` legitimately differ, because they
    describe the layout rather than the data; everything else must agree.

    Args:
        report (Report): Where to record outcomes.
        dataset (xarray.Dataset): The decoded store.
        other_settings (dict): Config of the store to compare against.
    """
    path = store_path(other_settings)
    repository = icechunk.Repository.open(icechunk.local_filesystem_storage(path))
    other = xr.open_zarr(
        repository.readonly_session(branch=BRANCH).store, consolidated=False
    )
    LOG.info(f'comparing against {path.split("/")[-1]}')

    for name in ('time', 'lat', 'lon'):
        report.check(
            f'{name} coordinate is identical to the sibling store',
            dataset[name].equals(other[name]),
            f'{dataset.sizes[name]} vs {other.sizes[name]}',
        )
    report.check(
        'both stores hold the same data variables',
        sorted(dataset.data_vars) == sorted(other.data_vars),
        f'{sorted(dataset.data_vars)} vs {sorted(other.data_vars)}',
    )

    shared = (set(dataset.attrs) | set(other.attrs)) - LAYOUT_ATTRS
    differing = [
        a for a in sorted(shared) if dataset.attrs.get(a) != other.attrs.get(a)
    ]
    report.check(
        f'the {len(shared)} non-layout global attributes agree',
        not differing,
        f'differ on {differing}' if differing else '',
    )
    for name in sorted(dataset.data_vars):
        if name in other.data_vars:
            mine, theirs = dict(dataset[name].attrs), dict(other[name].attrs)
            report.check(
                f'{name} variable attributes agree with the sibling store',
                mine == theirs,
                '' if mine == theirs else f'{mine} vs {theirs}',
            )


def main(settings, args):
    """Run the selected phases and return a process exit status.

    Args:
        settings (dict): The loaded configuration.
        args (argparse.Namespace): Parsed arguments.

    Returns:
        int: 0 if every check passed, 1 otherwise.

    Raises:
        ValueError: If --phases names something that is not a phase, or names
            the metadata phase without a store to compare against.
    """
    phases = [p.strip() for p in args.phases.split(',') if p.strip()]
    unknown = [p for p in phases if p not in PHASES]
    if unknown:
        raise ValueError(f'unknown phases {unknown}; choose from {list(PHASES)}')
    if 'metadata' in phases and not args.compare_with:
        raise ValueError('the metadata phase needs --compare-with <config>')

    report = Report()
    repository, session, dataset = open_store(settings)
    chunks = resolve_chunks(settings['chunks'], dataset.sizes)
    layout = store_layout(chunks, dataset.sizes)
    sentinels = sentinel_values(settings)
    LOG.info(f'phases: {phases}, chunks {chunks}, layout {layout!r}')

    # every phase but 'structure' and 'metadata' traces store values back to raw
    index = (
        build_raw_index(settings)
        if set(phases) - {'structure', 'metadata'}
        else {}
    )

    if 'structure' in phases:
        LOG.info('--- structure')
        check_structure(report, dataset, settings, chunks, layout)
        check_repository(report, repository, dataset, settings, chunks, layout)
    if 'index' in phases:
        LOG.info('--- index')
        check_index(report, dataset, index, settings)
    if 'samples' in phases:
        LOG.info('--- samples')
        check_samples(report, dataset, index, args, chunks, layout, sentinels)
    if 'sweep' in phases:
        LOG.info('--- sweep')
        asyncio.run(
            check_sweep(report, session, dataset, index, chunks, layout, args, sentinels)
        )
    if 'sentinel' in phases:
        LOG.info('--- sentinel')
        check_sentinel(
            report, dataset, index, args, sentinels, settings, chunks, layout
        )
    if 'metadata' in phases:
        LOG.info('--- metadata')
        check_metadata(report, dataset, load_config(args.compare_with))

    LOG.info(f'{report.n_checks} checks, {len(report.failures)} failures')
    if report.failures:
        for label in report.failures:
            LOG.error(f'failed: {label}')
        return 1
    LOG.info('store verified :-)')
    return 0


if __name__ == '__main__':
    arguments = parse_args()
    configuration = load_config(arguments.config)
    # named after the store's own log file, so two stores' forensics do not
    # interleave in one file
    setup_logging(
        os.path.join(
            configuration['directories']['logs'],
            f'verify_{configuration["log_file"]}',
        )
    )
    sys.exit(main(configuration, arguments))
