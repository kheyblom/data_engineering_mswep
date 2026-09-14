"""Helpers for building icechunk backed zarr stores from netCDF inputs.

Either way a store is written, it is written in pieces, each committed to the
icechunk repository before the next one starts. That is what makes a run
restartable, which matters when a store is large enough that a single write will
not fit in one job's walltime. Which piece is the unit depends on how the store
is chunked, and ``write_strategy`` in the config picks between the two:

``append`` is for a store chunked shallowly along time, where a timestep is
cheap to write and the whole spatial grid is not. The dataset is assembled
lazily and pushed out in batches of timesteps, each appended to the last. A
batch is streamed chunk by chunk rather than held whole, so peak memory follows
the chunks in flight, which ``configure_runtime`` bounds. Batch boundaries are
always a whole number of time chunks: appending onto a partially filled chunk
would mean rewriting it, which xarray refuses to do from dask, so only the final
batch is allowed to be short.

``region`` is for a store chunked along the whole time axis, where appending is
impossible -- every chunk spans every timestep, so there is no boundary to
append at and a batched write collapses into one uninterruptible commit. Instead
the store's metadata and coordinates are laid down first as a skeleton, and the
arrays are then filled in place, one lat/lon block at a time across the full
record. A block is read whole into memory rather than streamed, so peak memory
is the block, and the block is deliberately much larger than an output chunk:
the raw files are chunked coarsely in space, so a small block re-reads and
re-decompresses the same source chunk for every tile it overlaps.

Two things here differ from the GLEAM pipeline this is modelled on, both
because MSWEP holds one timestep per file rather than one year:

- The dataset is opened from ~17,000 files, so the dask chunk structure those
  files arrive with is one chunk per *timestep*. ``read_into_buffer`` therefore
  reads a configurable number of timesteps at a time rather than walking the
  dask chunks, which in GLEAM were whole years and here would mean ~17,000
  separate scheduler round trips per block.
- The raw tree can be missing a timestep outright rather than holding an
  all-fill one, so ``fill_missing_times`` splices an all-NaN plane in at each
  gap. It is spliced rather than reindexed so the dask chunk structure the
  files arrive with survives intact.
"""

from __future__ import annotations

import logging
import re

import dask
from dask.system import CPU_COUNT
import netCDF4
import numpy as np
import pandas as pd
import xarray as xr
import icechunk
from icechunk.xarray import to_icechunk

LOG = logging.getLogger(__name__)

# timesteps written per commit when the config does not say otherwise; small
# enough to keep a single batch cheap to redo after a failure, large enough that
# commit overhead stays negligible
DEFAULT_TIMESTEPS_PER_COMMIT = 100

# timesteps pulled out of the lazy dataset per read on the region path. One
# MSWEP file is one timestep, so the dask chunks along time are one step each
# and reading a block chunk by chunk would be one scheduler round trip per file
# -- ~17,000 of them per block. Reading a year at a time instead gives dask
# something worth parallelising and costs about 1 GiB of transient buffer on
# top of the block at the production block shape.
DEFAULT_TIMESTEPS_PER_READ = 365

# chunk cache slots, passed to HDF5 alongside chunk_cache_size_mib. The MSWEP
# v3.16 files are chunked [1, 200, 200], so one timestep is 9 x 18 = 162 chunks
# and the cache never needs to hold many; the default is generous and a prime
# spreads the hashed slots evenly.
DEFAULT_CHUNK_CACHE_SLOTS = 2003

# branch every commit is written to
BRANCH = 'main'

# how a store is filled. 'append' extends the time axis batch by batch; 'region'
# lays down a skeleton and fills it block by block. See the module docstring.
DEFAULT_WRITE_STRATEGY = 'append'
WRITE_STRATEGIES = ('append', 'region')

# netCDF encoding keys that describe the data rather than its container, and so
# are provenance worth keeping. 'least_significant_digit' records that MSWEP
# quantised the values before compressing them -- 2 decimal places in v3.16, 1
# in v2.8 -- which is what a downstream consumer needs in order to know how
# much of the float32 precision is real. The zarr backend rejects the key, and
# build_encoding clears the encoding wholesale, so it is moved into the
# variable's attributes first rather than lost.
CARRIED_ENCODING_KEYS = ('least_significant_digit',)

# the region path's resume state lives in the commit messages rather than in the
# store, so that reading it back needs nothing but the history icechunk already
# keeps, and so finalize_mswep_zarr.py does not have to strip build bookkeeping
# out of the attributes it publishes. The pair has to stay in step.
SKELETON_MESSAGE = 'create skeleton'
BLOCK_MESSAGE = 'write {variable} lat[{lat0}:{lat1}) lon[{lon0}:{lon1})'
BLOCK_MESSAGE_RE = re.compile(
    r'^write (?P<variable>\S+) '
    r'lat\[(?P<lat0>\d+):(?P<lat1>\d+)\) '
    r'lon\[(?P<lon0>\d+):(?P<lon1>\d+)\)$'
)


def configure_runtime(settings):
    """Apply the optional execution settings that bound cost and peak memory.

    None of these change the store that is written, only what it costs to write
    it. Dask's worker count sets how many chunks are held in flight at once;
    xarray's open file cache sets how many netCDF files stay open; and the HDF5
    chunk cache sets how much decompressed data each of those files keeps. All
    are left to the library default when the config does not set them, so an
    unset ``num_workers`` still follows the cpuset the batch job was given.

    The last two multiply: the chunk cache is per open file, so the memory this
    can reach is roughly ``file_cache_maxsize`` x ``chunk_cache_size_mib``. That
    product matters more here than it did in GLEAM, because a run holds far more
    files open.

    Note ``chunk_cache_size_mib`` is *not* the dominant setting it was in GLEAM.
    There the raw files were chunked twelve timesteps deep while the store was
    written one timestep at a time, so a cache too small to hold a whole
    12-deep row meant decompressing every source chunk twelve times -- worth
    7.6x end to end. MSWEP source chunks are one timestep deep ([1, 200, 200]),
    so there is nothing to re-decompress and the cache has no such job to do.

    Args:
        settings (dict): The loaded configuration.
    """
    num_workers = settings.get('num_workers')
    if num_workers:
        dask.config.set(num_workers=num_workers)

    file_cache_maxsize = settings.get('file_cache_maxsize')
    if file_cache_maxsize:
        xr.set_options(file_cache_maxsize=file_cache_maxsize)

    chunk_cache_size_mib = settings.get('chunk_cache_size_mib')
    if chunk_cache_size_mib:
        netCDF4.set_chunk_cache(
            size=chunk_cache_size_mib * 1024**2,
            nelems=settings.get('chunk_cache_slots', DEFAULT_CHUNK_CACHE_SLOTS),
            # keep fully read chunks in preference to partially read ones
            preemption=0.75,
        )

    # log what was resolved rather than what was asked for, so a run killed for
    # running out of memory can be read back against the settings it really used
    LOG.info(
        f'dask workers: {num_workers or CPU_COUNT}'
        f'{"" if num_workers else " (detected)"}, '
        f'open file cache: {xr.get_options()["file_cache_maxsize"]} files, '
        f'hdf5 chunk cache: {chunk_cache_size_mib or "default"} MiB per file'
    )


def resolve_chunks(chunks, sizes):
    """Turn the configured chunk sizes into concrete lengths.

    A chunk of -1 means 'the whole dimension', following the dask convention
    used in the config.

    Every dimension has to be named. That is not pedantry: the same dict is
    handed to ``open_mfdataset`` on the append path, and a dimension left out
    there does not default to the whole axis -- it inherits the *file's* own
    chunking. The MSWEP files are chunked [1, 200, 200], so omitting lat and lon
    silently turns one dask chunk per file into 9 x 18 = 162 of them, which over
    the full record is millions of tasks rather than tens of thousands. It was
    measured at 118 ms per file to open against 5 ms with all three named.

    Args:
        chunks (dict): Chunk size per dimension, as written in the config.
        sizes (Mapping): Length of each dimension, e.g. ``dataset.sizes``.

    Returns:
        dict: Chunk size per dimension, with -1 replaced by the dimension
            length.

    Raises:
        ValueError: If a chunked dimension is not present in the dataset, or a
            dimension of the dataset is not chunked.
    """
    missing = [dimension for dimension in sizes if dimension not in chunks]
    if missing:
        raise ValueError(
            f'config does not chunk {missing}; every dimension must be named '
            f'(use -1 for the whole dimension), because an unnamed one is read '
            f"with the netCDF file's own chunking rather than whole"
        )

    resolved = {}
    for dimension, size in chunks.items():
        if dimension not in sizes:
            raise ValueError(
                f'config chunks a dimension {dimension!r} that the data does '
                f'not have; dimensions are {list(sizes)}'
            )
        resolved[dimension] = sizes[dimension] if size == -1 else size
    return resolved


def commit_batch_size(chunks, settings):
    """Number of timesteps to write per commit, rounded to whole time chunks.

    Args:
        chunks (dict): Resolved chunk sizes, must include 'time'.
        settings (dict): The loaded configuration; an optional
            ``timesteps_per_commit`` key overrides the default target.

    Returns:
        int: A positive multiple of the time chunk size.
    """
    target = settings.get('timesteps_per_commit', DEFAULT_TIMESTEPS_PER_COMMIT)
    time_chunk = chunks['time']
    # round to the nearest whole number of chunks, but never down to zero
    n_chunks = max(1, round(target / time_chunk))
    return n_chunks * time_chunk


def open_files(files, chunks):
    """Open the raw files as a single lazy dataset concatenated along time.

    Args:
        files (Sequence): Paths to the netCDF files, in time order.
        chunks (dict): Chunk sizes to open with, naming every dimension.

    Returns:
        xarray.Dataset: The files concatenated along time, dask backed.
    """
    return xr.open_mfdataset(
        files,
        # the caller has already sorted the files by timestamp, so concatenate
        # them in the order given rather than paying to inspect 17,000 sets of
        # coordinates for an ordering that is already known
        combine='nested',
        concat_dim='time',
        chunks=chunks,
        # only time varying variables differ between files; taking the lat/lon
        # coordinates from the first file avoids reading them once per file.
        # note parallel=True is deliberately left off: opening netCDF files from
        # several threads crashes the HDF5 library underneath this build
        data_vars='minimal',
        coords='minimal',
        compat='override',
    )


def discover_variables(dataset, requested):
    """Return the variables to write, expanding the ``all`` shorthand.

    Unlike GLEAM, where each variable was its own directory of files, MSWEP
    carries its variables inside the file, so this is resolved against an open
    dataset rather than against the tree.

    Args:
        dataset (xarray.Dataset): A dataset opened from the raw files.
        requested: ``'all'``, or a list of variable names from the config.

    Returns:
        list: The variable names to write, in a stable order.

    Raises:
        ValueError: If a requested variable is not in the files, or the files
            hold no data variables at all.
    """
    available = sorted(dataset.data_vars)
    if isinstance(requested, str) and requested == 'all':
        variables = available
    else:
        missing = [name for name in requested if name not in available]
        if missing:
            raise ValueError(
                f'variables {missing} are not in the raw files; '
                f'available: {available}'
            )
        variables = list(requested)

    if not variables:
        raise ValueError('the raw files hold no data variables')
    return variables


def fill_missing_times(dataset, expected):
    """Splice an all-fill plane in at every timestep the raw tree lacks.

    The store is written on the complete ``expected`` axis so that its spacing
    is regular and downstream date arithmetic cannot land on the wrong
    timestep. ``check_coverage`` has already established that the gaps are the
    ones the config declares, so what is inserted here is a documented upstream
    absence rather than an unnoticed hole.

    The planes are spliced with ``concat`` rather than filled by
    ``reindex(time=expected)``, which produces the same values. ``reindex``
    rebuilds the array through a fancy index over the whole time axis, which
    means a larger graph and no guarantee about the chunk structure that comes
    out; splicing leaves every existing segment exactly as ``open_mfdataset``
    chunked it and adds one chunk per gap. With only a handful of gaps the
    concat is over a handful of pieces and costs nothing.

    Args:
        dataset (xarray.Dataset): The lazily opened raw files.
        expected (pandas.DatetimeIndex): The complete axis to write.

    Returns:
        xarray.Dataset: The dataset on the ``expected`` axis, with the absent
            timesteps present and all-NaN.

    Raises:
        ValueError: If the result does not land exactly on ``expected``.
    """
    present = dataset.indexes['time']
    missing = expected.difference(present)
    if not len(missing):
        return dataset

    LOG.info(
        f'splicing {len(missing)} absent timesteps in as all-fill: '
        f'{[str(stamp.date()) for stamp in missing]}'
    )

    pieces, start = [], 0
    for stamp in missing:
        position = present.searchsorted(stamp)
        if position > start:
            pieces.append(dataset.isel(time=slice(start, position)))
        # full_like on a one step slice keeps the dtype, dims and chunking of a
        # real plane, so the gap is indistinguishable from a raw plane that
        # happens to be entirely fill
        plane = xr.full_like(dataset.isel(time=slice(0, 1)), np.nan)
        pieces.append(plane.assign_coords(time=[stamp]))
        start = position
    pieces.append(dataset.isel(time=slice(start, None)))

    filled = xr.concat(
        pieces,
        dim='time',
        data_vars='minimal',
        coords='minimal',
        compat='override',
    )

    if not filled.indexes['time'].equals(expected):
        raise ValueError(
            f'splicing the gaps produced {filled.sizes["time"]} timesteps that '
            f'do not match the {len(expected)} expected; this is a bug in '
            f'fill_missing_times, not a data problem'
        )
    return filled


def carry_source_encoding(dataset):
    """Move provenance out of the netCDF encoding and into the attributes.

    ``build_encoding`` clears every variable's encoding, because the HDF5
    settings xarray carries over are rejected by the zarr backend. A few of
    those keys describe the *data* rather than its container and would be lost
    with the rest, so they are promoted to attributes first. See
    ``CARRIED_ENCODING_KEYS``.

    Args:
        dataset (xarray.Dataset): The dataset about to be written; modified in
            place.

    Returns:
        xarray.Dataset: The same dataset, for chaining.
    """
    for name, variable in dataset.variables.items():
        for key in CARRIED_ENCODING_KEYS:
            if key in variable.encoding and key not in variable.attrs:
                variable.attrs[key] = variable.encoding[key]
                LOG.info(f'carried {key}={variable.encoding[key]!r} onto {name}')
    return dataset


def check_source_fill(dataset, fill_values):
    """Confirm each declared undeclared-fill sentinel actually occurs.

    ``source_fill_values`` names values the source files use as fill without
    declaring them in ``_FillValue``, which the CF decoding therefore leaves in
    the data. A value listed there that does not occur is far more likely to be
    a typo, or a sentinel that changed between releases, than a gap that closed
    -- and either way it would mask nothing while the config claims it did.

    Checked against the first timestep only, which is one plane rather than the
    record. That is enough to catch a wrong value, since these sentinels mark a
    tile the release never produced and so are present on every timestep;
    proving they occur *nowhere else* is a full-coverage question and belongs to
    the verifier.

    Args:
        dataset (xarray.Dataset): The raw dataset, before any masking.
        fill_values (Sequence): The declared sentinels.

    Raises:
        ValueError: If a declared sentinel does not occur on the first timestep.
    """
    if not fill_values:
        return

    first = dataset.isel(time=0)
    for name, array in first.data_vars.items():
        values = array.values
        for value in fill_values:
            count = int((values == np.asarray(value, dtype=values.dtype)).sum())
            if not count:
                raise ValueError(
                    f'source_fill_values declares {value!r}, which does not '
                    f'occur in {name} on the first timestep; it is either a '
                    f'typo or a sentinel this release no longer uses, and as '
                    f'written it would mask nothing'
                )
            LOG.info(
                f'{name}: sentinel {value!r} occurs on {count} cells '
                f'({count / values.size * 100:.3f}%) of the first timestep'
            )


def mask_source_fill(dataset, fill_values):
    """Mask values the source files use as fill but do not declare as such.

    MSWEP V3.16 marks a 150x150 block at the corner of the grid -- the whole
    record, every day -- with -239976.0, which is the declared ``_FillValue`` of
    -9999 summed over the 24 hourly steps the daily total is built from. Because
    the value in the data is not the value in the attribute, xarray's CF masking
    does not touch it, and it would otherwise be written to the store as though
    it were a precipitation measurement four orders of magnitude below zero.

    Declared in the config rather than detected, for the same reason
    ``expected_missing_times`` is: a rule like 'mask anything negative' would
    also hide a defect nobody has noticed yet, while a named value fails loudly
    when the next release changes it. ``check_source_fill`` is the other half of
    that.

    Args:
        dataset (xarray.Dataset): The dataset to mask; modified in place.
        fill_values (Sequence): Values to replace with NaN.

    Returns:
        xarray.Dataset: The same dataset, for chaining.
    """
    if not fill_values:
        return dataset

    for name in list(dataset.data_vars):
        array = dataset[name]
        dtype = array.dtype
        condition = None
        for value in fill_values:
            match = array == np.asarray(value, dtype=dtype)
            condition = match if condition is None else (condition | match)
        # astype rather than trusting the promotion: under NEP 50 a python float
        # is weak and float32 survives, but the store's size on disk doubles if
        # that ever stops being true, so it is pinned rather than assumed
        dataset[name] = array.where(~condition).astype(dtype, copy=False)
        dataset[name].attrs = dict(array.attrs)

    LOG.info(f'masking {list(fill_values)} to NaN as undeclared source fill')
    return dataset


def build_encoding(dataset, chunks, time_coord_chunk):
    """Build zarr encoding for every variable, replacing the netCDF encoding.

    The encoding xarray carries over from the netCDF files describes HDF5
    settings (``zlib``, ``chunksizes``, ...) that the zarr backend rejects, so
    it is dropped and rebuilt here. Call ``carry_source_encoding`` first if any
    of it is worth keeping.

    Args:
        dataset (xarray.Dataset): The dataset about to be written.
        chunks (dict): Resolved chunk sizes.
        time_coord_chunk (int): Chunk length for the time coordinate. The append
            path passes the commit batch size, so that each append lands on a
            chunk boundary; the region path has no batches and passes the whole
            axis.

    Returns:
        dict: Encoding to hand to the zarr writer.
    """
    encoding = {}
    for name, variable in dataset.variables.items():
        # chunk each variable along whichever of its dimensions are chunked, and
        # keep any dimension the config does not mention whole
        shape = tuple(
            chunks.get(dimension, variable.sizes[dimension])
            for dimension in variable.dims
        )
        encoding[name] = {'chunks': shape}

        # an index coordinate is a label rather than a field: anything that
        # opens the store reads it whole, and it is small enough that splitting
        # it buys nothing. Without this it inherits the chunking of the data it
        # indexes, which at chunks.lat = 20 would give the 1800 long lat
        # coordinate 90 chunks. Harmless at chunks.lat = -1, which is why it
        # never showed in the spatial build.
        if variable.ndim == 1 and variable.dims[0] == name:
            encoding[name]['chunks'] = (variable.sizes[name],)

        if name == 'time':
            encoding[name]['chunks'] = (time_coord_chunk,)
            # pin the calendar so every appended batch is encoded identically.
            # The raw files say 'days since 1900-1-1 00:00:00' and store the
            # offset as float32 (v3.16) or int32 (v2.8); an int64 day count
            # is exact for both and is what the sibling GLEAM stores carry
            encoding[name]['units'] = 'days since 1900-01-01'
            encoding[name]['calendar'] = 'proleptic_gregorian'
            encoding[name]['dtype'] = 'int64'
        elif np.issubdtype(variable.dtype, np.floating):
            # store the decoded fill value rather than the netCDF sentinel
            # (-9999 in MSWEP), so masked cells read back as NaN without a
            # decoding step
            encoding[name]['_FillValue'] = np.nan

    # xarray merges these dicts with each variable's own .encoding, so the stale
    # netCDF settings have to be cleared rather than just overridden
    for variable in dataset.variables.values():
        variable.encoding = {}

    return encoding


def derive_attrs(dataset, duration):
    """Global attributes that can only be read off the data itself.

    Kept apart from the config's ``attrs`` so the coverage and grid figures
    always describe what was actually written, rather than a number someone has
    to remember to update. Callers merge the config on top, so any of these can
    still be overridden by hand.

    Args:
        dataset (xarray.Dataset): The dataset being written or inspected.
        duration (str): ISO 8601 spelling of the axis spacing, from the
            resolution's ``FileNaming``.

    Returns:
        dict: ACDD coverage and geospatial attributes.

    Raises:
        ValueError: If the time axis is not regularly spaced, which would make
            the reported ``time_coverage_resolution`` a false claim.
    """
    time = dataset.indexes['time']
    lat = dataset['lat'].values
    lon = dataset['lon'].values

    # the axis is written complete and regular by construction -- the gaps are
    # spliced in as fill rather than skipped -- so an irregular one here means
    # that construction failed, and the attribute would otherwise publish a
    # spacing the data does not have
    steps = np.unique(np.diff(time.values))
    if len(steps) > 1:
        raise ValueError(
            f'the time axis is not regularly spaced ({len(steps)} distinct '
            f'steps), so it cannot be described as {duration}'
        )

    # the grid is regular, so one step describes it; abs() because lat runs
    # north to south and the resolution is not a signed quantity
    lat_step = abs(float(lat[1] - lat[0]))
    lon_step = abs(float(lon[1] - lon[0]))
    # the coordinates are float32, so widening them to python floats exposes the
    # representation error (89.94999999998977 for a cell centred on 89.95).
    # Four decimals is finer than the 0.1 degree grid and coarser than the
    # error, so it reports the grid the files describe rather than the float
    return {
        'time_coverage_start': str(time.min())[:10],
        'time_coverage_end': str(time.max())[:10],
        'time_coverage_resolution': duration,
        'geospatial_lat_min': round(float(lat.min()), 4),
        'geospatial_lat_max': round(float(lat.max()), 4),
        'geospatial_lon_min': round(float(lon.min()), 4),
        'geospatial_lon_max': round(float(lon.max()), 4),
        'geospatial_lat_resolution': round(lat_step, 4),
        'geospatial_lon_resolution': round(lon_step, 4),
    }


def apply_variable_attrs(dataset, variable_attrs):
    """Merge configured per variable attributes onto the dataset.

    MSWEP's data variables carry almost nothing -- ``units`` and
    ``least_significant_digit``, with no ``long_name`` and no
    ``standard_name`` -- so unlike GLEAM there is a real gap for the config to
    fill. Whatever is set here goes on top of the attributes the source files
    carry, so upstream values survive unless deliberately overridden.

    Args:
        dataset (xarray.Dataset): The dataset about to be written; modified in
            place.
        variable_attrs (dict): Variable name -> attribute mapping, already
            rendered by ``format_attrs``.

    Returns:
        xarray.Dataset: The same dataset, for chaining.

    Raises:
        ValueError: If the config names a variable the dataset does not have.
    """
    unknown = [name for name in variable_attrs if name not in dataset.variables]
    if unknown:
        raise ValueError(
            f'variable_attrs names {unknown}, which the dataset does not have; '
            f'it has {sorted(dataset.variables)}'
        )
    for name, attrs in variable_attrs.items():
        dataset[name].attrs.update(attrs)
        LOG.info(f'set {len(attrs)} configured attributes on {name}')
    return dataset


def open_repository(path):
    """Open the icechunk repository at a path, creating it if it is not there.

    Args:
        path (str): Directory holding the store.

    Returns:
        icechunk.Repository: The opened repository.
    """
    storage = icechunk.local_filesystem_storage(path)
    return icechunk.Repository.open_or_create(storage)


def open_existing_repository(path):
    """Open an icechunk repository that must already exist.

    ``open_repository`` creates one if it is absent, which is right for the
    build and wrong for anything administrative: a mistyped path would create an
    empty repository beside the real store, and every subsequent report --
    a garbage collection that freed nothing, a store that verified clean --
    would be true of that empty repository rather than of the store.

    Args:
        path (str): Directory holding the store.

    Returns:
        icechunk.Repository: The opened repository.

    Raises:
        Exception: If there is no repository at the path.
    """
    return icechunk.Repository.open(icechunk.local_filesystem_storage(path))


def committed_timesteps(repository):
    """Number of timesteps already written to the store.

    Args:
        repository (icechunk.Repository): The repository to inspect.

    Returns:
        int: The length of the store's time dimension, or 0 if nothing has been
            written yet.
    """
    session = repository.readonly_session(branch=BRANCH)
    try:
        stored = xr.open_zarr(session.store, consolidated=False)
    except Exception:
        # a freshly created repository has an empty root group and no arrays
        return 0
    return stored.sizes.get('time', 0)


def check_resume(repository, dataset, n_written):
    """Check a partially written store lines up with the dataset being written.

    Guards against resuming into a store built from a different configuration,
    which would otherwise append mismatched data onto the existing timesteps.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        dataset (xarray.Dataset): The dataset about to be written.
        n_written (int): Timesteps already in the store.

    Raises:
        ValueError: If the store's variables or timesteps do not match.
    """
    session = repository.readonly_session(branch=BRANCH)
    stored = xr.open_zarr(session.store, consolidated=False)

    expected = set(dataset.data_vars)
    found = set(stored.data_vars)
    if found != expected:
        raise ValueError(
            f'store already holds variables {sorted(found)} but this run would '
            f'write {sorted(expected)}; delete the store to rebuild it'
        )

    # comparing the whole overlap is cheap next to the data itself, and catches
    # a store built from a different time range or resolution
    if not stored['time'].equals(dataset['time'].isel(time=slice(0, n_written))):
        raise ValueError(
            'the timesteps already in the store do not match the input files; '
            'delete the store to rebuild it'
        )


def write_dataset(repository, dataset, encoding, batch_size, start=0):
    """Write a dataset to the repository in batches, committing each one.

    Args:
        repository (icechunk.Repository): The repository to write into.
        dataset (xarray.Dataset): The lazy dataset to write.
        encoding (dict): Encoding for the initial write; ignored when resuming.
        batch_size (int): Timesteps per batch, a whole number of time chunks.
        start (int): First timestep to write, for resuming a partial store.

    Returns:
        int: The number of batches written.
    """
    n_time = dataset.sizes['time']
    n_batches = 0
    for begin in range(start, n_time, batch_size):
        end = min(begin + batch_size, n_time)
        batch = dataset.isel(time=slice(begin, end))
        session = repository.writable_session(branch=BRANCH)

        LOG.info(
            f'writing timesteps {begin}-{end - 1} of {n_time} '
            f'({end - begin} steps, {batch.nbytes / 1024**3:.1f} GiB)'
        )
        if begin == 0:
            # the first batch lays down the arrays and their chunking
            to_icechunk(batch, session, mode='w', encoding=encoding)
        else:
            # later batches extend the existing arrays along time
            to_icechunk(batch, session, append_dim='time')

        snapshot = session.commit(f'write timesteps {begin}-{end - 1}')
        LOG.info(f'committed timesteps {begin}-{end - 1} as {snapshot}')
        n_batches += 1
    return n_batches


def resolve_write_strategy(settings, chunks, sizes):
    """Pick the write strategy and check it suits the configured chunking.

    The two strategies are not interchangeable, and getting the pairing wrong
    fails quietly rather than loudly, which is why it is checked here. An
    ``append`` build of a store whose time chunk spans the whole record
    degenerates to a single batch: ``commit_batch_size`` rounds the target up to
    one whole chunk, ``write_dataset`` runs one iteration, and the run becomes
    one uninterruptible commit that a walltime kill loses entirely. A ``region``
    build of a shallowly chunked store is merely wasteful, reading the whole
    record into memory to write chunks one timestep deep.

    Args:
        settings (dict): The loaded configuration.
        chunks (dict): Resolved chunk sizes.
        sizes (Mapping): Length of each dimension, e.g. ``dataset.sizes``.

    Returns:
        str: The strategy to use, one of ``WRITE_STRATEGIES``.

    Raises:
        ValueError: If the strategy is unknown, or does not match the chunking.
    """
    strategy = settings.get('write_strategy', DEFAULT_WRITE_STRATEGY)
    if strategy not in WRITE_STRATEGIES:
        raise ValueError(
            f'unknown write_strategy {strategy!r}; expected one of '
            f'{list(WRITE_STRATEGIES)}'
        )

    whole_axis = chunks['time'] >= sizes['time']
    if strategy == 'append' and whole_axis:
        raise ValueError(
            f"write_strategy 'append' needs a time chunk shorter than the "
            f'record, but chunks.time resolves to {chunks["time"]} against '
            f'{sizes["time"]} timesteps; the whole build would be one commit '
            f"with no resume. Use write_strategy 'region'"
        )
    if strategy == 'region' and not whole_axis:
        raise ValueError(
            f"write_strategy 'region' fills whole time chunks in place, but "
            f'chunks.time resolves to {chunks["time"]} against '
            f'{sizes["time"]} timesteps; set chunks.time to -1 or use '
            f"write_strategy 'append'"
        )
    return strategy


def block_read_chunks(settings):
    """Chunks to open the raw files with on the region path.

    The store's own chunking is the wrong thing to read with here. Opening the
    files as 20x20 tiles would build tens of millions of dask chunks before a
    single byte is read, and each tile would decompress the 200x200 source
    chunk it sits inside. The block is the read unit instead, so one source
    chunk is decompressed once for all the tiles it covers.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        dict: Chunk sizes for ``open_mfdataset``. Whole along time because each
            file holds one timestep, so there is nothing to divide.
    """
    block_shape = settings.get('block_shape') or {}
    return {
        'time': -1,
        'lat': block_shape.get('lat', -1),
        'lon': block_shape.get('lon', -1),
    }


def resolve_block_shape(settings, chunks, sizes):
    """Resolve the lat/lon block the region path reads and commits in.

    Args:
        settings (dict): The loaded configuration; ``block_shape`` may set
            either dimension, and -1 or an absent key means the whole one.
        chunks (dict): Resolved chunk sizes.
        sizes (Mapping): Length of each dimension, e.g. ``dataset.sizes``.

    Returns:
        dict: Block length per spatial dimension.

    Raises:
        ValueError: If a block is not a whole number of output chunks.
    """
    block_shape = settings.get('block_shape') or {}
    resolved = {}
    for dimension in ('lat', 'lon'):
        size = block_shape.get(dimension, -1)
        size = sizes[dimension] if size == -1 else size
        # a region write addresses the store's chunk grid directly, so a block
        # that is not a whole number of chunks would land mid chunk and force
        # zarr to read, patch and rewrite chunks the next block also touches
        if size % chunks[dimension]:
            raise ValueError(
                f'block_shape.{dimension} of {size} is not a whole number of '
                f'{chunks[dimension]} cell chunks'
            )
        resolved[dimension] = min(size, sizes[dimension])
    return resolved


def iter_blocks(variables, sizes, block_shape):
    """Every block of the region build, in the order it should be written.

    Variable major, so one variable's files stay open across all its blocks
    rather than being reopened once per block. MSWEP has a single variable, so
    that ordering is currently a no-op, but it costs nothing and the pipeline is
    meant to stay usable for a multi-variable product.

    Args:
        variables (list): Variable names to write.
        sizes (Mapping): Length of each dimension, e.g. ``dataset.sizes``.
        block_shape (dict): Resolved block length per spatial dimension.

    Returns:
        list: ``(variable, lat0, lat1, lon0, lon1)`` tuples, each of which is
            both the unit of work and its own resume key.
    """
    blocks = []
    for variable in variables:
        for lat0 in range(0, sizes['lat'], block_shape['lat']):
            lat1 = min(lat0 + block_shape['lat'], sizes['lat'])
            for lon0 in range(0, sizes['lon'], block_shape['lon']):
                lon1 = min(lon0 + block_shape['lon'], sizes['lon'])
                blocks.append((variable, lat0, lat1, lon0, lon1))
    return blocks


def block_message(block):
    """Render a block as the commit message that records it.

    Args:
        block (tuple): ``(variable, lat0, lat1, lon0, lon1)``.

    Returns:
        str: The commit message ``committed_blocks`` parses back.
    """
    variable, lat0, lat1, lon0, lon1 = block
    return BLOCK_MESSAGE.format(
        variable=variable, lat0=lat0, lat1=lat1, lon0=lon0, lon1=lon1
    )


def create_skeleton(repository, dataset, encoding):
    """Lay down the store's metadata and coordinates, with no data chunks.

    ``compute=False`` writes the group, every array's metadata and every
    variable already in memory, and defers only the dask backed ones -- which
    here is all of the data. The result is a store of the right shape and
    chunking that ``write_by_region`` can then fill in place.

    Args:
        repository (icechunk.Repository): The repository to write into.
        dataset (xarray.Dataset): The lazy dataset whose shape to lay down.
        encoding (dict): Encoding for the arrays being created.

    Returns:
        str: The snapshot id of the commit.
    """
    # a dask backed coordinate would have its array created and left empty,
    # since compute=False defers exactly the writes that are not yet in memory
    dataset = dataset.assign_coords(
        {name: coordinate.load() for name, coordinate in dataset.coords.items()}
    )

    # open_mfdataset leaves one dask chunk per file along time, so a store chunk
    # spanning the whole record would straddle several of them and xarray
    # refuses the write rather than risk two dask tasks writing one chunk. No
    # data is written here, but the check runs anyway, so present the time axis
    # as the single chunk the region strategy requires it to be. This is a
    # graph operation on a lazy dataset and reads nothing.
    dataset = dataset.chunk({'time': -1})

    session = repository.writable_session(branch=BRANCH)
    dataset.to_zarr(
        session.store,
        mode='w',
        encoding=encoding,
        consolidated=False,
        zarr_format=3,
        compute=False,
    )
    snapshot = session.commit(SKELETON_MESSAGE)
    LOG.info(f'created skeleton as {snapshot}')
    return snapshot


def store_variables(repository):
    """Data variables already present in the store.

    Args:
        repository (icechunk.Repository): The repository to inspect.

    Returns:
        set: The store's data variable names, empty if nothing is written yet.
    """
    session = repository.readonly_session(branch=BRANCH)
    try:
        stored = xr.open_zarr(session.store, consolidated=False)
    except Exception:
        # a freshly created repository has an empty root group and no arrays
        return set()
    return set(stored.data_vars)


def committed_blocks(repository):
    """The blocks a previous run already wrote, read back from the history.

    Progress lives in the commit messages rather than in the store so that
    resuming needs nothing but the history icechunk keeps anyway, and so that
    ``finalize_mswep_zarr.py`` does not have to strip build bookkeeping out of
    the attributes it publishes.

    Args:
        repository (icechunk.Repository): The repository to inspect.

    Returns:
        set: ``(variable, lat0, lat1, lon0, lon1)`` tuples already written.
    """
    blocks = set()
    for snapshot in repository.ancestry(branch=BRANCH):
        match = BLOCK_MESSAGE_RE.match(snapshot.message)
        if match is None:
            continue
        blocks.add(
            (
                match['variable'],
                int(match['lat0']),
                int(match['lat1']),
                int(match['lon0']),
                int(match['lon1']),
            )
        )
    return blocks


def check_resume_region(repository, dataset, chunks):
    """Check a partially filled store matches the dataset being written.

    The region path never appends, so a mismatch would not fail at the write:
    it would quietly overwrite part of one store with data belonging to
    another. This is the guard ``check_resume`` is for the append path.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        dataset (xarray.Dataset): The dataset about to be written.
        chunks (dict): Resolved chunk sizes.

    Raises:
        ValueError: If the store's variables, coordinates or chunk grid do not
            match.
    """
    session = repository.readonly_session(branch=BRANCH)
    stored = xr.open_zarr(session.store, consolidated=False)

    expected = set(dataset.data_vars)
    found = set(stored.data_vars)
    if found != expected:
        raise ValueError(
            f'store already holds variables {sorted(found)} but this run would '
            f'write {sorted(expected)}; delete the store to rebuild it'
        )

    # the whole axis, not an overlap: a region build creates every coordinate up
    # front, so anything short of equality is a different dataset
    for name in ('time', 'lat', 'lon'):
        if not stored[name].equals(dataset[name]):
            raise ValueError(
                f'the {name} coordinate in the store does not match the input '
                f'files; delete the store to rebuild it'
            )

    for name in sorted(expected):
        shape = tuple(chunks[dimension] for dimension in stored[name].dims)
        if tuple(stored[name].encoding['chunks']) != shape:
            raise ValueError(
                f'{name} in the store is chunked '
                f'{tuple(stored[name].encoding["chunks"])} but this run would '
                f'write {shape}; delete the store to rebuild it'
            )


def read_into_buffer(array, timesteps_per_read=DEFAULT_TIMESTEPS_PER_READ):
    """Read a lazy array into one preallocated buffer, a slab of time at a time.

    ``.load()`` would be the obvious thing here and it is the wrong one at this
    size. Dask assembles a block by holding every input chunk and allocating the
    concatenated output alongside them, so the block is transiently **doubled**
    at the moment it comes together -- 45 GiB becomes 90 GiB with no warning.
    Against a job's memory cgroup that does not fail cleanly: the kernel spends
    itself on page reclaim and the allocation eventually fails inside HDF5,
    which reports it as the uninformative ``NetCDF: HDF error``.

    Filling a buffer instead makes peak memory the block plus one slab.

    GLEAM read one dask chunk per iteration, because there a chunk was a whole
    year file and there were 46 of them. Here a chunk is a single timestep, so
    that would be ~17,000 scheduler round trips per block, each for 2.9 MB.
    The slab is sized in timesteps instead and spans many chunks, which gives
    dask something to parallelise and keeps the reads contiguous.

    Args:
        array (xarray.DataArray): The lazy, dask backed block to read.
        timesteps_per_read (int): Timesteps to pull out per read.

    Returns:
        numpy.ndarray: The block's values, CF decoded.

    Raises:
        ValueError: If time is not the leading dimension, which the buffer
            indexing below assumes.
    """
    if array.dims[0] != 'time':
        raise ValueError(
            f'expected time to be the leading dimension, got {array.dims}'
        )

    values = np.empty(array.shape, dtype=array.dtype)
    n_time = array.sizes['time']
    for begin in range(0, n_time, timesteps_per_read):
        end = min(begin + timesteps_per_read, n_time)
        values[begin:end] = array.isel(time=slice(begin, end)).values
    return values


def write_by_region(repository, dataset, blocks, done=frozenset(), settings=None):
    """Fill a skeleton store block by block, committing each one.

    Each block is one variable's full time record over a lat/lon tile. It is
    read into memory whole -- the point of the strategy is that the source
    chunks underneath it are decompressed once rather than once per output
    chunk -- so peak memory here is the block, not the chunks in flight. See
    ``read_into_buffer`` for why it is not read with ``.load()``.

    Args:
        repository (icechunk.Repository): The repository to write into.
        dataset (xarray.Dataset): The lazy dataset to read blocks from.
        blocks (list): Every block of the build, from ``iter_blocks``.
        done (Container): Blocks a previous run already committed.
        settings (dict): The loaded configuration; an optional
            ``timesteps_per_read`` key sizes the read slab.

    Returns:
        int: The number of blocks written by this call.
    """
    timesteps_per_read = (settings or {}).get(
        'timesteps_per_read', DEFAULT_TIMESTEPS_PER_READ
    )
    n_time = dataset.sizes['time']
    remaining = [block for block in blocks if block not in done]
    if not remaining:
        LOG.info('store is already complete, nothing to write')
        return 0
    LOG.info(
        f'{len(remaining)} blocks to write of {len(blocks)}, '
        f'reading {timesteps_per_read} timesteps at a time'
    )

    n_written = 0
    for position, block in enumerate(remaining, start=1):
        variable, lat0, lat1, lon0, lon1 = block
        message = block_message(block)
        region = {
            'time': slice(0, n_time),
            'lat': slice(lat0, lat1),
            'lon': slice(lon0, lon1),
        }

        array = dataset[variable].isel(lat=region['lat'], lon=region['lon'])
        LOG.info(
            f'block {position}/{len(remaining)}: reading {message} '
            f'({array.nbytes / 1024**3:.1f} GiB)'
        )
        values = read_into_buffer(array, timesteps_per_read)

        # a region write addresses arrays that already exist, so the index
        # coordinates naming the region are not part of what is written
        data = xr.Dataset({variable: (array.dims, values, dict(array.attrs))})

        session = repository.writable_session(branch=BRANCH)
        to_icechunk(data, session, region=region)
        snapshot = session.commit(message)
        LOG.info(f'committed {message} as {snapshot}')

        # drop the block before the next iteration reads one. The read happens
        # while these are still bound, so without this the outgoing and the
        # incoming block coexist and peak memory is two blocks rather than one
        # -- the same doubling read_into_buffer exists to avoid, moved one
        # level out.
        del values, data
        n_written += 1
    return n_written
