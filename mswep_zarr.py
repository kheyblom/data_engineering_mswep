"""Build an icechunk backed zarr store from the raw MSWEP netCDF files.

The raw tree written by the download step holds one netCDF file per timestep,
optionally split into a directory per year:
``<download>/<version>/raw/<product>/<year>/<file>.nc``, with the version
written as ``v_3_16`` rather than ``V3.16`` and the product path lowercased, so
``Past/Daily`` becomes ``past/daily``. This script opens the whole record, fills
in any timestep the release never published, and writes the result as a zarr
store named after ``output_conventions.filename``, next to ``raw`` in the same
version tree.

Everything is driven by the config: ``version``, ``product`` (one product per
store), ``variables`` (a list, or ``all`` to take every variable in the files),
and ``chunks``, where -1 means the whole dimension.

The time axis is discovered from the *filenames* rather than from the data --
each name is a timestamp -- and the store is written on the complete, regularly
spaced axis between the first and last of them. A timestep the raw tree does not
hold is spliced in as an all-fill plane, but only after ``check_coverage`` has
confirmed that what is absent is exactly what ``expected_missing_times``
declares: filling a documented upstream gap is right, and filling an incomplete
download is how a store silently becomes NaN.

The write is incremental either way, so an interrupted run resumes from the last
commit rather than starting over, but what a commit covers follows the chunking
and is picked by ``write_strategy``:

``append`` suits a store chunked shallowly along time. Timesteps are pushed out
in batches, each appended to the last; the batch size defaults to
``zarr_utils.DEFAULT_TIMESTEPS_PER_COMMIT`` and can be set with an optional
``timesteps_per_commit`` key. A batch is streamed out chunk by chunk rather than
held whole, so what bounds peak memory is the number of chunks in flight and the
number of netCDF files left open behind them: the optional ``num_workers`` and
``file_cache_maxsize`` keys, applied by ``configure_runtime``.

``region`` suits a store chunked along the whole time axis, where there is no
append boundary to stop at. The store's metadata and coordinates are written
first as a skeleton, then filled in place one ``block_shape`` sized lat/lon
block at a time, each block spanning the full record and each committed on its
own. A block is held in memory whole, so peak memory here is the block itself.
"""

import logging
import argparse
import os

from utils.path_utils import (
    check_coverage,
    expected_times,
    file_naming,
    format_attrs,
    load_config,
    raw_dir,
    raw_files,
    store_path,
)
from utils.log_utils import (
    setup_logging,
)
from utils.zarr_utils import (
    DEFAULT_WRITE_STRATEGY,
    apply_variable_attrs,
    block_read_chunks,
    build_encoding,
    carry_source_encoding,
    check_resume,
    check_resume_region,
    check_source_fill,
    commit_batch_size,
    committed_blocks,
    committed_timesteps,
    configure_runtime,
    create_skeleton,
    derive_attrs,
    discover_variables,
    fill_missing_times,
    iter_blocks,
    mask_source_fill,
    open_files,
    open_repository,
    resolve_block_shape,
    resolve_chunks,
    resolve_write_strategy,
    store_variables,
    write_by_region,
    write_dataset,
)

LOG = logging.getLogger(__name__)


def build_dataset(settings, files, expected):
    """Assemble the dataset to write, on the complete time axis.

    Args:
        settings (dict): The loaded configuration.
        files (Sequence): Raw file paths in time order.
        expected (pandas.DatetimeIndex): The complete axis the store covers.

    Returns:
        tuple: The xarray.Dataset, its resolved chunk sizes, the write strategy
            to use, and the variable names being written.
    """
    # the chunks a file is opened with are the unit dask reads in, which is the
    # store's own chunking on the append path but the block on the region path.
    # Read unvalidated, since validating the strategy needs the dimension
    # lengths and those are only known once the files are open
    strategy = settings.get('write_strategy', DEFAULT_WRITE_STRATEGY)
    read_chunks = (
        block_read_chunks(settings) if strategy == 'region' else settings['chunks']
    )

    LOG.info(f'opening {len(files)} files with chunks {read_chunks}')
    dataset = open_files(files, read_chunks)

    # before anything subsets, concatenates or rechunks: those all drop the
    # netCDF encoding that carries MSWEP's quantisation depth
    carry_source_encoding(dataset)

    variables = discover_variables(dataset, settings['variables'])
    LOG.info(f'writing {len(variables)} variables: {variables}')
    dataset = dataset[variables]

    # values the files use as fill without declaring them, which the CF decoding
    # therefore left in the data. Checked before masking, so a sentinel that
    # does not occur fails rather than quietly masking nothing
    fill_values = settings.get('source_fill_values') or []
    check_source_fill(dataset, fill_values)
    mask_source_fill(dataset, fill_values)

    # the raw tree can be missing a timestep outright rather than holding an
    # all-fill one; check_coverage has already vouched for which
    dataset = fill_missing_times(dataset, expected)

    # -1 in the config means the whole dimension, which is only known now that
    # the files are open
    chunks = resolve_chunks(settings['chunks'], dataset.sizes)
    strategy = resolve_write_strategy(settings, chunks, dataset.sizes)

    if strategy == 'append':
        # open_mfdataset chunks each file separately, so the time chunks follow
        # the per file boundaries until they are squared up here. The region
        # path writes from memory rather than from dask, so its blocks are left
        # chunked the way they were read
        dataset = dataset.chunk(chunks)

    apply_variable_attrs(dataset, format_attrs(settings, 'variable_attrs'))

    # open_files carries the source files' own attributes over; the derived and
    # configured ones go on top of those so upstream provenance survives beside
    # them. Only the first write lays them down
    duration = file_naming(settings).duration
    derived = derive_attrs(dataset, duration)
    if fill_values:
        # recorded from what the build did rather than left to the prose in
        # attrs, so the store cannot claim a masking it did not apply
        derived['source_fill_values_masked'] = ', '.join(repr(v) for v in fill_values)
    dataset.attrs.update(derived | format_attrs(settings))

    LOG.info(
        f'assembled {dict(dataset.sizes)}, {dataset.nbytes / 1024**4:.2f} TiB'
    )
    LOG.info(f'carrying {len(dataset.attrs)} global attributes')
    LOG.info(f'chunking as {chunks}, write strategy {strategy!r}')
    return dataset, chunks, strategy, variables


def write_append(repository, dataset, chunks, settings):
    """Write the store by appending batches of timesteps.

    Args:
        repository (icechunk.Repository): The repository to write into.
        dataset (xarray.Dataset): The lazy dataset to write.
        chunks (dict): Resolved chunk sizes.
        settings (dict): The loaded configuration.

    Returns:
        int: The number of batches written.

    Raises:
        ValueError: If a partially written store cannot be resumed into.
    """
    batch_size = commit_batch_size(chunks, settings)
    encoding = build_encoding(dataset, chunks, batch_size)

    # a store left behind by an interrupted run is continued rather than redone
    n_written = committed_timesteps(repository)
    if n_written:
        LOG.info(f'store already holds {n_written} timesteps, checking before resuming')
        check_resume(repository, dataset, n_written)
        if n_written >= dataset.sizes['time']:
            LOG.info('store is already complete, nothing to write')
            return 0
        # resume on a batch boundary; a short final batch cannot have been
        # written without the run having finished
        if n_written % batch_size:
            raise ValueError(
                f'store holds {n_written} timesteps, which is not a whole '
                f'number of {batch_size} step batches; delete it to rebuild'
            )
        LOG.info(f'resuming from timestep {n_written}')

    LOG.info(f'writing {batch_size} timesteps per commit')
    return write_dataset(repository, dataset, encoding, batch_size, start=n_written)


def write_region(repository, dataset, chunks, settings, variables):
    """Write the store as a skeleton, then fill it block by block.

    Args:
        repository (icechunk.Repository): The repository to write into.
        dataset (xarray.Dataset): The lazy dataset to write.
        chunks (dict): Resolved chunk sizes.
        settings (dict): The loaded configuration.
        variables (list): Variable names to write, in a stable order.

    Returns:
        int: The number of blocks written.

    Raises:
        ValueError: If a partially filled store cannot be resumed into.
    """
    block_shape = resolve_block_shape(settings, chunks, dataset.sizes)
    blocks = iter_blocks(variables, dataset.sizes, block_shape)
    LOG.info(f'blocking as {block_shape}: {len(blocks)} blocks')

    # build_encoding also clears the stale netCDF encoding off every variable,
    # which a region write needs just as much as the skeleton does
    encoding = build_encoding(dataset, chunks, dataset.sizes['time'])

    if store_variables(repository):
        LOG.info('store already exists, checking before resuming')
        check_resume_region(repository, dataset, chunks)
        done = committed_blocks(repository)
        # a block is its own resume key, so changing block_shape between runs
        # is safe but wasteful: nothing already written matches, and all of it
        # is written again
        stray = done - set(blocks)
        if stray:
            LOG.warning(
                f'{len(stray)} committed blocks do not match the configured '
                f'block_shape; that work will be redone'
            )
        LOG.info(f'{len(done)} of {len(blocks)} blocks already committed')
    else:
        create_skeleton(repository, dataset, encoding)
        done = set()

    return write_by_region(repository, dataset, blocks, done, settings)


def main(settings):

    os.makedirs(settings['directories']['logs'], exist_ok=True)
    log_file = os.path.join(settings['directories']['logs'], settings['log_file'])
    setup_logging(log_file)

    # set before anything opens a file or builds a graph, since both settings
    # only take effect for work started after them
    configure_runtime(settings)

    path = store_path(settings)
    LOG.info(
        f'building zarr store for MSWEP {settings["version"]} '
        f'({settings["product"]}, variables: {settings["variables"]})'
    )
    LOG.info(f'reading from {raw_dir(settings)}')
    LOG.info(f'writing to {path}')

    # the axis comes from the filenames, so it is known before a file is opened
    files = raw_files(settings)
    present = files.index
    expected = expected_times(present, settings)
    LOG.info(
        f'found {len(files)} files covering {present[0].date()} to '
        f'{present[-1].date()}, an axis of {len(expected)} timesteps'
    )
    missing = check_coverage(present, expected, settings)
    if len(missing):
        LOG.info(
            f'{len(missing)} timesteps are absent upstream as the config '
            f'declares, and will be written as fill'
        )

    dataset, chunks, strategy, variables = build_dataset(
        settings, list(files.values), expected
    )

    os.makedirs(os.path.dirname(path), exist_ok=True)
    repository = open_repository(path)

    if strategy == 'append':
        written = write_append(repository, dataset, chunks, settings)
        LOG.info(f'wrote {written} batches to {path}')
    else:
        written = write_region(repository, dataset, chunks, settings, variables)
        LOG.info(f'wrote {written} blocks to {path}')

    LOG.info('done :-)')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='build an icechunk zarr store from raw mswep netcdf files.'
    )
    parser.add_argument(
        '--config',
        type=str,
        required=True,
        help='Path to YAML configuration file.',
    )
    args = parser.parse_args()
    settings = load_config(args.config)
    main(settings)
