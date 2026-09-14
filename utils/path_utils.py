"""Configuration loading, path construction and time-axis discovery for MSWEP.

The download step (``access_mswep``) lays the raw netCDF files out as
``<download>/<version>/raw/<product>/<year>/<file>.nc``, with the version
written as ``v_3_16`` rather than ``V3.16`` and the product path lowercased, so
``Past/Daily`` becomes ``past/daily``. The year directory is optional -- the
downloader's ``year_subdirectories`` key decides whether it is there, and both
layouts are read here -- but for a daily product it is worth keeping, since the
whole record is ~17,000 files and GLADE is unhappy above 2,000-3,000 entries in
one directory. Derived zarr stores are written to a ``zarr`` directory sitting
alongside ``raw`` in the same version tree.

Unlike GLEAM, MSWEP holds one variable in one file per *timestep*, so the
store's time axis is discovered from the filenames rather than from the data.
That is the cheap way to do it -- a filename is a timestamp, so the whole axis
is known after two directory listings and no file has to be opened -- and it is
also the only way to notice a timestep that is missing outright rather than
present and all-fill. ``check_coverage`` is what turns that discovery into a
guarantee: it refuses to proceed unless the gaps on disk are exactly the ones
the config declares, so an incomplete download cannot quietly become a
NaN-filled store.
"""

from __future__ import annotations

import os
import re
from collections import namedtuple

import pandas as pd
import yaml # type: ignore

# 'V3.16' -> '3.16', 'V2.8' -> '2.8'. A release can carry more dot separated
# parts than the two on disk do, so the count is not fixed. Kept identical to
# access_mswep's regex: the two projects have to spell a version the same way
# or one config cannot drive both.
VERSION_RE = re.compile(r'^[Vv](?P<number>\d+(?:\.\d+)*)$')

# a four digit year directory sitting between the product and the files
YEAR_DIR_RE = re.compile(r'^\d{4}$')

# literal segment of the download tree holding the raw netCDF files
RAW_DIRNAME = 'raw'
# sibling of RAW_DIRNAME holding the zarr stores derived from those files
ZARR_DIRNAME = 'zarr'

# How one temporal resolution's files are named and how its axis is spaced.
#
# ``pattern`` matches a filename and captures the timestamp, ``stamp_format``
# parses that capture, ``freq`` is the pandas offset alias that generates the
# complete axis between two timestamps, and ``duration`` is the ISO 8601
# spelling of the same spacing for an ACDD ``time_coverage_resolution``.
FileNaming = namedtuple('FileNaming', 'pattern stamp_format freq duration')

# Only 'daily' is implemented. MSWEP names its 3hourly files 'YYYYDOY.HH.nc'
# and its monthly ones 'YYYYMM.nc', so each needs its own pattern and parser
# rather than an adjustment to this one; adding an entry here is all that a new
# resolution should need, and everything downstream reads the axis off these
# fields rather than assuming days.
FILE_NAMING = {
    'daily': FileNaming(
        pattern=re.compile(r'^(?P<stamp>\d{4}\d{3})\.nc$'),
        stamp_format='%Y%j',
        freq='D',
        duration='P1D',
    ),
}


def load_config(file_path: str) -> dict:
    """Load YAML configuration from a file.

    Args:
        file_path (str): Path to the YAML configuration file.

    Returns:
        dict: The loaded configuration.
    """
    with open(file_path, 'r', encoding='utf-8') as file:
        config = yaml.safe_load(file)
    return config


def format_version(version):
    """Rewrite an MSWEP version for use in a directory or store name.

    Args:
        version (str): Version as written in the config, e.g. 'V3.16' or
            'V2.8'.

    Returns:
        str: The version lowercased with the parts underscore separated, so
            'V3.16' -> 'v_3_16' and 'V2.8' -> 'v_2_8'. However many parts the
            version has are kept, since the count varies by release.

    Raises:
        ValueError: If the version is not a 'V' followed by dot separated
            numbers.
    """
    match = VERSION_RE.match(version)
    if match is None:
        raise ValueError(
            f"cannot parse version {version!r}, expected e.g. 'V3.16' or 'V2.8'"
        )
    return 'v_' + '_'.join(match.group('number').split('.'))


def product_parts(settings):
    """Split the configured product into its period and temporal resolution.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        tuple: Lowercased ``(period, temporal_resolution)``, e.g.
            ``('past', 'daily')`` for a product of 'Past/Daily'.

    Raises:
        ValueError: If the product is not two '/' separated parts.
    """
    parts = [part.lower() for part in settings['product'].split('/') if part]
    if len(parts) != 2:
        raise ValueError(
            f'cannot parse product {settings["product"]!r}, expected a period '
            f"and a resolution such as 'Past/Daily'"
        )
    return tuple(parts)


def format_product(settings):
    """Rewrite the configured product as the local directory path it becomes.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. 'past/daily'.
    """
    return os.path.join(*product_parts(settings))


def file_naming(settings):
    """The filename convention and axis spacing for the configured resolution.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        FileNaming: How this resolution's files are named and spaced.

    Raises:
        NotImplementedError: If the resolution has no entry in ``FILE_NAMING``.
    """
    _, resolution = product_parts(settings)
    if resolution not in FILE_NAMING:
        raise NotImplementedError(
            f'no filename convention for temporal resolution {resolution!r}; '
            f'known: {sorted(FILE_NAMING)}. MSWEP names each resolution '
            f'differently, so add a FileNaming entry rather than adjusting an '
            f'existing pattern'
        )
    return FILE_NAMING[resolution]


def version_root(settings):
    """Root of the local tree for the configured version.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<download>/v_3_16'.
    """
    return os.path.join(
        settings['directories']['download'], format_version(settings['version'])
    )


def raw_root(settings):
    """Root of the tree holding the raw netCDF files.

    Falls back to ``directories.download`` so a config that does not separate
    the two keeps working. The separate key exists because a relocated store has
    to stay verifiable against raw files that did not move with it:
    ``store_path`` follows ``download``, so without this the inputs and the
    outputs are pinned to the same filesystem.

    Like ``download``, ``directories.raw`` is the root *above* the version
    directory, not the ``raw`` segment itself.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<raw or download>/v_3_16'.
    """
    directories = settings['directories']
    return os.path.join(
        directories.get('raw') or directories['download'],
        format_version(settings['version']),
    )


def raw_dir(settings):
    """Directory holding the raw netCDF files for the configured product.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<raw or download>/v_3_16/raw/past/daily'; it holds either the
            netCDF files directly or a year directory per year of the record.
    """
    return os.path.join(raw_root(settings), RAW_DIRNAME, format_product(settings))


def template_fields(settings):
    """Config fields a ``str.format`` template may reference.

    Every scalar at the top level of the config, plus everything under
    ``output_conventions`` and the pieces derived from ``version`` and
    ``product``; nested sections are skipped because only scalars render
    usefully into a string.

    ``version`` is substituted in its directory form so rendered text carries
    the same spelling as the input tree, and ``version_label`` holds it as the
    config writes it. ``period`` and ``temporal_resolution`` are split out of
    ``product`` so a filename or an attribute can name either without the
    config repeating itself.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        dict: Field name -> value.
    """
    fields = {
        key: value
        for key, value in settings.items()
        if isinstance(value, (str, int, float))
    }
    fields.update(settings.get('output_conventions', {}))
    period, resolution = product_parts(settings)
    # a filename carries the directory spelling, but prose wants the version as
    # it is actually written, so both are offered rather than one converted
    fields['version_label'] = settings['version']
    fields['version'] = format_version(settings['version'])
    fields['product_label'] = settings['product']
    fields['product'] = format_product(settings)
    fields['period'] = period
    fields['temporal_resolution'] = resolution
    return fields


def format_filename(settings):
    """Render ``output_conventions.filename`` from the rest of the config.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. 'mswep.v_3_16.past.daily.native_0p1x0p1.spatial.zarr'.

    Raises:
        ValueError: If the template refers to a field the config does not
            define.
    """
    fields = template_fields(settings)
    template = settings['output_conventions']['filename']
    try:
        return template.format_map(fields)
    except KeyError as error:
        raise ValueError(
            f'filename template {template!r} refers to {error} '
            f'which is not set in the config'
        ) from None


def format_attrs(settings, section='attrs'):
    """Render a config attribute section into netCDF style attributes.

    Each string value is a template over the same fields ``format_filename``
    uses, so attribute text can refer to ``{version}`` or ``{grid_name}``
    instead of repeating them and drifting from the rest of the config.

    Args:
        settings (dict): The loaded configuration.
        section (str): Which top level config section to render. ``'attrs'``
            holds the store's global attributes; ``'variable_attrs'`` holds a
            mapping of variable name -> attributes, whose values are rendered
            one level deeper.

    Returns:
        dict: Attribute name -> value, empty if the config has no such section.

    Raises:
        ValueError: If a template refers to a field the config does not define.
    """
    fields = template_fields(settings)

    def render(name, value):
        # numbers and booleans are legitimate attribute values with nothing to
        # template, so only strings go through format_map
        if not isinstance(value, str):
            return value
        try:
            return value.format_map(fields)
        except KeyError as error:
            raise ValueError(
                f'attribute {name!r} refers to {error} which is not set '
                f'in the config'
            ) from None

    rendered = {}
    for name, value in (settings.get(section) or {}).items():
        if isinstance(value, dict):
            rendered[name] = {
                key: render(f'{name}.{key}', item) for key, item in value.items()
            }
        else:
            rendered[name] = render(name, value)
    return rendered


def store_path(settings):
    """Full path of the zarr store to build.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        str: e.g. '<download>/v_3_16/zarr/mswep.v_3_16.past.daily....spatial.zarr'.
    """
    return os.path.join(version_root(settings), ZARR_DIRNAME, format_filename(settings))


def raw_files(settings):
    """Every raw file for the configured product, indexed by its timestamp.

    Reads the timestamps out of the filenames rather than out of the files: a
    daily record is ~17,000 files, and opening each one to ask when it is would
    cost minutes for something the name already says. Both tree layouts the
    downloader can produce are read -- files directly under the product
    directory, or split into a directory per year.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        pandas.Series: Absolute file paths, indexed by timestamp in ascending
            order. The index is the axis the raw tree actually covers.

    Raises:
        FileNotFoundError: If the product directory is absent or holds no files
            matching the resolution's naming convention.
    """
    directory = raw_dir(settings)
    if not os.path.isdir(directory):
        raise FileNotFoundError(f'no raw data directory at {directory}')

    naming = file_naming(settings)
    entries = sorted(os.listdir(directory))
    years = [
        entry
        for entry in entries
        if YEAR_DIR_RE.match(entry) and os.path.isdir(os.path.join(directory, entry))
    ]
    # a year split tree and a flat one are both read, but never mixed: if the
    # downloader wrote year directories then every file is inside one, and a
    # stray .nc at the top level would be a leftover rather than data
    if years:
        candidates = [
            (year_dir, name)
            for year in years
            for year_dir in [os.path.join(directory, year)]
            for name in sorted(os.listdir(year_dir))
        ]
    else:
        candidates = [(directory, name) for name in entries]

    paths, stamps = [], []
    for parent, name in candidates:
        match = naming.pattern.match(name)
        if match is None:
            continue
        paths.append(os.path.join(parent, name))
        stamps.append(match.group('stamp'))

    if not paths:
        raise FileNotFoundError(
            f'no files matching {naming.pattern.pattern} under {directory}'
        )

    index = pd.DatetimeIndex(
        pd.to_datetime(stamps, format=naming.stamp_format), name='time'
    )
    # sorted by timestamp rather than by path: for 'YYYYDOY.nc' the two orders
    # agree, but that is a property of this naming convention rather than of
    # the tree, and a resolution added to FILE_NAMING later need not share it
    return pd.Series(paths, index=index, name='path').sort_index()


def expected_times(present, settings):
    """The complete, regularly spaced axis the record should cover.

    Args:
        present (pandas.DatetimeIndex): Timestamps the raw tree actually holds.
        settings (dict): The loaded configuration.

    Returns:
        pandas.DatetimeIndex: Every timestep from the first present one to the
            last, at the resolution's own spacing.
    """
    naming = file_naming(settings)
    return pd.date_range(present[0], present[-1], freq=naming.freq, name='time')


def check_coverage(present, expected, settings):
    """Fail unless the timesteps absent from the raw tree are the declared ones.

    The store is written on the complete ``expected`` axis, with anything
    missing from the raw tree filled as NaN. That is the right thing to do for
    a handful of timesteps the upstream release never published, and exactly
    the wrong thing to do for an incomplete download, which it would turn into
    a silently NaN-filled store. The two are indistinguishable from the data,
    so they are distinguished from the config instead: the build proceeds only
    if what is missing is precisely what ``expected_missing_times`` declares.

    With one variable there is no cross-variable time-axis comparison to catch
    this the way there was in GLEAM, which makes this the only such guard.

    Args:
        present (pandas.DatetimeIndex): Timestamps the raw tree holds.
        expected (pandas.DatetimeIndex): The complete axis to write.
        settings (dict): The loaded configuration; ``expected_missing_times``
            lists the timesteps known to be absent upstream, and defaults to
            none.

    Returns:
        pandas.DatetimeIndex: The missing timesteps, in ascending order.

    Raises:
        ValueError: If the raw tree holds duplicate timestamps, or if what is
            missing differs at all from what the config declares.
    """
    duplicated = present[present.duplicated()]
    if len(duplicated):
        raise ValueError(
            f'{len(duplicated)} timestamps appear more than once in the raw '
            f'tree, first {duplicated[0].date()}; the tree may hold two '
            f'layouts at once'
        )

    missing = expected.difference(present)
    declared = pd.DatetimeIndex(
        pd.to_datetime(list(settings.get('expected_missing_times') or [])),
        name='time',
    ).sort_values()

    if not missing.equals(declared):
        unexpected = missing.difference(declared)
        recovered = declared.difference(missing)
        detail = []
        if len(unexpected):
            detail.append(
                f'{len(unexpected)} timesteps are missing that the config does '
                f'not declare: {[str(t.date()) for t in unexpected[:10]]}'
                f'{" ..." if len(unexpected) > 10 else ""}'
            )
        if len(recovered):
            detail.append(
                f'{len(recovered)} timesteps the config declares missing are '
                f'present: {[str(t.date()) for t in recovered[:10]]}'
                f'{" ..." if len(recovered) > 10 else ""}'
            )
        raise ValueError(
            'the raw tree does not cover what the config says it should; '
            + '; '.join(detail)
            + '. Either the download is incomplete -- rerun it before building '
            '-- or expected_missing_times needs updating for this release'
        )
    return missing
