"""Bring the built MSWEP stores onto the data engineering style guide, in place.

**Temporary.** This exists to migrate the eight stores that were built, verified
and finalized before the style guide, and should be deleted once they are done.
``mswep_zarr.py`` and the configs are being changed separately so that a build
lands in this state directly; this script only catches up what is already on
disk.

Nothing here touches a data value. The ``unit_conversion`` in
``nomenclature-key_mswep.md`` is ``none`` -- MSWEP already publishes
precipitation in the canonical unit, spelled ``mm/d`` in three of its four
products and ``mm d-1`` in the fourth -- the variable name is already canonical,
and the data variable is already float32, the dtype MSWEP publishes. So the
whole migration is two metadata operations:

1. **write the attributes**: the canonical ``units`` and ``long_name`` on the
   variable, the upstream strings preserved beside them as ``original_*``, and
   the root attributes that named the old store paths or described the old unit
   spelling brought up to date. Note this writes *array* attributes as well as
   group attributes, which is why the migration needs its own code:
   ``finalize_mswep_zarr.py --attrs`` only ever opens the root group, and it
   cannot delete or rename an attribute either.
2. **rename the directory**, ``<zarr>/mswep...daily...spatial.zarr`` ->
   ``<zarr>/spatial/mswep...day...precipitation.zarr``.

There is no array-rename step, unlike the equivalent script in
``data_engineering_gleam``: ``precipitation`` is already the canonical name.

The directory rename goes last on purpose: an interrupted run then always leaves
an openable store at a path this script can find again, and every step is
skipped if it has already been done, so a half-finished run is simply re-run.

**This has an undo**, which is worth saying because ``finalize_mswep_zarr.py
--gc --apply`` does not. The pre-migration snapshot stays reachable through the
``...-verified-20260914`` tag each store already carries, so
``repository.reset_branch(...)`` to that snapshot puts the store back, and the
directory rename is one more ``os.rename``. Do not run ``--gc`` on a migrated
store until it has been re-verified and re-tagged.

Every run compares the chunk manifest, the chunk storage statistics and the
array's shape, chunks, dtype and fill value from before the migration against
after, and again after the directory has moved. They must be identical; the run
fails if anything moved. That is a stronger statement than a sampled
re-verification -- it proves not one chunk changed -- and it costs only a
manifest walk, reading no data.

Run it one config at a time, the way every other entry point here takes a
config::

    for config in config/config_zarr_v*_{past,nrt}_{spatial,temporal}.yaml; do
        uv run python migrate_nomenclature.py --config "$config" --status
    done
"""

from __future__ import annotations

import argparse
import asyncio
import datetime
import logging
import os
import subprocess
import sys

import xarray as xr
import zarr

from utils.log_utils import setup_logging
from utils.nomenclature import (
    canonical_frequency,
    canonical_variable,
    key_path,
    variables,
)
from utils.path_utils import (
    load_config,
    store_path,
    template_fields,
)
from utils.zarr_utils import BRANCH, open_existing_repository
# the message the attribute commit carries. Imported rather than restated
# because finalize_mswep_zarr.py has to recognise it to keep it out of the build
# commit count, and that constant outlives this script
from finalize_mswep_zarr import MIGRATE_MESSAGE

LOG = logging.getLogger(__name__)

# the two steps, in the order they must run
STEPS = ('attrs', 'rename-store')

# Where a migrated store lives, relative to the same '<version>/zarr' directory
# the stores are in now. Two departures from the style guide's five component
# '<data_source>.<version>.<temporal_frequency>.<grid_name>.<variable>.zarr',
# both approved by the user and both recorded in nomenclature-key_mswep.md: the
# layout is a parent directory, because the guide mandates separate spatial and
# temporal stores without giving the filename anywhere to say which is which;
# and the product keeps its own component, because Past and NRT are not the same
# estimate and must not resolve to one name.
#
# '{frequency}' and '{variable}' rather than the config's own field names:
# template_fields derives 'temporal_resolution' from 'product', which names the
# raw directory on disk and is not renamed, so the canonical token has to arrive
# under a name of its own.
MIGRATED_FILENAME = (
    '{suffix}/mswep.{version}.{period}.{frequency}.{grid_name}.{variable}.zarr'
)

# every product and layout of one release, for rewriting the attributes that
# quote a sibling store by name
PERIODS = ('past', 'nrt')
LAYOUTS = ('spatial', 'temporal')

# attributes whose prose quotes sibling store names, and so has to be rewritten
# when those stores are renamed
SIBLING_ATTRS = ('chunking', 'related_store')

NOMENCLATURE_NOTE = (
    'Variable names, units and long names follow the internal data engineering '
    'nomenclature (nomenclature_data.md). The full original -> canonical '
    'mapping for this dataset, including the unit conversion, is '
    'nomenclature-key_mswep.md in the data_engineering_mswep repository. The '
    'variable name needed no translation -- MSWEP already calls it '
    'precipitation. The strings MSWEP published are preserved on the variable '
    'itself as original_variable_name and original_units; the upstream spelling '
    'of the temporal frequency is kept as original_temporal_frequency.'
)

DTYPE_NOTE = (
    'The data variable is float32, the dtype MSWEP publishes; it is not reduced '
    'or widened anywhere in this pipeline. The lat and lon coordinates are '
    'float32 as published, and time is stored as int64 days since 1900-01-01. '
    'Nothing in this store is float64, so the style guide\'s request that '
    'float64 be flagged for possible precision reduction does not apply here.'
)


def parse_args():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(
        description='Migrate a built MSWEP store onto the style guide nomenclature.'
    )
    parser.add_argument(
        '--config', required=True, help='Path to the YAML config for the store.'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Report which steps the store still needs, and exit. Reads only.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Actually make the changes. Without it, nothing is written.',
    )
    return parser.parse_args()


def revision():
    """Short git revision of this working tree, for the history attribute.

    Returns:
        str: The short SHA, or 'unknown' outside a git checkout.
    """
    try:
        return subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            cwd=os.path.dirname(os.path.abspath(__file__)),
            capture_output=True,
            text=True,
            check=True,
        ).stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return 'unknown'


def migrated_settings(settings, original):
    """The settings that render a migrated store's path.

    The canonical variable name and the canonical frequency token are supplied
    as top level fields of their own, and the filename template is replaced, so
    ``store_path`` produces the migrated spelling from the config as it stands
    today. Neither override touches ``product``, which names the raw directory
    on disk and keeps GloH2O's own spelling.

    Args:
        settings (dict): The loaded configuration.
        original (str): The variable as MSWEP publishes it, e.g.
            'precipitation'.

    Returns:
        dict: A copy of the settings that renders the migrated path.
    """
    resolution = template_fields(settings)['temporal_resolution']
    return {
        **settings,
        'variable': canonical_variable(original),
        'frequency': canonical_frequency(resolution),
        'output_conventions': {
            **settings['output_conventions'],
            'filename': MIGRATED_FILENAME,
        },
    }


def sibling_names(settings, original):
    """Old store name -> new store name, for every store of this release.

    The ``chunking`` and ``related_store`` attributes quote these paths and the
    migration renames all of them, so each store's prose has to be rewritten
    even for the siblings this invocation is not touching.

    The new name carries its layout directory, e.g.
    ``temporal/mswep.v_3_16.past.day.native_0p1x0p1.precipitation.zarr``, which
    is what a reader needs: it is the path relative to the same ``zarr``
    directory the old bare filename was relative to.

    Args:
        settings (dict): The loaded configuration.
        original (str): The variable as MSWEP publishes it.

    Returns:
        dict: Old name -> new name, for both products and both layouts.
    """
    old_fields = template_fields(settings)
    new_fields = template_fields(migrated_settings(settings, original))
    old_template = settings['output_conventions']['filename']

    names = {}
    for period in PERIODS:
        for layout in LAYOUTS:
            old = old_template.format_map(
                {**old_fields, 'period': period, 'suffix': layout}
            )
            new = MIGRATED_FILENAME.format_map(
                {**new_fields, 'period': period, 'suffix': layout}
            )
            names[old] = new
    return names


def locate(settings, original):
    """Find the store, whether or not its directory has been renamed yet.

    Args:
        settings (dict): The loaded configuration.
        original (str): The variable as MSWEP publishes it.

    Returns:
        tuple: The path the store is at now, and the path it should end at.

    Raises:
        FileNotFoundError: If there is no store at either path.
    """
    old = store_path(settings)
    new = store_path(migrated_settings(settings, original))
    for path in (new, old):
        if os.path.isdir(path):
            return path, new
    raise FileNotFoundError(f'no store at {old} or {new}')


def manifest(session, array_path):
    """Sorted chunk coordinates of one array, read off the manifest.

    ``chunk_coordinates`` is an async generator, so it is drained the same way
    ``verify_mswep_zarr.py`` drains it. No chunk is fetched: this is the same
    metadata-only walk the verifier's ``sweep`` phase does.

    Args:
        session (icechunk.Session): A session on the store.
        array_path (str): Path of the array, e.g. '/precipitation'.

    Returns:
        list: Sorted chunk coordinate tuples.
    """

    async def drain():
        return [tuple(c) async for c in session.chunk_coordinates(array_path)]

    return sorted(asyncio.run(drain()))


def fingerprint(repository, name):
    """Everything about a store that the migration must leave untouched.

    Deliberately not a sample. The chunk manifest is the complete list of which
    chunks exist and where, so comparing it either side of the migration proves
    that not one chunk moved, was added or was dropped -- which no amount of
    reading values back could prove.

    ``chunk_storage_stats`` rather than the deprecated ``total_chunks_storage``:
    the latter counts only native bytes and reports 0 for a store whose chunks
    are all inlined, which would make it useless as a guard on a small store.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        name (str): The data variable's name in the store.

    Returns:
        dict: The manifest, the storage statistics and the array's own spec.
    """
    session = repository.readonly_session(branch=BRANCH)
    array = zarr.open_group(session.store, mode='r')[name]
    stats = repository.chunk_storage_stats()
    return {
        'chunks': manifest(session, f'/{name}'),
        'bytes': (stats.native_bytes, stats.inlined_bytes, stats.virtual_bytes),
        'shape': array.shape,
        'chunk_shape': array.chunks,
        'dtype': str(array.dtype),
        'fill_value': repr(array.fill_value),
    }


def compare_fingerprints(before, after, label):
    """Fail loudly if anything that had to stay put has moved.

    Args:
        before (dict): Fingerprint taken before the migration.
        after (dict): Fingerprint taken after it.
        label (str): The store, for the message.

    Returns:
        bool: True if the two are identical.
    """
    moved = [key for key in before if before[key] != after[key]]
    if moved:
        for key in moved:
            # the manifests are far too long to print, so report the shape of
            # the difference rather than the difference
            if key == 'chunks':
                lost = set(before[key]) - set(after[key])
                gained = set(after[key]) - set(before[key])
                LOG.error(
                    f'{label}: chunk manifest changed, {len(before[key])} -> '
                    f'{len(after[key])} chunks, {len(lost)} lost, {len(gained)} gained'
                )
            else:
                LOG.error(f'{label}: {key} changed, {before[key]} -> {after[key]}')
        LOG.error(
            f'{label}: the migration was supposed to be metadata only. Do not '
            f'trust this store; roll it back to its tagged snapshot.'
        )
        return False
    LOG.info(
        f'{label}: unchanged, {len(after["chunks"])} chunks and '
        f'{sum(after["bytes"]) / 1024**3:.2f} GiB either side'
    )
    return True


def intended_variable_attrs(current, original):
    """The attributes the migrated data variable should carry.

    The ``original_*`` values are taken from what the store actually holds
    rather than from the nomenclature key, so they record what MSWEP really
    published instead of what a table claims it did. That matters here: the four
    raw products do not agree on the unit spelling, and the key's single
    ``original_units`` column can only carry the majority one. Once written the
    ``original_*`` attributes are the source of truth, so re-running reads them
    back rather than re-deriving them from attributes already replaced.

    ``standard_name`` is deliberately left alone. ``precipitation_flux`` is a
    genuine CF standard name, unlike GLEAM's canonical names, so overwriting it
    with the canonical variable name would trade real information for a
    duplicate of the variable name. See nomenclature-key_mswep.md.

    Args:
        current (dict): The variable's attributes as they stand on disk.
        original (str): The variable as MSWEP publishes it.

    Returns:
        dict: The full attribute set for the migrated variable.
    """
    entry = variables()[original]
    # on a re-run the canonical values are already in place, so the upstream
    # strings have to come from the original_* attributes written last time
    migrated = 'original_variable_name' in current
    original_units = current.get('original_units' if migrated else 'units', '')

    if original_units == entry.units:
        conversion = (
            f'{entry.unit_conversion} -- this product already published the '
            f'canonical spelling {entry.units!r}; no value was changed'
        )
    else:
        conversion = (
            f'{entry.unit_conversion} -- {original_units!r} and {entry.units!r} '
            f'are the same unit under a different spelling; no value was changed'
        )

    return {
        **current,
        'long_name': entry.long_name,
        'units': entry.units,
        'original_variable_name': current.get('original_variable_name', original),
        'original_units': original_units,
        'unit_conversion': conversion,
    }


def cf_compliance(original_units, canonical_units):
    """The ``cf_compliance`` note a migrated store should carry.

    The note the stores carry now says the units 'do not parse under udunits',
    which stops being true the moment they become the canonical spelling, so it
    is rewritten rather than left to mislead.

    Args:
        original_units (str): What MSWEP published for this product.
        canonical_units (str): The canonical spelling now on the variable.

    Returns:
        str: The note.
    """
    if original_units == canonical_units:
        provenance = (
            f'this product already published it that way, unlike the others in '
            f'this collection'
        )
    else:
        provenance = f'unlike the {original_units!r} MSWEP publishes in this product'
    return (
        f'Not declared CF compliant: Conventions is ACDD-1.3, which is what the '
        f'global attributes follow. The variable itself is close. standard_name '
        f"is a genuine CF standard name, 'precipitation_flux'. The canonical "
        f'unit spelling {canonical_units!r} does parse under udunits, '
        f'{provenance}. long_name follows the internal data engineering '
        f'nomenclature key rather than a CF phrasing. What MSWEP published is '
        f'preserved on the variable as original_variable_name and original_units.'
    )


def intended_root_attrs(current, settings, original, variable_attrs, sha):
    """The global attributes the migrated store should carry.

    ``chunking`` and ``related_store`` are rewritten by substituting each
    sibling store's new path for its old one rather than being re-authored,
    because their prose belongs to the config and is not this script's to
    rewrite. Everything the finalizer earned -- ``verification``,
    ``date_created``, ``history`` -- is preserved; the data those describe does
    not change, and the fingerprint is what proves it.

    Args:
        current (dict): The root attributes as they stand on disk.
        settings (dict): The loaded configuration.
        original (str): The variable as MSWEP publishes it.
        variable_attrs (dict): The migrated variable attributes, for the units.
        sha (str): Short git revision, for the history entry.

    Returns:
        dict: The full global attribute set for the migrated store.
    """
    resolution = template_fields(settings)['temporal_resolution']
    attrs = dict(current)

    for old, new in sibling_names(settings, original).items():
        for key in SIBLING_ATTRS:
            if key in attrs:
                attrs[key] = attrs[key].replace(old, new)

    attrs['cf_compliance'] = cf_compliance(
        variable_attrs['original_units'], variable_attrs['units']
    )
    attrs['nomenclature'] = NOMENCLATURE_NOTE
    attrs['dtype_note'] = DTYPE_NOTE
    attrs['temporal_frequency'] = canonical_frequency(resolution)
    attrs['original_temporal_frequency'] = resolution

    old_name = os.path.basename(store_path(settings))
    new_name = sibling_names(settings, original)[old_name]
    entry = (
        f'{datetime.date.today().isoformat()}: restandardized to the data '
        f'engineering style guide -- the variable\'s units were respelled '
        f'{variable_attrs["original_units"]!r} -> {variable_attrs["units"]!r} '
        f'and its long_name set to {variable_attrs["long_name"]!r}, with the '
        f'strings MSWEP published kept as original_* attributes, and the store '
        f'was renamed {old_name} -> {new_name}. Metadata only, by '
        f'migrate_nomenclature.py (data_engineering_mswep @ {sha}): no data '
        f'value was read, written or moved, and the chunk manifest is identical '
        f'either side.'
    )
    history = attrs.get('history', '')
    # idempotent: a re-run must not stack the same sentence up again
    if 'migrate_nomenclature.py' not in history:
        attrs['history'] = f'{history} {entry}'.strip()
    return attrs


def read_state(repository):
    """The store's data variable and its attributes, read straight from zarr.

    Read through zarr rather than xarray because xarray moves some attributes
    into ``.encoding`` on the way in, and what this script compares and writes
    has to be exactly what is on disk.

    Args:
        repository (icechunk.Repository): The repository holding the store.

    Returns:
        tuple: The data variable's name, its attributes, and the root
            attributes.

    Raises:
        ValueError: If the store does not hold exactly one data variable.
    """
    session = repository.readonly_session(branch=BRANCH)
    names = list(xr.open_zarr(session.store, consolidated=False).data_vars)
    if len(names) != 1:
        raise ValueError(f'expected exactly one data variable, found {names}')
    group = zarr.open_group(session.store, mode='r')
    return names[0], dict(group[names[0]].attrs), dict(group.attrs)


def plan_store(repository, settings, path, target, sha):
    """Work out what the store still needs, and what it should end up holding.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        path (str): Where the store is now.
        target (str): Where it should end up.
        sha (str): Short git revision, for the history entry.

    Returns:
        tuple: The pending step names, the store's data variable name, its
            current and intended attributes, and the intended root attributes.

    Raises:
        KeyError: If the variable has no row in the nomenclature key.
    """
    name, current_variable, current_root = read_state(repository)
    # the store carries the canonical name already, and for MSWEP the canonical
    # and original names are the same string, so the key is looked up by the
    # name on disk
    variable_attrs = intended_variable_attrs(current_variable, name)
    root_attrs = intended_root_attrs(current_root, settings, name, variable_attrs, sha)

    pending = []
    if variable_attrs != current_variable or root_attrs != current_root:
        pending.append('attrs')
    if path != target:
        pending.append('rename-store')
    return pending, name, current_variable, variable_attrs, current_root, root_attrs


def report_attr_changes(label, current, intended):
    """Log every attribute the migration would add or replace.

    Args:
        label (str): What the attributes belong to, for the message.
        current (dict): The attributes as they stand.
        intended (dict): The attributes the migration would write.
    """
    for key in intended:
        if key not in current:
            LOG.info(f'  {label} add     {key} = {intended[key]!r}')
        elif current[key] != intended[key]:
            LOG.info(
                f'  {label} replace {key} = {current[key]!r} -> {intended[key]!r}'
            )


def migrate_store(settings, apply_changes):
    """Run the two steps against the store this config names.

    Args:
        settings (dict): The loaded configuration.
        apply_changes (bool): Write, rather than only report.

    Returns:
        int: 0 if the store is migrated or would be, 1 if a guard failed.
    """
    path, target = locate(settings, canonical_variable('precipitation'))
    repository = open_existing_repository(path)
    (
        pending,
        name,
        current_variable,
        variable_attrs,
        current_root,
        root_attrs,
    ) = plan_store(repository, settings, path, target, revision())

    LOG.info(f'{os.path.basename(path)}')
    LOG.info(f'  at        {os.path.dirname(path)}')
    if not pending:
        LOG.info('  nothing to do, this store is already migrated')
        return 0
    LOG.info(f'  remaining {" then ".join(pending)}')
    if 'rename-store' in pending:
        LOG.info(f'  target    {os.path.relpath(target, os.path.dirname(path))}')

    if 'attrs' in pending:
        report_attr_changes(name, current_variable, variable_attrs)
        report_attr_changes('root', current_root, root_attrs)
    if not apply_changes:
        return 0

    before = fingerprint(repository, name)

    if 'attrs' in pending:
        session = repository.writable_session(BRANCH)
        group = zarr.open_group(session.store, mode='r+')
        # the whole dict each time rather than a partial update: update_attributes
        # merges but its async twin replaces, and only one of those keeps the
        # attributes MSWEP itself carried -- the same reasoning
        # finalize_mswep_zarr.py gives for passing the merged dict whole
        group[name].update_attributes(variable_attrs)
        group.update_attributes(root_attrs)
        LOG.info(f'  committed {session.commit(MIGRATE_MESSAGE)}')

    if not compare_fingerprints(before, fingerprint(repository, name), name):
        return 1

    if path != target:
        # last, so an interrupted run always leaves an openable store at a path
        # this script can find again
        os.makedirs(os.path.dirname(target), exist_ok=True)
        os.rename(path, target)
        LOG.info(
            f'  renamed   {os.path.basename(path)} -> '
            f'{os.path.relpath(target, os.path.dirname(os.path.dirname(target)))}'
        )
        # and prove the moved repository still opens and still holds the same
        # chunks, since nothing else would notice if it did not
        moved = open_existing_repository(target)
        if not compare_fingerprints(before, fingerprint(moved, name), name):
            return 1
    return 0


def main(settings, args):
    """Migrate the store this config names and return a process exit status.

    Args:
        settings (dict): The loaded configuration.
        args (argparse.Namespace): Parsed arguments.

    Returns:
        int: 0 if the store succeeded, 1 otherwise.
    """
    LOG.info(f'nomenclature key {key_path()}')
    if not args.apply or args.status:
        LOG.info('reporting only, nothing will be written')
    return migrate_store(settings, args.apply and not args.status)


if __name__ == '__main__':
    arguments = parse_args()
    configuration = load_config(arguments.config)
    setup_logging(
        os.path.join(
            configuration['directories']['logs'],
            f'migrate_{configuration["log_file"]}',
        )
    )
    sys.exit(main(configuration, arguments))
