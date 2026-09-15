"""The MSWEP nomenclature key, read from the markdown file that documents it.

The data engineering style guide requires every project to keep a
``nomenclature-key_<dataset_source>.md`` recording how its original variable
names, units and temporal frequency map onto the authoritative
``nomenclature_data.md``. This module parses that file rather than restating it
in Python, so there is exactly one place the mapping is written down and no way
for the documentation and the stores to disagree. The alternative -- a dict here
and a table there -- is two sources of truth for the one thing the guide is
trying to make single.

The key is located relative to this file, not by an absolute path, so a clone of
the repository anywhere carries its own key with it.

Only the *canonical* side of the mapping is used by callers. The ``original_*``
columns are documentation: the migration and the build both read the actual
upstream strings off the netCDF or off the store, so a store's ``original_*``
attributes record what was really there rather than what the table claims was.
That matters more here than in ``data_engineering_gleam``, because MSWEP's four
raw products do not all spell the units the same way and a single table column
can only carry one of those spellings.

Ported from ``data_engineering_gleam/utils/nomenclature.py``. The one deliberate
difference is that the frequency table is parsed into a named tuple rather than
a bare string, because MSWEP's key carries a ``canonical_long_name`` for the
frequency and the store's ``title`` and ``summary`` read better with it.
"""

from __future__ import annotations

import functools
import os
from typing import NamedTuple

# the key sits at the repository root, one level above this package
KEY_FILENAME = 'nomenclature-key_mswep.md'

# the columns each table is recognised by. A markdown file has no schema, so the
# tables are found by their headers rather than by their order in the file --
# that way prose can be added or moved around them without breaking the parse
VARIABLE_COLUMNS = frozenset(
    {'original_variable_name', 'canonical_name', 'canonical_units'}
)
FREQUENCY_COLUMNS = frozenset({'original_frequency_name', 'canonical_name'})


class Variable(NamedTuple):
    """One row of the variable key, canonical side only.

    Attributes:
        original (str): The name MSWEP publishes, e.g. 'precipitation'.
        canonical (str): The name from nomenclature_data.md. For MSWEP this is
            the same string as ``original``, which is why this project has no
            array-rename step.
        units (str): The canonical units, e.g. 'mm d-1'.
        long_name (str): The canonical long name, e.g. 'precipitation'.
        unit_conversion (str): What was done to the values to reach those units.
    """

    original: str
    canonical: str
    units: str
    long_name: str
    unit_conversion: str


class Frequency(NamedTuple):
    """One row of the temporal frequency key.

    Attributes:
        original (str): The frequency as MSWEP and the configs spell it, e.g.
            'daily'.
        canonical (str): The token from nomenclature_data.md, e.g. 'day'. This
            is what goes in the store name and the temporal_frequency attribute.
        long_name (str): The canonical long name, e.g. 'daily average', for
            prose rather than for a filename.
    """

    original: str
    canonical: str
    long_name: str


def key_path():
    """Path to the nomenclature key markdown file.

    Returns:
        str: Absolute path to ``nomenclature-key_mswep.md`` at the repo root.
    """
    return os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))), KEY_FILENAME
    )


def parse_tables(text):
    """Pull every pipe-delimited markdown table out of a document.

    Args:
        text (str): The markdown document.

    Returns:
        list: One list of row dicts per table, each keyed by column header.
    """
    tables, rows, header = [], [], None
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith('|'):
            # any non-table line ends the table that was being read
            if header and rows:
                tables.append(rows)
            rows, header = [], None
            continue
        cells = [cell.strip().strip('`') for cell in stripped.strip('|').split('|')]
        if header is None:
            header = cells
        elif set(''.join(cells)) <= set('-: '):
            continue                    # the |---|---| separator row
        else:
            rows.append(dict(zip(header, cells)))
    if header and rows:
        tables.append(rows)
    return tables


def find_table(tables, columns, path):
    """Pick the one table carrying a given set of columns.

    Args:
        tables (list): Tables as returned by ``parse_tables``.
        columns (frozenset): Column names the wanted table must have.
        path (str): The file the tables came from, for the error message.

    Returns:
        list: The matching table's rows.

    Raises:
        ValueError: If no table has those columns, or more than one does.
    """
    matches = [rows for rows in tables if columns <= set(rows[0])]
    if len(matches) != 1:
        raise ValueError(
            f'expected exactly one table in {path} with columns '
            f'{sorted(columns)}, found {len(matches)}'
        )
    return matches[0]


@functools.lru_cache(maxsize=None)
def variables(path=None):
    """The variable key, keyed by the name MSWEP publishes.

    Args:
        path (str, optional): The key file. Defaults to the repo's own.

    Returns:
        dict: Original name -> :class:`Variable`.

    Raises:
        ValueError: If the table is missing, malformed, or maps two originals
            onto one canonical name.
    """
    path = path or key_path()
    with open(path, 'r', encoding='utf-8') as file:
        tables = parse_tables(file.read())
    rows = find_table(tables, VARIABLE_COLUMNS, path)

    key = {}
    for row in rows:
        variable = Variable(
            original=row['original_variable_name'],
            canonical=row['canonical_name'],
            units=row['canonical_units'],
            long_name=row['canonical_long_name'],
            unit_conversion=row['unit_conversion'],
        )
        if not all(variable):
            raise ValueError(f'incomplete row for {variable.original!r} in {path}')
        key[variable.original] = variable

    # a collision here would silently point two stores at one name, and the
    # second build would overwrite the first
    canonical = [entry.canonical for entry in key.values()]
    if len(set(canonical)) != len(canonical):
        raise ValueError(f'two variables share a canonical name in {path}')
    return key


@functools.lru_cache(maxsize=None)
def frequencies(path=None):
    """The temporal frequency key, keyed by the name the configs spell.

    Args:
        path (str, optional): The key file. Defaults to the repo's own.

    Returns:
        dict: Original frequency name -> :class:`Frequency`.

    Raises:
        ValueError: If the table is missing or a row is incomplete.
    """
    path = path or key_path()
    with open(path, 'r', encoding='utf-8') as file:
        tables = parse_tables(file.read())
    rows = find_table(tables, FREQUENCY_COLUMNS, path)

    key = {}
    for row in rows:
        frequency = Frequency(
            original=row['original_frequency_name'],
            canonical=row['canonical_name'],
            long_name=row['canonical_long_name'],
        )
        if not all(frequency):
            raise ValueError(f'incomplete row for {frequency.original!r} in {path}')
        key[frequency.original] = frequency
    return key


def canonical_variable(original, path=None):
    """Canonical name for a variable MSWEP publishes.

    Args:
        original (str): The MSWEP name, e.g. 'precipitation'.
        path (str, optional): The key file. Defaults to the repo's own.

    Returns:
        str: The canonical name.

    Raises:
        KeyError: If the variable has no row in the key. The style guide calls
            for a new entry in nomenclature_data.md rather than a guess, so this
            is deliberately fatal.
    """
    key = variables(path)
    if original not in key:
        raise KeyError(
            f'{original!r} has no row in {path or key_path()}; add it there, and '
            f'to nomenclature_data.md first if it is not already in that key'
        )
    return key[original].canonical


def canonical_frequency(original, path=None):
    """Canonical token for a temporal frequency.

    Args:
        original (str): The frequency as the config spells it, e.g. 'daily'.
        path (str, optional): The key file. Defaults to the repo's own.

    Returns:
        str: The canonical token, e.g. 'day'.

    Raises:
        KeyError: If the frequency has no row in the key.
    """
    key = frequencies(path)
    if original not in key:
        raise KeyError(
            f'{original!r} has no row in the temporal frequency key of '
            f'{path or key_path()}'
        )
    return key[original].canonical


def original_variable(canonical, path=None):
    """The MSWEP name behind a canonical name.

    For MSWEP the two are the same string, so this is an identity today. It
    exists because nothing should assume that: the store carries the canonical
    name while the raw netCDF files carry whatever GloH2O published, and
    anything that reads a store and its source files together -- the verifier
    above all -- needs the mapping in this direction rather than an assumption
    that it is a no-op.

    Args:
        canonical (str): The canonical name, e.g. 'precipitation'.
        path (str, optional): The key file. Defaults to the repo's own.

    Returns:
        str: The MSWEP name.

    Raises:
        KeyError: If no row maps onto that canonical name.
    """
    for entry in variables(path).values():
        if entry.canonical == canonical:
            return entry.original
    raise KeyError(f'no variable maps onto {canonical!r} in {path or key_path()}')
