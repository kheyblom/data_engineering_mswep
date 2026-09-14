"""Finalize a verified MSWEP icechunk zarr store for delivery.

This is the only script here that mutates a finished store, and it is separate
from ``verify_mswep_zarr.py`` on purpose. The verifier is how you prove a
finalization step did no harm, so it stays read only and safe to run by reflex;
a tool that both acted and audited would report one exit status for two
questions.

Four actions, exactly one per invocation. The three that write must be run in
the order below: a tag is immutable, so tagging before the attributes exist
permanently names a store that does not describe itself, and tagging before
collection makes the reachability set explicit rather than implicit in wherever
``main`` points. ``--status`` reports which of them a store still needs.

``--status``
    Where the store stands: attributes written and pending, tags, reachable
    history, the objects on disk, what is unreachable, and what remains to do.
    Reads only, and the thing to run first -- an unreachable object count is not
    interpretable without the reachable history beside it.
``--attrs``
    Write the config's ``attrs`` section, the coverage and grid attributes
    derived from the data, and the provenance derived from the repository and
    the verifier's log, onto the store's root group. Additive: it rewrites one
    metadata object as a new commit and touches no chunk or manifest.
``--tag NAME``
    Name the current branch tip. icechunk tags are immutable, so a consumer can
    pin a tag and be unaffected by any later commit; a branch tip cannot offer
    that.
``--gc``
    Delete objects no surviving snapshot references -- the chunks and snapshots
    left behind by a write that was killed before it could commit.

Nothing happens without ``--apply``: by default each action reports what it
would do and exits, so every step can be previewed.

**The store cannot claim a verification it has not had.** ``--attrs`` derives
``verification``, ``history`` and ``date_created`` rather than taking them from
the config, and it **refuses to write anything** unless this store's own
verifier log holds a run that passed. Hand-writing that attribute into a config
would let a store assert an audit nobody performed; deriving it from the log
means the claim and the evidence cannot drift apart. That is the one place this
deliberately differs from ``data_engineering_gleam``, which carried the text in
its config.

``--gc --apply`` is **irreversible**. It is safe in the sense that icechunk only
ever collects unreachable objects, and this script checks that claim by
comparing reachable bytes and history length either side of the call, but a
store with no second copy has nothing to restore from if that check ever fails.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import os
import re
import subprocess
import sys

import xarray as xr
import zarr

from utils.log_utils import setup_logging
from utils.path_utils import (
    file_naming,
    format_attrs,
    load_config,
    store_path,
)
from utils.zarr_utils import BRANCH, derive_attrs, open_existing_repository
# imported rather than restated, so the bar for 'verified' cannot drift from
# what the verifier actually runs by default
from verify_mswep_zarr import DEFAULT_PHASES

LOG = logging.getLogger(__name__)

STORE_DIRECTORIES = ('chunks', 'manifests', 'snapshots', 'transactions', 'overwritten')

# lines verify_mswep_zarr.py writes that say how a run ended. The pair has to
# stay in step with the verifier's own logging.
VERIFY_SUMMARY_RE = re.compile(
    r'^(?P<when>\d{4}-\d\d-\d\d \d\d:\d\d:\d\d).*?'
    r'(?P<checks>\d+) checks, (?P<failures>\d+) failures'
)
VERIFY_PHASES_RE = re.compile(r"phases: \[(?P<phases>[^\]]*)\]")
VERIFY_PASSED = 'store verified'

# The message this script's own attribute commit carries. It is written and
# matched in one place because build_history has to tell a build commit from a
# finalization one: counting its own commit as a build would make the commit
# count and date_created move every time --attrs was re-run, which is exactly
# what an idempotency check caught.
ATTRS_MESSAGE = 'add provenance and discovery attributes'
# how build_history recognises a history string it wrote itself, as opposed to
# the one the upstream netCDF files carry
OUR_HISTORY_MARKER = 'by mswep_zarr.py'
# icechunk's own first snapshot, which is not a commit anyone made
INITIAL_MESSAGE = 'Repository initialized'


def parse_args():
    """Parse the command line.

    Returns:
        argparse.Namespace: The parsed arguments.

    Raises:
        SystemExit: If no action, or more than one, is given.
    """
    parser = argparse.ArgumentParser(
        description='finalize a verified mswep zarr store for delivery.'
    )
    parser.add_argument(
        '--config', type=str, required=True, help='Path to YAML configuration file.'
    )
    action = parser.add_mutually_exclusive_group(required=True)
    action.add_argument(
        '--status',
        action='store_true',
        help='Report where the store stands and what it still needs. Read only.',
    )
    action.add_argument(
        '--attrs',
        action='store_true',
        help="Write the store's global attributes. Refuses unless the verifier "
        'has passed against this store.',
    )
    action.add_argument(
        '--tag', type=str, default=None, help='Create this tag at the branch tip.'
    )
    action.add_argument(
        '--gc',
        action='store_true',
        help='Delete objects no surviving snapshot references. Irreversible '
        'with --apply.',
    )
    parser.add_argument(
        '--apply',
        action='store_true',
        help='Actually make the change. Without it every action only reports.',
    )
    return parser.parse_args()


def repository_revision():
    """The git revision of this working tree, for the history attribute.

    Returns:
        str: The short revision, with '-dirty' appended if the tree has
            uncommitted changes, or 'unknown' if git cannot answer.
    """
    try:
        revision = subprocess.run(
            ['git', 'rev-parse', '--short', 'HEAD'],
            capture_output=True, text=True, check=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        ).stdout.strip()
        dirty = subprocess.run(
            ['git', 'status', '--porcelain'],
            capture_output=True, text=True, check=True,
            cwd=os.path.dirname(os.path.abspath(__file__)),
        ).stdout.strip()
        return f'{revision}-dirty' if dirty else revision
    except Exception:
        # provenance is worth recording but not worth failing a finalization for
        return 'unknown'


def verification_evidence(settings):
    """The last verifier run against this store, read from its own log.

    This is what makes the ``verification`` attribute a report rather than a
    claim. The verifier writes one log per store, so the evidence for a store
    sits beside the store's own name and cannot be borrowed from a sibling.

    A run only counts if it passed **and** covered every default phase. A
    partial run such as ``--phases index`` can pass while proving almost
    nothing, and accepting one would let a store claim a verification that
    never looked at its data. Partial passes are reported but not returned.

    Args:
        settings (dict): The loaded configuration.

    Returns:
        dict | None: ``when``, ``checks``, ``failures`` and ``phases`` of the
            last complete run that reported success, or None if the log holds
            none. A run that ended in failures, or that skipped a phase, is not
            returned, so neither can be written into a store.
    """
    path = os.path.join(
        settings['directories']['logs'], f'verify_{settings["log_file"]}'
    )
    if not os.path.isfile(path):
        LOG.warning(f'no verifier log at {path}')
        return None

    passed, partial, pending, phases = None, None, None, None
    with open(path, 'r', encoding='utf-8') as handle:
        for line in handle:
            match = VERIFY_PHASES_RE.search(line)
            if match:
                phases = [
                    p.strip().strip("'\"") for p in match.group('phases').split(',')
                ]
            match = VERIFY_SUMMARY_RE.match(line)
            if match:
                pending = {
                    'when': match.group('when'),
                    'checks': int(match.group('checks')),
                    'failures': int(match.group('failures')),
                    'phases': phases,
                }
                continue
            # the success line follows the summary it belongs to
            if VERIFY_PASSED in line and pending and not pending['failures']:
                if set(DEFAULT_PHASES) <= set(pending['phases'] or []):
                    passed = pending
                else:
                    partial = pending

    if passed is None and partial is not None:
        missing = sorted(set(DEFAULT_PHASES) - set(partial['phases'] or []))
        LOG.warning(
            f'the last passing run in {os.path.basename(path)} covered only '
            f'{partial["phases"]} and skipped {missing}; a partial run does not '
            f'count as verification'
        )
    return passed


def build_history(repository, settings, evidence, current):
    """Describe how the store was built and audited, from the repository itself.

    Derived rather than configured so it cannot drift from what happened: the
    commit count, the dates and the verification all come from the store and
    its log.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        evidence (dict): From ``verification_evidence``.
        current (dict): The store's attributes as they stand.

    Returns:
        tuple: The history string, the ISO 8601 UTC creation timestamp, and the
            upstream history to preserve, or None if there is nothing to keep.
    """
    # ancestry yields newest first; the initial snapshot is the oldest.
    # Finalization commits are excluded: they are not part of the build, and
    # counting them would move the commit count and date_created on every
    # re-run of --attrs
    snapshots = list(repository.ancestry(branch=BRANCH))
    built = [
        s for s in snapshots
        if s.message not in (INITIAL_MESSAGE, ATTRS_MESSAGE)
    ]
    first, last = built[-1], built[0]
    started = first.written_at.astimezone(datetime.timezone.utc)
    finished = last.written_at.astimezone(datetime.timezone.utc)

    # Once written, the history is left exactly as it stands. The revision it
    # names is the one that produced and finalized the store, which is the
    # provenance worth keeping; regenerating it would replace that with
    # whatever HEAD happens to be today, churn the attribute on every unrelated
    # commit to this repository, and leave --status permanently reporting
    # --attrs outstanding. A rebuilt store has no history to preserve, so it
    # gets a fresh one.
    existing = current.get('history', '')
    if OUR_HISTORY_MARKER in existing:
        # no upstream history to move aside: it was moved on the first write
        return existing, finished.strftime('%Y-%m-%dT%H:%M:%SZ'), None

    strategy = settings.get('write_strategy', 'append')
    unit = 'block commits' if strategy == 'region' else 'append commits'
    # deliberately no 'written today by finalize' clause: it would change on
    # every invocation and make --attrs non-idempotent, and the finalization
    # commit already carries its own timestamp in the repository history
    history = (
        f'{started.date().isoformat()}/{finished.date().isoformat()}: written '
        f'from the MSWEP {settings["version"]} {settings["product"]} netCDF '
        f'files by mswep_zarr.py (data_engineering_mswep @ '
        f'{repository_revision()}) in {len(built)} {unit}. '
        f'{evidence["when"][:10]}: verified against those same files by '
        f'verify_mswep_zarr.py, then finalized by finalize_mswep_zarr.py.'
    )
    # the upstream files carry their own history and it is provenance worth
    # keeping. Moved aside rather than appended to, so re-running --attrs
    # cannot grow the string without bound
    upstream = current.get('history') if 'source_history' not in current else None
    return history, finished.strftime('%Y-%m-%dT%H:%M:%SZ'), upstream


def derived_provenance(repository, settings, evidence, current):
    """Attributes that describe the build and the audit, not the data.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        evidence (dict): From ``verification_evidence``.
        current (dict): The store's attributes as they stand.

    Returns:
        dict: ``history``, ``date_created``, ``verification``, and
            ``source_history`` when there is an upstream one to preserve.
    """
    history, created, upstream = build_history(
        repository, settings, evidence, current
    )
    phases = ', '.join(evidence['phases'] or []) or 'the default phases'
    preserved = {'source_history': upstream} if upstream else {}
    return preserved | {
        'history': history,
        'date_created': created,
        'verification': (
            f'Verified {evidence["when"][:10]} against the source netCDF files '
            f'by verify_mswep_zarr.py: {evidence["checks"]} checks, '
            f'{evidence["failures"]} failures across {phases}. Value '
            f'comparisons are bit-exact rather than approximate, and the raw '
            f'side is read with CF masking disabled so the two sides cannot '
            f'agree by construction.'
        ),
    }


def resolve_attrs(repository, settings, evidence):
    """Work out the attributes the store should carry.

    The store's own attributes are the base, so upstream provenance survives;
    the derived coverage and grid values go on top; the config goes on top of
    those, so any derived value can be overridden by hand; and the derived
    provenance goes on last, because ``verification`` must describe what the
    verifier actually reported rather than what a config claims.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        evidence (dict): From ``verification_evidence``.

    Returns:
        tuple: The merged attributes and the store's current attributes.
    """
    session = repository.readonly_session(branch=BRANCH)
    dataset = xr.open_zarr(session.store, consolidated=False)
    current = dict(dataset.attrs)
    duration = file_naming(settings).duration
    merged = current | derive_attrs(dataset, duration) | format_attrs(settings)
    if evidence:
        merged = merged | derived_provenance(
            repository, settings, evidence, current
        )
    return merged, current


def object_counts(path):
    """Count the objects in each of the repository's on-disk directories.

    Args:
        path (str): Directory holding the store.

    Returns:
        dict: Directory name -> number of objects in it.
    """
    counts = {}
    for name in STORE_DIRECTORIES:
        directory = os.path.join(path, name)
        if os.path.isdir(directory):
            counts[name] = len(os.listdir(directory))
    return counts


def summarize(summary):
    """Render a garbage collection summary as one line.

    Args:
        summary (icechunk.GCSummary): The summary to render.

    Returns:
        str: The counts, with bytes in GiB.
    """
    return (
        f'{summary.bytes_deleted / 1024**3:.2f} GiB: '
        f'{summary.chunks_deleted} chunks, '
        f'{summary.manifests_deleted} manifests, '
        f'{summary.snapshots_deleted} snapshots, '
        f'{summary.attributes_deleted} attributes, '
        f'{summary.transaction_logs_deleted} transaction logs'
    )


def run_status(repository, settings, path):
    """Report where a store stands in the finalization procedure.

    Reads only, and the measurement that makes a garbage collection summary
    interpretable. A count of unreachable snapshots means nothing on its own --
    it reads equally as 'the whole commit history' and as 'objects nothing
    points at' -- and everything next to the reachable history and the objects
    actually on disk.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        path (str): Directory holding the store.

    Returns:
        int: 0.
    """
    evidence = verification_evidence(settings)
    merged, current = resolve_attrs(repository, settings, evidence)
    pending = [
        key for key in merged if key not in current or merged[key] != current[key]
    ]
    reachable = len(list(repository.ancestry(branch=BRANCH)))
    tags = repository.list_tags()

    if evidence:
        LOG.info(
            f'verified      {evidence["when"][:10]}, {evidence["checks"]} checks, '
            f'{evidence["failures"]} failures'
        )
    else:
        LOG.info('verified      NO passing verifier run found for this store')
    LOG.info(f'attributes    {len(current)} written, {len(pending)} pending')
    LOG.info(f'tags          {sorted(tags) or "none"}')
    LOG.info(
        f'reachable     {repository.chunk_storage_stats().native_bytes / 1024**3:.2f} '
        f'GiB across {reachable} snapshots'
    )
    for name, count in object_counts(path).items():
        # the orphan count is the whole point: it is what a garbage collection
        # summary has to be read against before it means anything
        note = (
            f'  ({reachable} reachable, {count - reachable} orphaned)'
            if name == 'snapshots'
            else ''
        )
        LOG.info(f'on disk       {name:13s} {count}{note}')

    summary = repository.garbage_collect(
        datetime.datetime.now(datetime.timezone.utc), dry_run=True
    )
    LOG.info(f'unreachable   {summarize(summary)}')

    # spell out what is left, so the order does not have to be remembered
    remaining = []
    if not evidence:
        remaining.append('run verify_mswep_zarr.py (nothing may be written first)')
    else:
        if pending:
            remaining.append('--attrs')
        if not tags:
            remaining.append('--tag NAME')
        if summary.bytes_deleted or summary.snapshots_deleted:
            remaining.append('--gc')
    LOG.info(
        f'remaining     {" then ".join(remaining) or "nothing, this store is finalized"}'
    )
    return 0


def run_attrs(repository, settings, apply_changes):
    """Write the store's global attributes.

    Refuses outright if the verifier has not passed against this store. The
    attributes include a ``verification`` claim, and a store must not assert an
    audit that did not happen; making this a hard gate enforces the order
    mechanically rather than by discipline.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        settings (dict): The loaded configuration.
        apply_changes (bool): Whether to commit, rather than only report.

    Returns:
        int: 0 on success, 1 if the store has not been verified.
    """
    evidence = verification_evidence(settings)
    if not evidence:
        LOG.error(
            'refusing to write attributes: no passing verify_mswep_zarr.py run '
            f'for this store in logs/verify_{settings["log_file"]}. These '
            'attributes include a verification claim, and a store must not '
            'carry one it has not earned. Run the verifier first.'
        )
        return 1
    LOG.info(
        f'verified {evidence["when"]}: {evidence["checks"]} checks, '
        f'{evidence["failures"]} failures'
    )

    merged, current = resolve_attrs(repository, settings, evidence)
    added = [key for key in merged if key not in current]
    changed = [key for key in merged if key in current and merged[key] != current[key]]
    LOG.info(f'{len(current)} attributes now, {len(merged)} after')
    for key in added:
        LOG.info(f'  add     {key} = {merged[key]!r}')
    for key in changed:
        LOG.info(f'  replace {key} = {current[key]!r} -> {merged[key]!r}')
    if not added and not changed:
        LOG.info('attributes already up to date, nothing to write')
        return 0
    if not apply_changes:
        LOG.info('dry run, nothing written; pass --apply to commit')
        return 0

    session = repository.writable_session(branch=BRANCH)
    group = zarr.open_group(session.store, mode='r+')
    # the merged dict is passed whole rather than relying on update semantics:
    # Group.update_attributes merges but its async twin replaces, and only one
    # of those preserves the upstream attributes
    group.update_attributes(merged)
    snapshot_id = session.commit(ATTRS_MESSAGE)
    LOG.info(f'committed {snapshot_id}')
    return 0


def run_tag(repository, tag, apply_changes):
    """Create a tag at the branch tip.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        tag (str): Name of the tag to create.
        apply_changes (bool): Whether to create it, rather than only report.

    Returns:
        int: 0 on success, 1 if the tag already exists.
    """
    existing = repository.list_tags()
    # ancestry yields newest first, so the tip is the head of the list
    tip = next(iter(repository.ancestry(branch=BRANCH)))
    LOG.info(f'tip {tip.id} written {tip.written_at.isoformat()} -- {tip.message}')
    LOG.info(f'existing tags: {sorted(existing) or "none"}')
    if tag in existing:
        LOG.error(f'tag {tag!r} already exists; tags are immutable, pick another')
        return 1
    if not apply_changes:
        LOG.info(f'dry run, nothing created; --apply would tag {tip.id} as {tag!r}')
        return 0

    repository.create_tag(tag, tip.id)
    LOG.info(f'created tag {tag!r} at {tip.id}')
    return 0


def run_gc(repository, apply_changes):
    """Delete objects no surviving snapshot references.

    Brackets the collection with the two properties it must not change:
    reachable chunk bytes and the length of the branch's history. Garbage
    collection is defined to remove only unreachable objects, so either of those
    moving means something went wrong, and on a store without a second copy that
    is worth checking rather than assuming.

    Args:
        repository (icechunk.Repository): The repository holding the store.
        apply_changes (bool): Whether to delete, rather than only report.

    Returns:
        int: 0 on success, 1 if the reachable state changed.
    """
    cutoff = datetime.datetime.now(datetime.timezone.utc)
    before_bytes = repository.chunk_storage_stats().native_bytes
    before_history = len(list(repository.ancestry(branch=BRANCH)))
    LOG.info(
        f'before: {before_bytes / 1024**3:.2f} GiB reachable across '
        f'{before_history} snapshots'
    )

    dry = repository.garbage_collect(cutoff, dry_run=True)
    LOG.info(f'dry run would delete {summarize(dry)}')
    if not apply_changes:
        LOG.info('dry run, nothing deleted; pass --apply to collect')
        return 0

    summary = repository.garbage_collect(cutoff, dry_run=False)
    LOG.info(f'deleted {summarize(summary)}')

    after_bytes = repository.chunk_storage_stats().native_bytes
    after_history = len(list(repository.ancestry(branch=BRANCH)))
    LOG.info(
        f'after: {after_bytes / 1024**3:.2f} GiB reachable across '
        f'{after_history} snapshots'
    )
    if after_bytes != before_bytes or after_history != before_history:
        LOG.error(
            f'garbage collection changed the reachable store: '
            f'{before_bytes} -> {after_bytes} bytes, '
            f'{before_history} -> {after_history} snapshots. '
            f'Do not trust this store; restore it before using it.'
        )
        return 1
    LOG.info('reachable bytes and history unchanged, as garbage collection requires')
    return 0


def main(settings, args):
    """Run the selected action and return a process exit status.

    Args:
        settings (dict): The loaded configuration.
        args (argparse.Namespace): Parsed arguments.

    Returns:
        int: 0 if the action succeeded, 1 otherwise.
    """
    path = store_path(settings)
    LOG.info(f'finalizing {path}')
    if not args.apply and not args.status:
        LOG.info('--apply not given: reporting only, nothing will be written')
    # Repository.open rather than open_or_create, so a mistyped path fails here
    # instead of producing an empty repository that every later step agrees with
    repository = open_existing_repository(path)

    if args.status:
        return run_status(repository, settings, path)
    if args.attrs:
        return run_attrs(repository, settings, args.apply)
    if args.tag:
        return run_tag(repository, args.tag, args.apply)
    return run_gc(repository, args.apply)


if __name__ == '__main__':
    arguments = parse_args()
    configuration = load_config(arguments.config)
    # named after the store's own log file, so two stores' forensics do not
    # interleave in one file
    setup_logging(
        os.path.join(
            configuration['directories']['logs'],
            f'finalize_{configuration["log_file"]}',
        )
    )
    sys.exit(main(configuration, arguments))
