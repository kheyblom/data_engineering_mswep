We need to build a pipeline to take raw MSWEP data and produce final zarr stores for downstream usage. We will work through each task one-at-a-time together.

# 1. Align to standard nomenclature and units

I want the data store this project creates to be aligned with the new data engineering style guide: @/glade/u/home/kheyblom/work/style_guides/style-guide_data-engineering_raw-to-datalake.md

Here are your following tasks:
a. ~~Read @/glade/u/home/kheyblom/work/style_guides/style-guide_data-engineering_raw-to-datalake.md and develop a plan on how to update the current datastores to align with the new style guide. I don't want to rerun the whole pipeline as that would be costly. Update the stores in a way that just modifies them to get the same end result. Any scripts used to make these modifications will be temporary (see below).~~ **COMPLETED 2026-09-15** -- all eight stores migrated in place by `migrate_nomenclature.py` (temporary, delete after 1.b/1.c). Metadata only, zero core-hours; chunk manifests identical either side. See STATE.md.
b. ~~Update the codebase so that the original pipeline so that if it were ran from start to finish, it would create data stores that align with the style guide.~~ **COMPLETED 2026-09-15** -- configs, build, verifier and finalizer all refactored. A build from three real files reproduces every attribute of all eight migrated stores; `finalize --attrs` reports 0 pending. No store has been built end to end yet -- that is 1.c.
c. ~~Test the codebase refactor.~~ **COMPLETED 2026-09-15** -- Tier 1 fixture built and verified on both write paths from raw: spatial 60 checks / 0 failures (148.5M cells bit-checked), temporal 64 checks / 0 failures. Against the pre-refactor fixtures the shape, chunking, dtype and chunk occupancy are identical, so the refactor changed metadata only. See TESTING.md findings 19-21.


# 2. Align stores with the 2026-10-14 style guide updates -- DEFERRED (user: clean up at a later date)

The style guide gained general rules while the Level 2 section was written for
data_engineering_wiso-sat (style_guides commit after 7bc9f72). The stores
here predate them:
- **`cdm_data_type: Grid`** is now a required global attribute (ACDD-1.3).
  Add it to the config `attrs` so new builds carry it, and to the existing
  eight stores with a metadata-only `finalize_mswep_zarr.py --attrs` pass. No
  data is rewritten; re-run the verifier's `structure` phase afterwards.
- **`time` encoding:** the rule is now "int64 since 1900-01-01,
  proleptic_gregorian, in the coarsest unit that represents the source times
  exactly". `days since 1900-01-01` is compliant for daily data, so this needs
  no change; it is listed so nobody re-derives it.
- **`standard_name` policy (decided 2026-10-14): the lake sets no
  `standard_name`;** any source one is kept as `original_standard_name`.
  `standard_name: precipitation_flux` was added by the config
  (`variable_attrs`), not taken from the source files, which carry none.
  Remove it from the configs and from all eight stores. `finalize --attrs`
  cannot drop a variable attribute (root group only, merge semantics); this
  needs a small temporary script, proven by the chunk-manifest comparison
  (CLAUDE.md, "Migrating a built store").


General notes:
- initalize a github repo and connect to remote: https://github.com/kheyblom/data_engineering_mswep.git if this is not already done. make sure to use proper version controlling as changes are made.
- build a CLAUDE.md that will take this information and effectively and efficiently handle this project. Use information learned in: @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam and add to this project's CLAUDE.md.
- it is always important to minimize compute costs on derecho. we have a finite allocation and managing this effective is very important.
- keep notes on your current working state. you may lose connection to the HPC system or I may need to start new sessions, so I need you to be able to easily pick up where you left off.
- additional tasks may come up that need to occur between the above tasks. this task list can be flexible, but if substantial changes are need, I need to approve them.
- mark tasks as completed when they are completed