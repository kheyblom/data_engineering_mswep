We need to build a pipeline to take raw MSWEP data and produce final zarr stores for downstream usage. We will work through each task one-at-a-time together.

# 1. Align to standard nomenclature and units

I want the data store this project creates to be aligned with the new data engineering style guide: @/glade/u/home/kheyblom/work/style_guides/style-guide_data_engineering.md

Here are your following tasks:
a. ~~Read @/glade/u/home/kheyblom/work/style_guides/style-guide_data_engineering.md and develop a plan on how to update the current datastores to align with the new style guide. I don't want to rerun the whole pipeline as that would be costly. Update the stores in a way that just modifies them to get the same end result. Any scripts used to make these modifications will be temporary (see below).~~ **COMPLETED 2026-09-15** -- all eight stores migrated in place by `migrate_nomenclature.py` (temporary, delete after 1.b/1.c). Metadata only, zero core-hours; chunk manifests identical either side. See STATE.md.
b. Update the codebase so that the original pipeline so that if it were ran from start to finish, it would create data stores that align with the style guide.
c. Test the codebase refactor.


General notes:
- initalize a github repo and connect to remote: https://github.com/kheyblom/data_engineering_mswep.git if this is not already done. make sure to use proper version controlling as changes are made.
- build a CLAUDE.md that will take this information and effectively and efficiently handle this project. Use information learned in: @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam and add to this project's CLAUDE.md.
- it is always important to minimize compute costs on derecho. we have a finite allocation and managing this effective is very important.
- keep notes on your current working state. you may lose connection to the HPC system or I may need to start new sessions, so I need you to be able to easily pick up where you left off.
- additional tasks may come up that need to occur between the above tasks. this task list can be flexible, but if substantial changes are need, I need to approve them.
- mark tasks as completed when they are completed