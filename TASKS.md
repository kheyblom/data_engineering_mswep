We need to build a pipeline to take raw MSWEP data and produce final zarr stores for downstream usage. We will work through each task one-at-a-time together.

This project will conduct the following tasks:
1. [DONE 2026-09-13] set up git repo
2. [DONE 2026-09-13] build a codebase that will build zarr stores of MSWEP data
    > Need to build zarr stores of MSWEP data downloaded by codebase in @/glade/u/home/kheyblom/work/data_access/access_mswep
    > Use overall structure and approach as in @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam
    > Will need to have two zarr stores: one optimized for spatial retrieval (e.g., retriving all spatial locations at a single time step) and another optimized for temporal retireval (e.g., a time series of a single location). Design the codebase to be flexible for either scenario (like in @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam)
    > Output structure should be inline with @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam
3. [DONE 2026-09-13] determine a chunking strategy for spatial and temporal zarr stores
    > I need to approve final decisions for this
4. [DONE 2026-09-14 - Tier 0, Tier 1 and Tier 2 all passed] test the codebase for the spatial zarr build. use a similar testing procedure as in @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam
5. [DONE 2026-09-14 - all 8 stores built, spatial and temporal] after testing is complete and codebase is verified, run the spatial zarr build.
6. [DONE 2026-09-14 - all 4 spatial stores verified, 0 failures] verify the spatial zarr store. run a similar verification to @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam
7. [DONE 2026-09-14 - Tier 0, Tier 1 and Tier 2 all passed] test the codebase again for the temporal zarr build.
8. [DONE 2026-09-14 - all 4 temporal stores verified, 0 failures] verify the temporal zarr store

SCOPE CHANGE, approved 2026-09-13. Tasks 4-8 were written for ONE spatial and
ONE temporal store. MSWEP publishes each release as two products -- Past
(gauge-corrected) and NRT (near-real-time) -- and Past stops well short of the
present in both releases, so a Past-only store silently ends in 2025-06 (V3.16)
or 2020-12 (V2.8). Past and NRT are kept as SEPARATE stores rather than merged,
because they are measurably different estimates (corr 0.94-0.96, RMSE
~1.9 mm/day where they overlap).

Tasks 4-8 therefore cover 8 stores, not 2:
    {V3.16, V2.8} x {past, nrt} x {spatial, temporal}
Two further scripts are still needed and do not yet exist:
    verify_mswep_zarr.py    - read-only audit, required by tasks 6 and 8
    finalize_mswep_zarr.py  - attrs, then tag, then gc, in that order


General notes:
- initalize a github repo and connect to remote: https://github.com/kheyblom/data_engineering_mswep.git . make sure to use proper version controlling as changes are made.
- build a CLAUDE.md that will take this information and effectively and efficiently handle this project. Use information learned in: @/glade/u/home/kheyblom/work/data_engineering/data_engineering_gleam and add to this project's CLAUDE.md.
- it is always important to minimize compute costs on derecho. we have a finite allocation and managing this effective is very important.
- keep notes on your current working state. you may lose connection to the HPC system or I may need to start new sessions, so I need you to be able to easily pick up where you left off.
- additional tasks may come up that need to occur between the above tasks. this task list can be flexible, but if substantial changes are need, I need to approve them.
- mark tasks as completed when they are completed