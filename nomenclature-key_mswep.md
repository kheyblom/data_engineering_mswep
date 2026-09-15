# Nomenclature Key — data_engineering_mswep

Maps MSWEP's published nomenclature onto the authoritative
[nomenclature_data.md](/glade/u/home/kheyblom/work/style_guides/nomenclature_data.md),
as required by the data engineering style guide.

**This file is machine read.** `utils/nomenclature.py` parses the two tables
below and is the only place the pipeline learns what a variable is called, so
the key and the stores cannot drift apart. Edit the tables, not the code.
Columns are matched by their header name, so they may be reordered but not
renamed.

The `unit_conversion` here is `none`: MSWEP already publishes precipitation in
the canonical unit, spelled differently in three of its four products. **No data
value is altered anywhere in this mapping** -- see the note under the table.

## Variable Key

| original_variable_name | original_units | canonical_name | canonical_units | canonical_long_name | unit_conversion | notes |
| --- | --- | --- | --- | --- | --- | --- |
| `precipitation` | `mm/d` | `precipitation` | `mm d-1` | precipitation | none | spelling only; the name is already canonical |

The variable name needs no translation, which is why this project has no
array-rename step where `data_engineering_gleam` does. Only the unit spelling
and the frequency token change.

`original_units` is not the same string in all four raw products, and the table
can only carry one. V3.16 Past, V3.16 NRT and V2.8 NRT publish `mm/d`; **V2.8
Past already publishes `mm d-1`**, the canonical spelling. The column records
the majority spelling for the reader. The migration and the build both read the
*actual* upstream string off the data rather than from this table, so a store's
`original_units` attribute cannot be wrong even if this column is.

`mm/d` and `mm d-1` are the same unit. Both denote millimetres per day; the
second is the udunits-parseable spelling. Converting between them is the
identity, so `unit_conversion` is `none` and no arithmetic is applied.

## Temporal Frequency Key

| original_frequency_name | canonical_name | canonical_long_name |
| --- | --- | --- |
| `daily` | `day` | daily average |

`canonical_long_name` is what `nomenclature_data.md` calls the frequency. The
token itself is for the filename and for a store's `temporal_frequency`
attribute.

**On "average" against MSWEP's "total".** MSWEP publishes a daily *total*, and
the key entry reads *daily average*. These are the same number here: the
canonical unit is `mm d-1`, a rate, so a day's total in mm and that day's mean
rate in mm d-1 are numerically identical. `day` is therefore an exact match and
no new entry in `nomenclature_data.md` is called for.

`product: <Period>/Daily` in every config under `config/` names the raw
directory on disk (`<download>/v_3_16/raw/past/daily/`), which is GloH2O's own
spelling and is not renamed. The canonical `day` is what appears in the store
name and in the stores' `temporal_frequency` attribute. This is the same split
`format_version` already makes between `V3.16` in the config and `v_3_16` in the
name.

## Standard names

`precipitation_flux` is kept as the data variable's `standard_name`, and is
**not** overwritten with the canonical name. This is a deliberate divergence
from `data_engineering_gleam`, which does overwrite it: GLEAM's canonical names
are not CF standard names, so its `standard_name` had nothing to lose. MSWEP's
`precipitation_flux` is a genuine CF standard name, and replacing it with
`precipitation` would trade real information for a duplicate of the variable
name. The style guide has no rule about `standard_name`.

## Coordinates

`time`, `lat` and `lon` are **not** renamed. `nomenclature_data.md` has no rows
for coordinate variables, and the style guide's naming rules are written against
its Data Variable Key. They keep the CF names MSWEP publishes.

## Store naming, and the two approved deviations

```
<zarr>/spatial/mswep.<version>.<period>.day.<grid_name>.precipitation.zarr
<zarr>/temporal/mswep.<version>.<period>.day.<grid_name>.precipitation.zarr

<zarr>/spatial/mswep.v_3_16.past.day.native_0p1x0p1.precipitation.zarr
<zarr>/temporal/mswep.v_2_8.nrt.day.native_0p1x0p1.precipitation.zarr
```

The style guide specifies
`<data_source>.<version>.<temporal_frequency>.<grid_name>.<variable>.zarr` --
five components. MSWEP needs two facts that convention has no slot for, so it
departs from it twice. **Both deviations were approved by the user on
2026-09-15**, as the guide requires any deviation to be.

1. **The layout is a parent directory**, `spatial/` or `temporal/`, rather than a
   filename component. The guide *mandates* separate spatial and temporal stores
   and then gives the filename nowhere to say which is which. A directory keeps
   the filename to the convention and matches how `data_engineering_gleam`
   already lays its stores out.
2. **The product keeps its own component**, `past` or `nrt`, making six
   components rather than five. MSWEP publishes each release as two products
   that are *not the same estimate* -- `Past` is gauge-corrected, `NRT` is the
   near-real-time stream -- and they must not resolve to one name. Folding it
   into the version (`v_3_16_past`) or into the data source (`mswep_past`) would
   have kept the five, and both were rejected: the product is neither a release
   nor a source, and spelling it as one would misdescribe it in the slot that
   downstream tooling reads as a release or a source.
