# How It Works

## Loader Design

`survey-kit-data` loaders are source-oriented but return analysis-friendly
tables. The package does not try to catalog every agency detail. Instead, each
loader captures enough source knowledge to turn public files into stable Polars
outputs.

Typical loader responsibilities are:

- Source discovery or source URL selection.
- Download with a browser-like user agent when needed.
- Local cache management.
- File parsing through structured readers such as `polars.read_excel`,
  `polars.read_csv`, or `polars-readstat`.
- Light normalization such as `year`, `month`, dates, numeric columns, and FIPS.
- Parquet caching of parsed outputs.

## Caching

Parsed outputs are cached as parquet files under the configured cache directory.
Calling a loader a second time usually scans the cached parquet instead of
downloading and parsing again.

Use `force_reload=True` when parser logic changed or when you want to rebuild
the cache from source files. Use `reload_if_updated=False` when you want a
strictly local cached result and do not want the loader to check the remote
source.

## Source Columns

Some loaders expose source audit columns as an option. For example,
`tanf_caseload(include_source=True)` adds:

| Column | Meaning |
| ------ | ------- |
| `source_url` | Workbook URL used for the row. |
| `source_sheet` | Source worksheet used for the row. |

These are useful for parser validation, but the default output omits them to
keep normal analysis tables compact.

## Calendar Months From Fiscal Workbooks

Several public workbooks are organized by fiscal year while the downstream
analysis needs calendar year and calendar month. The HHS TANF loader handles
this by parsing all monthly sheets in the relevant source files and then
filtering the final table to the requested calendar year.

For example, `tanf_caseload(years=[2021])` reads FY2021 files and, when
available, FY2022 files so that October through December 2021 are included. The
returned table still contains only `year == 2021`.

## Mirror Repositories

The optional `survey-kit-download` mirror is intended to store unedited source
files plus cache metadata. This repository should not run the mirror's browser
automation machinery. It only reads the mirror as a raw-file fallback when the
agency source cannot be reached reliably.
