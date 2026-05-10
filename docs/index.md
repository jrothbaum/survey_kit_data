# survey-kit-data

`survey-kit-data` loads public U.S. survey, administrative, and economic data into
[Polars](https://pola.rs/) `LazyFrame` and `DataFrame` objects.

The package is meant to remove the repetitive parts of working with public data:
finding the right source files, downloading them with appropriate cache checks,
parsing agency-specific file layouts, and returning tables that are ready for
analysis.

## Project Scope

The package focuses on practical loaders for public datasets whose source files
are useful but inconvenient: Excel workbooks with changing layouts, CSV
downloads, public APIs, survey microdata files, and agency data organized around
fiscal or reporting periods.

The goal is to provide a small, predictable Python API that returns usable
Polars tables while preserving enough source context for validation and
debugging.

## What The Package Does

Each loader generally follows the same pattern:

1. Locate or accept source URLs for public files or APIs.
2. Download the source data, or reuse an existing cache.
3. Parse source-specific layouts into Polars tables.
4. Apply light cleanup such as date parsing, numeric casting, and geography IDs.
5. Cache parsed parquet outputs for faster repeat calls.

The package deliberately avoids becoming a full metadata catalog. Loader outputs
are documented enough to make the tables usable, while source-file audit columns
are optional where they would otherwise clutter normal analysis.

## Current Data Sources

| Source | Examples |
| ---- | -------- |
| Census | CPS ASEC, ACS5 API helpers |
| Federal Reserve | FRED state panels, SCF |
| BLS | Consumer Expenditure Survey |
| USDA | SNAP national, state, and county/substate history |
| DOL | Weekly UI claims and insured unemployed characteristics |
| HHS | TANF/AFDC caseload workbooks |

See [Data Sources](data-sources.md) for loader-level details.
