# Data Sources

## Administrative Sources

These loaders cover public program and administrative aggregates, often at a
state, county, or other sub-national level.

### USDA SNAP

Module: `survey_kit_data.usda.snap`

| Loader | Output | Notes |
| ------ | ------ | ----- |
| `snap_monthly()` | National monthly aggregate history | Drops annual fiscal-year summary rows. |
| `snap_state_history()` | State monthly history | FY1989-present; includes `state_fips` by default. |
| `snap_county_history()` | County/substate snapshots | January and July snapshots, FY1989-present. |
| `snap_persons_snapshot()` | Recent state persons snapshot | Convenience wrapper around the current snapshot workbook. |
| `snap_households_snapshot()` | Recent state households snapshot | Convenience wrapper around the current snapshot workbook. |
| `snap_benefits_snapshot()` | Recent state benefit snapshot | Convenience wrapper around the current snapshot workbook. |

State history columns include `fiscal_year`, `region`, `state`, `year`,
`month`, `households`, `persons`, `total_cost`, `cost_per_household`,
`cost_per_person`, and optionally `state_fips`.

Example:

```python
import polars as pl

from survey_kit_data.usda.snap import snap_state_history, snap_county_history

state_snap = snap_state_history().collect()
county_snap = snap_county_history(include_fips=True).collect()

recent_state = (
    state_snap
    .filter((pl.col("year") == 2024) & (pl.col("month") == 12))
    .select("state", "state_fips", "persons", "households", "total_cost")
)
```

### DOL Unemployment Insurance

Module: `survey_kit_data.dol.ui`

| Loader | Output | Notes |
| ------ | ------ | ----- |
| `weekly_ui_claims()` | Weekly ETA 539 state UI claims | Coverage begins in 1986. |
| `insured_unemployed_characteristics()` | Monthly ETA 203 characteristics | State/month table. |

`weekly_ui_claims()` returns columns such as `state_abbr`, `week_ending`,
`year`, `month`, `initial_claims`, `continued_weeks`,
`extended_benefits_total`, `adjusted_total`, and `claims_estimate`.

Example:

```python
import polars as pl

from survey_kit_data.dol.ui import weekly_ui_claims, insured_unemployed_characteristics

claims = weekly_ui_claims().collect()
characteristics = insured_unemployed_characteristics().collect()

recent_claims = (
    claims
    .filter(pl.col("year") >= 2024)
    .select("state_abbr", "week_ending", "initial_claims", "continued_weeks")
)
```

### HHS TANF/AFDC

Module: `survey_kit_data.hhs.tanf`

| Loader | Output | Notes |
| ------ | ------ | ----- |
| `tanf_caseload()` | Monthly TANF/SSP/TANF+SSP caseload table | TANF years 1996-2025; direct URL list currently covers 2006-2025. |
| `afdc_caseload()` | Monthly AFDC caseload table | AFDC years 1960-1995; source discovery support is in place. |
| `tanf_caseload_source_sheets()` | Raw source worksheets | Debugging and parser validation. |
| `afdc_caseload_source_sheets()` | Raw source worksheets | Debugging and parser validation. |

The normalized TANF table has one row per `source_program`, calendar `year`,
calendar `month`, and `state`.

Core columns:

| Column | Meaning |
| ------ | ------- |
| `source_program` | One of `tanf`, `ssp`, or `tanf_ssp`. |
| `year`, `month` | Calendar year and month. |
| `state` | State or source geography label. |
| `total_families` | Total families receiving assistance. |
| `two_parent_families` | Two-parent families. |
| `one_parent_families` | One-parent families. |
| `no_parent_families` | No-parent families. |
| `total_recipients` | Total recipients. |
| `adult_recipients` | Adult recipients. |
| `child_recipients` | Child recipients. |
| `state_fips` | Added by default when the state is in the FIPS lookup. |

`include_source=True` adds `source_url` and `source_sheet`.

Example:

```python
import polars as pl

from survey_kit_data.hhs.tanf import tanf_caseload

tanf = tanf_caseload(
    years=[2021, 2022],
    download_mirror="jrothbaum/survey_kit_download",
).collect()

state_totals = (
    tanf
    .filter((pl.col("source_program") == "tanf") & (pl.col("state") != "U.S. Totals"))
    .group_by("year", "month")
    .agg(pl.col("total_recipients").sum().alias("state_total_recipients"))
    .sort("year", "month")
)
```

To inspect source worksheets while validating a parser:

```python
from survey_kit_data.hhs.tanf import tanf_caseload_source_sheets

sheets = tanf_caseload_source_sheets(
    years=[2025],
    download_mirror="jrothbaum/survey_kit_download",
)
print(sheets.keys())
```

## Survey And Economic Sources

These loaders are useful both directly and as worked examples for survey data
handling.

### Census

| Loader | Output |
| ------ | ------ |
| `cps_asec(year)` | CPS ASEC household, person, and replicate-weight tables. |
| ACS5 helpers in `survey_kit_data.census.api` | Census API tables as Polars outputs. |

Example:

```python
from survey_kit_data.census.cps_asec import cps_asec
from survey_kit_data.census.api import acs5_income

cps = cps_asec(2023)
people = cps["person"].collect()

acs_income = acs5_income(year=2022, geo_for="state:*").collect()
```

### Federal Reserve

| Loader | Output |
| ------ | ------ |
| `scf(year)` | Survey of Consumer Finances implicates and replicate weights. |
| `StatePanels` methods | FRED state-level panels such as unemployment rates. |

Example:

```python
from survey_kit_data.fed.scf import scf
from survey_kit_data.fed.fred import StatePanels

scf_2022 = scf(2022)
unemployment = StatePanels.unemployment_rate(observation_start="2018-01-01")
```

### BLS Consumer Expenditure Survey

| Loader | Output |
| ------ | ------ |
| `cex(year)` | Interview and diary tables. |

Example:

```python
from survey_kit_data.bls.cex import cex

tables = cex(2023)
print(tables.keys())
```
