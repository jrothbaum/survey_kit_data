# API

This is a hand-written API summary for the current public loader surface. See
the examples directory for runnable scripts.

## Common Patterns

Most loaders return `polars.LazyFrame`.

```python
lf = loader(...)
df = lf.collect()
```

Common keyword arguments:

| Argument | Common default | Meaning |
| -------- | -------------- | ------- |
| `force_reload` | `False` | Rebuild the parsed cache. |
| `reload_if_updated` | `True` | Check source metadata when supported. |
| `include_fips` | `True` where supported | Add state or county FIPS columns. |

## HHS TANF/AFDC

```python
from survey_kit_data.hhs.tanf import tanf_caseload, afdc_caseload

tanf = tanf_caseload(years=[2021]).collect()
```

### `tanf_caseload(...)`

Returns one monthly table.

Important arguments:

| Argument | Meaning |
| -------- | ------- |
| `years` | Calendar years to return. `None` loads supported years. |
| `download_mirror` | Optional local, installed, or GitHub `survey-kit-download` mirror. |
| `download_mirror_mode` | `"prefer"`, `"fallback"`, or `"only"`. |
| `include_source` | Include source URL and worksheet columns. |
| `include_fips` | Include `state_fips`. |
| `file_urls_by_year` | Override or provide workbook URLs by source year. |

The loader may read adjacent source-year files to assemble the requested
calendar year, then filters the output back to the requested years.

### `afdc_caseload(...)`

Uses the same normalized monthly table shape as `tanf_caseload`.

### Source Sheet Loaders

Use `tanf_caseload_source_sheets(...)` and `afdc_caseload_source_sheets(...)`
when validating parser behavior against raw workbook sheets. These return a
dictionary of source-shaped `LazyFrame` objects keyed by program, source year,
workbook label, and worksheet name.

## USDA SNAP

```python
from survey_kit_data.usda.snap import snap_state_history, snap_county_history

state = snap_state_history().collect()
county = snap_county_history().collect()
```

| Function | Return |
| -------- | ------ |
| `snap_monthly()` | National monthly aggregate history. |
| `snap_state_history()` | State monthly history. |
| `snap_county_history()` | County/substate January and July snapshots. |
| `snap_persons_snapshot()` | Recent state persons snapshot. |
| `snap_households_snapshot()` | Recent state households snapshot. |
| `snap_benefits_snapshot()` | Recent state benefit snapshot. |

## DOL UI

```python
from survey_kit_data.dol.ui import weekly_ui_claims, insured_unemployed_characteristics

claims = weekly_ui_claims().collect()
characteristics = insured_unemployed_characteristics().collect()
```

| Function | Return |
| -------- | ------ |
| `weekly_ui_claims()` | ETA 539 weekly claims by state. |
| `insured_unemployed_characteristics()` | ETA 203 monthly insured unemployed characteristics. |

## Census

```python
from survey_kit_data.census.cps_asec import cps_asec
from survey_kit_data.census.api import acs5_income
```

| Function | Return |
| -------- | ------ |
| `cps_asec(year)` | Dict of CPS ASEC tables including household, person, and replicate weights. |
| `acs5_income(...)` | ACS5 API output as a Polars table. |

## Federal Reserve

```python
from survey_kit_data.fed.scf import scf
from survey_kit_data.fed.fred import StatePanels
```

| Function | Return |
| -------- | ------ |
| `scf(year)` | Dict of SCF implicates and replicate weights. |
| `StatePanels.unemployment_rate(...)` | State unemployment panel from FRED. |

## BLS CEX

```python
from survey_kit_data.bls.cex import cex

tables = cex(2023)
```

`cex(year)` returns a dictionary of Consumer Expenditure Survey tables.
