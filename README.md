# survey-kit-data

Python library for loading U.S. government survey and economic data into [Polars](https://pola.rs/) DataFrames. It wraps the download, parsing, and light reshaping for a handful of major public datasets — CPS, SCF, CEX, FRED, SNAP, and others — so you can get to analysis without tracking down file formats or agency-specific APIs. Downloads are cached locally so repeated calls skip re-downloading.

Documentation is available at <https://jrothbaum.github.io/survey_kit_data/>.

## Data sources

| Agency | Data |
|--------|------|
| Census Bureau | CPS ASEC (1988–present), ACS5 via API |
| Federal Reserve | FRED time series & state panels, SCF |
| BLS | Consumer Expenditure Survey (CEX) |
| USDA | SNAP persons, households, benefits, state/county history |
| HHS | TANF/AFDC caseload workbooks |
| DOL | Unemployment Insurance summary reports |

## Installation

```bash
pip install survey-kit-data
```

## Configuration

Set a cache directory and any needed API keys — either directly or via environment variables:

```python
from survey_kit_data import config

config.data_root = "/path/to/data"        # cache lives at {data_root}/cached_files
config.api_key_census = "your_key"        # https://api.census.gov/data/key_signup.html
config.api_key_fred   = "your_key"        # https://fred.stlouisfed.org/docs/api/api_key.html
```

Or via `.env` / shell exports using the corresponding env var names (see `Config` docstring).

## Loader behavior

Loaders are intentionally source-oriented: they download public files or API results, parse them into Polars, apply light cleanup such as date parsing and unambiguous geography IDs, and cache the parsed parquet output. Single-table loaders return a `polars.LazyFrame`; naturally multi-table sources return a `dict[str, polars.LazyFrame]`. Most loaders support `force_reload` to rebuild the cache and `reload_if_updated` to refresh when the source advertises a newer file.

## Quick examples

**CPS ASEC**
```python
from survey_kit_data.census.cps_asec import cps_asec
d = cps_asec(2023)          # returns dict with "hhld", "person", "replicate_weights"
print(d["hhld"].collect())
```

**SCF**
```python
from survey_kit_data.fed.scf import scf
d = scf(2022)               # returns dict of implicates + replicate weights
```

**FRED state panels**
```python
from survey_kit_data.fed.fred import StatePanels
df = StatePanels.unemployment_rate(observation_start="2018-01-01")
```

**CEX**
```python
from survey_kit_data.bls.cex import cex
d = cex(2023)               # returns dict of interview / diary tables
```

**SNAP**
```python
from survey_kit_data.usda.snap import snap_state_history, snap_county_history
state_df  = snap_state_history().collect()
county_df = snap_county_history().collect()
```

**DOL UI**
```python
from survey_kit_data.dol.ui import insured_unemployed_characteristics, weekly_ui_claims

characteristics = insured_unemployed_characteristics().collect()  # ETA 203
claims = weekly_ui_claims().collect()                             # ETA 539
```

**HHS TANF/AFDC**
```python
from survey_kit_data.hhs.tanf import tanf_caseload, afdc_caseload

tanf = tanf_caseload(years=[2025]).collect()  # monthly LazyFrame
afdc = afdc_caseload(years=[1995]).collect()
```

For sources that block scripted downloads, HHS loaders can read first from a
`survey-kit-download` raw-file mirror and only retry the agency site if the
mirror does not have the file:

```python
tanf = tanf_caseload(
    years=[2025],
    download_mirror="jrothbaum/survey_kit_download",  # GitHub raw mirror
    # download_mirror="../survey_kit_download",        # local checkout
    # include_source=True,  # add source_url/source_sheet audit columns
    # include_fips=False,   # omit state_fips column
)
```

**Census API (ACS5)**
```python
from survey_kit_data.census.api import acs5_income
df = acs5_income(year=2022, geo_for="state:*").collect()
```

See the [`examples/`](examples/) directory for fuller usage including replicate-weight standard errors and multiple imputation.

## License

CC0 1.0 Universal
