# Getting Started

## Installation

```bash
pip install survey-kit-data
```

For local development:

```bash
uv sync
uv run python examples/snap.py
```

## Configuration

Set package configuration in Python:

```python
from survey_kit_data import config

config.data_root = "/path/to/data"
config.api_key_census = "your-census-api-key"
config.api_key_fred = "your-fred-api-key"
```

The cache directory is derived from `config.data_root` and is used for raw and
parsed files depending on the loader. Most loaders expose:

| Argument | Meaning |
| -------- | ------- |
| `force_reload=True` | Ignore existing parsed cache and rebuild it. |
| `reload_if_updated=True` | Check source metadata when supported and refresh if changed. |
| `include_fips=False` | Omit generated FIPS columns where supported. |

## Basic Usage

Loaders usually return a Polars `LazyFrame`.

```python
from survey_kit_data.usda.snap import snap_state_history

snap = snap_state_history()
df = snap.collect()
```

Some sources naturally produce multiple tables and return a dictionary of
`LazyFrame` objects:

```python
from survey_kit_data.census.cps_asec import cps_asec

cps = cps_asec(2023)
households = cps["hhld"].collect()
people = cps["person"].collect()
```

## HHS Download Mirrors

Some HHS/ACF files can be blocked by an AWS WAF challenge for normal scripted
requests. HHS loaders can read from a `survey-kit-download` raw-file mirror.

```python
from survey_kit_data.hhs.tanf import tanf_caseload

tanf = tanf_caseload(
    years=[2021],
    download_mirror="jrothbaum/survey_kit_download",
    download_mirror_mode="prefer",
)
```

Supported mirror forms:

| Value | Meaning |
| ----- | ------- |
| Local path | A checkout containing `manifest.json` and cached data files. |
| `"installed"` | An importable `survey_kit_download` package with a manifest nearby. |
| `"owner/repo"` | A GitHub repository read through `raw.githubusercontent.com`. |
| GitHub URL | A full GitHub repository URL, optionally including `/tree/<ref>`. |

`download_mirror_mode` controls the fallback order:

| Mode | Behavior |
| ---- | -------- |
| `"prefer"` | Try the mirror first, then agency source. |
| `"fallback"` | Try agency source first, then mirror. |
| `"only"` | Require the mirror and never try the agency source. |
