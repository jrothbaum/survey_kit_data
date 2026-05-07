from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Iterable

import polars as pl
import requests

from .. import config, logger
from ..cache_manager import FileCacheManager


CENSUS_API_ROOT = "https://api.census.gov/data"


class CensusClient:
    """Small raw client for Census Data API calls used by cached loaders."""

    def __init__(
        self,
        api_key: str | None = None,
        api_root: str = CENSUS_API_ROOT,
    ) -> None:
        self.api_key = api_key if api_key is not None else config.api_key_census
        self.api_root = api_root.rstrip("/")

    def get(
        self,
        *,
        year: int,
        dataset: str,
        variables: str | Iterable[str],
        geo_for: str,
        geo_in: str | Iterable[str] | None = None,
        predicates: dict[str, Any] | None = None,
    ) -> list[list[str]]:
        params = _query_params(
            variables=variables,
            geo_for=geo_for,
            geo_in=geo_in,
            predicates=predicates,
        )
        if self.api_key:
            params["key"] = self.api_key
        return self._get_json(f"{year}/{dataset.strip('/')}", params)

    def _get_json(self, path: str, params: dict[str, Any]) -> list[list[str]]:
        url = f"{self.api_root}/{path.lstrip('/')}"
        response = None
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=60)
                if response.status_code < 500:
                    break
            except requests.RequestException:
                if attempt == 2:
                    raise
            time.sleep(0.5 * (attempt + 1))

        if response is None:
            raise RuntimeError(f"Census request failed before receiving a response: {url}")
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and "error" in payload:
            raise RuntimeError(f"Census API error: {payload['error']}")
        return payload


def get(
    *,
    year: int,
    dataset: str,
    variables: str | Iterable[str],
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    predicates: dict[str, Any] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """
    Fetch a Census API table, cache it as parquet, and return a LazyFrame.

    Parameters mirror the Census API query shape. For example:

    >>> get(
    ...     year=2022,
    ...     dataset="acs/acs5",
    ...     variables=["NAME", "B19013_001E"],
    ...     geo_for="state:*",
    ... )
    """
    query = _normalized_query(
        year=year,
        dataset=dataset,
        variables=variables,
        geo_for=geo_for,
        geo_in=geo_in,
        predicates=predicates,
    )
    path = _cache_path(query)
    path.parent.mkdir(parents=True, exist_ok=True)

    fcm = FileCacheManager(
        path_save=str(path),
        api_call=_census_cache_signature,
        api_args={**query, "version": 1},
    )
    if not force_reload and fcm.is_cached():
        logger.info(f"Census: loading {year}/{dataset} from cache")
        return pl.scan_parquet(path)

    logger.info(f"Census: fetching {year}/{dataset} from API")
    client = CensusClient()
    payload = client.get(
        year=year,
        dataset=dataset,
        variables=variables,
        geo_for=geo_for,
        geo_in=geo_in,
        predicates=predicates,
    )
    df = _payload_to_frame(payload)
    df.write_parquet(path)
    fcm.save_metadata()
    return pl.scan_parquet(path)


def acs5(
    *,
    year: int,
    variables: str | Iterable[str],
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    predicates: dict[str, Any] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """Fetch ACS 5-year data from the Census API."""
    return get(
        year=year,
        dataset="acs/acs5",
        variables=variables,
        geo_for=geo_for,
        geo_in=geo_in,
        predicates=predicates,
        force_reload=force_reload,
    )


def acs1(
    *,
    year: int,
    variables: str | Iterable[str],
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    predicates: dict[str, Any] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """Fetch ACS 1-year data from the Census API."""
    return get(
        year=year,
        dataset="acs/acs1",
        variables=variables,
        geo_for=geo_for,
        geo_in=geo_in,
        predicates=predicates,
        force_reload=force_reload,
    )


# ── ACS variable catalogs ────────────────────────────────────────────────────

_DEMOGRAPHICS_VARS = [
    "NAME",
    "B01003_001E",  # total population
    "B01002_001E",  # median age
    "B02001_002E",  # white alone
    "B02001_003E",  # black or african american alone
    "B02001_005E",  # asian alone
    "B03003_003E",  # hispanic or latino
    "B01001_002E",  # male
    "B01001_026E",  # female
]

_DEMOGRAPHICS_RENAME = {
    "B01003_001E": "population",
    "B01002_001E": "median_age",
    "B02001_002E": "white",
    "B02001_003E": "black",
    "B02001_005E": "asian",
    "B03003_003E": "hispanic",
    "B01001_002E": "male",
    "B01001_026E": "female",
}

_INCOME_VARS = [
    "NAME",
    "B19013_001E",  # median household income
    "B19301_001E",  # per capita income
    "B17001_001E",  # poverty universe
    "B17001_002E",  # below poverty level
    "B20004_001E",  # median earnings (workers 16+)
]

_INCOME_RENAME = {
    "B19013_001E": "median_household_income",
    "B19301_001E": "per_capita_income",
    "B17001_001E": "poverty_universe",
    "B17001_002E": "below_poverty",
    "B20004_001E": "median_earnings",
}

_NUMERIC_COLS = set(_DEMOGRAPHICS_RENAME.values()) | set(_INCOME_RENAME.values())


def _cast_numeric(lf: pl.LazyFrame) -> pl.LazyFrame:
    present = [c for c in lf.collect_schema().names() if c in _NUMERIC_COLS]
    if not present:
        return lf
    return lf.with_columns(
        [
            pl.when(pl.col(c).is_in(["-666666666", "-999999999"]))
            .then(None)
            .otherwise(pl.col(c))
            .cast(pl.Float64)
            .alias(c)
            for c in present
        ]
    )


def acs5_demographics(
    *,
    year: int,
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """
    ACS 5-year population and demographic profile by geography.

    Columns: NAME, population, median_age, white, black, asian, hispanic,
    male, female, plus geography identifier columns returned by the API.
    Sentinel values (-666666666, -999999999) are replaced with null.
    """
    lf = acs5(
        year=year,
        variables=_DEMOGRAPHICS_VARS,
        geo_for=geo_for,
        geo_in=geo_in,
        force_reload=force_reload,
    ).rename(_DEMOGRAPHICS_RENAME)
    return _cast_numeric(lf)


def acs1_demographics(
    *,
    year: int,
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """ACS 1-year population and demographic profile. See acs5_demographics."""
    lf = acs1(
        year=year,
        variables=_DEMOGRAPHICS_VARS,
        geo_for=geo_for,
        geo_in=geo_in,
        force_reload=force_reload,
    ).rename(_DEMOGRAPHICS_RENAME)
    return _cast_numeric(lf)


def acs5_income(
    *,
    year: int,
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """
    ACS 5-year income and poverty profile by geography.

    Columns: NAME, median_household_income, per_capita_income,
    poverty_universe, below_poverty, median_earnings, plus geography
    identifier columns. Sentinel values replaced with null.
    """
    lf = acs5(
        year=year,
        variables=_INCOME_VARS,
        geo_for=geo_for,
        geo_in=geo_in,
        force_reload=force_reload,
    ).rename(_INCOME_RENAME)
    return _cast_numeric(lf)


def acs1_income(
    *,
    year: int,
    geo_for: str,
    geo_in: str | Iterable[str] | None = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """ACS 1-year income and poverty profile. See acs5_income."""
    lf = acs1(
        year=year,
        variables=_INCOME_VARS,
        geo_for=geo_for,
        geo_in=geo_in,
        force_reload=force_reload,
    ).rename(_INCOME_RENAME)
    return _cast_numeric(lf)


def _census_cache_signature() -> None:
    return None


def _query_params(
    *,
    variables: str | Iterable[str],
    geo_for: str,
    geo_in: str | Iterable[str] | None,
    predicates: dict[str, Any] | None,
) -> dict[str, Any]:
    params: dict[str, Any] = {
        "get": ",".join(_as_list(variables)),
        "for": geo_for,
    }
    if geo_in is not None:
        params["in"] = " ".join(_as_list(geo_in))
    if predicates:
        params.update({key: value for key, value in predicates.items() if value is not None})
    return params


def _normalized_query(
    *,
    year: int,
    dataset: str,
    variables: str | Iterable[str],
    geo_for: str,
    geo_in: str | Iterable[str] | None,
    predicates: dict[str, Any] | None,
) -> dict[str, Any]:
    return {
        "year": year,
        "dataset": dataset.strip("/"),
        "variables": _as_list(variables),
        "geo_for": geo_for,
        "geo_in": _as_list(geo_in) if geo_in is not None else None,
        "predicates": {
            key: predicates[key]
            for key in sorted(predicates or {})
            if predicates[key] is not None
        },
    }


def _cache_path(query: dict[str, Any]) -> Path:
    payload = json.dumps(query, sort_keys=True, default=str)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()
    dataset = query["dataset"].replace("/", "_")
    return Path(config.path_cache_files) / "census" / dataset / f"{query['year']}_{digest}.parquet"


def _payload_to_frame(payload: list[list[str]]) -> pl.DataFrame:
    if not payload:
        return pl.DataFrame()
    header = payload[0]
    rows = payload[1:]
    if not rows:
        return pl.DataFrame(schema={name: pl.String for name in header})
    return pl.DataFrame(rows, schema=header, orient="row")


def _as_list(value: str | Iterable[str]) -> list[str]:
    if isinstance(value, str):
        return [value]
    return list(value)
