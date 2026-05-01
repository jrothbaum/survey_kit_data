# src/survey_kit_data/fed/fred.py
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import polars as pl

from ..cache_manager import FileCacheManager
from .. import config, logger


# ── FRED state postal code list ──────────────────────────────────────────────

ALL_STATES: List[str] = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC",
]


# ── Internal helpers ─────────────────────────────────────────────────────────

def _get_client():
    """Return an authenticated fredapi.Fred instance."""
    try:
        from fredapi import Fred
    except ImportError:
        raise ImportError(
            "fredapi is required for FRED data. Install with: pip install fredapi"
        )
    key = config.api_key_fred
    if not key:
        raise ValueError(
            "No FRED API key found. Set config.api_key_fred or "
            "env var 'survey_kit_data_api_fred'. "
            "Get a free key at https://fred.stlouisfed.org/docs/api/api_key.html"
        )
    return Fred(api_key=key)


def _cache_path(series_id: str, observation_start: Optional[str]) -> Path:
    """Canonical parquet path for a single series."""
    safe_id = series_id.replace("/", "_")
    suffix = f"_from_{observation_start}" if observation_start else ""
    return Path(config.path_cache_files) / "fred" / f"{safe_id}{suffix}.parquet"


def _is_stale(path: Path, max_age_days: float) -> bool:
    """True if the file is older than max_age_days (or doesn't exist)."""
    if not path.exists():
        return True
    age_seconds = time.time() - path.stat().st_mtime
    return age_seconds > max_age_days * 86_400


def _series_to_frame(series_id: str, pd_series) -> pl.DataFrame:
    """Convert a pandas Series returned by fredapi into a tidy Polars DataFrame."""
    import pandas as pd
    df = pd_series.reset_index()
    df.columns = ["date", "value"]
    df["series_id"] = series_id
    df["date"] = pd.to_datetime(df["date"])
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    return pl.from_pandas(df)


# ── Core single-series loader ────────────────────────────────────────────────

def get_series(
    series_id: str,
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,      # explicitly declared
    force_reload: bool = False,      # explicitly declared
) -> pl.LazyFrame:

    client = _get_client()
    info = client.get_series_info(series_id)
    last_updated = str(info["last_updated"])

    path = _cache_path(series_id, observation_start)
    path.parent.mkdir(parents=True, exist_ok=True)

    fcm = FileCacheManager(
        path_save=str(path),
        api_call=client.get_series,
        api_args={
            "args": (series_id,),
            "kwargs": {"observation_start": observation_start},
            "last_updated": last_updated,
        },
    )

    if not force_reload and fcm.is_cached():
        logger.info(f"FRED: loading {series_id} from cache")
        return pl.scan_parquet(path)

    logger.info(f"FRED: fetching {series_id} from API")
    
    # Only fredapi-valid kwargs passed here — not max_age_days, not force_reload
    kwargs = {}
    if observation_start:
        kwargs["observation_start"] = observation_start

    pd_series = client.get_series(series_id, **kwargs)
    df = _series_to_frame(series_id, pd_series)
    df.write_parquet(path)
    fcm.save_metadata()

    return pl.scan_parquet(path)



# ── State panel builder ──────────────────────────────────────────────────────

def get_state_panel(
    series_key: str,
    states: List[str] = ALL_STATES,
    state_position: str = "prefix",
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """
    Fetch a FRED series for every state and return a tidy state × date panel.

    FRED state series follow one of two naming conventions:

    * **Prefix**: ``{ST}{KEY}``  e.g. ``"CAUR"`` (CA + UR = unemployment rate)
    * **Suffix**: ``{KEY}{ST}``  e.g. ``"ICLAIMS_CA"`` (less common)

    Each state series is cached independently, so a partial update (when only
    some states have new data) is handled gracefully.

    Parameters
    ----------
    series_key : str
        The non-state portion of the series ID.
    states : list of str
        Two-letter postal codes. Defaults to all 50 states + DC.
    state_position : {"prefix", "suffix"}
        Whether the state code precedes or follows the key.
    observation_start : str, optional
        ISO date string for the earliest observation.
    max_age_days : float
        Cache TTL in days. Default is 7.
    force_reload : bool
        Bypass cache for all states.

    Returns
    -------
    pl.LazyFrame
        Columns: ``date`` (Date), ``value`` (Float64), ``series_id`` (String),
        ``state`` (String).

    Examples
    --------
    >>> from survey_kit_data.fed.fred import get_state_panel
    >>> # Monthly unemployment rate, all states, from 2018
    >>> lf = get_state_panel("UR", observation_start="2018-01-01")
    >>> lf.collect().shape
    """
    frames = []
    failed = []

    for st in states:
        if state_position == "prefix":
            series_id = f"{st}{series_key}"
        else:
            series_id = f"{series_key}{st}"

        try:
            lf = get_series(
                series_id,
                observation_start=observation_start,
                max_age_days=max_age_days,
                force_reload=force_reload,
            )
            frames.append(lf.with_columns(pl.lit(st).alias("state")))
        except Exception as e:
            logger.warning(f"FRED: could not fetch {series_id}: {e}")
            failed.append(series_id)

    if failed:
        logger.warning(f"FRED: missing series for {len(failed)} states: {failed}")

    if not frames:
        raise ValueError(f"No data retrieved for series_key='{series_key}'")

    return pl.concat(frames)


# ── Named convenience functions ──────────────────────────────────────────────
# These encode the FRED naming conventions so callers don't need to know them.

def ui_initial_claims(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Weekly initial UI claims by state. FRED key: ``{ST}ICLAIMS``."""
    return get_state_panel(
        "ICLAIMS", states=states,
        observation_start=observation_start, max_age_days=max_age_days,
    )


def ui_continuing_claims(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Weekly continuing UI claims by state. FRED key: ``{ST}CCLAIMS``."""
    return get_state_panel(
        "CCLAIMS", states=states,
        observation_start=observation_start, max_age_days=max_age_days,
    )


def unemployment_rate(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Monthly unemployment rate by state (LAUS). FRED key: ``{ST}UR``."""
    return get_state_panel(
        "UR", states=states,
        observation_start=observation_start, max_age_days=max_age_days,
    )


def nonfarm_payroll(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Monthly nonfarm payroll employment by state. FRED key: ``{ST}NA``."""
    return get_state_panel(
        "NA", states=states,
        observation_start=observation_start, max_age_days=max_age_days,
    )


def build_indicator_panel(
    observation_start: Optional[str] = "2010-01-01",
    states: List[str] = ALL_STATES,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """
    Assemble all Tier-1 FRED state indicators into a single wide panel.

    Fetches UI initial claims, UI continuing claims, unemployment rate, and
    nonfarm payroll for every state, then pivots to wide format keyed on
    ``(state, date)``.

    Returns
    -------
    pl.LazyFrame
        Columns: ``state``, ``date``, ``ui_initial_claims``,
        ``ui_continuing_claims``, ``unemployment_rate``, ``nonfarm_payroll``.
    """
    series_map = {
        "ui_initial_claims":    ("ICLAIMS",  "prefix"),
        "ui_continuing_claims": ("CCLAIMS",  "prefix"),
        "unemployment_rate":    ("UR",        "prefix"),
        "nonfarm_payroll":      ("NA",        "prefix"),
    }

    panels = []
    for col_name, (key, pos) in series_map.items():
        lf = (
            get_state_panel(
                key, states=states,
                state_position=pos,
                observation_start=observation_start,
                max_age_days=max_age_days,
            )
            .select(["state", "date", "value"])
            .rename({"value": col_name})
        )
        panels.append(lf)

    # Join all panels on state + date
    result = panels[0]
    for lf in panels[1:]:
        result = result.join(lf, on=["state", "date"], how="outer_coalesce")

    return result.sort(["state", "date"])



@dataclass
class StateSeries:
    """
    Descriptor for a FRED state-level series.
    Doubles as callable when accessed from the class.
    """
    key: str
    description: str
    frequency: str  # "weekly", "monthly", etc. — useful for docs

    def __set_name__(self, owner, name):
        self.name = name

    def __get__(self, obj, objtype=None):
        # Return a callable when accessed from either instance or class
        series = self

        def loader(
            states: List[str] = ALL_STATES,
            observation_start: Optional[str] = None,
        ) -> pl.LazyFrame:
            f"""
            {series.description}

            FRED key: {{ST}}{series.key} ({series.frequency})

            Parameters
            ----------
            states : list of str
                State postal codes. Default is all 50 states + DC.
            observation_start : str, optional
                ISO date string, e.g. "2018-01-01".

            Returns
            -------
            pl.LazyFrame
                Columns: state, date, value, series_id.
            """
            return get_state_panel(
                series_key=series.key,
                states=states,
                observation_start=observation_start,
            )

        loader.__name__ = self.name
        loader.__qualname__ = f"StatePanels.{self.name}"
        loader.__doc__ = (
            f"{series.description}\n\n"
            f"FRED key: {{ST}}{series.key} ({series.frequency})"
        )
        return loader


class StatePanels:
    """
    Pre-built state panel loaders for common FRED indicators.

    Each attribute is directly callable — no instantiation needed.

    Examples
    --------
    >>> from survey_kit_data.fed.fred import StatePanels
    >>> lf = StatePanels.unemployment_rate(observation_start="2018-01-01")
    >>> lf = StatePanels.initial_claims(states=["CA", "TX", "NY"])
    """

    unemployment_rate = StateSeries(
        key="UR",
        description="Monthly unemployment rate by state (LAUS/BLS).",
        frequency="monthly",
    )
    initial_claims = StateSeries(
        key="ICLAIMS",
        description="Weekly initial UI claims by state (DOL).",
        frequency="weekly",
    )
    continuing_claims = StateSeries(
        key="CCLAIMS",
        description="Weekly continuing UI claims by state (DOL).",
        frequency="weekly",
    )
    nonfarm_payroll = StateSeries(
        key="NA",
        description="Monthly nonfarm payroll employment by state (CES/BLS).",
        frequency="monthly",
    )