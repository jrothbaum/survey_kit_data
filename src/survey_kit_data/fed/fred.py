# src/survey_kit_data/fed/fred.py
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional
import polars as pl

from ..cache_manager import FileCacheManager
from .. import config, logger
from .fred_catalog import (
    BUILDING_PERMITS,
    FRED_RELEASES,
    HOUSE_PRICE_INDEX,
    LAUS_ALL_OTHER_AREAS,
    PERSONAL_INCOME_BY_STATE,
    QCEW,
    REAL_PERSONAL_INCOME_BY_STATE,
    STATE_UI_WEEKLY_CLAIMS,
    STATE_UNEMPLOYMENT_RATE,
    ZILLOW_HOME_VALUE_INDEX,
    FREDReleaseSpec,
)
from .fred_client import FREDClient
from .fred_releases import load_release_observations


# ── FRED state postal code list ──────────────────────────────────────────────

ALL_STATES: List[str] = [
    "AL","AK","AZ","AR","CA","CO","CT","DE","FL","GA",
    "HI","ID","IL","IN","IA","KS","KY","LA","ME","MD",
    "MA","MI","MN","MS","MO","MT","NE","NV","NH","NJ",
    "NM","NY","NC","ND","OH","OK","OR","PA","RI","SC",
    "SD","TN","TX","UT","VT","VA","WA","WV","WI","WY",
    "DC",
]


def _cache_path(
    series_id: str,
    observation_start: Optional[str],
    observation_end: Optional[str] = None,
    realtime_start: Optional[str] = None,
    realtime_end: Optional[str] = None,
) -> Path:
    """Canonical parquet path for a single series."""
    safe_id = series_id.replace("/", "_")
    suffix = f"_from_{observation_start}" if observation_start else ""
    suffix += f"_to_{observation_end}" if observation_end else ""
    if realtime_start and realtime_end and realtime_start == realtime_end:
        suffix += f"_asof_{realtime_start}"
    elif realtime_start or realtime_end:
        suffix += f"_rt_{realtime_start or 'start'}_{realtime_end or 'end'}"
    return Path(config.path_cache_files) / "fred" / f"{safe_id}{suffix}.parquet"


def _observations_to_frame(series_id: str, observations: list[dict]) -> pl.DataFrame:
    """Convert FRED v1 observation JSON into a tidy Polars DataFrame."""
    if not observations:
        return pl.DataFrame(
            schema={
                "date": pl.Date,
                "value": pl.Float64,
                "series_id": pl.String,
            }
        )

    return (
        pl.DataFrame(observations)
        .select(["date", "value"])
        .with_columns(
            pl.col("date").str.to_date(),
            pl.when(pl.col("value") == ".")
            .then(None)
            .otherwise(pl.col("value"))
            .cast(pl.Float64)
            .alias("value"),
            pl.lit(series_id).alias("series_id"),
        )
        .sort("date")
    )


# ── Core single-series loader ────────────────────────────────────────────────

def get_series(
    series_id: str,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    realtime_start: Optional[str] = None,
    realtime_end: Optional[str] = None,
    as_of: Optional[str] = None,
    max_age_days: float = 7.0,      # retained for compatibility; freshness uses last_updated
    force_reload: bool = False,      # explicitly declared
) -> pl.LazyFrame:

    if as_of and (realtime_start or realtime_end):
        raise ValueError("Use either as_of or realtime_start/realtime_end, not both")
    if as_of:
        realtime_start = as_of
        realtime_end = as_of

    client = FREDClient()
    path = _cache_path(
        series_id,
        observation_start,
        observation_end,
        realtime_start,
        realtime_end,
    )
    path.parent.mkdir(parents=True, exist_ok=True)

    metadata_kwargs = {}
    if realtime_start:
        metadata_kwargs["realtime_start"] = realtime_start
    if realtime_end:
        metadata_kwargs["realtime_end"] = realtime_end
    metadata = client.v1_series_info(series_id, **metadata_kwargs)
    seriess = metadata.get("seriess", [])
    series_info = seriess[0] if seriess else {}
    last_updated = series_info.get("last_updated")

    observation_kwargs = {}
    if observation_start:
        observation_kwargs["observation_start"] = observation_start
    if observation_end:
        observation_kwargs["observation_end"] = observation_end
    if realtime_start:
        observation_kwargs["realtime_start"] = realtime_start
    if realtime_end:
        observation_kwargs["realtime_end"] = realtime_end

    fcm = FileCacheManager(
        path_save=str(path),
        api_call=client.v1_series_observations,
        api_args={
            "args": (series_id,),
            "kwargs": observation_kwargs,
            "last_updated": last_updated,
            "version": 2,
        },
    )

    if not force_reload and fcm.is_cached():
        logger.info(f"FRED: loading {series_id} from cache")
        return pl.scan_parquet(path)

    logger.info(f"FRED: fetching {series_id} from API")

    payload = client.v1_series_observations(series_id, **observation_kwargs)
    df = _observations_to_frame(series_id, payload.get("observations", []))
    df.write_parquet(path)
    fcm.save_metadata()

    return pl.scan_parquet(path)



# ── State panel builder ──────────────────────────────────────────────────────

def get_state_panel(
    series_key: str,
    states: List[str] = ALL_STATES,
    state_position: str = "prefix",
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    as_of: Optional[str] = None,
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
    observation_end : str, optional
        ISO date string for the latest observation.
    as_of : str, optional
        Historical real-time date. Passed to FRED as realtime_start and
        realtime_end.
    max_age_days : float
        Retained for compatibility. Cache freshness uses FRED last_updated.
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
                observation_end=observation_end,
                as_of=as_of,
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
    observation_end: Optional[str] = None,
    as_of: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Weekly initial UI claims by state. FRED key: ``{ST}ICLAIMS``."""
    return get_state_panel(
        "ICLAIMS", states=states,
        observation_start=observation_start,
        observation_end=observation_end,
        as_of=as_of,
        max_age_days=max_age_days,
    )


def ui_continuing_claims(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    as_of: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Weekly continuing UI claims by state. FRED key: ``{ST}CCLAIMS``."""
    return get_state_panel(
        "CCLAIMS", states=states,
        observation_start=observation_start,
        observation_end=observation_end,
        as_of=as_of,
        max_age_days=max_age_days,
    )


def unemployment_rate(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    as_of: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Monthly unemployment rate by state (LAUS). FRED key: ``{ST}UR``."""
    return get_state_panel(
        "UR", states=states,
        observation_start=observation_start,
        observation_end=observation_end,
        as_of=as_of,
        max_age_days=max_age_days,
    )


def state_unemployment_rate(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    force_reload: bool = False,
) -> pl.LazyFrame:
    """
    Monthly state unemployment rates from FRED's v2 release bulk endpoint.

    This is the release-oriented path: FRED release IDs and state series codes
    are internal details, and the normalized release cache is reused across
    calls.
    """
    lf = (
        load_release_observations(
            STATE_UNEMPLOYMENT_RATE,
            force_reload=force_reload,
        )
        .with_columns(pl.col("series_id").str.slice(0, 2).alias("state"))
        .filter(pl.col("state").is_in(states))
        .select(
            [
                "state",
                "date",
                "value",
                "series_id",
                "title",
                "frequency",
                "units",
                "seasonal_adjustment",
                "last_updated",
            ]
        )
    )
    if observation_start:
        lf = lf.filter(pl.col("date") >= pl.lit(observation_start).str.to_date())
    return lf.sort(["state", "date"])


def _as_list(value: str | list[str] | tuple[str, ...] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, str):
        return [value]
    return list(value)


def _contains_any(column: str, patterns: list[str]) -> pl.Expr:
    expr = pl.lit(False)
    lowered = pl.col(column).str.to_lowercase()
    for pattern in patterns:
        expr = expr | lowered.str.contains(pattern.lower(), literal=True)
    return expr


def _resolve_release(release: str | FREDReleaseSpec) -> FREDReleaseSpec:
    if isinstance(release, FREDReleaseSpec):
        return release
    try:
        return FRED_RELEASES[release]
    except KeyError as exc:
        options = ", ".join(sorted(FRED_RELEASES))
        raise ValueError(f"Unknown FRED release '{release}'. Options: {options}") from exc


def _filter_release_frame(
    lf: pl.LazyFrame,
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    frequency: str | list[str] | tuple[str, ...] | None = None,
    units: str | list[str] | tuple[str, ...] | None = None,
    seasonal_adjustment: str | list[str] | tuple[str, ...] | None = None,
) -> pl.LazyFrame:
    ids = _as_list(series_ids)
    titles = _as_list(title_contains)
    frequencies = _as_list(frequency)
    units_list = _as_list(units)
    adjustments = _as_list(seasonal_adjustment)

    if observation_start:
        lf = lf.filter(pl.col("date") >= pl.lit(observation_start).str.to_date())
    if observation_end:
        lf = lf.filter(pl.col("date") <= pl.lit(observation_end).str.to_date())
    if ids:
        lf = lf.filter(pl.col("series_id").is_in(ids))
    if titles:
        lf = lf.filter(_contains_any("title", titles))
    if frequencies:
        lf = lf.filter(pl.col("frequency").is_in(frequencies))
    if units_list:
        lf = lf.filter(pl.col("units").is_in(units_list))
    if adjustments:
        lf = lf.filter(pl.col("seasonal_adjustment").is_in(adjustments))

    return lf.sort(["series_id", "date"])


def release_observations(
    release: str | FREDReleaseSpec,
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    frequency: str | list[str] | tuple[str, ...] | None = None,
    units: str | list[str] | tuple[str, ...] | None = None,
    seasonal_adjustment: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """
    Load a FRED v2 release and optionally filter the normalized observation panel.

    The cache is keyed by release-level API parameters. The filters here are
    applied to the cached parquet, so repeated exploration stays local once the
    release is cached.
    """
    spec = _resolve_release(release)
    lf = load_release_observations(
        spec,
        force_reload=force_reload,
        limit=limit,
    )
    return _filter_release_frame(
        lf,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        frequency=frequency,
        units=units,
        seasonal_adjustment=seasonal_adjustment,
    )


def state_ui_weekly_claims(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    measures: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """State UI weekly claims: initial claims, continued claims, IUR, covered employment."""
    title_filters = _as_list(title_contains) or _as_list(measures)
    lf = release_observations(
        STATE_UI_WEEKLY_CLAIMS,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_filters,
        frequency="Weekly",
        force_reload=force_reload,
        limit=limit,
    )
    return lf.with_columns(
        pl.when(pl.col("series_id").str.slice(0, 2).is_in(ALL_STATES))
        .then(pl.col("series_id").str.slice(0, 2))
        .otherwise(None)
        .alias("state")
    )


def local_area_unemployment(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    frequency: str | list[str] | tuple[str, ...] | None = "Monthly",
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """County, metro, and other local LAUS indicators from release 116."""
    return release_observations(
        LAUS_ALL_OTHER_AREAS,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        frequency=frequency,
        force_reload=force_reload,
        limit=limit,
    )


def qcew(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """Quarterly Census of Employment and Wages release observations."""
    return release_observations(
        QCEW,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        frequency="Quarterly",
        force_reload=force_reload,
        limit=limit,
    )


def building_permits(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    frequency: str | list[str] | tuple[str, ...] | None = "Monthly",
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """Housing units authorized by building permits."""
    return release_observations(
        BUILDING_PERMITS,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        frequency=frequency,
        force_reload=force_reload,
        limit=limit,
    )


def house_price_index(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """FHFA house price index release observations."""
    return release_observations(
        HOUSE_PRICE_INDEX,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        force_reload=force_reload,
        limit=limit,
    )


def zillow_home_value_index(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """Zillow Home Value Index release observations."""
    return release_observations(
        ZILLOW_HOME_VALUE_INDEX,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        frequency="Monthly",
        force_reload=force_reload,
        limit=limit,
    )


def personal_income_by_state(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """BEA personal income by state release observations."""
    return release_observations(
        PERSONAL_INCOME_BY_STATE,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        force_reload=force_reload,
        limit=limit,
    )


def real_personal_income_by_state(
    *,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    series_ids: str | list[str] | tuple[str, ...] | None = None,
    title_contains: str | list[str] | tuple[str, ...] | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """BEA real personal income and regional price parity release observations."""
    return release_observations(
        REAL_PERSONAL_INCOME_BY_STATE,
        observation_start=observation_start,
        observation_end=observation_end,
        series_ids=series_ids,
        title_contains=title_contains,
        force_reload=force_reload,
        limit=limit,
    )


def nonfarm_payroll(
    states: List[str] = ALL_STATES,
    observation_start: Optional[str] = None,
    observation_end: Optional[str] = None,
    as_of: Optional[str] = None,
    max_age_days: float = 7.0,
) -> pl.LazyFrame:
    """Monthly nonfarm payroll employment by state. FRED key: ``{ST}NA``."""
    return get_state_panel(
        "NA", states=states,
        observation_start=observation_start,
        observation_end=observation_end,
        as_of=as_of,
        max_age_days=max_age_days,
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
            observation_end: Optional[str] = None,
            as_of: Optional[str] = None,
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
                observation_end=observation_end,
                as_of=as_of,
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
