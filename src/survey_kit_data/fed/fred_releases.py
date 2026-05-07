from __future__ import annotations

from pathlib import Path
from typing import Iterable

import polars as pl

from .. import config, logger
from ..cache_manager import FileCacheManager
from .fred_catalog import FREDReleaseSpec
from .fred_client import FREDClient


def release_cache_path(spec: FREDReleaseSpec) -> Path:
    return Path(config.path_cache_files) / "fred" / "releases" / f"{spec.name}.parquet"


def load_release_observations(
    spec: FREDReleaseSpec,
    *,
    client: FREDClient | None = None,
    force_reload: bool = False,
    limit: int = 500_000,
) -> pl.LazyFrame:
    """Load a FRED v2 release, normalize observations, and cache as parquet."""
    path = release_cache_path(spec)
    path.parent.mkdir(parents=True, exist_ok=True)

    fcm = FileCacheManager(
        path_save=str(path),
        api_call=_release_cache_signature,
        api_args={"release_id": spec.release_id, "limit": limit, "version": 1},
    )
    if not force_reload and fcm.is_cached():
        logger.info(f"FRED: loading release {spec.name} from cache")
        return pl.scan_parquet(path)

    logger.info(f"FRED: fetching release {spec.name} from API v2")
    client = client or FREDClient()
    rows = list(_iter_release_rows(client, spec, limit=limit))
    df = _rows_to_frame(rows)
    df.write_parquet(path)
    fcm.save_metadata()
    return pl.scan_parquet(path)


def _release_cache_signature() -> None:
    return None


def _iter_release_rows(
    client: FREDClient,
    spec: FREDReleaseSpec,
    *,
    limit: int,
) -> Iterable[dict[str, object]]:
    next_cursor = None
    while True:
        payload = client.v2_release_observations(
            spec.release_id,
            limit=limit,
            next_cursor=next_cursor,
        )
        for series in payload.get("series", []):
            series_id = series["series_id"]
            if not spec.series_filter(series_id):
                continue
            for obs in series.get("observations", []):
                yield {
                    "series_id": series_id,
                    "date": obs.get("date"),
                    "value": obs.get("value"),
                    "title": series.get("title"),
                    "frequency": series.get("frequency"),
                    "units": series.get("units"),
                    "seasonal_adjustment": series.get("seasonal_adjustment"),
                    "last_updated": series.get("last_updated"),
                }

        if not payload.get("has_more"):
            break
        next_cursor = payload.get("next_cursor")
        if not next_cursor:
            raise RuntimeError("FRED v2 response had has_more=true but no next_cursor")


def _rows_to_frame(rows: list[dict[str, object]]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(
            schema={
                "series_id": pl.String,
                "date": pl.Date,
                "value": pl.Float64,
                "title": pl.String,
                "frequency": pl.String,
                "units": pl.String,
                "seasonal_adjustment": pl.String,
                "last_updated": pl.String,
            }
        )

    return (
        pl.DataFrame(rows)
        .with_columns(
            pl.col("date").str.to_date(),
            pl.when(pl.col("value") == ".")
            .then(None)
            .otherwise(pl.col("value"))
            .cast(pl.Float64)
            .alias("value"),
        )
        .sort(["series_id", "date"])
    )
