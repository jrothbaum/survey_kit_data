from __future__ import annotations

import io
from pathlib import Path
from typing import Optional

import polars as pl
import requests

from .. import config, logger
from ..cache_manager import FileCacheManager

# Check https://oui.doleta.gov/unemploy/DataDownloads.asp for updated URLs.
_ETA203_URL = "https://oui.doleta.gov/unemploy/csv/ar203.csv"
_ETA539_URL = "https://oui.doleta.gov/unemploy/csv/ar539.csv"

_HEADERS = {"User-Agent": "Mozilla/5.0"}

# Column map from 4024c6.pdf (DATADOWNLOAD DATA MAP)
_ETA203_RENAME: dict[str, str] = {
    "st": "state_abbr",
    "c1": "sample_population",
    # Sex
    "c2": "sex_male",
    "c3": "sex_female",
    "c4": "sex_ina",
    # Age
    "c12": "age_lt22",
    "c13": "age_22_24",
    "c14": "age_25_34",
    "c15": "age_35_44",
    "c16": "age_45_54",
    "c17": "age_55_59",
    "c18": "age_60_64",
    "c19": "age_ge65",
    "c20": "age_ina",
    # Ethnicity
    "c40": "ethnicity_hispanic",
    "c41": "ethnicity_not_hispanic",
    "c42": "ethnicity_ina",
    # Race
    "c43": "race_american_indian",
    "c44": "race_asian",
    "c45": "race_black",
    "c46": "race_native_hawaiian",
    "c47": "race_white",
    "c48": "race_ina",
    # Industry (NAICS major groups)
    "c49": "ind_agriculture_forestry_fishing_hunting",
    "c50": "ind_mining",
    "c51": "ind_utilities",
    "c52": "ind_construction",
    "c53": "ind_manufacturing",
    "c54": "ind_wholesale_trade",
    "c55": "ind_retail_trade",
    "c56": "ind_transportation_warehousing",
    "c57": "ind_information",
    "c58": "ind_finance_insurance",
    "c59": "ind_real_estate",
    "c60": "ind_professional_scientific_technical",
    "c61": "ind_management_companies",
    "c62": "ind_admin_support_waste_management",
    "c63": "ind_educational_services",
    "c64": "ind_healthcare_social_assistance",
    "c65": "ind_arts_entertainment_recreation",
    "c66": "ind_accommodation_food_services",
    "c67": "ind_other_services",
    "c68": "ind_public_administration",
    "c69": "ind_ina",
    # Occupation (SOC major groups)
    "c70": "occ_management",
    "c71": "occ_business_financial",
    "c72": "occ_computer_math",
    "c73": "occ_architecture_engineering",
    "c74": "occ_life_physical_social_sciences",
    "c75": "occ_community_social_services",
    "c76": "occ_legal",
    "c77": "occ_education_training_library",
    "c78": "occ_arts_design_entertainment",
    "c79": "occ_healthcare_practitioner",
    "c80": "occ_healthcare_support",
    "c81": "occ_protective_services",
    "c82": "occ_food_preparation_serving",
    "c83": "occ_building_grounds_cleaning",
    "c84": "occ_personal_care_services",
    "c85": "occ_sales",
    "c86": "occ_office_admin_support",
    "c87": "occ_farming_fishing_forestry",
    "c88": "occ_construction_extraction",
    "c89": "occ_installation_maintenance_repair",
    "c90": "occ_production",
    "c91": "occ_transportation_material_moving",
    "c92": "occ_military",
    "c93": "occ_ina",
}

_ETA539_RENAME: dict[str, str] = {
    "st": "state_abbr",
    "c1": "week_number",
    "c2": "reflected_week_ending",
    "c3": "initial_claims",
    "c4": "federal_initial_claims",
    "c5": "extended_initial_claims",
    "c6": "workshare_initial_claims",
    "c7": "workshare_extended_initial_claims",
    "c8": "continued_weeks",
    "c9": "federal_continued_weeks",
    "c10": "extended_continued_weeks",
    "c11": "workshare_continued_weeks",
    "c12": "workshare_extended_continued_weeks",
    "c13": "extended_benefits_total",
    "c14": "extended_benefits_ui",
    "c15": "additional_benefits_total",
    "c16": "additional_benefits_ui",
    "c17": "adjusted_total",
    "c18": "claims_estimate",
    "c19": "revision",
    "c20": "adjustment_reason",
    "c21": "percentage",
    "c22": "status",
    "c23": "status_change_date",
}


def _download_csv(url: str) -> tuple[bytes, dict]:
    response = requests.get(url, headers=_HEADERS, timeout=120)
    response.raise_for_status()
    return response.content, dict(response.headers)


def eta203(
    force_reload: bool = False,
    url: Optional[str] = None,
    reload_if_updated: bool = True,
) -> pl.LazyFrame:
    """
    ETA 203 – Characteristics of the Insured Unemployed, monthly by state.

    Source: https://oui.doleta.gov/unemploy/DataDownloads.asp
    Coverage: ~1994–present.

    Columns: state_abbr, year (Int32), month (Int8), sample_population,
    then counts by sex, age, ethnicity, race, industry (NAICS), and
    occupation (SOC). Zero means not reported for that period.
    """
    resolved_url = url or _ETA203_URL
    cache_dir = Path(config.path_cache_files) / "dol" / "ui"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / "eta203.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)
    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info("DOL: loading ETA 203 from cache")
        return pl.scan_parquet(parquet_path)

    logger.info(f"DOL: downloading ETA 203 from {resolved_url}")
    csv_bytes, headers = _download_csv(resolved_url)
    df = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=2000, null_values=[""])

    df = (
        df.with_columns(
            pl.col("rptdate").str.to_date("%m/%d/%Y")
        )
        .with_columns([
            pl.col("rptdate").dt.year().cast(pl.Int32).alias("year"),
            pl.col("rptdate").dt.month().cast(pl.Int8).alias("month"),
        ])
        .drop("rptdate")
    )

    rename_map = {k: v for k, v in _ETA203_RENAME.items() if k in df.columns}
    df = df.rename(rename_map)

    if "g_states.region" in df.columns:
        df = df.drop("g_states.region")

    id_cols = ["state_abbr", "year", "month"]
    other_cols = [c for c in df.columns if c not in id_cols]
    df = df.select(id_cols + other_cols)

    df.write_parquet(parquet_path)
    fcm.save_metadata(response_headers=headers)
    return pl.scan_parquet(parquet_path)


def eta539(
    force_reload: bool = False,
    url: Optional[str] = None,
    reload_if_updated: bool = True,
) -> pl.LazyFrame:
    """
    ETA 539 – Claims and Extended Benefits Data, weekly by state.

    Source: https://oui.doleta.gov/unemploy/DataDownloads.asp
    Coverage: 1986–present.

    Key columns: state_abbr, week_ending (Date), year (Int32), month (Int8),
    week_number, initial_claims, continued_weeks, extended_benefits_total,
    adjusted_total, claims_estimate.

    IC  = Initial Claims (regular state UI)
    CW  = Continued Weeks claimed
    EBT = Extended Benefits Total (EB trigger periods)
    AT  = Adjusted Total (insured unemployment)
    CE  = Claims Estimate
    """
    resolved_url = url or _ETA539_URL
    cache_dir = Path(config.path_cache_files) / "dol" / "ui"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / "eta539.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)
    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info("DOL: loading ETA 539 from cache")
        return pl.scan_parquet(parquet_path)

    logger.info(f"DOL: downloading ETA 539 from {resolved_url}")
    csv_bytes, headers = _download_csv(resolved_url)
    df = pl.read_csv(io.BytesIO(csv_bytes), infer_schema_length=2000, null_values=[""])

    df = (
        df.with_columns(
            pl.col("rptdate").str.to_date("%m/%d/%Y").alias("week_ending")
        )
        .drop("rptdate")
    )

    # Parse reflected week ending and status change date if present
    if "c2" in df.columns:
        df = df.with_columns(
            pl.col("c2").str.to_date("%m/%d/%Y", strict=False).alias("c2")
        )
    if "c23" in df.columns:
        df = df.with_columns(
            pl.col("c23").str.to_date("%m/%d/%Y", strict=False).alias("c23")
        )

    rename_map = {k: v for k, v in _ETA539_RENAME.items() if k in df.columns}
    df = df.rename(rename_map)

    drop_cols = [c for c in ["g_states.region", "curdate", "priorwk_pub", "priorwk"] if c in df.columns]
    if drop_cols:
        df = df.drop(drop_cols)

    df = df.with_columns([
        pl.col("week_ending").dt.year().cast(pl.Int32).alias("year"),
        pl.col("week_ending").dt.month().cast(pl.Int8).alias("month"),
    ])

    id_cols = ["state_abbr", "week_ending", "year", "month"]
    other_cols = [c for c in df.columns if c not in id_cols]
    df = df.select(id_cols + other_cols)

    df.write_parquet(parquet_path)
    fcm.save_metadata(response_headers=headers)
    return pl.scan_parquet(parquet_path)
