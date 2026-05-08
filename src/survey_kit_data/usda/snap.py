from __future__ import annotations

import io
import re
import time
import zipfile
from pathlib import Path
from typing import Optional

import polars as pl
import requests

from .. import config, logger
from ..cache_manager import FileCacheManager
from ..geo import state_fips as _state_fips


def _join_state_fips(lf: pl.LazyFrame, join_on: str) -> pl.LazyFrame:
    fips = _state_fips().rename({"state_name": join_on}).select([join_on, "state_abbr", pl.col("fips").alias("state_fips")])
    return lf.join(fips, on=join_on, how="left")


def _county_fips_cols(lf: pl.LazyFrame, include_fips: bool) -> pl.LazyFrame:
    if not include_fips:
        return lf
    return lf.with_columns([
        pl.col("substate_code").str.slice(0, 2).cast(pl.Int32).alias("state_fips"),
        pl.col("substate_code").str.slice(0, 5).alias("county_fips"),
    ])


# Check fns.usda.gov/pd/supplemental-nutrition-assistance-program-snap for updated URLs.
# The trailing number in each filename is a USDA version counter; increment if a URL 404s.
SNAP_URLS: dict[str, str] = {
    "persons": "https://www.fns.usda.gov/sites/default/files/resource-files/snap-persons-4.xlsx",
    "households": "https://www.fns.usda.gov/sites/default/files/resource-files/snap-households-4.xlsx",
    "benefits": "https://www.fns.usda.gov/sites/default/files/resource-files/snap-benefits-4.xlsx",
    "monthly": "https://www.fns.usda.gov/sites/default/files/resource-files/snap-4fymonthly-4.xlsx",
}

_HEADERS = {"User-Agent": "Mozilla/5.0"}


def _download_xlsx(url: str) -> tuple[bytes, dict]:
    response = None
    for attempt in range(3):
        try:
            response = requests.get(url, headers=_HEADERS, timeout=60)
            if response.status_code < 500:
                break
        except requests.RequestException:
            if attempt == 2:
                raise
        time.sleep(0.5 * (attempt + 1))
    if response is None:
        raise RuntimeError(f"SNAP download failed before receiving a response: {url}")
    response.raise_for_status()
    return response.content, dict(response.headers)


def _clean_col_name(raw: str) -> str:
    """Collapse multi-line header text into a compact snake_case name."""
    text = raw.replace("\n", " ").strip()
    text = re.sub(r"[^a-zA-Z0-9\s]", "", text)
    text = re.sub(r"\s+", "_", text.strip()).lower()
    return text or "unnamed"


def _parse_snap_sheet(raw: pl.DataFrame) -> pl.DataFrame:
    """
    Convert a raw SNAP Excel sheet (read with has_header=False) into a
    clean DataFrame.

    Layout common to all four SNAP files:
      row 0 — title
      row 1 — "Data as of …"
      row 2 — column headers (may contain newlines inside cells)
      row 3+ — data rows, terminated by footnote rows where col 2 is null
    """
    # Extract and clean column names from row 2
    col_names = [_clean_col_name(str(raw[col][2] or "")) for col in raw.columns]
    # Deduplicate (percent-change columns can share a prefix)
    seen: dict[str, int] = {}
    unique_names = []
    for name in col_names:
        if name in seen:
            seen[name] += 1
            unique_names.append(f"{name}_{seen[name]}")
        else:
            seen[name] = 0
            unique_names.append(name)

    # Drop the three header/title rows, rename columns
    df = raw.slice(3).rename(dict(zip(raw.columns, unique_names)))

    # Keep only rows where the second column (first numeric column) is not null.
    # This removes section-header rows ("ANNUAL SUMMARY") and footnote rows.
    second_col = unique_names[1]
    df = df.filter(pl.col(second_col).is_not_null())

    # Replace USDA missing-value sentinel "--" with null across all columns
    df = df.with_columns(
        [
            pl.when(pl.col(c) == "--").then(None).otherwise(pl.col(c)).alias(c)
            for c in df.columns
        ]
    )

    # Cast every column except the first (geography/period label) to Float64
    numeric_cols = unique_names[1:]
    df = df.with_columns(
        [pl.col(c).cast(pl.Float64, strict=False) for c in numeric_cols]
    )

    return df


_MONTH_NAME_MAP = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_PRELIMINARY_SUFFIXES = {"preliminary", "initial", "revised"}


def _reshape_snapshot(df: pl.DataFrame, value_name: str) -> pl.DataFrame:
    """
    Pivot a wide SNAP snapshot sheet to tall format.

    Columns like 'january_2025', 'december_2025_preliminary', 'january_2026_initial'
    become rows with year (Int32), month (Int8), preliminary (Boolean), and a
    single value column named after the table (persons / households / benefits).
    Percent-change columns are dropped.
    """
    state_col = df.columns[0]

    # Identify the 3 data columns (month_year[_status]) vs percent-change columns
    data_cols = [
        c for c in df.columns[1:]
        if not c.startswith("percent_") and c.split("_")[0] in _MONTH_NAME_MAP
    ]

    melted = df.select([state_col] + data_cols).unpivot(
        index=[state_col], variable_name="_col", value_name=value_name
    )

    def _year(c: str) -> int:
        return int(c.split("_")[1])

    def _month(c: str) -> int:
        return _MONTH_NAME_MAP[c.split("_")[0]]

    def _prelim(c: str) -> bool:
        parts = c.split("_")
        return len(parts) > 2 and parts[2] in _PRELIMINARY_SUFFIXES

    return (
        melted
        .with_columns([
            pl.col("_col").map_elements(_year, return_dtype=pl.Int32).alias("year"),
            pl.col("_col").map_elements(_month, return_dtype=pl.Int8).alias("month"),
            pl.col("_col").map_elements(_prelim, return_dtype=pl.Boolean).alias("preliminary"),
        ])
        .drop("_col")
        .rename({state_col: "state"})
        .select(["state", "year", "month", "preliminary", value_name])
    )


def snap(
    table: str = "persons",
    force_reload: bool = False,
    url: Optional[str] = None,
    drop_empty: bool = True,
    include_fips: bool = True,
    reload_if_updated: bool = True,
) -> pl.LazyFrame:
    """
    Download and cache a USDA FNS SNAP aggregate table.

    Parameters
    ----------
    table : str
        One of "persons", "households", "benefits", "monthly".
        - persons / households / benefits: state-level 3-month snapshots.
        - monthly: national fiscal-year and month-level history.
    force_reload : bool
        Re-download and re-parse even if the parquet cache exists.
    url : str, optional
        Override the default URL (useful when USDA increments the version suffix).

    Returns
    -------
    pl.LazyFrame
        Parsed and typed data. Column names are derived from the Excel headers.
        The first column is the geography or period label; remaining columns are
        Float64. Sentinel values ("--") are replaced with null.
    """
    if table not in SNAP_URLS and url is None:
        raise ValueError(f"Unknown table '{table}'. Options: {list(SNAP_URLS)}")

    resolved_url = url or SNAP_URLS[table]
    cache_dir = Path(config.path_cache_files) / "usda" / "snap"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / f"{table}.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)

    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info(f"SNAP: loading {table} from cache")
    else:
        logger.info(f"SNAP: downloading {table} from {resolved_url}")
        xlsx_bytes, headers = _download_xlsx(resolved_url)
        raw = pl.read_excel(io.BytesIO(xlsx_bytes), has_header=False)
        df = _parse_snap_sheet(raw)
        if table in ("persons", "households", "benefits"):
            df = _reshape_snapshot(df, value_name=table)
        df.write_parquet(parquet_path)
        fcm.save_metadata(response_headers=headers)

    lf = pl.scan_parquet(parquet_path)
    if drop_empty and table in ("persons", "households", "benefits"):
        lf = lf.filter(pl.col(table).is_not_null())
    elif drop_empty:
        schema = lf.collect_schema()
        numeric_cols = [c for c, dtype in schema.items() if dtype == pl.Float64]
        lf = lf.filter(pl.any_horizontal([pl.col(c).is_not_null() for c in numeric_cols]))
    if include_fips and table in ("persons", "households", "benefits"):
        lf = _join_state_fips(lf, "state")
    return lf


def snap_persons_snapshot(force_reload: bool = False, drop_empty: bool = True, include_fips: bool = True, reload_if_updated: bool = True) -> pl.LazyFrame:
    """State-level SNAP persons participation (3-month snapshot). For full history use snap_state_history()."""
    return snap("persons", force_reload=force_reload, drop_empty=drop_empty, include_fips=include_fips, reload_if_updated=reload_if_updated)


def snap_households_snapshot(force_reload: bool = False, drop_empty: bool = True, include_fips: bool = True, reload_if_updated: bool = True) -> pl.LazyFrame:
    """State-level SNAP households participation (3-month snapshot). For full history use snap_state_history()."""
    return snap("households", force_reload=force_reload, drop_empty=drop_empty, include_fips=include_fips, reload_if_updated=reload_if_updated)


def snap_benefits_snapshot(force_reload: bool = False, drop_empty: bool = True, include_fips: bool = True, reload_if_updated: bool = True) -> pl.LazyFrame:
    """State-level SNAP benefit costs (3-month snapshot). For full history use snap_state_history()."""
    return snap("benefits", force_reload=force_reload, drop_empty=drop_empty, include_fips=include_fips, reload_if_updated=reload_if_updated)


def snap_monthly(force_reload: bool = False, drop_empty: bool = True, reload_if_updated: bool = True) -> pl.LazyFrame:
    """
    National SNAP monthly aggregate history.

    Annual FY summary rows are dropped; only calendar-month rows are kept.

    Columns: year (Int32), month (Int8, 1–12), plus the numeric participation
    and benefit fields from the source Excel file.
    """
    resolved_url = SNAP_URLS["monthly"]
    cache_dir = Path(config.path_cache_files) / "usda" / "snap"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / "monthly.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)
    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info("SNAP: loading monthly from cache")
    else:
        logger.info(f"SNAP: downloading monthly from {resolved_url}")
        xlsx_bytes, headers = _download_xlsx(resolved_url)
        raw = pl.read_excel(io.BytesIO(xlsx_bytes), has_header=False)
        df = _parse_snap_sheet(raw)

        period_col = df.columns[0]

        # Keep only month rows (e.g. "Oct 2025"); drop annual "FY XXXX" summary rows
        df = df.filter(pl.col(period_col).str.contains(
            r"(?i)^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$"
        ))

        # Parse "Oct 2025" → year=2025, month=10
        df = df.with_columns([
            pl.col(period_col).str.extract(r"(\d{4})$").cast(pl.Int32).alias("year"),
            pl.col(period_col).str.extract(r"^([A-Za-z]+)").str.to_uppercase()
                .replace(_MONTH_MAP).cast(pl.Int8).alias("month"),
        ]).drop(period_col)

        other_cols = [c for c in df.columns if c not in ("year", "month")]
        df = df.select(["year", "month"] + other_cols)

        df.write_parquet(parquet_path)
        fcm.save_metadata(response_headers=headers)

    lf = pl.scan_parquet(parquet_path)
    if drop_empty:
        schema = lf.collect_schema()
        numeric_cols = [c for c, dtype in schema.items() if dtype == pl.Float64]
        lf = lf.filter(pl.any_horizontal([pl.col(c).is_not_null() for c in numeric_cols]))
    return lf


# ── Historical state and county loaders ─────────────────────────────────────

# Check fns.usda.gov/pd/supplemental-nutrition-assistance-program-snap for updates.
STATE_ZIP_URL = "https://www.fns.usda.gov/sites/default/files/resource-files/snap-zip-fy69tocurrent-4.zip"
COUNTY_ZIP_URL = "https://www.fns.usda.gov/sites/default/files/resource-files/snap-zip-fns388a-2.zip"

_REGIONAL_SHEETS = frozenset(["NERO", "MARO", "SERO", "MWRO", "SWRO", "MPRO", "WRO"])

# Matches month-year labels like "Oct 2025" or "jan 1989"
_MONTH_RE = re.compile(
    r"^(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}$",
    re.IGNORECASE,
)

_MONTH_MAP = {
    "JAN": 1, "FEB": 2, "MAR": 3, "APR": 4, "MAY": 5, "JUN": 6,
    "JUL": 7, "AUG": 8, "SEP": 9, "OCT": 10, "NOV": 11, "DEC": 12,
}


def _download_zip(url: str) -> tuple[bytes, dict]:
    response = None
    for attempt in range(3):
        try:
            response = requests.get(url, headers=_HEADERS, timeout=180)
            if response.status_code < 500:
                break
        except requests.RequestException:
            if attempt == 2:
                raise
        time.sleep(1.0 * (attempt + 1))
    if response is None:
        raise RuntimeError(f"SNAP download failed: {url}")
    response.raise_for_status()
    return response.content, dict(response.headers)


def _fiscal_year_from_name(name: str) -> Optional[int]:
    m = re.search(r"FY(\d{2})\.", name, re.IGNORECASE)
    if not m:
        return None
    y2 = int(m.group(1))
    return 1900 + y2 if y2 >= 89 else 2000 + y2


def _detect_col_format(df: pl.DataFrame) -> str:
    """
    Return 'old' or 'new' based on where 'Cost' appears in the header rows.

    Old format (FY89-FY20 approx): [month, households, persons, cost_per_hh, cost_per_person, total_cost]
    New format (FY21+):             [month, households, persons, total_cost, cost_per_hh, cost_per_person]
    """
    for i in range(min(8, df.height)):
        row = df.row(i)
        v4 = str(row[3] or "").lower()
        v6 = str(row[5] or "").lower()
        if "cost" in v6 and "cost" not in v4:
            return "old"
        if "cost" in v4 and "cost" not in v6:
            return "new"
    return "old"


def _parse_state_sheet(df: pl.DataFrame, fiscal_year: int, region: str) -> list[dict]:
    fmt = _detect_col_format(df)
    records = []
    current_state = None

    for row in df.iter_rows():
        label = (row[0] or "").strip()
        v2 = row[1]

        if not label:
            continue
        # Skip known header/footer text by prefix
        if any(
            label.startswith(p)
            for p in ("National", "SNAP Monthly", "Fiscal Year", "1.", "ALL DATA", "Footnote")
        ):
            continue
        # Skip aggregate and separator rows
        if label.startswith("Total") or set(label) <= {"-", " "}:
            continue

        # State header row: label is a name, data column is null
        if v2 is None:
            # Sub-header rows like "Household" or "Participation 1/" still have
            # null in v2; skip them since they don't match the month pattern
            current_state = label
            continue

        # Data row: label must look like a month
        if not _MONTH_RE.match(label):
            continue

        if current_state is None:
            continue

        try:
            v3, v4, v5, v6 = row[2], row[3], row[4], row[5]

            def _f(v: object) -> Optional[float]:
                return float(v) if v and v != "--" else None

            hh = _f(v2)
            persons = _f(v3)
            if fmt == "new":
                cost = _f(v4)
                cost_per_hh = _f(v5)
                cost_per_person = _f(v6)
            else:
                cost_per_hh = _f(v4)
                cost_per_person = _f(v5)
                cost = _f(v6)

            mon_str, yr_str = label.split()
            records.append({
                "fiscal_year": fiscal_year,
                "region": region,
                "state": current_state,
                "year": int(yr_str),
                "month": _MONTH_MAP[mon_str[:3].upper()],
                "households": hh,
                "persons": persons,
                "total_cost": cost,
                "cost_per_household": cost_per_hh,
                "cost_per_person": cost_per_person,
            })
        except (ValueError, TypeError, IndexError):
            pass

    return records


def snap_state_history(
    force_reload: bool = False,
    url: Optional[str] = None,
    include_fips: bool = True,
    reload_if_updated: bool = True,
) -> pl.LazyFrame:
    """
    Monthly SNAP participation by state, FY1989–present, consolidated into
    a single parquet.

    Source: USDA FNS historical state zip (one xls/xlsx per fiscal year, seven
    FNS regional sheets each). State name rows act as section headers; each
    state block contains up to 12 monthly rows.

    Note: format (column ordering) changed around FY2021 when files switched
    from .xls to .xlsx. The parser detects the format per sheet automatically,
    but edge-case years may need inspection.

    Columns: fiscal_year (Int32), region (String), state (String),
    year (Int32), month (Int8, 1–12), households (Float64), persons (Float64),
    total_cost (Float64), cost_per_household (Float64), cost_per_person (Float64).
    """
    resolved_url = url or STATE_ZIP_URL
    cache_dir = Path(config.path_cache_files) / "usda" / "snap"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / "state_history.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)
    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info("SNAP: loading state_history from cache")
        lf = pl.scan_parquet(parquet_path)
        if include_fips:
            lf = _join_state_fips(lf, "state")
        return lf

    logger.info(f"SNAP: downloading state history zip from {resolved_url}")
    zip_bytes, headers = _download_zip(resolved_url)
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    all_records: list[dict] = []
    for name in sorted(zf.namelist()):
        fy = _fiscal_year_from_name(name)
        if fy is None:
            logger.info(f"SNAP: skipping {name} (no fiscal year found)")
            continue
        logger.info(f"SNAP: parsing {name} (FY{fy})")
        file_bytes = zf.read(name)
        try:
            sheets = pl.read_excel(io.BytesIO(file_bytes), has_header=False, sheet_id=0)
        except Exception as exc:
            logger.warning(f"SNAP: could not read {name}: {exc}")
            continue
        if isinstance(sheets, pl.DataFrame):
            sheets = {name: sheets}
        for sheet_name, sheet_df in sheets.items():
            region = sheet_name.strip()
            if region not in _REGIONAL_SHEETS:
                continue
            records = _parse_state_sheet(sheet_df, fy, region)
            all_records.extend(records)

    schema = {
        "fiscal_year": pl.Int32,
        "region": pl.String,
        "state": pl.String,
        "year": pl.Int32,
        "month": pl.Int8,
        "households": pl.Float64,
        "persons": pl.Float64,
        "total_cost": pl.Float64,
        "cost_per_household": pl.Float64,
        "cost_per_person": pl.Float64,
    }
    df = pl.DataFrame(all_records, schema=schema).sort(["fiscal_year", "state", "year", "month"])
    df.write_parquet(parquet_path)
    fcm.save_metadata(response_headers=headers)

    lf = pl.scan_parquet(parquet_path)
    if include_fips:
        lf = _join_state_fips(lf, "state")
    return lf


def _date_from_county_name(name: str) -> Optional[tuple[int, int]]:
    """Parse (year, month) from filenames like 'JAN 1989.xls' or 'Jul 2025.xlsx'."""
    m = re.match(r"([A-Za-z]+)\s+(\d{4})\.", name)
    if not m:
        return None
    month = _MONTH_MAP.get(m.group(1).upper())
    if month is None:
        return None
    return int(m.group(2)), month


def _parse_county_sheet(df: pl.DataFrame, year: int, month: int) -> Optional[pl.DataFrame]:
    """
    Parse one FNS-388A county snapshot sheet.

    Layout (consistent across years):
      row 0 — "NATIONAL DATA BANK VERSION 8.2..."
      row 1 — "SS7 - CHOOSE UP TO 99 VARIABLES..."
      row 2 — snapshot date (e.g. "Jan 1989")
      row 3 — column headers (14 cols; even-indexed cols are null from merged cells)
      row 4+ — data rows; last few are US Summary and footnotes
    """
    if df.height < 5:
        return None

    # Build column names from row 3, dropping null-header (merged-cell) columns
    raw_cols = list(df.columns)
    header_row = df.row(3)
    keep_indices = []
    col_names = []
    for i, (raw_col, hval) in enumerate(zip(raw_cols, header_row)):
        if hval is not None and str(hval).strip():
            name = _clean_col_name(str(hval))
            col_names.append(name or f"col_{i}")
            keep_indices.append(i)

    # Slice off header rows, keep only non-null-header columns
    data = df.slice(4).select([df.columns[i] for i in keep_indices])
    data = data.rename(dict(zip(data.columns, col_names)))

    first_col = col_names[0]

    # Drop rows where the identifier column is null or looks like a footer
    data = data.filter(
        pl.col(first_col).is_not_null()
        & ~pl.col(first_col).str.starts_with("U.S. Summary")
        & ~pl.col(first_col).str.starts_with("a ")
        & ~pl.col(first_col).str.starts_with("Automated")
    )

    # Split substate_region col into numeric code and name
    data = data.with_columns([
        pl.col(first_col).str.extract(r"^(\d+)").alias("substate_code"),
        pl.col(first_col).str.extract(r"^\d+\s*\n?(.+)$", group_index=1)
            .str.replace_all(r"\n", " ").str.strip_chars().alias("substate_name"),
    ]).drop(first_col)

    # Cast all remaining columns to numeric where possible
    for col in data.columns:
        if col in ("substate_code", "substate_name"):
            continue
        data = data.with_columns(
            pl.col(col).cast(pl.Float64, strict=False).alias(col)
        )

    return data.with_columns([
        pl.lit(year).cast(pl.Int32).alias("year"),
        pl.lit(month).cast(pl.Int8).alias("month"),
    ])


def snap_county_history(
    force_reload: bool = False,
    url: Optional[str] = None,
    include_fips: bool = True,
    reload_if_updated: bool = True,
) -> pl.LazyFrame:
    """
    SNAP sub-county/substate participation snapshots, January and July of each
    year, 1989–present, consolidated into a single parquet.

    Source: USDA FNS FNS-388A zip (one xls/xlsx per snapshot month). Each file
    covers ~2,000–2,800 substate reporting units (county offices, tribal areas,
    etc.). Column count is stable at 14, but the set of reporting units changes
    over time.

    Columns: substate_code, substate_name, year (Int32), month (Int8), plus the
    numeric SNAP participation fields from the source (persons by PA/non-PA
    category, benefit costs, calculated totals).
    """
    resolved_url = url or COUNTY_ZIP_URL
    cache_dir = Path(config.path_cache_files) / "usda" / "snap"
    cache_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = cache_dir / "county_history.parquet"

    fcm = FileCacheManager(path_save=str(parquet_path), url=resolved_url)
    if not force_reload and fcm.is_cached(reload_if_updated=reload_if_updated):
        logger.info("SNAP: loading county_history from cache")
        return _county_fips_cols(pl.scan_parquet(parquet_path), include_fips)

    logger.info(f"SNAP: downloading county history zip from {resolved_url}")
    zip_bytes, headers = _download_zip(resolved_url)
    zf = zipfile.ZipFile(io.BytesIO(zip_bytes))

    frames: list[pl.DataFrame] = []
    for name in sorted(zf.namelist()):
        date = _date_from_county_name(name)
        if date is None:
            logger.info(f"SNAP: skipping {name} (no date found)")
            continue
        year, month = date
        logger.info(f"SNAP: parsing {name} ({year}-{month:02d})")
        file_bytes = zf.read(name)
        try:
            raw = pl.read_excel(io.BytesIO(file_bytes), has_header=False, sheet_id=0)
        except Exception as exc:
            logger.warning(f"SNAP: could not read {name}: {exc}")
            continue
        if isinstance(raw, dict):
            raw = next(iter(raw.values()))
        parsed = _parse_county_sheet(raw, year, month)
        if parsed is not None and parsed.height > 0:
            frames.append(parsed)

    if not frames:
        raise RuntimeError("SNAP county history: no data parsed from zip")

    df = pl.concat(frames, how="diagonal").sort(["year", "month", "substate_code"])
    df.write_parquet(parquet_path)
    fcm.save_metadata(response_headers=headers)
    return _county_fips_cols(pl.scan_parquet(parquet_path), include_fips)
