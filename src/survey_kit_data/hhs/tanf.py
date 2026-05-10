from __future__ import annotations

import hashlib
import html
import io
import json
import re
from importlib.util import find_spec
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Iterable, Optional
from urllib.parse import urljoin, urlparse

import polars as pl
import requests

from .. import config, logger
from ..cache_manager import FileCacheManager
from ..geo import state_fips as _state_fips

_BASE_URL = "https://acf.gov"
_HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "text/html,application/xhtml+xml,application/vnd.ms-excel,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    "Accept-Language": "en-US,en;q=0.9",
}

TANF_YEARS = range(1996, 2026)
AFDC_YEARS = range(1960, 1996)
TANF_CASELOAD_URLS: dict[int, list[str]] = {
    2006: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2006_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2006_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2006_15months_tanssp.xls",
    ],
    2007: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2007_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2007_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2007_15months_tanssp.xls",
    ],
    2008: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2008_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2008_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2008_15months_tanssp.xls",
    ],
    2009: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2009_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2009_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2009_15months_tanssp.xls",
    ],
    2010: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2010_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2010_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2010_15months_tanssp.xls",
    ],
    2011: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2011_15months_tan_1.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2011_15months_ssp_0.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2011_15months_tanssp_0.xls",
    ],
    2012: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2012_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2012_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2012_15months_tanssp.xls",
    ],
    2013: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2013_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2013_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2013_15months_tanssp.xls",
    ],
    2014: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2014_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2014_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2014_15months_tanssp.xls",
    ],
    2015: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2015_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2015_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2015_15months_tanssp.xls",
    ],
    2016: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2016_15months_tan.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2016_15months_ssp.xls",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2016_15months_tanssp_0.xls",
    ],
    2017: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2017_15months_tan.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2017_15months_ssp.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2017_15months_tanssp.xlsx",
    ],
    2018: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/2018_15months_tan_web_03252019.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2018_15months_ssp_web_03252019.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/2018_15months_tanssp_web_03252019.xlsx",
    ],
    2019: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2019_tanf_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2019_ssp_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2019_tanfssp_caseload.xlsx",
    ],
    2020: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2020_tanf_caseload_0.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2020__ssp_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2020_tanfssp_caseload_0.xlsx",
    ],
    2021: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2021_tanf_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2021_ssp_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2021_tanfssp_caseload.xlsx",
    ],
    2022: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2022_tanf_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2022_ssp_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2022_tanfssp_caseload.xlsx",
    ],
    2023: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2023_tanf_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2023_ssp_caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2023_tanssp_caseload.xlsx",
    ],
    2024: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2024-15months-tanf-caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2024-15months-ssp-caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2024-15months-tanssp-caseload.xlsx",
    ],
    2025: [
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2025-ssp-caseload.xlsx",
        f"{_BASE_URL}/sites/default/files/documents/ofa/fy2025-tanssp-caseload.xlsx",
    ],
}
AFDC_CASELOAD_URLS: dict[int, list[str]] = {}
_TANF_INDEX_URLS = [
    f"{_BASE_URL}/ofa/data/tanf-caseload-data-1996-2015",
    f"{_BASE_URL}/ofa/resource/tanf-caseload-data-1996-2015",
]
_AFDC_INDEX_URLS = [
    f"{_BASE_URL}/ofa/data/afdc-caseload-data-1960-1995",
    f"{_BASE_URL}/ofa/resource/afdc-caseload-data-1960-1995",
]
_DISCOVERY_SCHEMA = {
    "program": pl.String,
    "year": pl.Int32,
    "source_page": pl.String,
    "label": pl.String,
    "url": pl.String,
}
_DOWNLOAD_MIRROR_MODES = {"fallback", "prefer", "only"}
_NORMALIZED_CACHE_VERSION = 4
_MONTHS = {
    "jan": 1,
    "january": 1,
    "feb": 2,
    "february": 2,
    "mar": 3,
    "march": 3,
    "apr": 4,
    "april": 4,
    "may": 5,
    "jun": 6,
    "june": 6,
    "jul": 7,
    "july": 7,
    "aug": 8,
    "august": 8,
    "sep": 9,
    "september": 9,
    "oct": 10,
    "october": 10,
    "nov": 11,
    "november": 11,
    "dec": 12,
    "december": 12,
}
_MONTHLY_SCHEMA = {
    "source_program": pl.String,
    "year": pl.Int32,
    "month": pl.Int8,
    "state": pl.String,
    "total_families": pl.Float64,
    "two_parent_families": pl.Float64,
    "one_parent_families": pl.Float64,
    "no_parent_families": pl.Float64,
    "total_recipients": pl.Float64,
    "adult_recipients": pl.Float64,
    "child_recipients": pl.Float64,
    "source_url": pl.String,
    "source_sheet": pl.String,
}


class _LinkExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        attr_map = {k.lower(): v for k, v in attrs if v is not None}
        href = attr_map.get("href")
        if href:
            self._current_href = href
            self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag.lower() != "a" or self._current_href is None:
            return
        label = " ".join("".join(self._current_text).split())
        self.links.append({"href": self._current_href, "label": html.unescape(label)})
        self._current_href = None
        self._current_text = []


def _slug(text: str) -> str:
    text = re.sub(r"[^a-zA-Z0-9]+", "_", text.strip().lower())
    return re.sub(r"_+", "_", text).strip("_") or "file"


def _year_list(years: Optional[Iterable[int]], valid: range) -> list[int]:
    out = list(valid if years is None else years)
    invalid = [year for year in out if year not in valid]
    if invalid:
        raise ValueError(f"Unsupported year(s): {invalid}. Valid range is {valid.start}-{valid.stop - 1}.")
    return out


def _source_years_for_calendar_years(years: list[int], valid: range) -> list[int]:
    source_years = set(years)
    source_years.update(year + 1 for year in years if year + 1 in valid)
    return sorted(source_years)


def _tanf_page_candidates(year: int) -> list[str]:
    return [
        f"{_BASE_URL}/ofa/data/tanf-caseload-data-{year}",
        f"{_BASE_URL}/ofa/resource/tanf-caseload-data-{year}",
    ]


def _afdc_page_candidates(year: int) -> list[str]:
    return [
        f"{_BASE_URL}/ofa/data/afdc-caseload-data-{year}",
        f"{_BASE_URL}/ofa/resource/caseload-data-{year}",
        f"{_BASE_URL}/ofa/resource/afdc-caseload-data-{year}",
    ]


def _fetch_page(url: str) -> tuple[str, dict[str, str]]:
    response = requests.get(url, headers=_HEADERS, timeout=120, allow_redirects=True)
    headers = dict(response.headers)
    if headers.get("x-amzn-waf-action") == "challenge":
        raise RuntimeError(f"ACF returned an AWS WAF challenge for {url}; cannot discover files from this client.")
    response.raise_for_status()
    if not response.text.strip():
        raise RuntimeError(f"ACF returned an empty response for {url}; cannot discover files.")
    return response.text, headers


def _extract_links(page_url: str, page_html: str) -> list[dict[str, str]]:
    parser = _LinkExtractor()
    parser.feed(page_html)
    links = []
    for link in parser.links:
        url = urljoin(page_url, link["href"])
        parsed = urlparse(url)
        if not parsed.scheme.startswith("http"):
            continue
        links.append({"url": url, "label": link["label"] or Path(parsed.path).name})
    return links


def _is_excel_link(link: dict[str, str]) -> bool:
    url_path = urlparse(link["url"]).path.lower()
    label = link["label"].lower()
    return url_path.endswith((".xls", ".xlsx")) or "(xls" in label or "excel" in label


def _resolve_excel_links(link: dict[str, str]) -> list[dict[str, str]]:
    url_path = urlparse(link["url"]).path.lower()
    if url_path.endswith((".xls", ".xlsx")):
        return [link]

    try:
        page_html, _headers = _fetch_page(link["url"])
    except Exception:
        return [link]

    direct_links = [
        child
        for child in _extract_links(link["url"], page_html)
        if urlparse(child["url"]).path.lower().endswith((".xls", ".xlsx"))
    ]
    if not direct_links:
        return [link]
    return [
        {
            "url": child["url"],
            "label": link["label"] or child["label"],
        }
        for child in direct_links
    ]


def _discover_excel_links_for_pages(
    program: str,
    year: int,
    page_candidates: list[str],
) -> list[dict[str, object]]:
    errors = []
    for page_url in page_candidates:
        try:
            page_html, _headers = _fetch_page(page_url)
        except Exception as exc:
            errors.append(f"{page_url}: {exc}")
            continue
        excel_links = []
        for link in _extract_links(page_url, page_html):
            if _is_excel_link(link):
                excel_links.extend(_resolve_excel_links(link))
        if excel_links:
            return [
                {
                    "program": program,
                    "year": year,
                    "source_page": page_url,
                    "label": link["label"],
                    "url": link["url"],
                }
                for link in excel_links
            ]
    logger.warning(f"HHS {program}: no Excel links discovered for {year}. Attempts: {' | '.join(errors)}")
    return []


def _file_records_from_urls(
    program: str,
    year: int,
    urls: Iterable[str],
    source_page: str = "direct",
) -> list[dict[str, object]]:
    return [
        {
            "program": program,
            "year": year,
            "source_page": source_page,
            "label": Path(urlparse(url).path).stem,
            "url": url,
        }
        for url in urls
    ]


def _discover_year_pages_from_indexes(
    program: str,
    index_urls: list[str],
    year_pattern: str,
    years: list[int],
) -> dict[int, list[str]]:
    year_pages: dict[int, list[str]] = {year: [] for year in years}
    errors = []
    for index_url in index_urls:
        try:
            page_html, _headers = _fetch_page(index_url)
        except Exception as exc:
            errors.append(f"{index_url}: {exc}")
            continue
        for link in _extract_links(index_url, page_html):
            match = re.search(year_pattern, link["url"])
            if not match:
                continue
            year = int(match.group(1))
            if year in year_pages and link["url"] not in year_pages[year]:
                year_pages[year].append(link["url"])
    if errors:
        logger.info(f"HHS {program}: index-page discovery skipped/partial. Attempts: {' | '.join(errors)}")
    return year_pages


def discover_tanf_caseload_files(years: Optional[Iterable[int]] = None) -> pl.DataFrame:
    """Discover HHS/ACF TANF caseload Excel links for fiscal years 1996-2025."""
    return _discover_tanf_caseload_files(years, file_urls_by_year=None)


def _discover_tanf_caseload_files(
    years: Optional[Iterable[int]],
    file_urls_by_year: Optional[dict[int, Iterable[str]]],
) -> pl.DataFrame:
    year_values = _year_list(years, TANF_YEARS)
    url_map = {**TANF_CASELOAD_URLS, **(file_urls_by_year or {})}
    years_requiring_discovery = [year for year in year_values if year not in url_map]
    index_pages = (
        _discover_year_pages_from_indexes(
            "tanf",
            _TANF_INDEX_URLS,
            r"tanf-caseload-data-(\d{4})",
            years_requiring_discovery,
        )
        if years_requiring_discovery
        else {}
    )
    records: list[dict[str, object]] = []
    for year in year_values:
        if year in url_map:
            records.extend(_file_records_from_urls("tanf", year, url_map[year]))
            continue
        records.extend(_discover_excel_links_for_pages("tanf", year, index_pages[year] + _tanf_page_candidates(year)))
    return pl.DataFrame(records, schema=_DISCOVERY_SCHEMA)


def discover_afdc_caseload_files(years: Optional[Iterable[int]] = None) -> pl.DataFrame:
    """Discover HHS/ACF AFDC caseload Excel links for years 1960-1995."""
    return _discover_afdc_caseload_files(years, file_urls_by_year=None)


def _discover_afdc_caseload_files(
    years: Optional[Iterable[int]],
    file_urls_by_year: Optional[dict[int, Iterable[str]]],
) -> pl.DataFrame:
    year_values = _year_list(years, AFDC_YEARS)
    url_map = {**AFDC_CASELOAD_URLS, **(file_urls_by_year or {})}
    years_requiring_discovery = [year for year in year_values if year not in url_map]
    index_pages = (
        _discover_year_pages_from_indexes(
            "afdc",
            _AFDC_INDEX_URLS,
            r"(?:afdc-)?caseload-data-(\d{4})",
            years_requiring_discovery,
        )
        if years_requiring_discovery
        else {}
    )
    records: list[dict[str, object]] = []
    for year in year_values:
        if year in url_map:
            records.extend(_file_records_from_urls("afdc", year, url_map[year]))
            continue
        records.extend(_discover_excel_links_for_pages("afdc", year, index_pages[year] + _afdc_page_candidates(year)))
    return pl.DataFrame(records, schema=_DISCOVERY_SCHEMA)


def _download_file(url: str) -> tuple[bytes, dict[str, str]]:
    response = requests.get(url, headers=_HEADERS, timeout=180, allow_redirects=True)
    headers = dict(response.headers)
    if headers.get("x-amzn-waf-action") == "challenge":
        raise RuntimeError(f"ACF returned an AWS WAF challenge for {url}; cannot download file from this client.")
    response.raise_for_status()
    content_type = headers.get("content-type", "").lower()
    if content_type.startswith("text/html"):
        raise RuntimeError(f"Expected an Excel file from {url}, but received HTML.")
    return response.content, headers


def _manifest_entries(manifest: dict[str, Any]) -> list[dict[str, Any]]:
    files = manifest.get("files", {})
    if isinstance(files, dict):
        return [entry for entry in files.values() if isinstance(entry, dict)]
    if isinstance(files, list):
        return [entry for entry in files if isinstance(entry, dict)]
    return []


def _mirror_entry_for_url(manifest: dict[str, Any], source_url: str) -> dict[str, Any]:
    for entry in _manifest_entries(manifest):
        if entry.get("source_url") == source_url:
            return entry
    raise FileNotFoundError(f"No survey-kit-download manifest entry for {source_url}")


def _verify_sha256(data: bytes, expected: str | None, source: str) -> None:
    if not expected:
        return
    actual = hashlib.sha256(data).hexdigest()
    if actual != expected:
        raise RuntimeError(f"Downloaded mirror file failed sha256 check for {source}.")


def _installed_download_repo_root() -> Path:
    spec = find_spec("survey_kit_download")
    if spec is None or spec.origin is None:
        raise FileNotFoundError("survey_kit_download is not importable in this environment.")

    package_file = Path(spec.origin).resolve()
    candidates = [
        package_file.parents[2] if len(package_file.parents) > 2 else package_file.parent,
        package_file.parents[1] if len(package_file.parents) > 1 else package_file.parent,
        package_file.parent,
    ]
    for candidate in candidates:
        if (candidate / "manifest.json").exists():
            return candidate
    raise FileNotFoundError(
        "survey_kit_download is importable, but no manifest.json was found near the package."
    )


def _github_raw_base(download_mirror: str | Path, ref: str) -> str:
    mirror = str(download_mirror).rstrip("/")
    parsed = urlparse(mirror)

    if parsed.netloc == "raw.githubusercontent.com":
        return mirror

    if parsed.netloc == "github.com":
        parts = [part for part in parsed.path.split("/") if part]
        if len(parts) < 2:
            raise ValueError(f"Expected a GitHub repository URL, got {download_mirror!r}.")
        owner, repo = parts[:2]
        repo_ref = ref
        if len(parts) >= 4 and parts[2] == "tree":
            repo_ref = parts[3]
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{repo_ref}"

    if not parsed.scheme and re.fullmatch(r"[^/\s]+/[^/\s]+", mirror):
        owner, repo = mirror.split("/", 1)
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{ref}"

    raise ValueError(
        "download_mirror must be a local path, 'installed', an owner/repo GitHub slug, "
        "a github.com repository URL, or a raw.githubusercontent.com base URL."
    )


def _load_json_url(url: str) -> dict[str, Any]:
    response = requests.get(url, headers=_HEADERS, timeout=120, allow_redirects=True)
    response.raise_for_status()
    return response.json()


def _download_from_local_mirror(source_url: str, root: Path) -> tuple[bytes, dict[str, str]]:
    manifest_path = root / "manifest.json"
    if not manifest_path.exists():
        raise FileNotFoundError(f"No survey-kit-download manifest found at {manifest_path}.")
    with manifest_path.open() as f:
        manifest = json.load(f)

    entry = _mirror_entry_for_url(manifest, source_url)
    raw_path = root / str(entry["path"])
    if not raw_path.exists():
        raise FileNotFoundError(f"survey-kit-download manifest points to missing file: {raw_path}")

    data = raw_path.read_bytes()
    _verify_sha256(data, entry.get("sha256"), raw_path.as_posix())
    return data, {"x-survey-kit-data-source": "survey-kit-download-local"}


def _download_from_github_mirror(
    source_url: str,
    download_mirror: str | Path,
    ref: str,
) -> tuple[bytes, dict[str, str]]:
    raw_base = _github_raw_base(download_mirror, ref)
    manifest = _load_json_url(f"{raw_base}/manifest.json")
    entry = _mirror_entry_for_url(manifest, source_url)
    raw_url = f"{raw_base}/{entry['path']}"
    response = requests.get(raw_url, headers=_HEADERS, timeout=180, allow_redirects=True)
    response.raise_for_status()
    data = response.content
    _verify_sha256(data, entry.get("sha256"), raw_url)
    headers = dict(response.headers)
    headers["x-survey-kit-data-source"] = "survey-kit-download-github"
    return data, headers


def _download_from_mirror(
    source_url: str,
    download_mirror: str | Path,
    download_mirror_ref: str,
) -> tuple[bytes, dict[str, str]]:
    if str(download_mirror) == "installed":
        return _download_from_local_mirror(source_url, _installed_download_repo_root())

    mirror_path = Path(download_mirror).expanduser()
    if mirror_path.exists():
        return _download_from_local_mirror(source_url, mirror_path)

    return _download_from_github_mirror(source_url, download_mirror, download_mirror_ref)


def _get_workbook_bytes(
    url: str,
    download_mirror: str | Path | None,
    download_mirror_ref: str,
    download_mirror_mode: str,
) -> tuple[bytes, dict[str, str]]:
    if download_mirror_mode not in _DOWNLOAD_MIRROR_MODES:
        raise ValueError(
            f"download_mirror_mode must be one of {sorted(_DOWNLOAD_MIRROR_MODES)}, "
            f"got {download_mirror_mode!r}."
        )
    if download_mirror is None:
        if download_mirror_mode == "only":
            raise ValueError("download_mirror_mode='only' requires download_mirror.")
        return _download_file(url)

    if download_mirror_mode in {"prefer", "only"}:
        try:
            return _download_from_mirror(url, download_mirror, download_mirror_ref)
        except Exception as mirror_exc:
            if download_mirror_mode == "only":
                raise
            logger.warning(f"HHS mirror read failed for {url}; trying agency source. Error: {mirror_exc}")
            return _download_file(url)

    try:
        return _download_file(url)
    except Exception as source_exc:
        logger.warning(f"HHS agency download failed for {url}; trying survey-kit-download mirror. Error: {source_exc}")
        try:
            return _download_from_mirror(url, download_mirror, download_mirror_ref)
        except Exception as mirror_exc:
            raise RuntimeError(
                f"HHS download failed from agency source and survey-kit-download mirror for {url}. "
                f"Agency error: {source_exc}. Mirror error: {mirror_exc}."
            ) from mirror_exc


def _read_workbook_sheets(excel_bytes: bytes) -> dict[str, pl.DataFrame]:
    sheets = pl.read_excel(io.BytesIO(excel_bytes), sheet_id=0, has_header=False, raise_if_empty=False)
    if isinstance(sheets, pl.DataFrame):
        return {} if sheets.is_empty() and len(sheets.columns) == 0 else {"sheet1": sheets}
    return {
        str(name): df
        for name, df in sheets.items()
        if not (df.is_empty() and len(df.columns) == 0)
    }


def _source_program_from_url(url: str) -> str:
    filename = Path(urlparse(url).path).name.lower()
    if "tanssp" in filename or "tanfssp" in filename:
        return "tanf_ssp"
    if "ssp" in filename:
        return "ssp"
    return "tanf"


def _join_state_fips(lf: pl.LazyFrame, include_fips: bool) -> pl.LazyFrame:
    if not include_fips:
        return lf
    fips = _state_fips().rename({"state_name": "state"}).select([
        "state",
        pl.col("fips").alias("state_fips"),
    ])
    return (
        lf.with_columns(pl.col("state").str.strip_chars().alias("state"))
        .join(fips, on="state", how="left")
    )


def _cell_text(value: object) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _to_float(value: object) -> float | None:
    text = _cell_text(value)
    if text == "":
        return None
    text = text.replace(",", "")
    if text in {"-", "--", "."}:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _month_date_from_sheet(sheet_name: str, fiscal_year: int) -> tuple[int, int] | None:
    cleaned = re.sub(r"\s+", " ", sheet_name.strip().lower())
    match = re.search(r"\b([a-z]+)\s*[-_ ]?\s*(\d{2,4})\b", cleaned)
    if not match:
        return None
    month = _MONTHS.get(match.group(1)[:3], _MONTHS.get(match.group(1)))
    if month is None:
        return None

    year_text = match.group(2)
    if len(year_text) == 4:
        year = int(year_text)
    else:
        suffix = int(year_text)
        candidates = [fiscal_year - 1, fiscal_year, fiscal_year + 1]
        year = min(candidates, key=lambda candidate: abs(candidate % 100 - suffix))
        if year % 100 != suffix:
            year = 2000 + suffix if suffix < 70 else 1900 + suffix
    return year, month


def _is_monthly_sheet(sheet_name: str, df: pl.DataFrame, fiscal_year: int) -> bool:
    return df.width >= 8 and _month_date_from_sheet(sheet_name, fiscal_year) is not None


def _data_rows_after_state_header(df: pl.DataFrame) -> list[dict[str, object]]:
    rows = df.to_dicts()
    start_idx = None
    for idx, row in enumerate(rows):
        if _cell_text(row.get("column_1")).lower() == "state":
            start_idx = idx + 1
            break
    if start_idx is None:
        return []

    out = []
    for row in rows[start_idx:]:
        state = _cell_text(row.get("column_1"))
        if not state or state.lower().startswith(("note", "source")):
            continue
        if state.lower() in {"state", "nan"}:
            continue
        out.append(row)
    return out


def _parse_monthly_sheet(
    df: pl.DataFrame,
    sheet_name: str,
    record: dict[str, object],
) -> pl.DataFrame:
    fiscal_year = int(record["year"])
    parsed_date = _month_date_from_sheet(sheet_name, fiscal_year)
    if parsed_date is None:
        return pl.DataFrame(schema=_MONTHLY_SCHEMA)
    year, month = parsed_date
    source_program = _source_program_from_url(str(record["url"]))
    rows = []
    for row in _data_rows_after_state_header(df):
        values = [
            _to_float(row.get("column_2")),
            _to_float(row.get("column_3")),
            _to_float(row.get("column_4")),
            _to_float(row.get("column_5")),
            _to_float(row.get("column_6")),
            _to_float(row.get("column_7")),
            _to_float(row.get("column_8")),
        ]
        if all(value is None for value in values):
            continue
        rows.append(
            {
                "source_program": source_program,
                "year": year,
                "month": month,
                "state": _cell_text(row.get("column_1")),
                "total_families": values[0],
                "two_parent_families": values[1],
                "one_parent_families": values[2],
                "no_parent_families": values[3],
                "total_recipients": values[4],
                "adult_recipients": values[5],
                "child_recipients": values[6],
                "source_url": str(record["url"]),
                "source_sheet": sheet_name,
            }
        )
    return pl.DataFrame(rows, schema=_MONTHLY_SCHEMA)


def _parse_workbook_tables(
    record: dict[str, object],
    sheets: dict[str, pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    monthly = []
    fiscal_year = int(record["year"])
    for sheet_name, df in sheets.items():
        if _is_monthly_sheet(sheet_name, df, fiscal_year):
            monthly.append(_parse_monthly_sheet(df, sheet_name, record))
    return {
        "monthly": pl.concat(monthly, how="diagonal_relaxed") if monthly else pl.DataFrame(schema=_MONTHLY_SCHEMA),
    }


def _cache_workbook(
    record: dict[str, object],
    force_reload: bool,
    reload_if_updated: bool,
    download_mirror: str | Path | None,
    download_mirror_ref: str,
    download_mirror_mode: str,
) -> dict[str, pl.LazyFrame]:
    program = str(record["program"])
    year = int(record["year"])
    url = str(record["url"])
    label = _slug(str(record["label"]) or Path(urlparse(url).path).stem)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]

    cache_dir = Path(config.path_cache_files) / "hhs" / "tanf" / program / str(year) / f"{label}_{digest}"
    cache_dir.mkdir(parents=True, exist_ok=True)
    fcm = FileCacheManager(path_save=str(cache_dir), url=url)

    check_agency_for_updates = reload_if_updated and download_mirror is None
    if not force_reload and fcm.is_cached(reload_if_updated=check_agency_for_updates):
        return {path.stem: pl.scan_parquet(path) for path in sorted(cache_dir.glob("*.parquet"))}

    logger.info(f"HHS {program.upper()}: loading {record['label']} from {url}")
    excel_bytes, headers = _get_workbook_bytes(
        url,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
    )
    sheets = _read_workbook_sheets(excel_bytes)
    for sheet_name, df in sheets.items():
        sheet_slug = _slug(sheet_name)
        df.write_parquet(cache_dir / f"{sheet_slug}.parquet")
    fcm.save_metadata(response_headers=headers)
    return {path.stem: pl.scan_parquet(path) for path in sorted(cache_dir.glob("*.parquet"))}


def _cache_workbook_tables(
    record: dict[str, object],
    force_reload: bool,
    reload_if_updated: bool,
    download_mirror: str | Path | None,
    download_mirror_ref: str,
    download_mirror_mode: str,
) -> dict[str, pl.LazyFrame]:
    program = str(record["program"])
    year = int(record["year"])
    url = str(record["url"])
    label = _slug(str(record["label"]) or Path(urlparse(url).path).stem)
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:10]

    cache_dir = Path(config.path_cache_files) / "hhs" / "tanf" / program / str(year) / f"{label}_{digest}_tables"
    cache_dir.mkdir(parents=True, exist_ok=True)
    fcm = FileCacheManager(
        path_save=str(cache_dir),
        url=url,
        api_call=_parse_workbook_tables,
        api_args={
            "normalized_schema_version": _NORMALIZED_CACHE_VERSION,
        },
    )

    check_agency_for_updates = reload_if_updated and download_mirror is None
    if not force_reload and fcm.is_cached(reload_if_updated=check_agency_for_updates):
        return {path.stem: pl.scan_parquet(path) for path in sorted(cache_dir.glob("*.parquet"))}

    logger.info(f"HHS {program.upper()}: parsing {record['label']} from {url}")
    excel_bytes, headers = _get_workbook_bytes(
        url,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
    )
    tables = _parse_workbook_tables(record, _read_workbook_sheets(excel_bytes))
    for table_name, df in tables.items():
        df.write_parquet(cache_dir / f"{table_name}.parquet")
    fcm.save_metadata(response_headers=headers)
    return {path.stem: pl.scan_parquet(path) for path in sorted(cache_dir.glob("*.parquet"))}


def _load_records(
    records: pl.DataFrame,
    force_reload: bool,
    reload_if_updated: bool,
    download_mirror: str | Path | None,
    download_mirror_ref: str,
    download_mirror_mode: str,
) -> dict[str, pl.LazyFrame]:
    out: dict[str, pl.LazyFrame] = {}
    if records.is_empty():
        return out
    for record in records.to_dicts():
        label = _slug(str(record["label"]))
        sheets = _cache_workbook(
            record,
            force_reload=force_reload,
            reload_if_updated=reload_if_updated,
            download_mirror=download_mirror,
            download_mirror_ref=download_mirror_ref,
            download_mirror_mode=download_mirror_mode,
        )
        for sheet_name, lf in sheets.items():
            key = f"{record['program']}_{record['year']}_{label}_{sheet_name}"
            out[key] = lf
    return out


def _load_table_records(
    records: pl.DataFrame,
    force_reload: bool,
    reload_if_updated: bool,
    download_mirror: str | Path | None,
    download_mirror_ref: str,
    download_mirror_mode: str,
    include_source: bool,
    include_fips: bool,
) -> pl.LazyFrame:
    table_frames: dict[str, list[pl.LazyFrame]] = {"monthly": []}
    if records.is_empty():
        return _join_state_fips(
            _maybe_drop_source_columns(pl.DataFrame(schema=_MONTHLY_SCHEMA).lazy(), include_source),
            include_fips,
        )

    for record in records.to_dicts():
        for table_name, lf in _cache_workbook_tables(
            record,
            force_reload=force_reload,
            reload_if_updated=reload_if_updated,
            download_mirror=download_mirror,
            download_mirror_ref=download_mirror_ref,
            download_mirror_mode=download_mirror_mode,
        ).items():
            table_frames.setdefault(table_name, []).append(lf)

    frames = table_frames.get("monthly", [])
    lf = pl.concat(frames, how="diagonal_relaxed") if frames else pl.DataFrame(schema=_MONTHLY_SCHEMA).lazy()
    return _join_state_fips(_maybe_drop_source_columns(lf, include_source), include_fips)


def _maybe_drop_source_columns(lf: pl.LazyFrame, include_source: bool) -> pl.LazyFrame:
    if include_source:
        return lf
    return lf.drop("source_url", "source_sheet")


def tanf_caseload_source_sheets(
    years: Optional[Iterable[int]] = None,
    force_reload: bool = False,
    reload_if_updated: bool = True,
    allow_empty: bool = False,
    file_urls_by_year: Optional[dict[int, Iterable[str]]] = None,
    download_mirror: str | Path | None = None,
    download_mirror_ref: str = "main",
    download_mirror_mode: str = "prefer",
) -> dict[str, pl.LazyFrame]:
    """
    Load HHS/ACF TANF caseload Excel workbooks as one LazyFrame per source sheet.

    Returns one source-shaped LazyFrame per workbook sheet. The keys include
    program, year, file label, and sheet name.

    Parameters
    ----------
    download_mirror
        Optional survey-kit-download mirror. Pass a local repository path,
        ``"installed"`` for an importable local install, ``"owner/repo"``, a
        GitHub repository URL, or a raw.githubusercontent.com base URL.
    download_mirror_ref
        Git ref to use when ``download_mirror`` points at a GitHub repository.
    download_mirror_mode
        ``"prefer"`` tries the mirror first when one is supplied, then ACF;
        ``"fallback"`` tries ACF first, then the mirror; ``"only"`` requires
        the mirror.
    """
    records = _discover_tanf_caseload_files(years, file_urls_by_year=file_urls_by_year)
    if records.is_empty() and not allow_empty:
        raise RuntimeError("No HHS/ACF TANF caseload Excel files were discovered.")
    return _load_records(
        records,
        force_reload=force_reload,
        reload_if_updated=reload_if_updated,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
    )


def tanf_caseload(
    years: Optional[Iterable[int]] = None,
    force_reload: bool = False,
    reload_if_updated: bool = True,
    allow_empty: bool = False,
    file_urls_by_year: Optional[dict[int, Iterable[str]]] = None,
    download_mirror: str | Path | None = None,
    download_mirror_ref: str = "main",
    download_mirror_mode: str = "prefer",
    include_source: bool = False,
    include_fips: bool = True,
) -> pl.LazyFrame:
    """
    Load HHS/ACF TANF caseload workbooks as a consolidated monthly table.

    Returns one row per state/source program/calendar month.

    Pass ``include_source=True`` to include source workbook URL and sheet columns.
    Pass ``include_fips=False`` to omit state FIPS columns.
    """
    calendar_years = _year_list(years, TANF_YEARS)
    source_years = _source_years_for_calendar_years(calendar_years, TANF_YEARS)
    records = _discover_tanf_caseload_files(source_years, file_urls_by_year=file_urls_by_year)
    if records.is_empty() and not allow_empty:
        raise RuntimeError("No HHS/ACF TANF caseload Excel files were discovered.")
    return _load_table_records(
        records,
        force_reload=force_reload,
        reload_if_updated=reload_if_updated,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
        include_source=include_source,
        include_fips=include_fips,
    ).filter(pl.col("year").is_in(calendar_years))


def afdc_caseload_source_sheets(
    years: Optional[Iterable[int]] = None,
    force_reload: bool = False,
    reload_if_updated: bool = True,
    allow_empty: bool = False,
    file_urls_by_year: Optional[dict[int, Iterable[str]]] = None,
    download_mirror: str | Path | None = None,
    download_mirror_ref: str = "main",
    download_mirror_mode: str = "prefer",
) -> dict[str, pl.LazyFrame]:
    """
    Load HHS/ACF AFDC caseload Excel workbooks as one LazyFrame per source sheet.

    Returns one source-shaped LazyFrame per workbook sheet. The keys include
    program, year, file label, and sheet name.

    Parameters mirror the TANF loader.
    """
    records = _discover_afdc_caseload_files(years, file_urls_by_year=file_urls_by_year)
    if records.is_empty() and not allow_empty:
        raise RuntimeError("No HHS/ACF AFDC caseload Excel files were discovered.")
    return _load_records(
        records,
        force_reload=force_reload,
        reload_if_updated=reload_if_updated,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
    )


def afdc_caseload(
    years: Optional[Iterable[int]] = None,
    force_reload: bool = False,
    reload_if_updated: bool = True,
    allow_empty: bool = False,
    file_urls_by_year: Optional[dict[int, Iterable[str]]] = None,
    download_mirror: str | Path | None = None,
    download_mirror_ref: str = "main",
    download_mirror_mode: str = "prefer",
    include_source: bool = False,
    include_fips: bool = True,
) -> pl.LazyFrame:
    """Load HHS/ACF AFDC caseload workbooks as a consolidated monthly table."""
    calendar_years = _year_list(years, AFDC_YEARS)
    source_years = _source_years_for_calendar_years(calendar_years, AFDC_YEARS)
    records = _discover_afdc_caseload_files(source_years, file_urls_by_year=file_urls_by_year)
    if records.is_empty() and not allow_empty:
        raise RuntimeError("No HHS/ACF AFDC caseload Excel files were discovered.")
    return _load_table_records(
        records,
        force_reload=force_reload,
        reload_if_updated=reload_if_updated,
        download_mirror=download_mirror,
        download_mirror_ref=download_mirror_ref,
        download_mirror_mode=download_mirror_mode,
        include_source=include_source,
        include_fips=include_fips,
    ).filter(pl.col("year").is_in(calendar_years))
