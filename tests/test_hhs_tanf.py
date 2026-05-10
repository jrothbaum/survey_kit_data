from __future__ import annotations

import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import polars as pl

from survey_kit_data import config
from survey_kit_data.hhs.tanf import (
    _cache_workbook,
    _download_from_mirror,
    _extract_links,
    _file_records_from_urls,
    _get_workbook_bytes,
    _github_raw_base,
    _parse_workbook_tables,
    _read_workbook_sheets,
    _source_program_from_url,
    _discover_tanf_caseload_files,
    _is_excel_link,
    _tanf_page_candidates,
    _year_list,
    discover_tanf_caseload_files,
    tanf_caseload,
    TANF_CASELOAD_URLS,
    TANF_YEARS,
)


class HHSTANFDiscoveryTest(unittest.TestCase):
    def test_extract_links_resolves_relative_urls(self) -> None:
        links = _extract_links(
            "https://acf.gov/ofa/data/tanf-caseload-data-2025",
            """
            <html>
              <a href="/media/123">FY2025 TANF Caseload (XLSX)</a>
              <a href="https://acf.gov/sites/default/files/documents/ofa/file.pdf">PDF</a>
            </html>
            """,
        )

        self.assertEqual(links[0]["url"], "https://acf.gov/media/123")
        self.assertEqual(links[0]["label"], "FY2025 TANF Caseload (XLSX)")
        self.assertTrue(_is_excel_link(links[0]))
        self.assertFalse(_is_excel_link(links[1]))

    def test_tanf_year_candidates_include_data_and_resource_paths(self) -> None:
        self.assertEqual(
            _tanf_page_candidates(2018),
            [
                "https://acf.gov/ofa/data/tanf-caseload-data-2018",
                "https://acf.gov/ofa/resource/tanf-caseload-data-2018",
            ],
        )

    def test_year_list_rejects_unsupported_years(self) -> None:
        with self.assertRaises(ValueError):
            _year_list([1995], TANF_YEARS)

    def test_direct_tanf_urls_are_used_before_page_discovery(self) -> None:
        records = discover_tanf_caseload_files([2025])

        self.assertEqual(records.height, 3)
        self.assertEqual(records.get_column("source_page").unique().to_list(), ["direct"])
        self.assertIn("fy2025-tanf-caseload.xlsx", records.get_column("url").to_list()[0])

    def test_direct_tanf_urls_cover_2006_through_2025(self) -> None:
        self.assertEqual(sorted(TANF_CASELOAD_URLS), list(range(2006, 2026)))
        self.assertTrue(all(len(urls) == 3 for urls in TANF_CASELOAD_URLS.values()))

    def test_direct_tanf_urls_include_known_odd_filenames(self) -> None:
        self.assertIn("2011_15months_tan_1.xls", TANF_CASELOAD_URLS[2011][0])
        self.assertIn("fy2020__ssp_caseload.xlsx", TANF_CASELOAD_URLS[2020][1])

    def test_file_records_label_from_filename(self) -> None:
        records = _file_records_from_urls(
            "tanf",
            2025,
            ["https://acf.gov/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx"],
        )

        self.assertEqual(records[0]["label"], "fy2025-tanf-caseload")

    def test_file_url_override_is_used(self) -> None:
        records = _discover_tanf_caseload_files(
            [2023],
            {
                2023: [
                    "https://acf.gov/sites/default/files/documents/ofa/fy2023-15months-tanf-caseload.xlsx"
                ]
            },
        )

        self.assertEqual(records.height, 1)
        self.assertEqual(records.row(0, named=True)["source_page"], "direct")

    def test_local_download_mirror_reads_manifest_entry(self) -> None:
        source_url = "https://acf.gov/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx"
        data = b"excel bytes"
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            raw_path = root / "data" / "hhs" / "tanf" / "2025" / "fy2025-tanf-caseload.xlsx"
            raw_path.parent.mkdir(parents=True)
            raw_path.write_bytes(data)
            (root / "manifest.json").write_text(
                json.dumps(
                    {
                        "files": {
                            raw_path.relative_to(root).as_posix(): {
                                "path": raw_path.relative_to(root).as_posix(),
                                "source_url": source_url,
                                "sha256": hashlib.sha256(data).hexdigest(),
                            }
                        }
                    }
                )
            )

            loaded, headers = _download_from_mirror(source_url, root, "main")

        self.assertEqual(loaded, data)
        self.assertEqual(headers["x-survey-kit-data-source"], "survey-kit-download-local")

    def test_github_raw_base_accepts_repo_forms(self) -> None:
        self.assertEqual(
            _github_raw_base("owner/survey-kit-download", "main"),
            "https://raw.githubusercontent.com/owner/survey-kit-download/main",
        )
        self.assertEqual(
            _github_raw_base("https://github.com/owner/survey-kit-download/tree/v1", "main"),
            "https://raw.githubusercontent.com/owner/survey-kit-download/v1",
        )

    def test_mirror_only_requires_mirror(self) -> None:
        with self.assertRaises(ValueError):
            _get_workbook_bytes(
                "https://acf.gov/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx",
                download_mirror=None,
                download_mirror_ref="main",
                download_mirror_mode="only",
            )

    def test_prefer_mode_does_not_try_agency_when_mirror_has_file(self) -> None:
        source_url = "https://acf.gov/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx"
        with patch(
            "survey_kit_data.hhs.tanf._download_from_mirror",
            return_value=(b"mirror bytes", {"x-survey-kit-data-source": "survey-kit-download-local"}),
        ) as mirror, patch("survey_kit_data.hhs.tanf._download_file") as agency:
            data, headers = _get_workbook_bytes(
                source_url,
                download_mirror="../survey_kit_download",
                download_mirror_ref="main",
                download_mirror_mode="prefer",
            )

        self.assertEqual(data, b"mirror bytes")
        self.assertEqual(headers["x-survey-kit-data-source"], "survey-kit-download-local")
        mirror.assert_called_once_with(source_url, "../survey_kit_download", "main")
        agency.assert_not_called()

    def test_cached_mirror_load_does_not_check_agency_for_updates(self) -> None:
        record = {
            "program": "tanf",
            "year": 2025,
            "label": "fy2025-tanf-caseload",
            "url": "https://acf.gov/sites/default/files/documents/ofa/fy2025-tanf-caseload.xlsx",
        }
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp, patch(
            "survey_kit_data.hhs.tanf.FileCacheManager.is_cached",
            return_value=True,
        ) as is_cached:
            config.path_cache_files = tmp
            _cache_workbook(
                record,
                force_reload=False,
                reload_if_updated=True,
                download_mirror="../survey_kit_download",
                download_mirror_ref="main",
                download_mirror_mode="prefer",
            )
        config.path_cache_files = old_cache

        is_cached.assert_called_once_with(reload_if_updated=False)

    def test_tanf_workbook_parser_consolidates_monthly_sheets(self) -> None:
        record = _file_records_from_urls("tanf", 2025, [TANF_CASELOAD_URLS[2025][0]])[0]
        excel_bytes, _headers = _download_from_mirror(str(record["url"]), "../survey_kit_download", "main")
        tables = _parse_workbook_tables(record, _read_workbook_sheets(excel_bytes))

        monthly = tables["monthly"]
        self.assertEqual(monthly.height, 825)
        self.assertEqual(
            monthly.filter(
                (pl.col("state") == "U.S. Totals")
                & (pl.col("year") == 2025)
                & (pl.col("month") == 1)
            )
            .select("total_families")
            .item(),
            860970.0,
        )
        self.assertEqual(monthly.get_column("source_program").unique().to_list(), ["tanf"])

    def test_normalized_loader_uses_adjacent_source_years_for_calendar_months(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            monthly = tanf_caseload(
                years=[2021],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
            ).collect()
        config.path_cache_files = old_cache

        self.assertEqual(monthly.get_column("year").unique().sort().to_list(), [2021])
        self.assertEqual(monthly.get_column("month").unique().sort().to_list(), list(range(1, 13)))
        self.assertEqual(monthly.height, 1980)

    def test_source_program_values_are_tanf_ssp_and_combined(self) -> None:
        self.assertEqual(_source_program_from_url(TANF_CASELOAD_URLS[2025][0]), "tanf")
        self.assertEqual(_source_program_from_url(TANF_CASELOAD_URLS[2025][1]), "ssp")
        self.assertEqual(_source_program_from_url(TANF_CASELOAD_URLS[2025][2]), "tanf_ssp")

    def test_normalized_loader_hides_source_columns_by_default(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            default_monthly = tanf_caseload(
                years=[2025],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
            )
            source_monthly = tanf_caseload(
                years=[2025],
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
            )

            default_columns = default_monthly.collect_schema().names()
            source_columns = source_monthly.collect_schema().names()
            self.assertNotIn("source_url", default_columns)
            self.assertNotIn("source_sheet", default_columns)
            self.assertNotIn("fiscal_year", default_columns)
            self.assertNotIn("calendar_year", default_columns)
            self.assertNotIn("in_fiscal_year", default_columns)
            self.assertNotIn("date", default_columns)
            self.assertIn("year", default_columns)
            self.assertIn("source_url", source_columns)
            self.assertIn("source_sheet", source_columns)
        config.path_cache_files = old_cache

    def test_normalized_loader_adds_state_fips_by_default(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            monthly = tanf_caseload(
                years=[2025],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
            )
            no_fips = tanf_caseload(
                years=[2025],
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_fips=False,
            )

            monthly = monthly.collect()
            alabama = monthly.filter(
                (pl.col("state") == "Alabama")
                & (pl.col("source_program") == "tanf")
                & (pl.col("year") == 2025)
                & (pl.col("month") == 1)
            ).row(0, named=True)
            us_total = monthly.filter(
                (pl.col("state") == "U.S. Totals")
                & (pl.col("source_program") == "tanf")
                & (pl.col("year") == 2025)
                & (pl.col("month") == 1)
            ).row(0, named=True)
            no_fips_columns = no_fips.collect_schema().names()
        config.path_cache_files = old_cache

        self.assertEqual(alabama["state_fips"], 1)
        self.assertIsNone(us_total["state_fips"])
        self.assertNotIn("state_fips", no_fips_columns)


if __name__ == "__main__":
    unittest.main()
