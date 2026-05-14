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
    discover_tanf_characteristics_files,
    discover_tanf_financial_files,
    _is_excel_link,
    _tanf_page_candidates,
    _year_list,
    discover_tanf_caseload_files,
    tanf_basic_assistance,
    tanf_cash_assistance_estimates,
    tanf_caseload,
    TANF_CASELOAD_URLS,
    TANF_CHARACTERISTICS_URLS,
    TANF_FINANCIAL_URLS,
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

    def test_direct_tanf_financial_urls_cover_2010_through_2024(self) -> None:
        self.assertEqual(sorted(TANF_FINANCIAL_URLS), list(range(2010, 2025)))
        self.assertTrue(all(len(urls) == 1 for urls in TANF_FINANCIAL_URLS.values()))

        records = discover_tanf_financial_files([2024])
        self.assertEqual(records.height, 1)
        self.assertEqual(records.row(0, named=True)["source_page"], "direct")
        self.assertIn("fy-2024-tanf-moe-financial-data.xlsx", records.row(0, named=True)["url"])

    def test_direct_tanf_characteristics_url_includes_2009_appendix(self) -> None:
        self.assertEqual(sorted(TANF_CHARACTERISTICS_URLS), list(range(2006, 2010)))

        records = discover_tanf_characteristics_files([2009])
        self.assertEqual(records.height, 1)
        self.assertEqual(records.row(0, named=True)["source_page"], "direct")
        self.assertIn("appendix_2009.xls", records.row(0, named=True)["url"])

        pdf_records = discover_tanf_characteristics_files([2008])
        self.assertEqual(pdf_records.height, 1)
        self.assertIn("characteristics2008.pdf", pdf_records.row(0, named=True)["url"])

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

    def test_basic_assistance_loader_reads_download_mirror_files(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            basic = tanf_basic_assistance(
                years=[2023],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
            ).collect()
        config.path_cache_files = old_cache

        self.assertEqual(basic.height, 51)
        alabama = basic.filter(pl.col("state") == "Alabama").row(0, named=True)
        self.assertEqual(alabama["state_fips"], 1)
        self.assertEqual(alabama["source_sheet"], "Alabama")
        self.assertAlmostEqual(alabama["federal_funds"], 25876656.81)
        self.assertAlmostEqual(alabama["state_moe"], 0.0)
        self.assertAlmostEqual(alabama["all_funds"], 25876656.81)

    def test_basic_assistance_loader_handles_legacy_wide_financial_sheet(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            basic = tanf_basic_assistance(
                years=[2010],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
                include_fips=False,
            ).collect()
        config.path_cache_files = old_cache

        self.assertEqual(basic.height, 51)
        alabama = basic.filter(pl.col("state") == "Alabama").row(0, named=True)
        self.assertEqual(alabama["source_sheet"], "Fed & State Assistance")
        self.assertIsNone(alabama["federal_funds"])
        self.assertIsNone(alabama["state_moe"])
        self.assertEqual(alabama["all_funds"], 49093777.0)

    def test_cash_assistance_estimates_loader_reads_characteristics_appendix(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            estimates = tanf_cash_assistance_estimates(
                years=[2009],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
                include_national=True,
            ).collect()
        config.path_cache_files = old_cache

        alabama = estimates.filter(
            (pl.col("source_program") == "tanf")
            & (pl.col("state") == "Alabama")
        ).row(0, named=True)
        us_total = estimates.filter(
            (pl.col("source_program") == "tanf")
            & (pl.col("state") == "U.S. Totals")
        ).row(0, named=True)
        hawaii_ssp = estimates.filter(
            (pl.col("source_program") == "ssp_moe")
            & (pl.col("state") == "Hawaii")
        ).row(0, named=True)

        self.assertEqual(alabama["source_sheet"], "41")
        self.assertEqual(alabama["state_fips"], 1)
        self.assertEqual(alabama["total_families"], 18442.0)
        self.assertEqual(alabama["cash_assistance_percent"], 100.0)
        self.assertEqual(alabama["average_months_received"], 24.2)
        self.assertEqual(alabama["average_monthly_cash_assistance"], 190.76)
        self.assertAlmostEqual(alabama["estimated_annual_cash_assistance"], 42215951.04)
        self.assertAlmostEqual(us_total["estimated_annual_cash_assistance"], 8035812321.2928)
        self.assertEqual(hawaii_ssp["source_sheet"], "70")
        self.assertAlmostEqual(hawaii_ssp["estimated_annual_cash_assistance"], 1477338.24)

    def test_cash_assistance_estimates_loader_reads_characteristics_pdf(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp, patch(
            "survey_kit_data.hhs.tanf._read_pdf_text",
            side_effect=AssertionError("mirror text should be used before local pdftotext"),
        ):
            config.path_cache_files = tmp
            estimates = tanf_cash_assistance_estimates(
                years=[2008],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="../survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
            ).collect()
        config.path_cache_files = old_cache

        alabama = estimates.filter(
            (pl.col("source_program") == "tanf")
            & (pl.col("state") == "Alabama")
        ).row(0, named=True)
        hawaii_ssp = estimates.filter(
            (pl.col("source_program") == "ssp_moe")
            & (pl.col("state") == "Hawaii")
        ).row(0, named=True)

        self.assertEqual(alabama["source_sheet"], "41")
        self.assertEqual(alabama["state_fips"], 1)
        self.assertEqual(alabama["total_families"], 17737.0)
        self.assertEqual(alabama["cash_assistance_percent"], 100.0)
        self.assertEqual(alabama["average_months_received"], 27.7)
        self.assertEqual(alabama["average_monthly_cash_assistance"], 192.82)
        self.assertAlmostEqual(alabama["estimated_annual_cash_assistance"], 41040580.08)
        self.assertEqual(hawaii_ssp["source_sheet"], "70")
        self.assertAlmostEqual(hawaii_ssp["estimated_annual_cash_assistance"], 5742417.6)

    def test_github_download_mirror_reads_financial_workbook(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp:
            config.path_cache_files = tmp
            basic = tanf_basic_assistance(
                years=[2023],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="jrothbaum/survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
            ).collect()
        config.path_cache_files = old_cache

        alabama = basic.filter(pl.col("state") == "Alabama").row(0, named=True)
        self.assertEqual(alabama["source_sheet"], "Alabama")
        self.assertAlmostEqual(alabama["all_funds"], 25876656.81)

    def test_github_download_mirror_reads_characteristics_text(self) -> None:
        old_cache = config.path_cache_files
        with tempfile.TemporaryDirectory() as tmp, patch(
            "survey_kit_data.hhs.tanf._read_pdf_text",
            side_effect=AssertionError("github mirror text should be used before local pdftotext"),
        ):
            config.path_cache_files = tmp
            estimates = tanf_cash_assistance_estimates(
                years=[2008],
                force_reload=True,
                reload_if_updated=False,
                download_mirror="jrothbaum/survey_kit_download",
                download_mirror_mode="only",
                include_source=True,
            ).collect()
        config.path_cache_files = old_cache

        alabama = estimates.filter(
            (pl.col("source_program") == "tanf")
            & (pl.col("state") == "Alabama")
        ).row(0, named=True)
        self.assertEqual(alabama["source_sheet"], "41")
        self.assertAlmostEqual(alabama["estimated_annual_cash_assistance"], 41040580.08)


if __name__ == "__main__":
    unittest.main()
