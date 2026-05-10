from __future__ import annotations

import unittest

import polars as pl

from survey_kit_data.dol.ui import (
    insured_unemployed_characteristics,
    weekly_ui_claims,
)
from survey_kit_data.usda.snap import (
    _join_state_fips,
    _parse_county_sheet,
    _parse_snap_sheet,
    _parse_state_sheet,
)


class LoaderSurfaceTest(unittest.TestCase):
    def test_dol_descriptive_names_are_callable(self) -> None:
        self.assertTrue(callable(insured_unemployed_characteristics))
        self.assertTrue(callable(weekly_ui_claims))


class SNAPParserCleanupTest(unittest.TestCase):
    def test_snapshot_sheet_strips_geography_labels_before_fips_join(self) -> None:
        raw = pl.DataFrame(
            {
                "column_1": ["title", "as of", "State", " Alabama ", " TOTAL "],
                "column_2": [None, None, "January 2025", "123", "456"],
            }
        )

        parsed = _parse_snap_sheet(raw)
        joined = _join_state_fips(
            parsed.lazy().rename({"state": "state"}),
            "state",
        ).collect()

        alabama = joined.filter(pl.col("state") == "Alabama").row(0, named=True)
        total = joined.filter(pl.col("state") == "TOTAL").row(0, named=True)

        self.assertEqual(alabama["state_abbr"], "AL")
        self.assertEqual(alabama["state_fips"], 1)
        self.assertIsNone(total["state_abbr"])

    def test_state_history_parser_skips_regional_summary_blocks(self) -> None:
        sheet = pl.DataFrame(
            {
                "column_1": [
                    "National Data Bank",
                    "SNAP Monthly State Participation",
                    "",
                    "",
                    "Fiscal Year and Month",
                    "MARO",
                    "Oct 2025",
                    "Alabama",
                    "Oct 2025",
                ],
                "column_2": [None, None, None, "Total 1/", "Participation 1/", None, "100", None, "10"],
                "column_3": [None, None, None, None, None, None, "200", None, "20"],
                "column_4": [None, None, None, None, "Cost", None, "300", None, "30"],
                "column_5": [None, None, None, None, "Cost Per", None, "400", None, "40"],
                "column_6": [None, None, None, None, None, None, "500", None, "50"],
            }
        )

        records = _parse_state_sheet(sheet, fiscal_year=2026, region="MARO")

        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["state"], "Alabama")
        self.assertEqual(records[0]["households"], 10.0)
        self.assertEqual(records[0]["persons"], 20.0)
        self.assertEqual(records[0]["total_cost"], 30.0)

    def test_county_parser_drops_footer_rows_without_substate_codes(self) -> None:
        raw = pl.DataFrame(
            {
                "column_1": [
                    "National Data Bank",
                    "SS7",
                    "Jan 2025",
                    "Substate Region",
                    "01001\nAutauga County",
                    "Data is Subject to Revision.",
                ],
                "column_2": [None, None, None, "Persons PA", "1", None],
                "column_3": [None, None, None, None, None, None],
                "column_4": [None, None, None, "Persons Non-PA", "2", None],
            }
        )

        parsed = _parse_county_sheet(raw, year=2025, month=1)

        self.assertIsNotNone(parsed)
        assert parsed is not None
        self.assertEqual(parsed.height, 1)
        self.assertEqual(parsed.row(0, named=True)["substate_code"], "01001")


if __name__ == "__main__":
    unittest.main()
