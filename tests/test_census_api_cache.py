from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from survey_kit_data import config
from survey_kit_data.census.api import _cache_path, _normalized_query, acs5


def _hash_path(path: Path) -> Path:
    return path.parent / f".{path.name}.hash"


class CensusAPICacheIntegrationTest(unittest.TestCase):
    """Integration tests that hit the real Census API."""

    def setUp(self) -> None:
        self._old_cache = os.environ.get("_survey_kit_data_path_cache_files_")
        self._tmpdir = tempfile.TemporaryDirectory()
        config.path_cache_files = self._tmpdir.name

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        if self._old_cache is None:
            os.environ.pop("_survey_kit_data_path_cache_files_", None)
        else:
            os.environ["_survey_kit_data_path_cache_files_"] = self._old_cache

    def test_acs5_cache_hit_force_reload_and_query_invalidation(self) -> None:
        query = _normalized_query(
            year=2022,
            dataset="acs/acs5",
            variables=["NAME", "B19013_001E"],
            geo_for="state:*",
            geo_in=None,
            predicates=None,
        )
        path = _cache_path(query)

        df = acs5(
            year=2022,
            variables=["NAME", "B19013_001E"],
            geo_for="state:*",
            force_reload=True,
        ).collect()
        self.assertGreaterEqual(df.height, 51)
        self.assertTrue(path.exists())
        self.assertTrue(_hash_path(path).exists())
        first_mtime = path.stat().st_mtime_ns

        df_cached = acs5(
            year=2022,
            variables=["NAME", "B19013_001E"],
            geo_for="state:*",
        ).collect()
        self.assertEqual(df_cached.height, df.height)
        self.assertEqual(path.stat().st_mtime_ns, first_mtime)

        time.sleep(1.1)
        acs5(
            year=2022,
            variables=["NAME", "B19013_001E"],
            geo_for="state:*",
            force_reload=True,
        ).collect()
        self.assertGreater(path.stat().st_mtime_ns, first_mtime)

        changed_query = _normalized_query(
            year=2022,
            dataset="acs/acs5",
            variables=["NAME", "B19013_001E", "B01003_001E"],
            geo_for="state:*",
            geo_in=None,
            predicates=None,
        )
        changed_path = _cache_path(changed_query)
        changed_df = acs5(
            year=2022,
            variables=["NAME", "B19013_001E", "B01003_001E"],
            geo_for="state:*",
        ).collect()
        self.assertGreaterEqual(changed_df.height, 51)
        self.assertTrue(changed_path.exists())
        self.assertNotEqual(path, changed_path)


if __name__ == "__main__":
    unittest.main()
