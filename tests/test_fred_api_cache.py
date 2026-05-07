from __future__ import annotations

import os
import tempfile
import time
import unittest
from pathlib import Path

from survey_kit_data import config
from survey_kit_data.fed.fred import _cache_path, get_series
from survey_kit_data.fed.fred_catalog import FREDReleaseSpec
from survey_kit_data.fed.fred_releases import (
    load_release_observations,
    release_cache_path,
)


def _hash_path(path: Path) -> Path:
    return path.parent / f".{path.name}.hash"


class FREDAPICacheIntegrationTest(unittest.TestCase):
    """Integration tests that hit real FRED v1/v2 endpoints."""

    def setUp(self) -> None:
        if not config.api_key_fred:
            self.skipTest("survey_kit_data_api_fred is not set")

        self._old_cache = os.environ.get("_survey_kit_data_path_cache_files_")
        self._tmpdir = tempfile.TemporaryDirectory()
        config.path_cache_files = self._tmpdir.name

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        if self._old_cache is None:
            os.environ.pop("_survey_kit_data_path_cache_files_", None)
        else:
            os.environ["_survey_kit_data_path_cache_files_"] = self._old_cache

    def test_v1_series_cache_hit_force_reload_and_as_of_cache(self) -> None:
        path = _cache_path("CAUR", "2020-01-01", "2020-02-01")

        df = get_series(
            "CAUR",
            observation_start="2020-01-01",
            observation_end="2020-02-01",
            force_reload=True,
        ).collect()
        self.assertEqual(df.height, 2)
        self.assertTrue(path.exists())
        self.assertTrue(_hash_path(path).exists())
        first_mtime = path.stat().st_mtime_ns

        df_cached = get_series(
            "CAUR",
            observation_start="2020-01-01",
            observation_end="2020-02-01",
        ).collect()
        self.assertEqual(df_cached.height, 2)
        self.assertEqual(path.stat().st_mtime_ns, first_mtime)

        time.sleep(1.1)
        get_series(
            "CAUR",
            observation_start="2020-01-01",
            observation_end="2020-02-01",
            force_reload=True,
        ).collect()
        self.assertGreater(path.stat().st_mtime_ns, first_mtime)

        as_of_path = _cache_path(
            "CAUR",
            "2020-01-01",
            "2020-02-01",
            "2024-01-01",
            "2024-01-01",
        )
        as_of_df = get_series(
            "CAUR",
            observation_start="2020-01-01",
            observation_end="2020-02-01",
            as_of="2024-01-01",
        ).collect()
        self.assertEqual(as_of_df.height, 2)
        self.assertTrue(as_of_path.exists())
        self.assertNotEqual(path, as_of_path)

    def test_v2_release_cache_hit_force_reload_and_signature_invalidation(self) -> None:
        spec = FREDReleaseSpec(
            name="test_state_unemployment_rate",
            release_id=112,
            series_filter=lambda series_id: series_id == "CAUR",
        )
        path = release_cache_path(spec)

        df = load_release_observations(
            spec,
            force_reload=True,
            limit=500_000,
        ).collect()
        self.assertGreaterEqual(df.height, 1)
        self.assertTrue(path.exists())
        self.assertTrue(_hash_path(path).exists())
        first_mtime = path.stat().st_mtime_ns

        df_cached = load_release_observations(spec, limit=500_000).collect()
        self.assertEqual(df_cached.height, df.height)
        self.assertEqual(path.stat().st_mtime_ns, first_mtime)

        time.sleep(1.1)
        load_release_observations(
            spec,
            force_reload=True,
            limit=500_000,
        ).collect()
        force_reload_mtime = path.stat().st_mtime_ns
        self.assertGreater(force_reload_mtime, first_mtime)

        time.sleep(1.1)
        load_release_observations(spec, limit=400_000).collect()
        self.assertGreater(path.stat().st_mtime_ns, force_reload_mtime)


if __name__ == "__main__":
    unittest.main()
