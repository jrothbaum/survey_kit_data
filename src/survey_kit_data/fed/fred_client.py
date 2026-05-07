from __future__ import annotations

import time
from typing import Any

import requests

from .. import config


FRED_API_ROOT = "https://api.stlouisfed.org/fred"


class FREDClient:
    """Small raw client for FRED endpoints used by higher-level loaders."""

    def __init__(self, api_key: str | None = None, api_root: str = FRED_API_ROOT):
        self.api_key = api_key if api_key is not None else config.api_key_fred
        self.api_root = api_root.rstrip("/")
        if not self.api_key:
            raise ValueError(
                "No FRED API key found. Set config.api_key_fred or "
                "env var 'survey_kit_data_api_fred'."
            )

    def v1_series_observations(self, series_id: str, **params: Any) -> dict[str, Any]:
        return self._get_json(
            "series/observations",
            {
                "series_id": series_id,
                "file_type": "json",
                "api_key": self.api_key,
                **params,
            },
            bearer_auth=False,
        )

    def v1_series_info(self, series_id: str, **params: Any) -> dict[str, Any]:
        return self._get_json(
            "series",
            {
                "series_id": series_id,
                "file_type": "json",
                "api_key": self.api_key,
                **params,
            },
            bearer_auth=False,
        )

    def v2_release_observations(
        self,
        release_id: int,
        *,
        limit: int = 500_000,
        next_cursor: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, Any] = {
            "release_id": release_id,
            "format": "json",
            "limit": limit,
        }
        if next_cursor is not None:
            params["next_cursor"] = next_cursor
        return self._get_json("v2/release/observations", params, bearer_auth=True)

    def _get_json(
        self,
        endpoint: str,
        params: dict[str, Any],
        *,
        bearer_auth: bool,
    ) -> dict[str, Any]:
        headers = {"Authorization": f"Bearer {self.api_key}"} if bearer_auth else None
        url = f"{self.api_root}/{endpoint.lstrip('/')}"
        response = None
        for attempt in range(3):
            try:
                response = requests.get(
                    url,
                    params=params,
                    headers=headers,
                    timeout=60,
                )
                if response.status_code < 500:
                    break
            except requests.RequestException:
                if attempt == 2:
                    raise
            time.sleep(0.5 * (attempt + 1))

        if response is None:
            raise RuntimeError(f"FRED request failed before receiving a response: {url}")
        response.raise_for_status()
        payload = response.json()
        if "error_code" in payload:
            raise RuntimeError(
                f"FRED API error {payload['error_code']}: {payload.get('error_message', '')}"
            )
        return payload
