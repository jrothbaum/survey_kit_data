from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
from typing import Callable, Mapping

import requests


class FileCacheManager:
    """
    Manages the cache lifecycle for a single file, retaining the source
    call and destination path in the object's state.
    """

    def __init__(
        self,
        path_save: str,
        url: str = "",
        api_call: Callable | None = None,
        api_args: dict | None = None,
    ):
        self.path_save = Path(path_save).as_posix()
        self.url = url
        self.api_call = api_call
        self.api_args = api_args
        self.hash = self._generate_source_hash()

    def _generate_source_hash(self) -> str:
        call_name = self.api_call.__name__ if self.api_call is not None else ""
        call_signature = str(
            dict(url=self.url, api_call=call_name, api_args=self.api_args)
        )
        return hashlib.sha256(call_signature.encode("utf-8")).hexdigest()

    def _get_metadata_path(self) -> str:
        dirname = os.path.dirname(self.path_save)
        basename = os.path.basename(self.path_save)
        return Path(os.path.join(dirname, f".{basename}.hash")).as_posix()

    def _load_metadata(self) -> dict:
        meta_path = self._get_metadata_path()
        if not os.path.exists(meta_path):
            return {}
        try:
            with open(meta_path) as f:
                content = f.read().strip()
            if content.startswith("{"):
                return json.loads(content)
            # old plain-hash format — treat as source_hash only
            return {"source_hash": content}
        except (OSError, json.JSONDecodeError):
            return {}

    def is_cached(self, reload_if_updated: bool = True) -> bool:
        """
        Return True if a valid local cache exists.

        Parameters
        ----------
        reload_if_updated : bool
            When True, send a lightweight HTTP HEAD request to check whether
            the remote file has changed since the last download (using ETag or
            Last-Modified headers). Returns False if the server reports a newer
            version, triggering a re-download. Falls back to True (use cache)
            if the server does not support validators or the request fails.
            Only applies when a URL is set.
        """
        b_exists = (
            os.path.exists(self.path_save)
            or os.path.exists(self.path_save + ".parquet")
            or (self.is_unzipped_folder_present() and self.url.endswith(".zip"))
        )
        if not b_exists:
            return False

        meta = self._load_metadata()
        if meta.get("source_hash") != self.hash:
            return False

        if reload_if_updated and self.url:
            stored_validator = meta.get("validator")
            if stored_validator is not None:
                try:
                    resp = requests.head(self.url, timeout=10, allow_redirects=True)
                    current = resp.headers.get("ETag") or resp.headers.get("Last-Modified")
                    if current and current != stored_validator:
                        return False
                except Exception:
                    pass  # Network error — use cache

        return True

    def save_metadata(self, response_headers: Mapping[str, str] | None = None) -> None:
        """
        Save cache metadata. Stores the ETag or Last-Modified from
        response_headers (if provided) so future calls can detect updates.
        """
        meta: dict[str, str] = {"source_hash": self.hash}
        if response_headers is not None:
            validator = response_headers.get("ETag") or response_headers.get("Last-Modified")
            if validator:
                meta["validator"] = validator

        meta_path = self._get_metadata_path()
        os.makedirs(os.path.dirname(meta_path), exist_ok=True)
        with open(meta_path, "w") as f:
            json.dump(meta, f)

    def is_unzipped_folder_present(self) -> bool:
        if not self.path_save.lower().endswith(".zip"):
            return False
        expected_dir = os.path.splitext(self.path_save)[0]
        return os.path.isdir(expected_dir)
