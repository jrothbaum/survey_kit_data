from __future__ import annotations

from typing import Callable

import os
import hashlib
from pathlib import Path
from typing import Dict

class FileCacheManager:
    """
    Manages the cache lifecycle for a single file, retaining the source 
    call and destination path in the object's state.
    """
    
    def __init__(
            self, 
            path_save: str, 
            url: str="",
            api_call:Callable | None=None,
            api_args:dict | None=None):
        """
        Initializes the manager with the final file destination and the 
        source call signature (URL or API call).
        """
        # Instance attributes storing the context
        self.path_save = Path(path_save).as_posix()
        self.url = url
        self.api_call = api_call
        self.api_args = api_args
        self.hash = self._generate_source_hash()

    def _generate_source_hash(self) -> str:
        """Generates the unique hash based on the stored call_signature."""

        call_name = ""
        if self.api_call is not None:
            call_name = self.api_call.__name__
        call_signature = str(
            dict(
                url=self.url,
                api_call=call_name,
                api_args=self.api_args
            )
        )
        return hashlib.sha256(call_signature.encode('utf-8')).hexdigest()

    def _get_metadata_path(self) -> str:
        """
        Generates the path for the hidden hash file based on the stored path_save.
        """
        dirname = os.path.dirname(self.path_save)
        basename = os.path.basename(self.path_save)
        # Using the dot prefix for Unix systems
        return Path(os.path.join(dirname, f".{basename}.hash")).as_posix()

    def is_cached(self) -> bool:
        """
        Checks if the file exists and its source hash matches the stored call_signature hash.
        """
        # Check against self.path_save
        b_exists = (
            os.path.exists(self.path_save)
            or os.path.exists(self.path_save + ".parquet")
            or (self.is_unzipped_folder_present() and self.url.endswith(".zip"))
        )
        
        if not b_exists:
            return False

        # Check against self._get_metadata_path()
        meta_path = self._get_metadata_path()
        if not os.path.exists(meta_path):
            return False

        try:
            with open(meta_path, 'r') as f:
                saved_hash = f.read().strip()
            
            return saved_hash == self.hash
            
        except IOError:
            return False

    def save_metadata(self):
        """
        Saves the hash of the stored call_signature to the companion file.
        Called after a successful download/processing.
        """
        meta_path = self._get_metadata_path()
        
        os.makedirs(os.path.dirname(meta_path), exist_ok=True)
        
        with open(meta_path, 'w') as f:
            f.write(self.hash)

    def is_unzipped_folder_present(self) -> bool:
        """
        Checks if the unzipped folder exists based on the path_save.
        (Assumes path_save is the ZIP path, not the final processed path).
        """
        if not self.path_save.lower().endswith('.zip'):
            return False
            
        expected_dir = os.path.splitext(self.path_save)[0]
        return os.path.isdir(expected_dir)