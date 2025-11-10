from __future__ import annotations

import os
import json
import tempfile
from pathlib import Path
from datetime import datetime


class TypedEnvVar:
    def __init__(self, env_name: str, default=None, convert=str):
        self.env_name = env_name
        self.default = default
        self.convert = convert

    def __get__(self, obj, objtype=None):
        value = os.getenv(self.env_name)
        if value is None:
            return self.default

        if self.convert in [list, dict]:
            return json.loads(value)

        return self.convert(value)

    def __set__(self, obj, value):
        # Convert to JSON when setting
        if isinstance(value, (list, dict)):
            os.environ[self.env_name] = json.dumps(value)
        else:
            os.environ[self.env_name] = str(value)


class Config:
    """
    Global configuration for survey-kit-data.
    
    Config manages package-wide settings including cache paths and API keys
    for government data sources. Settings can be configured via environment 
    variables or by directly setting attributes on the config instance.
    
    The config instance is typically accessed via:
```python
        from survey_kit_data import config
        config.data_root = "/path/to/data"
        config.api_key_census = "your_census_key"
```
    
    Attributes
    ----------
    code_root : str
        Root directory for code files. Set via environment variable 
        `_survey_kit_data_code_root_` or directly. Default is "".
    data_root : str
        Root directory for data files. Set via environment variable
        `_survey_kit_data_data_root_` or directly. Default is "".
    cpus : int
        Number of CPUs to use for parallel operations.
        Set via `_survey_kit_data_n_cpus_` or directly. Default is os.cpu_count().
    path_cache_files : str
        Directory for cached files. Set via `_survey_kit_data_path_cache_files_`
        or directly. Default is {data_root}/cached_files.
    api_key_census : str
        API key for Census Bureau API. Set via `_survey_kit_data_api_census_`
        or directly. Get a key at https://api.census.gov/data/key_signup.html.
        Default is "".
    api_key_fred : str
        API key for FRED API. Set via `_survey_kit_data_api_fred_`
        or directly. Get a key at https://fred.stlouisfed.org/docs/api/api_key.html.
        Default is "".
        
    Examples
    --------
    Basic configuration:
    
    >>> from survey_kit_data import config
    >>> config.data_root = "/projects/data/myproject"
    >>> config.api_key_census = "your_key_here"
    >>> print(config.path_cache_files)
    '/projects/data/myproject/cached_files'
    
    Using environment variables:
```bash
        export _survey_kit_data_data_root_="/projects/data/myproject"
        export _survey_kit_data_api_census_="your_key_here"
```
    
    Cache files:
    
    >>> cache_path = config.path_cache_with_random(as_parquet=True)
    >>> print(cache_path)
    '/projects/data/myproject/cached_files/abc123xyz.parquet'
    
    """



    _code_root_key = "_survey_kit_data_code_root_"
    _data_root_key = "_survey_kit_data_data_root_"
    _cpus_key = "_survey_kit_data_n_cpus_"
    _path_cache_files_key = "_survey_kit_data_path_cache_files_"

    #   api keys
    _api_key_census_key = "_survey_kit_data_api_census_"
    _api_key_fred_key = "_survey_kit_data_api_fred_"
    
    code_root = TypedEnvVar(_code_root_key, default="", convert=str)
    data_root = TypedEnvVar(_data_root_key, default="", convert=str)
    cpus = TypedEnvVar(_cpus_key, os.cpu_count(), int)
    api_key_census = TypedEnvVar(_api_key_census_key, default="",convert=str)
    api_key_fred = TypedEnvVar(_api_key_fred_key, default="",convert=str)
    _path_cache_files = TypedEnvVar(_path_cache_files_key, "", str)
    
    
    @property
    def path_cache_files(self) -> int:
        if self._path_cache_files != "":
            return self._path_cache_files
        else:
            if self.data_root == "":
                from . import logger

                message = "You must set Configs().data_root to get a default cache file directory"
                logger.error(message)
                raise Exception(message)

            return Path(self.data_root) / "cached_files"

    @path_cache_files.setter
    def path_cache_files(self, value: str):
        self._path_cache_files = value

    def path_cache_with_random(
        self, as_parquet: bool = False, underscore_prefix: bool = False
    ) -> str:
        """
        Generate a random file path in cache.
        
        Parameters
        ----------
        as_parquet : bool, optional
            Add .parquet extension. Default is False.
        underscore_prefix : bool, optional
            Prefix filename with underscore. Default is False.
            
        Returns
        -------
        str
            Full path to a uniquely-named temporary file.
            
        Examples
        --------
        >>> config.path_cache_with_random()
        '/data/cached_files/abc123xyz'
        
        >>> config.path_cache_with_random(as_parquet=True)
        '/data/cached_files/abc123xyz.parquet'
        """
        if as_parquet:
            parquet_suffix = ".parquet"
        else:
            parquet_suffix = ""

        if underscore_prefix:
            prefix = "_"
        else:
            prefix = ""

        return os.path.normpath(
            f"{self.path_cache_files}/{prefix}{next(tempfile._get_candidate_names())}{parquet_suffix}"
        )