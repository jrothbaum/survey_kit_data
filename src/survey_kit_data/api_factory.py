# survey_data/api_factory.py
import hashlib
import json
from pathlib import Path
from typing import Any
import polars as pl

from .cache_manager import FileCacheManager
from . import config


class APIFactory:
    """
    Factory that wraps external API library objects to add transparent caching.
    Users interact with the original API - caching happens automatically.
    """
    
    def __init__(self, cache_dir: str = ""):
        if cache_dir == "":
            cache_dir = config.path_cache_files
        
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
    
    def wrap(self, api_object: Any, name: str):
        """
        Wrap an API object so all its methods return cached results
        
        Parameters
        ----------
        api_object : Any
            The API client object (e.g., censusdata module, Fred instance)
        name : str
            Identifier for this API (used in cache keys)
            
        Returns
        -------
        Wrapped object with same API but cached results
        
        Examples
        --------
        >>> import censusdata
        >>> factory = APIFactory()
        >>> cached_census = factory.wrap(censusdata, 'censusdata')
        >>> 
        >>> # Use exactly like censusdata - but cached!
        >>> df = cached_census.download('acs5', 2022, geo, vars)
        """
        factory = self
        
        class CachedWrapper:
            def __init__(wrapper_self):
                wrapper_self._obj = api_object
                wrapper_self._name = name
            
            def __getattr__(wrapper_self, attr_name):
                """Intercept method calls and add caching"""
                original_attr = getattr(wrapper_self._obj, attr_name)
                
                # If it's not callable, just return it
                if not callable(original_attr):
                    return original_attr
                
                # If it's callable, wrap it with caching
                def cached_method(*args, **kwargs):
                    # Generate cache key from method name + args
                    cache_key = factory._make_cache_key(name, attr_name, args, kwargs)
                    parquet_path = factory.cache_dir / f"{cache_key}.parquet"
                    
                    # Create cache manager
                    fcm = FileCacheManager(
                        path_save=str(parquet_path),
                        api_call=original_attr,
                        api_args={'args': args, 'kwargs': kwargs}
                    )
                    
                    # Check cache
                    if fcm.is_cached():
                        return pl.scan_parquet(parquet_path)
                    
                    # Call original method
                    result = original_attr(*args, **kwargs)
                    
                    # Convert to polars if needed
                    if isinstance(result, pl.DataFrame):
                        df = result
                    elif isinstance(result, pl.LazyFrame):
                        df = result.collect()
                    elif hasattr(result, 'columns'):  # pandas-like
                        df = pl.from_pandas(result)
                    else:
                        df = pl.DataFrame(result)
                    
                    # Cache result
                    df.write_parquet(parquet_path)
                    fcm.save_metadata()
                    
                    return pl.scan_parquet(parquet_path)
                
                return cached_method
        
        return CachedWrapper()

    def _make_cache_key(
        self,
        api_name: str,
        method_name: str,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> str:
        payload = {
            "api": api_name,
            "method": method_name,
            "args": args,
            "kwargs": kwargs,
        }
        raw_key = json.dumps(payload, sort_keys=True, default=repr)
        digest = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
        return f"{api_name}_{method_name}_{digest}"
