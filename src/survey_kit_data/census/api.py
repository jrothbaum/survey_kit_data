# # survey_data/wrappers/census.py
# from typing import TYPE_CHECKING, Optional
# import os

# if TYPE_CHECKING:
#     from census import Census as _Census

# from ..api_factory import APIFactory
# from .. import config


# def Census(
#     api_key: Optional[str] = None, 
#     cache_dir: str = ""
# ) -> "_Census":
#     """
#     Get cached Census API client with same interface as census.Census
    
#     All methods from Census are available with automatic caching.
#     Results are cached as parquet files.
    
#     Parameters
#     ----------
#     api_key : str, optional
#         Census API key. If not provided, looks for CENSUS_API_KEY env var
#     cache_dir : str, optional
#         Directory for cache storage. Defaults to config.path_cache_files
        
#     Returns
#     -------
#     Cached Census client
    
#     Example
#     -------
#     >>> from survey_data.wrappers.census import Census
#     >>> from us import states
#     >>> 
#     >>> c = Census(api_key='your_key')
#     >>> df = c.acs5.get(('NAME', 'B25034_010E'), {'for': f'state:{states.MD.fips}'})
#     """
#     from census import Census as CensusClient
    
#     # Get API key
#     if api_key is None:
#         api_key = config.api_key_census
    
#     if api_key is None:
#         raise ValueError(
#             "Census API key required. Provide via api_key parameter or "
#             "set CENSUS_API_KEY environment variable"
#         )
    
#     # Create Census client instance
#     census_client = CensusClient(api_key)
    
#     # Wrap it
#     if cache_dir == "":
#         cache_dir = config.path_cache_files
    
#     factory = APIFactory(cache_dir)
#     return factory.wrap(census_client, 'census')  # type: ignore