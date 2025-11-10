from __future__ import annotations
from typing import Dict, TYPE_CHECKING
from ..load import load_from_url

from .. import logger

if TYPE_CHECKING:
    import polars as pl
def path_for_year(year:int) -> str:
    if year >= 2019:
        return f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/asecpub{year-2000}sas.zip"
    
def file_mapping(year:int) -> Dict[str,str]:
    if year >= 2019:
        return {
          f"asec_sas_repwgt_{year}":"replicate_weights",
          f"ffpub{year-2000}":"family",
          f"hhpub{year-2000}":"hhld",
          f"pppub{year-2000}":"person",
        }
    

def cps_asec(year:int) -> Dict[str,pl.LazyFrame]:
    url = path_for_year(year)

    if url is None:
        message = f"File not available for {year}"
        logger.info(message)
        raise Exception(message)

    data = load_from_url(
        save_name=f"cps_{year}",
        # url="https://www.federalreserve.gov/econres/files/scf2022s.zip",
        url=url
    )

    d_mapped = file_mapping(year)
    return {d_mapped[keyi]:valuei for keyi, valuei in data.items()}
