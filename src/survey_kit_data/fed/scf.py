from __future__ import annotations
from typing import Dict, TYPE_CHECKING
from ..load import load_from_url

from .. import logger

if TYPE_CHECKING:
    import polars as pl
def path_for_year(year:int,
                  replicate:bool=False) -> str:
    
    years = [2004, 2007, 2010, 2013, 2016, 2019, 2022]
        
    if replicate:
        years_short = [1989, 1992, 1995, 1998]
        years = [2001] + years
        d_files = {yeari:f"https://www.federalreserve.gov/econres/files/scf{yeari}rw1s.zip" for yeari in years}

        for yeari in years_short:
            d_files[yeari] = f"https://www.federalreserve.gov/econres/files/scf{str(yeari)[2:]}rw1s.zip"


        return d_files[year]
    else:
        years_short = [1989, 1992, 1995, 1998, 2001]

        d_files = {yeari:f"https://www.federalreserve.gov/econres/files/scf{yeari}s.zip" for yeari in years}

        for yeari in years_short:
            d_files[yeari] = f"https://www.federalreserve.gov/econres/files/scf{str(yeari)[2:]}s.zip"


        return d_files[year]
    

def scf(year:int) -> Dict[str,pl.LazyFrame]:
    url = path_for_year(year, replicate=False)
    url_replicate = path_for_year(year, replicate=True)

    if url is None:
        message = f"File not available for {year}"
        logger.info(message)
        raise Exception(message)

    data_main = load_from_url(
        save_name=f"scf_{year}",
        url=url
    )

    data_replicate = load_from_url(
        save_name=f"scf_replicate_{year}",
        url=url_replicate
    )
    return  dict(
        main=data_main,
        replicates=data_replicate
    )