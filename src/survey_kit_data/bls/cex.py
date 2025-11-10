from __future__ import annotations
from typing import Dict, TYPE_CHECKING
from ..load import load_from_url

from .. import logger

if TYPE_CHECKING:
    import polars as pl
def path_interview(year:int) -> str:
    if year >= 2022:
        return f"https://www.bls.gov/cex/pumd/data/csv/intrvw{str(year)[2:]}.zip"
    elif (year >= 1984) or year in [1980,1981]:
        return f"https://www.bls.gov/cex/pumd/data/comma/intrvw{str(year)[2:]}.zip"
    
def path_weights(year:int) -> str:
    if year >= 2018:
        return f"https://www.bls.gov/cex/pumd/data/csv/cex{str(year)[2:]}_csv.zip"
    elif year >= 2016:
        return f"https://www.bls.gov/cex/2016/research/cex{str(year)[2:]}_xlsx.zip"

def path_diary(year:int) -> str:
    if year >= 2022:
        return f"https://www.bls.gov/cex/pumd/data/csv/diary{str(year)[2:]}.zip"
    elif (year >= 1990) or year in [1980,1981]:
        return f"https://www.bls.gov/cex/pumd/data/comma/diary{str(year)[2:]}.zip"
    
# def file_mapping(year:int) -> Dict[str,str]:
#     if year >= 2019:
#         return {
#           f"asec_sas_repwgt_{year}":"replicate_weights",
#           f"ffpub{year-2000}":"family",
#           f"hhpub{year-2000}":"hhld",
#           f"pppub{year-2000}":"person",
#         }
    

def cex(year:int) -> Dict[str,pl.LazyFrame]:
    url = path_interview(year)

    if url is None:
        message = f"File not available for {year}"
        logger.info(message)
        raise Exception(message)


    url_diary = path_diary(year)
    url_weights = path_weights(year)


    data_interview = load_from_url(
        save_name=f"cex_{year}",
        url=url
    )

    
    data_out = {}
    for keyi, dfi in data_interview.items():
        data_out[f"interview_{keyi}"] = dfi
        

    if url_diary:
        data_diary = load_from_url(
            save_name=f"cex_diary_{year}",
            url=url_diary
        )

        for keyi, dfi in data_diary.items():
            data_out[f"diary_{keyi}"] = dfi
        

    if url_weights:
        data_weights = load_from_url(
            save_name=f"cex_weights_{year}",
            url=url_weights
        )

        for keyi, dfi in data_weights.items():
            data_out[f"state_weights_{keyi}"] = dfi

    return data_out
        