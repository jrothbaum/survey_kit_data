from __future__ import annotations
from typing import Dict, TYPE_CHECKING

import polars as pl
from pathlib import Path
from typing import Union, Optional

from ..load import load_from_url
from ..cache_manager import FileCacheManager
from ..utilities.sas_input_reader import read_sas_fwf

from .. import logger, config




def _cps_asec_2019_after(year:int) -> Dict[str,pl.LazyFrame]:
    url = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/asecpub{year-2000}sas.zip"
    

    d_mapped = {
        f"asec_sas_repwgt_{year}":"replicate_weights",
        f"ffpub{year-2000}":"family",
        f"hhpub{year-2000}":"hhld",
        f"pppub{year-2000}":"person",
    }
    
    data = load_from_url(
        save_name=f"cps_{year}",
        url=url
    )

    return {d_mapped[keyi]:valuei for keyi, valuei in data.items()}

def _cps_asec_1988_2018(year:int,
                        redesign_2014:bool=False) -> Dict[str,pl.LazyFrame]:
    
    if year == 2014:
        if redesign_2014:
            url_data = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/asec2014_pubuse_3x8_rerun_v2.zip"
            url_rep_weights = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/CPS_ASEC_ASCII_REPWGT_2014_3x8_run5.zip"
            url_data_dictionary = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/asec2014R_pubuse.dd.txt"
            url_rep_weights_input = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/CPS_ASEC_ASCII_REPWGT_2014_3x8_run5.SAS"
        else:
            url_data = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/asec2014_pubuse_tax_fix_5x8_2017.zip"
            url_rep_weights = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/CPS_ASEC_ASCII_REPWGT_2014.zip"
            url_data_dictionary = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/asec2014early_pubuse.dd.txt"
            url_rep_weights_input = f"https://www2.census.gov/programs-surveys/cps/datasets/2014/march/CPS_ASEC_ASCII_REPWGT_2014.SAS"

    elif year >= 2011:
        url_data = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/asec{year}_pubuse.zip"
        url_rep_weights = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/CPS_ASEC_ASCII_REPWGT_{year}.zip"
        url_data_dictionary = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/asec{year}_pubuse.dd.txt"
        url_rep_weights_input = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/CPS_ASEC_ASCII_REPWGT_{year}.SAS"


        if year in [2012, 2013, 2015,]:
            url_data_dictionary = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/asec{year}early_pubuse.dd.txt"
        elif year in [2016, 2017, 2018]:
            url_data_dictionary = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/Asec{year}_Data_Dict_Full.txt"
            

        if year == 2016:
            url_data = "https://www2.census.gov/programs-surveys/cps/datasets/2016/march/asec2016_pubuse_v3.zip"

    elif year >= 2005:
        url_data = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.zip"
        url_data_dictionary = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.ddf"
        url_rep_weights = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/CPS_ASEC_ASCII_REPWGT_{year}.zip"
        url_rep_weights_input = f"https://www2.census.gov/programs-surveys/cps/datasets/{year}/march/CPS_ASEC_ASCII_REPWGT_{year}.SAS"
    elif year == 1993:
        url_data = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.zip"
        url_data_dictionary = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.doc"
        url_rep_weights = ""
        url_rep_weights_input = ""
    elif year >= 1989:
        url_data = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.zip"
        url_data_dictionary = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.ddf"
        url_rep_weights = ""
        url_rep_weights_input = ""
    elif year == 1988:
        url_data = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.zip"
        url_data_dictionary = f"https://data.nber.org/cps/cpsmar{str(year)[2:]}.txt"
        url_rep_weights = ""
        url_rep_weights_input = ""
    save_name_base = f"cps_{year}"
    cache_dir = Path(config.path_cache_files)
    path_save_base = cache_dir / save_name_base
    
    path_save = cache_dir / save_name_base
    
    fcm = FileCacheManager(
        path_save=str(path_save),
        api_call=_cps_asec_1988_2018,
        api_args=dict(
            url_data=url_data,
            url_rep_weights=url_rep_weights,
            url_data_dictionary=url_data_dictionary,
            url_rep_weights_input=url_rep_weights_input
        )
    )
    if fcm.is_cached():
        #   Load the files directly
        logger.info("Loading from cached data")
        
        d_output = {}
        for filei in ["replicate_weights","family","hhld","person"]:
            if Path(f"{path_save.as_posix()}/{filei}.parquet").exists():
                d_output[filei] = pl.scan_parquet(f"{path_save.as_posix()}/{filei}.parquet")
        return d_output
    else:
        f_dictionary = load_from_url(
            save_name=f"cps_{year}_dictionary",
            url=url_data_dictionary,
            no_data_error=False
        )

        f_data = load_from_url(
            save_name=f"cps_{year}",
            url=url_data,
            no_data_error=False
        )

        if url_rep_weights != "":
            f_rep_inputs = load_from_url(
                save_name=f"cps_{year}_repweights_input",
                url=url_rep_weights_input,
                no_data_error=False
            )
                
            f_repweights = load_from_url(
                save_name=f"cps_{year}_repweights",
                url=url_rep_weights,
                no_data_error=False
            )
            

        path_data = Path(f_data[0])
        path_dictionary = Path(f_dictionary[0])
        data_sas_input_hh = parse_data_dictionary(
            path_dictionary.as_posix(),
            output_path=f"{path_dictionary.as_posix()}_hhld.txt",
            from_line=["HOUSEHOLD RECORD","*Household Record"],
            to_line=["FAMILY RECORD","*Family Record"],
            skip_values="FILLER"
        )

        data_sas_input_pp = parse_data_dictionary(
            path_dictionary.as_posix(),
            output_path=f"{path_dictionary.as_posix()}_person.txt",
            from_line=["PERSON RECORD","*Person Record"],
            to_line="",
            skip_values="FILLER"
        )

        data_sas_input_ff = parse_data_dictionary(
            path_dictionary.as_posix(),
            output_path=f"{path_dictionary.as_posix()}_family.txt",
            from_line=["FAMILY RECORD","*Family Record"],
            to_line=["PERSON RECORD","*Person Record"],
            skip_values="FILLER"
        )

        #   Split the combined dat file into separate hh, family, and person files
        split_dat(
            path=path_data.as_posix(),
            path_hhld=f"{path_data.as_posix()}_hhld.dat",
            path_family=f"{path_data.as_posix()}_family.dat",
            path_person=f"{path_data.as_posix()}_person.dat",
        )


        #   Convert each dat to parquet
        d_output = {}
        for typei in ["hhld", "family", "person"]:
            logger.info(f"   Converting {typei} from dat to parquet")
            dfi = read_sas_fwf(
                f"{path_data.as_posix()}_{typei}.dat",
                sas_script_path=f"{path_dictionary.as_posix()}_{typei}.txt"
            )

            path_savei = f"{(path_save_base / typei).as_posix()}.parquet"
            dfi.lazy().sink_parquet(path_savei)
            d_output[typei] = pl.scan_parquet(path_savei)

            #   Delete the temporary files
            Path(f"{path_data.as_posix()}_{typei}.dat").unlink()
            Path(f"{path_dictionary.as_posix()}_{typei}.txt").unlink()

        path_data.unlink()
        path_dictionary.unlink()


        #   Get rep weights, if available
        if url_rep_weights != "":
            #   Convert the rep weights file to parquet
            typei = "replicate_weights"
            logger.info(f"   Converting {typei} from dat to parquet")
            df_rw = read_sas_fwf(
                Path(f_repweights[0]).as_posix(),
                sas_script_path=Path(f_rep_inputs[0]).as_posix()
            )
            path_savei = f"{(path_save_base / typei).as_posix()}.parquet"
            df_rw.lazy().sink_parquet(path_savei)
            d_output[typei] = pl.scan_parquet(path_savei)

            # Clean up
            Path(f_repweights[0]).unlink()
            Path(f_rep_inputs[0]).unlink()

            Path(f_repweights[0]).parent.rmdir()
            Path(f_rep_inputs[0]).parent.rmdir()
        
        # Clean up
        Path(f_dictionary[0]).parent.rmdir()
        
        
        fcm.save_metadata()

        return d_output
def cps_asec(year:int,
             redesign_2014:bool=False) -> Dict[str,pl.LazyFrame]:
    if year >= 2019:
        return _cps_asec_2019_after(year)
    elif year >= 1988:
        return _cps_asec_1988_2018(year,
                                   redesign_2014=redesign_2014)
    else:
        message = f"Loader not available for {year} (only 1988-)"
        logger.error(message)
        raise Exception(message)


def parse_data_dictionary(
    dict_path: Union[str, Path],
    output_path: Optional[Union[str, Path]] = None,
    auto_detect_char: bool = True,
    line_prefix: str = "D",
    from_line:str|list[str]="",
    to_line:str|list[str]="",
    skip_values:list[str] | str | None = None,
) -> str:
    """
    Parse a data dictionary with automatic character type detection.
    
    Uses heuristics to determine if a variable should be character type:
    - Variable name contains 'ID', 'CODE', 'NAME', 'DESC'
    - Range values contain non-numeric characters
    - Size is very small (1 character) might be a code
    
    Parameters
    ----------
    dict_path : str or Path
        Path to the data dictionary file
    output_path : str or Path, optional
        Path to write the SAS script
    auto_detect_char : bool, default True
        Automatically detect character variables using heuristics
    line_prefix : str, default "D"
        Prefix that indicates a data line
    
    Returns
    -------
    str
        The generated SAS INPUT script
    """

    if type(from_line) is str:
        from_line = list[from_line]
    if type(to_line) is str:
        to_line = list[to_line] 
    if type(skip_values) is str:
        skip_values = [skip_values]

    if skip_values is None:
        skip_values = []
    
    with open(dict_path, 'r') as f:
        lines = f.readlines()
    
    # Filter to only lines starting with the prefix
    start_found = from_line == ""
    end_found = False

    data_lines = []
    for line in lines:
        if not start_found:
            start_found = line.strip() in from_line
        if not end_found and to_line != "":
            end_found = line.strip() in to_line

        keep_line = start_found and not end_found and line.strip().startswith(line_prefix)

        if keep_line:
            data_lines.append(line)

    columns = []
    for line in data_lines:
        # Remove the prefix
        line = line.strip()
        if line.startswith(line_prefix):
            line = line[len(line_prefix):].strip()
        
        # Split by whitespace
        parts = line.split()
        
        if len(parts) < 3:
            continue
        
        varname = parts[0].replace("-", "_")
        try:
            size = int(parts[1])
            begin = int(parts[2])
        except ValueError:
            continue
        
        # Extract range if present (for analysis)
        range_str = parts[3] if len(parts) > 3 else ""
        
        end = begin + size - 1
        
        # Auto-detect character fields
        is_char = False
        if auto_detect_char:
            varname_upper = varname.upper()
            
            # Check for common character variable name patterns
            char_indicators = [
                'ID', 'CODE', 'NAME', 'DESC', 'TYPE', 
                'CAT', 'CLASS', 'GROUP', 'STATUS', 'FLAG'
            ]
            
            is_char = any(indicator in varname_upper for indicator in char_indicators)
            
            # Check if range contains letters (suggesting categorical/character)
            if range_str and not is_char:
                # Remove parentheses and check content
                range_content = range_str.strip('()')
                if ':' in range_content:
                    parts = range_content.split(':')
                    # If either part has letters, might be character
                    if any(not p.replace('-', '').replace('.', '').isdigit() 
                          for p in parts if p.strip()):
                        is_char = True
        
        if varname not in skip_values:
            columns.append({
                'varname': varname,
                'begin': begin,
                'end': end,
                'size': size,
                'is_char': is_char,
                'range': range_str
            })
    
    # Sort by begin position
    columns.sort(key=lambda x: x['begin'])
    
    # Generate SAS INPUT statement with comments
    sas_lines = ["/* Generated from data dictionary */", "INPUT"]
    
    for col in columns:
        char_marker = " $" if col['is_char'] else ""
        comment = f"  /* {col['range']} */" if col['range'] else ""
        sas_lines.append(f"    {col['varname']}{char_marker} {col['begin']}-{col['end']}{comment}")
    
    sas_lines.append(";")
    
    sas_script = "\n".join(sas_lines)
    
    if output_path:
        with open(output_path, 'w') as f:
            f.write(sas_script)
        print(f"SAS input script written to: {output_path}")
    
    return sas_script


def split_dat(path: str,
                  path_hhld: str,
                  path_family: str,
                  path_person: str) -> None:
    """
    Split a CPS .dat file into three separate files based on record type.
    
    Parameters
    ----------
    path : str
        Path to the input .dat file
    path_hhld : str
        Path for household records (first char = '1')
    path_family : str
        Path for family records (first char = '2')
    path_person : str
        Path for person records (first char = '3')
    """
    with (open(path, 'r') as f_in, 
         open(path_hhld, 'w') as f_hhld, 
         open(path_family, 'w') as f_family, 
         open(path_person, 'w') as f_person):
        
        for line in f_in:
            if not line:  # Skip empty lines
                continue
                
            first_char = line[0]
            
            if first_char == '1':
                f_hhld.write(line)
            elif first_char == '2':
                f_family.write(line)
            elif first_char == '3':
                f_person.write(line)
                