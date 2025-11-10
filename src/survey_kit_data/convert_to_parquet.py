from __future__ import annotations

import os
from pathlib import Path
from typing import List, Optional

import polars as pl
from polars_readstat import scan_readstat

from . import logger
types_to_convert = [
    "csv",
    "dta",
    "sas7bdat",
    "sav"
]

def convert_path(
    path_to_convert: str,
    file_types: Optional[List[str]] = None,
    recursive: bool = True,
    overwrite: bool = False
) -> List[Path]:
    """
    Convert files to Parquet format.
    
    Parameters
    ----------
    path_to_convert : str
        Path to a file or directory to convert
    file_types : List[str], optional
        List of file extensions to convert (without dots).
        Defaults to types_to_convert global.
    recursive : bool, default True
        If True, recursively search subdirectories
    overwrite : bool, default False
        If True, overwrite existing parquet files
        
    Returns
    -------
    List[Path]
        List of paths to created parquet files
    """
    if file_types is None:
        file_types = types_to_convert
    
    path = Path(path_to_convert)
    parquet_files = []
    
    if path.is_dir():
        # Get all files matching the extensions
        glob_prefix = "**/" if recursive else ""
        for ext in file_types:
            for file_path in path.glob(f"{glob_prefix}*.{ext}"):
                if file_path.is_file():
                    result = _convert_file(file_path, overwrite, delete_original=True)
                    if result:
                        parquet_files.append(result)
    elif path.is_file():
        # Check if file extension is in the conversion list
        file_type = path.suffix.lstrip('.')
        if file_type in file_types:
            result = _convert_file(path, overwrite, delete_original=True)
            if result:
                parquet_files.append(result)
        elif file_type == "parquet":
            logger.info("{path} already parquet")
            parquet_files.append(path)
        else:
            logger.info(f"Skipping {path}: extension '{path.suffix}' not in conversion list")
    else:
        raise ValueError(f"Path does not exist: {path_to_convert}")
    
    return [filei for filei in parquet_files if filei is not None]


def _convert_file(file_path: Path, overwrite: bool = False, delete_original:bool=False) -> Optional[Path]:
    """
    Convert a single file to Parquet.
    
    Parameters
    ----------
    file_path : Path
        Path to the file to convert
    overwrite : bool, default False
        If True, overwrite existing parquet file
        
    Returns
    -------
    Path or None
        Path to created parquet file, or None if skipped
    """
    output_path = file_path.with_suffix('.parquet')


    if output_path.exists() and not overwrite:
        logger.info(f"Skipping {file_path.name}: {output_path.name} already exists")
        return None

    output_path = output_path.as_posix()
        
    # try:
    ext = file_path.suffix.lstrip('.').lower()
    file_path = file_path.as_posix()
        
    # Read the file based on extension
    if ext == 'csv':
        df = pl.scan_csv(file_path)
        df.sink_parquet(output_path)
    elif ext in ('dta', 'sas7bdat', 'sav'):
        if ext == "sas7bdat":
            engine = "cpp"
        else:
            engine = "readstat"
        df = scan_readstat(file_path, engine)
        try:
            df.sink_parquet(output_path)
        except:
            df = df.collect()
            df.write_parquet(output_path)
    else:
        logger.info(f"Unsupported file type: {ext}")
        return None
    
    

    logger.info(f"Converted: {os.path.basename(file_path)} -> {os.path.basename(output_path)}")

    if delete_original:
        os.remove(file_path)

    return output_path

