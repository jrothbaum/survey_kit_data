from __future__ import annotations

import hashlib
import shutil
from pathlib import Path
from typing import List, Optional, Union, Dict
import polars as pl
import pooch
import zipfile
from zipfile import BadZipFile
import struct
from pooch import HTTPDownloader

from polars_readstat import scan_readstat
            
from .cache_manager import FileCacheManager
from . import config, logger



def load_from_url(
    url: str,
    save_name: str = "",
    cache_dir: Path = "", 
    force_reload: bool = False,
    no_data_error:bool=True
) -> Union[pl.LazyFrame, Dict[pl.LazyFrame], List[str]]:
    """
    Download file from URL, convert to parquet, and cache.
    
    Handles both single files and archives (zip, tar.gz, etc.).
    Returns LazyFrame for single files, List[LazyFrame] for archives with multiple files.
    
    Parameters
    ----------
    url : str
        Direct URL to download from
    save_name : str, optional
        Name for cached location. If empty, generated from URL hash.
        For single files: becomes {save_name}.parquet
        For archives: becomes a directory containing converted parquet files
    cache_dir : Path, optional
        Directory to cache parquet files. Defaults to config.path_cache_files.
    force_reload : bool, optional
        Force re-download and conversion even if cached. Default is False.
        
    Returns
    -------
    pl.LazyFrame or List[pl.LazyFrame]
        Single LazyFrame if one data file, List of LazyFrames if multiple data files.
        
    Examples
    --------
    >>> # Single file
    >>> df = load_from_url("https://example.com/data.csv")
    
    >>> # Zip archive with multiple files
    >>> dfs = load_from_url("https://example.com/data.zip")
    >>> combined = pl.concat([df.collect() for df in dfs])
    """
    
    # Set cache directory
    if cache_dir == "":
        cache_dir = Path(config.path_cache_files)
    else:
        cache_dir = Path(cache_dir)
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate save_name if not provided
    if save_name == "":
        save_name = hashlib.md5(url.encode()).hexdigest()[:16]
    
    path_save = cache_dir / save_name
    
    # Create cache manager
    fcm = FileCacheManager(
        path_save=str(path_save),
        url=url
    )
    
    # Check cache
    if fcm.is_cached() and not force_reload:
        # Check if it's a directory (multiple files) or single file
        logger.info("Loading from cached data")
        if path_save.is_dir():
            # Load all parquet files from directory
            parquet_files = sorted(path_save.glob("*.parquet"))
            return {f.stem: pl.scan_parquet(f) for f in parquet_files}
        else:
            # Single parquet file - add .parquet if not present
            parquet_path = path_save if str(path_save).endswith('.parquet') else Path(str(path_save) + '.parquet')
            return pl.scan_parquet(parquet_path)
    
    try:
        temp_dir = cache_dir / "temp" / fcm.hash[:8]
        temp_dir.mkdir(parents=True, exist_ok=True)

        # Download with pooch directly to cache directory
        processor = _get_processor(url)
        try:
            result = pooch.retrieve(
                url=url,
                known_hash=None,
                path=temp_dir,
                processor=processor,
                progressbar=True,
            )
        except:
            logger.info("Failed to download, trying again with useragent")
            result = pooch.retrieve(
                url=url,
                known_hash=None,
                path=temp_dir,
                processor=processor,
                progressbar=True,
                downloader=download_with_user_agent()
            )
            
        
        # Convert result to list of files
        if isinstance(result, list):
            downloaded_files = [Path(f) for f in result]
        else:
            downloaded_files = [Path(result)]
        
        # Separate data files from other files
        data_files = []
        other_files = []
        for f in downloaded_files:
            if _is_data_file(f):
                data_files.append(f)
            else:
                other_files.append(f)
        
        if len(data_files) == 0:
            if no_data_error and len(other_files):
                raise ValueError(f"No data files found in download from {url}")
            else:
                other_files_final = []
                path_save.mkdir(parents=True, exist_ok=True)
                for other_file in other_files:
                    if other_file.parent != path_save:
                        shutil.move(str(other_file), str(path_save / other_file.name))
                        other_files_final.append(str(path_save / other_file.name))
        
                return other_files_final
        
        # Convert to parquet and organize
        if len(data_files) == 1:
            # Single data file
            logger.info(f"  Converting {data_files[0].stem} to parquet")
            parquet_path = path_save if str(path_save).endswith('.parquet') else Path(str(path_save) + '.parquet')
            df = _read_file(data_files[0])

            if data_files[0].suffix.lower() in ['.dta', '.sas7bdat', '.sav']:
                try:
                    df.sink_parquet(parquet_path.as_posix())
                except:
                    df.collect().write_parquet(parquet_path.as_posix())
            else:
                df.sink_parquet(parquet_path.as_posix())

                
            
            # Delete original data file
            data_files[0].unlink()
            
            # Move other files to path_save location if they're not already there
            for other_file in other_files:
                if other_file.parent != parquet_path.parent:
                    shutil.move(str(other_file), str(parquet_path.parent / other_file.name))
            
            fcm.save_metadata()
            return pl.scan_parquet(parquet_path)
        else:
            # Multiple data files - create directory
            path_save.mkdir(exist_ok=True)
            lazy_frames = {}
            
            for data_file in data_files:
                logger.info(f"  Converting {data_file.stem} to parquet")
                df = _read_file(data_file)

                parquet_file = path_save / f"{data_file.stem}.parquet"
                if data_file.suffix.lower() in ['.dta', '.sas7bdat', '.sav']:
                    try:
                        df.sink_parquet(parquet_file.as_posix())
                    except:
                        df.collect().write_parquet(parquet_file.as_posix())
                else:
                    df.sink_parquet(parquet_file.as_posix())

                lazy_frames[parquet_file.stem] = pl.scan_parquet(parquet_file)
                
                # Delete original data file
                data_file.unlink()
            
            # Move other files (READMEs, etc.) to the directory
            for other_file in other_files:
                target = path_save / other_file.name
                if other_file != target:
                    shutil.move(str(other_file), str(target))
            
            fcm.save_metadata()
            return lazy_frames
    finally:
        # Clean up temp directory (deletes all pooch downloads)
        if temp_dir.exists():
            shutil.rmtree(temp_dir)


class LenientUnzip(pooch.Unzip):
    """
    Unzip processor that handles corrupt extra fields in older ZIP files.
    
    Some legacy ZIP files have malformed extra field metadata (e.g., field 0x0ca0
    with invalid sizes) that causes BadZipFile exceptions. This processor
    monkey-patches zipfile to ignore these corrupt fields during extraction.
    """

    def __call__(self, fname, action, pooch_instance):
        # Monkey-patch both _decodeExtra and the ZipFile class
        _original_decode = zipfile.ZipInfo._decodeExtra
        _original_zipfile_init = zipfile.ZipFile.__init__
        
        def _lenient_decode(zip_info_self, *args, **kwargs):
            try:
                _original_decode(zip_info_self, *args, **kwargs)
            except Exception:
                zip_info_self.extra = b''
        
        def _lenient_init(self, *args, **kwargs):
            try:
                _original_zipfile_init(self, *args, **kwargs)
            except BadZipFile as e:
                if "Corrupt extra field" in str(e):
                    # Try to open with strict=False or handle it
                    kwargs['strict_timestamps'] = False
                    _original_zipfile_init(self, *args, **kwargs)
                else:
                    raise
        
        zipfile.ZipInfo._decodeExtra = _lenient_decode
        zipfile.ZipFile.__init__ = _lenient_init
        
        try:
            return super().__call__(fname, action, pooch_instance)
        finally:
            # Restore originals
            zipfile.ZipInfo._decodeExtra = _original_decode
            zipfile.ZipFile.__init__ = _original_zipfile_init



def _get_processor(url: str) -> Optional[any]:
    """Determine the appropriate pooch processor based on URL extension"""
    url_lower = url.lower()
    
    if url_lower.endswith('.zip'):
        return LenientUnzip()
    elif url_lower.endswith(('.tar.gz', '.tgz')):
        return pooch.Untar()
    elif url_lower.endswith('.tar'):
        return pooch.Untar()
    elif url_lower.endswith(('.tar.bz2', '.tbz2')):
        return pooch.Untar()
    elif url_lower.endswith('.gz') and not url_lower.endswith('.tar.gz'):
        return pooch.Decompress()
    else:
        return None
    
def _is_data_file(file_path: Path) -> bool:
    """Check if file is a data file that should be converted"""
    data_extensions = {'.csv', '.dta', '.sas7bdat', '.sav', '.parquet', '.xlsx', '.xls'}
    return file_path.suffix.lower() in data_extensions


def _read_file(file_path: Path) -> pl.LazyFrame:
    """Read a single file into a Polars LazyFrame"""
    suffix = file_path.suffix.lower()
    
    if suffix == '.csv':
        return pl.scan_csv(file_path, infer_schema_length=100_000_000)
    elif suffix == '.parquet':
        return pl.scan_parquet(file_path)
    elif suffix in ['.dta', '.sas7bdat', '.sav']:
        return scan_readstat(str(file_path))
    elif suffix in ['.xlsx', '.xls']:
        try:
            return pl.read_excel(file_path).lazy()
        except Exception:
            raise ValueError(f"Could not read Excel file: {file_path}")
    else:
        raise ValueError(f"Unsupported file format: {suffix}")


def get_default_cache_dir() -> Path:
    """Get default cache directory for datasets."""
    return Path(config.path_cache_files)



def download_with_user_agent():
    """Create a downloader configured for BLS website."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    }
    return HTTPDownloader(headers=headers,
                          timeout=120)