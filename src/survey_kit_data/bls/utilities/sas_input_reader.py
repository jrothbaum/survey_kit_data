"""
Read fixed-width files using SAS input scripts.

This module provides functionality to parse SAS INPUT statements and read
fixed-width format files into Polars DataFrames or LazyFrames.
"""

import re
from pathlib import Path
from typing import Optional, Union, Literal
from dataclasses import dataclass
import warnings

import polars as pl


@dataclass
class ColumnSpec:
    """Specification for a fixed-width column."""
    varname: Optional[str]
    width: int
    is_char: bool
    divisor: float = 1.0


def _uncomment_sas(
    sas_lines: list[str],
    start_comment: str,
    end_comment: str
) -> list[str]:
    """
    Remove comments from SAS input lines.
    
    Parameters
    ----------
    sas_lines : list[str]
        Lines from SAS input script
    start_comment : str
        Opening comment delimiter (e.g., "/*" or "*")
    end_comment : str
        Closing comment delimiter (e.g., "*/" or ";")
    
    Returns
    -------
    list[str]
        Lines with comments removed
    """
    result = sas_lines.copy()
    i = 0
    
    while i < len(result):
        line = result[i]
        
        # Find opening comment
        start_pos = line.find(start_comment)
        
        if start_pos != -1:
            # Find closing comment on same line
            end_pos = line.find(end_comment, start_pos + len(start_comment))
            
            if end_pos != -1:
                # Comment starts and ends on same line
                comment_text = line[start_pos:end_pos + len(end_comment)]
                result[i] = line.replace(comment_text, "", 1)
                # Re-process this line in case there are more comments
                continue
            else:
                # Comment extends to next line(s)
                result[i] = line[:start_pos]
                
                # Find where comment ends
                j = i + 1
                while j < len(result):
                    end_pos = result[j].find(end_comment)
                    if end_pos == -1:
                        result[j] = ""
                    else:
                        result[j] = result[j][end_pos + len(end_comment):]
                        break
                    j += 1
        
        i += 1
    
    return result


def parse_sas_script(
    sas_script_path: Union[str, Path],
    beginline: int = 1,
    lrecl: Optional[int] = None
) -> list[ColumnSpec]:
    """
    Parse a SAS input script to extract column specifications.
    
    Parameters
    ----------
    sas_script_path : str or Path
        Path to SAS input script file
    beginline : int, default 1
        Line number to start parsing from (1-indexed)
    lrecl : int, optional
        Logical record length. If specified and greater than the sum of column
        widths, padding will be added to the end.
    
    Returns
    -------
    list[ColumnSpec]
        List of column specifications
    """
    # Read the SAS input script
    with open(sas_script_path, 'r') as f:
        sas_lines = f.readlines()
    
    # Remove tabs and convert to uppercase
    sas_lines = [line.replace("\t", " ") for line in sas_lines]
    
    # Start from specified line
    sas_lines = sas_lines[beginline - 1:]
    
    sas_lines = [line.upper() for line in sas_lines]
    
    # Remove comments
    sas_lines = _uncomment_sas(sas_lines, "/*", "*/")
    sas_lines = _uncomment_sas(sas_lines, "*", ";")
    
    # Find INPUT statement
    input_line_idx = None
    for i, line in enumerate(sas_lines):
        if "INPUT" in line:
            input_line_idx = i
            break
    
    if input_line_idx is None:
        raise ValueError("No INPUT statement found in SAS script")
    
    # Find semicolon ending the INPUT statement
    end_line_idx = None
    for i in range(input_line_idx, len(sas_lines)):
        if ";" in sas_lines[i]:
            end_line_idx = i
            break
    
    if end_line_idx is None:
        raise ValueError("No semicolon found to end INPUT statement")
    
    # Extract the INPUT block
    fwf_lines = sas_lines[input_line_idx:end_line_idx + 1]
    
    # Remove "INPUT" from first line
    fwf_lines[0] = re.sub(r'INPUT', '', fwf_lines[0], count=1)
    
    # Remove semicolon from last line
    fwf_lines[-1] = fwf_lines[-1].replace(";", "", 1)
    
    # Add spaces around $ and - for easier parsing
    fwf_lines = [line.replace("$", " $ ").replace("-", " - ") for line in fwf_lines]
    
    # Remove blank lines
    fwf_lines = [line for line in fwf_lines if line.strip()]
    
    # Split into tokens
    tokens = []
    for line in fwf_lines:
        line_tokens = [t.replace("-", " ").strip() for t in line.split()]
        tokens.extend([t for t in line_tokens if t])
    
    # Determine format type
    has_at_symbols = any("@" in token for token in tokens)
    
    # Check if format is "VARNAME WIDTH" by examining first few tokens
    if len(tokens) >= 4:
        elements_2_4 = [t for t in tokens[1:4] if t != "$"]
        widths_not_places = (
            len(elements_2_4) >= 2 and
            not elements_2_4[1].replace(".", "").replace("-", "").isdigit()
        )
    else:
        widths_not_places = False
    
    columns = []
    i = 0
    
    if has_at_symbols:
        # Format: @START VARNAME [$ ] FORMAT
        columns = _parse_at_format(tokens)
    elif widths_not_places:
        # Format: VARNAME [$ ] WIDTH [.DECIMALS]
        columns = _parse_width_format(tokens)
    else:
        # Format: VARNAME [$ ] START [-] END [.DECIMALS]
        columns = _parse_position_format(tokens)
    
    # Handle lrecl parameter
    if lrecl is not None:
        total_width = sum(abs(col.width) for col in columns)
        
        if lrecl < total_width:
            raise ValueError(
                f"Specified lrecl ({lrecl}) is shorter than total column width ({total_width})"
            )
        
        if lrecl > total_width:
            # Add padding to the end
            padding_width = lrecl - total_width
            columns.append(ColumnSpec(
                varname=None,
                width=-padding_width,  # Negative width = skip
                is_char=False
            ))
    
    return columns


def _parse_at_format(tokens: list[str]) -> list[ColumnSpec]:
    """Parse SAS input format: @START VARNAME [$ ] FORMAT."""
    columns = []
    i = 0
    current_pos = 1  # Track current position (1-indexed)
    
    while i < len(tokens):
        # Look for @ symbol
        if not tokens[i].startswith("@"):
            i += 1
            continue
        
        start_pos = int(tokens[i].replace("@", ""))
        
        # Add padding if needed between current position and start
        if start_pos > current_pos:
            padding = start_pos - current_pos
            columns.append(ColumnSpec(None, -padding, False))
            current_pos = start_pos
        
        varname = tokens[i + 1]
        
        # Check for $ (character indicator)
        offset = 2
        is_char = False
        if i + offset < len(tokens) and tokens[i + offset] == "$":
            is_char = True
            offset += 1
        
        # Parse format (e.g., "10", "F10.2", "CHAR10")
        format_str = tokens[i + offset]
        format_str = format_str.replace("F", "").replace("CHAR", "")
        
        if "." in format_str:
            width_str, decimals_str = format_str.split(".", 1)
            width = int(width_str)
            divisor = 10 ** (-int(decimals_str))
        else:
            width = int(format_str)
            divisor = 1.0
        
        columns.append(ColumnSpec(varname, width, is_char, divisor))
        current_pos += width
        
        i += offset + 1
    
    return columns


def _parse_width_format(tokens: list[str]) -> list[ColumnSpec]:
    """Parse SAS input format: VARNAME [$ ] WIDTH [.DECIMALS]."""
    columns = []
    i = 0
    
    while i < len(tokens):
        varname = tokens[i]
        
        # Check for $
        offset = 1
        is_char = False
        if i + offset < len(tokens) and tokens[i + offset] == "$":
            is_char = True
            offset += 1
        
        # Get width
        width = int(tokens[i + offset])
        offset += 1
        
        # Check for decimals
        divisor = 1.0
        if i + offset < len(tokens) and "." in tokens[i + offset]:
            decimals = int(tokens[i + offset].split(".", 1)[1])
            divisor = 10 ** (-decimals)
            offset += 1
        
        columns.append(ColumnSpec(varname, width, is_char, divisor))
        i += offset
    
    return columns


def _parse_position_format(tokens: list[str]) -> list[ColumnSpec]:
    """Parse SAS input format: VARNAME [$ ] START [-] END [.DECIMALS]."""
    columns = []
    i = 0
    
    while i < len(tokens):
        varname = tokens[i]
        
        # Check for $
        offset = 1
        is_char = False
        if i + offset < len(tokens) and tokens[i + offset] == "$":
            is_char = True
            offset += 1
        
        # Get start position
        start = int(tokens[i + offset])
        offset += 1
        
        # Get end position (might be same as start)
        if (i + offset < len(tokens) and 
            tokens[i + offset].replace(".", "").isdigit()):
            end = int(tokens[i + offset])
            offset += 1
        else:
            end = start
        
        # Check for decimals
        divisor = 1.0
        if i + offset < len(tokens) and "." in tokens[i + offset]:
            decimals = int(tokens[i + offset].split(".", 1)[1])
            divisor = 10 ** (-decimals)
            offset += 1
        
        # Calculate width
        width = end - start + 1
        
        # Add padding if there's a gap
        if columns:
            prev_start = sum(abs(c.width) for c in columns) + 1
            if start > prev_start:
                gap = start - prev_start
                columns.append(ColumnSpec(None, -gap, False))
        
        columns.append(ColumnSpec(varname, width, is_char, divisor))
        i += offset
    
    return columns


def read_sas_fwf(
    filepath: Union[str, Path],
    sas_script_path: Union[str, Path],
    *,
    beginline: int = 1,
    lrecl: Optional[int] = None,
    n_rows: Optional[int] = None,
    skip_rows: int = 0,
    skip_decimal_division: Optional[bool] = None,
    infer_schema_length: int = 100,
    lazy: bool = False,
    encoding: str = "utf-8"
) -> Union[pl.DataFrame, pl.LazyFrame]:
    """
    Read a fixed-width file using a SAS input script.
    
    Parameters
    ----------
    filepath : str or Path
        Path to the fixed-width data file
    sas_script_path : str or Path
        Path to the SAS input script
    beginline : int, default 1
        Line number in SAS script to start parsing from (1-indexed)
    lrecl : int, optional
        Logical record length
    n_rows : int, optional
        Maximum number of rows to read
    skip_rows : int, default 0
        Number of rows to skip at the beginning of the data file
    skip_decimal_division : bool, optional
        If True, don't divide numeric columns by their decimal divisors.
        If None (default), automatically detect whether division is needed.
    infer_schema_length : int, default 100
        Number of rows to use for schema inference
    lazy : bool, default False
        If True, return a LazyFrame instead of a DataFrame
    encoding : str, default "utf-8"
        File encoding
    
    Returns
    -------
    pl.DataFrame or pl.LazyFrame
        The parsed data
    """
    # Parse the SAS script
    columns = parse_sas_script(sas_script_path, beginline, lrecl)
    
    # Build column specifications for Polars
    # Filter out padding columns (negative width or no varname)
    data_columns = [col for col in columns if col.varname is not None and col.width > 0]
    
    # Create widths list (including padding)
    widths = [col.width for col in columns]
    
    # Create column names
    col_names = [col.varname for col in data_columns]
    
    # Read the fixed-width file
    df = pl.read_csv(
        filepath,
        has_header=False,
        skip_rows=skip_rows,
        n_rows=n_rows,
        encoding=encoding,
        infer_schema_length=infer_schema_length,
        truncate_ragged_lines=True,
        # Use a custom schema to read as strings first
        schema={f"_col_{i}": pl.Utf8 for i in range(len(data_columns))}
    )
    
    # Alternative: use string slicing for exact FWF parsing
    # This is more reliable for true fixed-width formats
    with open(filepath, 'r', encoding=encoding) as f:
        # Skip rows
        for _ in range(skip_rows):
            next(f, None)
        
        # Read data
        lines = []
        for i, line in enumerate(f):
            if n_rows is not None and i >= n_rows:
                break
            lines.append(line.rstrip('\n\r'))
    
    # Parse fixed-width format manually
    records = []
    for line in lines:
        pos = 0
        record = []
        for col in columns:
            width = abs(col.width)
            chunk = line[pos:pos + width] if pos < len(line) else ""
            
            if col.varname is not None and col.width > 0:
                record.append(chunk.strip())
            
            pos += width
        
        records.append(record)
    
    # Create DataFrame
    df = pl.DataFrame(
        {name: [rec[i] for rec in records] for i, name in enumerate(col_names)},
        schema={name: pl.Utf8 for name in col_names}
    )
    
    # Convert types and apply divisors
    for i, col in enumerate(data_columns):
        col_name = col.varname
        
        if not col.is_char:
            # Try to convert to numeric
            df = df.with_columns(
                pl.col(col_name)
                .str.strip_chars()
                .cast(pl.Float64, strict=False)
                .alias(col_name)
            )
            
            # Check if conversion resulted in all nulls (might need to stay as string)
            if df[col_name].is_null().all():
                warnings.warn(
                    f"Column '{col_name}' could not be converted to numeric. "
                    f"Consider adding a '$' in the SAS script if this is a character column."
                )
            
            # Apply decimal divisor if needed
            if skip_decimal_division is None:
                # Auto-detect: if column has decimals already, don't divide
                # Check if any values contain a decimal point
                has_decimals = (
                    df[col_name]
                    .cast(pl.Utf8)
                    .str.contains(r"\.")
                    .fill_null(False)
                    .any()
                )
                
                should_divide = not has_decimals and col.divisor != 1.0
            else:
                should_divide = not skip_decimal_division and col.divisor != 1.0
            
            if should_divide:
                df = df.with_columns(
                    (pl.col(col_name) * col.divisor).alias(col_name)
                )
    
    if lazy:
        return df.lazy()
    
    return df

