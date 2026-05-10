import polars as pl
from survey_kit_data.hhs.tanf import tanf_caseload

# HHS/ACF TANF caseload Excel workbooks, FY1996-FY2025.
# Tables are consolidated from the source workbook sheets.
tanf = tanf_caseload(
    years=[2021],
    # Optional raw-file mirror. When provided, the loader uses it before
    # retrying ACF. This can also be a GitHub slug such as
    # "your-org/survey-kit-download" once that repo exists.
    download_mirror="../survey_kit_download",
    # force_reload=True,
)
print("HHS/ACF TANF monthly caseload")

monthly = tanf.collect()

print(monthly.head())
print(monthly.describe())
print(monthly.schema)
print(monthly.select(pl.col("state").value_counts()))
