import polars as pl
from survey_kit_data.fed.fred import get_state_panel, StatePanels
from survey_kit import logger
from survey_kit_data.config import Config

from survey_kit.utilities.dataframe import summary




df = (
    StatePanels.unemployment_rate(observation_start="2018-01-01")
    .lazy()
    .collect()
)
print(df.lazy().collect())



