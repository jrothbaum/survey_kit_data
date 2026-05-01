import polars as pl
from survey_kit_data.fed.fred import get_state_panel, StatePanels
from survey_kit import logger
from survey_kit_data.config import Config

print("H")



df = StatePanels.unemployment_rate(observation_start="2018-01-01")
print(df)



