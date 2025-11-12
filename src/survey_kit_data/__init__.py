import os
import logging
import survey_kit_data.custom_logging as custom_logging
from pathlib import Path
from .custom_logging import set_logging

from dotenv import load_dotenv

load_dotenv()

logger = set_logging(name=__name__, level=logging.INFO)
from survey_kit_data.config import Config
config = Config()
config.code_root = os.path.dirname(__file__)

if config.data_root == "":
    config.data_root = Path(config.code_root).as_posix().replace("/src/survey_kit_data","") + "/.scratch"

