from survey_kit_data import config, logger
from survey_kit_data.load import load_from_url


data = load_from_url(
    save_name="cps_2025",
    # url="https://www.federalreserve.gov/econres/files/scf2022s.zip",
    url="https://www2.census.gov/programs-surveys/cps/datasets/2025/march/asecpub25sas.zip"
)

logger.info(data)