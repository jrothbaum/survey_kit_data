from __future__ import annotations

from dataclasses import dataclass
from typing import Callable


@dataclass(frozen=True)
class FREDReleaseSpec:
    name: str
    release_id: int
    series_filter: Callable[[str], bool]
    description: str = ""


STATE_UNEMPLOYMENT_RATE = FREDReleaseSpec(
    name="state_unemployment_rate",
    release_id=112,
    series_filter=lambda series_id: series_id.endswith("UR") and len(series_id) == 4,
    description="State Employment and Unemployment",
)


LAUS_ALL_OTHER_AREAS = FREDReleaseSpec(
    name="laus_all_other_areas",
    release_id=116,
    series_filter=lambda series_id: True,
    description="Unemployment in States and Local Areas (all other areas)",
)


STATE_UI_WEEKLY_CLAIMS = FREDReleaseSpec(
    name="state_ui_weekly_claims",
    release_id=469,
    series_filter=lambda series_id: True,
    description="State Unemployment Insurance Weekly Claims Report",
)


QCEW = FREDReleaseSpec(
    name="qcew",
    release_id=362,
    series_filter=lambda series_id: True,
    description="Quarterly Census of Employment and Wages",
)


BUILDING_PERMITS = FREDReleaseSpec(
    name="building_permits",
    release_id=148,
    series_filter=lambda series_id: True,
    description="Housing Units Authorized By Building Permits",
)


HOUSE_PRICE_INDEX = FREDReleaseSpec(
    name="house_price_index",
    release_id=171,
    series_filter=lambda series_id: True,
    description="House Price Index",
)


ZILLOW_HOME_VALUE_INDEX = FREDReleaseSpec(
    name="zillow_home_value_index",
    release_id=503,
    series_filter=lambda series_id: True,
    description="Zillow Home Value Index",
)


PERSONAL_INCOME_BY_STATE = FREDReleaseSpec(
    name="personal_income_by_state",
    release_id=110,
    series_filter=lambda series_id: True,
    description="Personal Income by State",
)


REAL_PERSONAL_INCOME_BY_STATE = FREDReleaseSpec(
    name="real_personal_income_by_state",
    release_id=403,
    series_filter=lambda series_id: True,
    description="Real Personal Income by State",
)


FRED_RELEASES = {
    spec.name: spec
    for spec in [
        STATE_UNEMPLOYMENT_RATE,
        LAUS_ALL_OTHER_AREAS,
        STATE_UI_WEEKLY_CLAIMS,
        QCEW,
        BUILDING_PERMITS,
        HOUSE_PRICE_INDEX,
        ZILLOW_HOME_VALUE_INDEX,
        PERSONAL_INCOME_BY_STATE,
        REAL_PERSONAL_INCOME_BY_STATE,
    ]
}


RECOMMENDED_MODELING_RELEASES = {
    "state_ui_weekly_claims": STATE_UI_WEEKLY_CLAIMS,
    "local_area_unemployment": LAUS_ALL_OTHER_AREAS,
    "state_unemployment_rate": STATE_UNEMPLOYMENT_RATE,
    "qcew": QCEW,
    "building_permits": BUILDING_PERMITS,
    "house_price_index": HOUSE_PRICE_INDEX,
    "zillow_home_value_index": ZILLOW_HOME_VALUE_INDEX,
    "personal_income_by_state": PERSONAL_INCOME_BY_STATE,
    "real_personal_income_by_state": REAL_PERSONAL_INCOME_BY_STATE,
}
