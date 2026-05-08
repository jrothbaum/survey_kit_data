from __future__ import annotations

import polars as pl

# fmt: off
_STATE_FIPS_DATA = [
    ("Alabama",                          "AL", 1),
    ("Alaska",                           "AK", 2),
    ("Arizona",                          "AZ", 4),
    ("Arkansas",                         "AR", 5),
    ("California",                       "CA", 6),
    ("Colorado",                         "CO", 8),
    ("Connecticut",                      "CT", 9),
    ("Delaware",                         "DE", 10),
    ("District of Columbia",             "DC", 11),
    ("Florida",                          "FL", 12),
    ("Georgia",                          "GA", 13),
    ("Hawaii",                           "HI", 15),
    ("Idaho",                            "ID", 16),
    ("Illinois",                         "IL", 17),
    ("Indiana",                          "IN", 18),
    ("Iowa",                             "IA", 19),
    ("Kansas",                           "KS", 20),
    ("Kentucky",                         "KY", 21),
    ("Louisiana",                        "LA", 22),
    ("Maine",                            "ME", 23),
    ("Maryland",                         "MD", 24),
    ("Massachusetts",                    "MA", 25),
    ("Michigan",                         "MI", 26),
    ("Minnesota",                        "MN", 27),
    ("Mississippi",                      "MS", 28),
    ("Missouri",                         "MO", 29),
    ("Montana",                          "MT", 30),
    ("Nebraska",                         "NE", 31),
    ("Nevada",                           "NV", 32),
    ("New Hampshire",                    "NH", 33),
    ("New Jersey",                       "NJ", 34),
    ("New Mexico",                       "NM", 35),
    ("New York",                         "NY", 36),
    ("North Carolina",                   "NC", 37),
    ("North Dakota",                     "ND", 38),
    ("Ohio",                             "OH", 39),
    ("Oklahoma",                         "OK", 40),
    ("Oregon",                           "OR", 41),
    ("Pennsylvania",                     "PA", 42),
    ("Rhode Island",                     "RI", 44),
    ("South Carolina",                   "SC", 45),
    ("South Dakota",                     "SD", 46),
    ("Tennessee",                        "TN", 47),
    ("Texas",                            "TX", 48),
    ("Utah",                             "UT", 49),
    ("Vermont",                          "VT", 50),
    ("Virginia",                         "VA", 51),
    ("Washington",                       "WA", 53),
    ("West Virginia",                    "WV", 54),
    ("Wisconsin",                        "WI", 55),
    ("Wyoming",                          "WY", 56),
    # Territories
    ("American Samoa",                   "AS", 60),
    ("Guam",                             "GU", 66),
    ("Northern Mariana Islands",         "MP", 69),
    ("Puerto Rico",                      "PR", 72),
    ("Virgin Islands",                   "VI", 78),
]
# fmt: on

_STATE_FIPS_DF: pl.DataFrame | None = None


def state_fips() -> pl.LazyFrame:
    """
    State and territory FIPS codes.

    Returns a LazyFrame with columns:
      state_name (String), state_abbr (String), fips (Int32)

    The SNAP substate_code field embeds state and county FIPS:
      digits 1-2 = state FIPS, digits 3-5 = county FIPS, digits 6-7 = office.
    """
    global _STATE_FIPS_DF
    if _STATE_FIPS_DF is None:
        names, abbrs, codes = zip(*_STATE_FIPS_DATA)
        _STATE_FIPS_DF = pl.DataFrame({
            "state_name": list(names),
            "state_abbr": list(abbrs),
            "fips": pl.Series(list(codes), dtype=pl.Int32),
        })
    return _STATE_FIPS_DF.lazy()
