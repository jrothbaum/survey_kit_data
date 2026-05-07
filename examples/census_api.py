from survey_kit_data.census.api import acs5_demographics, acs5_income

# Demographics by state
demographics = acs5_demographics(year=2022, geo_for="state:*").collect()
print("ACS5 Demographics — all states")
print(demographics)

# Income by state
income = acs5_income(year=2022, geo_for="state:*").collect()
print("\nACS5 Income — all states")
print(income)

# County-level example (all counties in a single state, e.g. California = 06)
ca_income = acs5_income(
    year=2022,
    geo_for="county:*",
    geo_in="state:06",
).collect()
print("\nACS5 Income — California counties")
print(ca_income)
