from survey_kit_data.dol.ui import (
    insured_unemployed_characteristics,
    weekly_ui_claims,
)

# ETA 203 — monthly characteristics of the insured unemployed by state
# (sex, age, race/ethnicity, industry, occupation) ~1994–present
df_203 = insured_unemployed_characteristics().collect()
print("ETA 203 — characteristics of insured unemployed")
print(df_203)

# ETA 539 — weekly initial and continued claims by state, 1986–present
df_539 = weekly_ui_claims().collect()
print("\nETA 539 — weekly UI claims")
print(df_539)
